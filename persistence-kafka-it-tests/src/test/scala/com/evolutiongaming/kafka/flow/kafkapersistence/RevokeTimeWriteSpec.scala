package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.unsafe.IORuntime
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.kafkapersistence.RevokeTimeWriteSpec.*
import com.evolutiongaming.kafka.flow.persistence.{Persistence, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, Timestamps, TimersOf}
import com.evolutiongaming.kafka.flow.{
  ConsumerFlowOf,
  FoldOption,
  ForAllKafkaSuite,
  KafkaFlow,
  KafkaKey,
  PartitionAssignment,
  PartitionFlowConfig,
  TickOption,
  TopicFlowOf
}
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerOf,
  ConsumerRecord,
  IsolationLevel,
  RebalanceCallback,
  RebalanceListener1
}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, OffsetAndMetadata, Partition, Topic, TopicPartition}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{CommitFailedException, CooperativeStickyAssignor}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Settles empirically what the revoke-time transaction can and cannot do once the revoke callback publishes the
  * generation the client has already moved to (so that, under the classic cooperative assignor, `commitOnRevoke` and
  * `flushOnRevoke` are no longer fenced on every revocation).
  *
  * The question is whether that can let a stale write land, in the shape of
  * [[https://github.com/evolution-gaming/kafka-flow/issues/732 issue #732]]: the revoking owner's write arriving after
  * another member has taken the partition over and written newer state. The design argument says no - cooperative
  * withholds a revoked partition from its next owner for one further round, and the next round cannot complete before
  * the revoking member's callback has returned, so the only overlap left is a member evicted mid-callback, whose write
  * is then rejected for its stale generation. Both halves are observed here against a real broker rather than argued:
  *
  *   - `a cooperative handover ...` - the ordinary revocation. The revoke-time flush and offset commit land, and the
  *     next owner starts only after the callback has returned, so nothing of the new owner's exists to be overwritten.
  *   - `an owner evicted inside its revoke callback ...` - the hole in the argument, made real. The revoke-time flush
  *     stalls inside the callback (after the generation was published) until the broker drops the member for a breached
  *     poll interval; the new owner then takes over, replays and persists, and only then is the stalled write let go.
  *     Run twice: with the library's shared per-partition `transactional.id` (producer epoch fence active) and with
  *     per-instance ids (the coordinator's generation and member validation alone).
  *
  * Two input partitions, because the cooperative sticky assignor does not move a single partition between two members
  * (an assignment of one and none is already balanced), so a two-member group has to be given something to move.
  *
  * A live eviction is paced by the broker, so the scenarios take tens of seconds; closing an evicted consumer waits out
  * Kafka's 30s close timeout on cleanup it can no longer complete.
  */
class RevokeTimeWriteSpec extends ForAllKafkaSuite {

  // set above every step's own wait budget so those fail first: they name the step that hung, this only says
  // "timed out"
  override def munitTimeout: Duration = 12.minutes

  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift
  // no retry: a fenced flow must surface to the test instead of being restarted under it
  implicit val retry: Retry[IO] = Retry.empty[IO]

  private val appId = "app-id"

  private val partitionCount = 2
  private val partitions     = (0 until partitionCount).toList

  private def keyOf(partition: Int): String = s"key-$partition"

  private val eventsBeforeHandover = (1 to 5).toList.map(i => s"e$i")
  private val eventsAfterHandover  = (6 to 8).toList.map(i => s"e$i")

  /** What the revoking owner holds when it is asked to give the partition up. */
  private val handoverState = eventsBeforeHandover.mkString(",")

  /** What the next owner holds after replaying from offset zero and folding on - the state that must survive. */
  private val newOwnerState = (eventsBeforeHandover ++ eventsAfterHandover).mkString(",")

  private val clientIdA = "instance-a"
  private val clientIdB = "instance-b"

  private def commonConfig = CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers))

  private def producerConfig = ProducerConfig(common = commonConfig)

  private def producerOf = ProducerOf.apply1[IO]()

  private def consumerOf = ConsumerOf.apply1[IO]()

  /** The persistence module's own consumer: group-less, for the recovery read only. */
  private def persistenceConsumerConfig =
    ConsumerConfig(common = commonConfig, autoCommit = false, autoOffsetReset = AutoOffsetReset.Earliest)

  /** The flow-driving consumer, under the assignor being evaluated.
    *
    * Only the stalled instance needs to be evictable in seconds rather than the default five minutes, so only it gets
    * the tight timeouts: the session timeout is the smallest the broker accepts (`group.min.session.timeout.ms` is 6s)
    * and the poll interval sits just above it. The other instance keeps the defaults - it has to stay in the group
    * through the rest of the scenario.
    */
  private def drivingConsumerConfig(group: String, clientId: String, evictable: Boolean) = ConsumerConfig(
    common                      = commonConfig.copy(clientId = clientId.some),
    groupId                     = group.some,
    autoCommit                  = false,
    autoOffsetReset             = AutoOffsetReset.Earliest,
    partitionAssignmentStrategy = classOf[CooperativeStickyAssignor].getName,
    sessionTimeout              = if (evictable) 6.seconds else 30.seconds,
    heartbeatInterval           = if (evictable) 2.seconds else 3.seconds,
    maxPollInterval             = if (evictable) 7.seconds else 5.minutes,
  )

  /** Recovery read of the snapshot topic, as performed on partition assignment: `read_committed`, last write per key -
    * exactly what a new owner would adopt.
    */
  private def readSnapshots(stateTopic: String, partition: Int): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = consumerOf,
      consumerConfig = persistenceConsumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
      snapshotTopic  = stateTopic,
      partition      = Partition.unsafe(partition),
      stall = KafkaPartitionPersistence
        .Stall(KafkaPersistenceModule.TransactionalConfig.DefaultRecoveryStallTimeout, IO.monotonic)
        .some,
    )

  private def utf8(value: String): Option[ByteVector] = ByteVector.encodeUtf8(value).toOption

  private def transactionalModuleOf(
    stateTopic: String,
    transactionalIdPrefix: String = appId,
  ): KafkaPersistenceModuleOf[IO, String] =
    KafkaPersistenceModuleOf.cachingTransactional[IO, String](
      consumerOf = consumerOf,
      producerOf = producerOf,
      config = KafkaPersistenceModule.TransactionalConfig(
        consumerConfig        = persistenceConsumerConfig,
        producerConfig        = producerConfig,
        transactionalIdPrefix = transactionalIdPrefix,
        snapshotTopic         = stateTopic,
      ),
    )

  /** Puts the stall inside the flush itself, which on revocation runs inside the rebalance callback - after the
    * generation was published and before the transaction is opened - and reports what became of it once released.
    * One-shot: with no periodic persistence configured, the flush it catches is the revoke-time one.
    */
  private def stalling(
    underlying: KafkaPersistenceModuleOf[IO, String],
    stall: FlushStall,
  ): KafkaPersistenceModuleOf[IO, String] =
    new KafkaPersistenceModuleOf[IO, String] {
      def make(assignment: PartitionAssignment[IO]): Resource[IO, KafkaPersistenceModule[IO, String]] =
        underlying.make(assignment).map { module =>
          new KafkaPersistenceModule[IO, String] {
            def keysOf         = module.keysOf
            def scheduleCommit = module.scheduleCommit

            def persistenceOf: SnapshotPersistenceOf[IO, KafkaKey, String, ConsumerRecord[String, ByteVector]] =
              new SnapshotPersistenceOf[IO, KafkaKey, String, ConsumerRecord[String, ByteVector]] {
                def apply(
                  key: KafkaKey,
                  timestamps: Timestamps[IO]
                ): IO[Persistence[IO, String, ConsumerRecord[String, ByteVector]]] =
                  module.persistenceOf(key, timestamps).map { persistence =>
                    new Persistence[IO, String, ConsumerRecord[String, ByteVector]] {
                      def read                                                   = persistence.read
                      def appendEvent(event: ConsumerRecord[String, ByteVector]) = persistence.appendEvent(event)
                      def replaceState(state: String)                            = persistence.replaceState(state)
                      def delete                                                 = persistence.delete

                      def flush: IO[Unit] = stall.reached.complete(key).flatMap { first =>
                        if (first)
                          (stall.release.get *> persistence
                            .flush
                            .attempt
                            .flatTap(stall.outcome.complete(_).void)).rethrow
                        else persistence.flush
                      }
                    }
                  }
              }
          }
        }
    }

  /** State is the comma-joined list of folded events, as a stand-in for a real aggregate. Every folded record and the
    * state it produced are recorded per key, so a replay after the handover is countable.
    */
  private def fold(observed: Observed): FoldOption[IO, String, ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      val key   = record.key.map(_.value).getOrElse(sys.error("record key missing"))
      val event = record.value.flatMap(_.value.decodeUtf8.toOption).getOrElse(sys.error("event payload missing"))
      val next  = state.fold(event)(s => s"$s,$event")
      observed.events.update(events => events.updated(key, events.getOrElse(key, List.empty) :+ event)) *>
        observed.states.update(_.updated(key, next)).as(next.some)
    }

  /** Timestamps the rebalance callbacks of one instance, so the handover can be ordered against the next owner's start.
    * Wrapping the flow's listener from outside `Consumer.of` puts these stamps inside the decorated callback: the
    * generation is published first, then the entry stamp, then the flow's own revoke path.
    */
  private def observing(consumer: Consumer[IO], rebalances: Rebalances): Consumer[IO] =
    new Consumer[IO] {
      def poll(timeout: FiniteDuration)                                   = consumer.poll(timeout)
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) = consumer.commit(offsets)
      def groupMetadata                                                   = consumer.groupMetadata

      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]): IO[Unit] =
        consumer.subscribe(topics, observe(listener))

      private def observe(listener: RebalanceListener1[IO]): RebalanceListener1[IO] =
        new RebalanceListener1[IO] {
          def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]) =
            timed(Assigned, partitions, listener.onPartitionsAssigned(partitions))
          def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]) =
            timed(Revoked, partitions, listener.onPartitionsRevoked(partitions))
          def onPartitionsLost(partitions: NonEmptySet[TopicPartition]) =
            timed(Lost, partitions, listener.onPartitionsLost(partitions))
        }

      private def timed(
        phase: String,
        partitions: NonEmptySet[TopicPartition],
        callback: RebalanceCallback[IO, Unit],
      ): RebalanceCallback[IO, Unit] = {
        val touched = partitions.toSortedSet.toList.map(_.partition.value).toSet
        RebalanceCallback.lift(rebalances.mark(phase, touched, returned = false)) *>
          callback *>
          RebalanceCallback.lift(rebalances.mark(phase, touched, returned = true))
      }
    }

  /** One running instance of the app: a real consumer in `group`, the production flow wiring, offsets committed on
    * revoke. Returns the flow's completion, so a failed flow is observable.
    */
  private def instance(
    group: String,
    clientId: String,
    inputTopic: String,
    moduleOf: KafkaPersistenceModuleOf[IO, String],
    timerFlowOf: TimerFlowOf[IO],
    evictable: Boolean,
    rebalances: Rebalances,
    observed: Observed,
  ): Resource[IO, IO[Unit]] =
    for {
      timersOf <- TimersOf.memory[IO, KafkaKey].toResource
      partitionFlowOf = kafkaEagerRecovery[IO, String](
        kafkaPersistenceModuleOf = moduleOf,
        applicationId            = appId,
        groupId                  = group,
        timersOf                 = timersOf,
        timerFlowOf              = timerFlowOf,
        fold                     = fold(observed),
        tick                     = TickOption.id[IO, String],
        partitionFlowConfig = PartitionFlowConfig(
          triggerTimersInterval = 0.seconds,
          commitOffsetsInterval = 0.seconds,
          commitOnRevoke        = true,
        ),
        registry = EntityRegistry.empty[IO, KafkaKey, String],
      )
      consumer = consumerOf
        .apply[String, ByteVector](drivingConsumerConfig(group, clientId, evictable))
        .evalMap(Consumer.of[IO](_))
        .map(observing(_, rebalances))
      completion <- KafkaFlow.resource(
        consumer = consumer,
        flowOf   = ConsumerFlowOf[IO](topic = inputTopic, flowOf = TopicFlowOf(partitionFlowOf)),
      )
    } yield completion

  /** Nothing is persisted periodically, so the revoke-time flush is the only writer and its effect is unambiguous. */
  private def flushOnRevokeOnly: TimerFlowOf[IO] =
    TimerFlowOf.persistPeriodically[IO](fireEvery = 1.hour, persistEvery = 1.hour, flushOnRevoke = true)

  /** Persists on every poll cycle - what the new owner needs so its state is in the store before the stalled write is
    * released.
    */
  private def persistEveryPoll: TimerFlowOf[IO] =
    TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds, flushOnRevoke = true)

  private def produce(inputTopic: String, partition: Int, key: String, events: List[String]): IO[Unit] =
    producerOf(producerConfig).use { producer =>
      events.traverse_ { event =>
        producer
          .send(ProducerRecord[String, String](inputTopic, event.some, key.some, Partition.unsafe(partition).some))
          .flatten
          .void
      }
    }

  private def produceToAll(inputTopic: String, events: List[String]): IO[Unit] =
    partitions.traverse_(partition => produce(inputTopic, partition, keyOf(partition), events))

  private def adminClient: Resource[IO, AdminClient] = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)
    Resource.make(IO.delay(AdminClient.create(props)))(client => IO(client.close()))
  }

  /** The group's members as the coordinator sees them, by client id - the broker's own answer to "is this instance
    * still a member?"
    */
  private def groupMembers(client: AdminClient, group: String): IO[Set[String]] =
    IO.blocking(client.describeConsumerGroups(List(group).asJava).all().get(10, TimeUnit.SECONDS))
      .map(_.asScala.get(group).toList.flatMap(_.members().asScala.map(_.clientId())).toSet)

  /** The group's committed input offsets, by partition. */
  private def committedOffsets(client: AdminClient, group: String): IO[Map[Int, Long]] =
    IO.blocking(client.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS))
      .map(_.asScala.toMap.map { case (tp, offset) => tp.partition -> offset.offset })

  private def eventually[A](what: String, timeout: FiniteDuration)(fa: IO[A])(p: A => Boolean): IO[A] = {
    def loop(deadline: FiniteDuration): IO[A] =
      fa.flatMap { a =>
        if (p(a)) a.pure[IO]
        else
          IO.monotonic.flatMap { now =>
            if (now >= deadline)
              IO.raiseError(new AssertionError(s"timed out after $timeout waiting for $what; last observed: $a"))
            else IO.sleep(250.millis) *> loop(deadline)
          }
      }

    IO.monotonic.flatMap(now => loop(now + timeout))
  }

  private def causeChain(e: Throwable): List[Throwable] =
    List.unfold(Option(e))(current => current.map(c => (c, Option(c.getCause).filter(_ ne c))))

  test("a cooperative handover lands the revoke-time write before the next owner starts") {
    val stateTopic = "revoke-write-handover-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = s"group-$stateTopic"
    val moduleOf   = transactionalModuleOf(stateTopic)

    val test = for {
      _           <- createTopic(inputTopic, partitionCount)
      _           <- createTopic(stateTopic, partitionCount)
      _           <- produceToAll(inputTopic, eventsBeforeHandover)
      observedA   <- Observed.of
      observedB   <- Observed.of
      rebalancesA <- Rebalances.of
      rebalancesB <- Rebalances.of
      _ <- adminClient.use { admin =>
        instance(group, clientIdA, inputTopic, moduleOf, flushOnRevokeOnly, false, rebalancesA, observedA).use { _ =>
          for {
            // A owns both partitions and has folded everything produced so far
            _ <- eventually("A to fold the pre-handover events", 90.seconds)(observedA.events.get)(
              _ == partitions.map(partition => keyOf(partition) -> eventsBeforeHandover).toMap
            )
            // nothing is persisted periodically and no offset is committed while a key holds its first offset,
            // so the revoke-time transaction is the only writer in this scenario
            storeBefore     <- partitions.traverse(readSnapshots(stateTopic, _))
            _                = assertEquals(clue(storeBefore), partitions.map(_ => BytesByKey.empty))
            committedBefore <- committedOffsets(admin, group)
            _                = assertEquals(clue(committedBefore), Map.empty[Int, Long])
            _ <- instance(group, clientIdB, inputTopic, moduleOf, flushOnRevokeOnly, false, rebalancesB, observedB)
              .use { _ =>
                for {
                  // B took exactly one partition over; the cooperative assignor moves it in a second round,
                  // after A's revoke callback of the first has returned
                  assignedB <- eventually("B to be assigned a partition", 150.seconds)(
                    rebalancesB.returnedFrom(Assigned)
                  )(_.nonEmpty)
                  handedOver = assignedB.head.partitions
                  _          = assertEquals(clue(handedOver.size), 1)
                  partition  = handedOver.head
                  key        = keyOf(partition)
                  _         <- log.info(s"partition $partition handed over to B")
                  // A's revoke-time flush landed the state A held at revocation ...
                  stored <- readSnapshots(stateTopic, partition)
                  _       = assertEquals(clue(stored.get(key)), utf8(handoverState))
                  // ... and its revoke-time commit advanced the input offset to A's position, so B has nothing
                  // to replay
                  committedAfter <- committedOffsets(admin, group)
                  _ = assertEquals(
                    clue(committedAfter.get(partition)),
                    eventsBeforeHandover.size.toLong.some
                  )
                  // B adopts A's revoke-time state and folds on from it
                  _ <- produce(inputTopic, partition, key, eventsAfterHandover)
                  _ <- eventually("B to fold the post-handover events", 90.seconds)(observedB.events.get)(
                    _.get(key).contains(eventsAfterHandover)
                  )
                  statesB <- observedB.states.get
                  _        = assertEquals(clue(statesB.get(key)), newOwnerState.some)
                  // B replayed nothing: every record it folded for the handed-over key is a post-handover one
                  eventsB <- observedB.events.get
                  _        = assertEquals(clue(eventsB.get(key)), eventsAfterHandover.some)
                  // the handover ordering itself: A's revoke callback returned before B's assignment started
                  revokedA        <- rebalancesA.returnedFrom(Revoked)
                  _                = assert(clue(revokedA).exists(_.partitions == Set(partition)))
                  enteredB        <- rebalancesB.entered(Assigned)
                  revokeReturnedAt = revokedA.filter(_.partitions.contains(partition)).map(_.at).min
                  assignStartedAt  = enteredB.filter(_.partitions.contains(partition)).map(_.at).min
                  _ = assert(
                    clue(revokeReturnedAt) <= clue(assignStartedAt),
                    "the next owner must not start before the revoke callback has returned"
                  )
                } yield ()
              }
          } yield ()
        }
      }
    } yield ()

    test.unsafeRunSync()
  }

  /** Runs the eviction-mid-callback scenario and returns what became of the stalled revoke-time write, the snapshot
    * store as a new owner would read it, and the key it was all about.
    */
  private def evictionMidRevoke(
    label: String,
    stateTopic: String,
    moduleOfA: KafkaPersistenceModuleOf[IO, String],
    moduleOfB: KafkaPersistenceModuleOf[IO, String],
  ): IO[(Either[Throwable, Unit], BytesByKey, String)] = {
    val inputTopic = s"input-$stateTopic"
    val group      = s"group-$stateTopic"

    for {
      _           <- createTopic(inputTopic, partitionCount)
      _           <- createTopic(stateTopic, partitionCount)
      _           <- produceToAll(inputTopic, eventsBeforeHandover)
      stall       <- FlushStall.of
      observedA   <- Observed.of
      observedB   <- Observed.of
      rebalancesA <- Rebalances.of
      rebalancesB <- Rebalances.of
      result <- adminClient.use { admin =>
        instance(
          group,
          clientIdA,
          inputTopic,
          stalling(moduleOfA, stall),
          flushOnRevokeOnly,
          true,
          rebalancesA,
          observedA
        ).use { _ =>
          for {
            _ <- eventually(s"$label: A to fold the pre-handover events", 90.seconds)(observedA.events.get)(
              _ == partitions.map(partition => keyOf(partition) -> eventsBeforeHandover).toMap
            )
            result <- instance(
              group,
              clientIdB,
              inputTopic,
              moduleOfB,
              persistEveryPoll,
              false,
              rebalancesB,
              observedB
            ).use { _ =>
              for {
                // A is inside its revoke callback, in the flush, with the new generation already published
                stalled  <- stall.reached.get.timeout(150.seconds)
                partition = stalled.topicPartition.partition.value
                key       = stalled.key
                _ <- log.info(s"$label: A stalled flushing $key of partition $partition inside its revoke callback")
                // the broker drops A for the poll interval its stalled callback breached, while A keeps its
                // flows, its buffered state and its producer
                members <- eventually(s"$label: the coordinator to drop A", 120.seconds)(groupMembers(admin, group))(
                  members => members.nonEmpty && !members.exists(_.startsWith(clientIdA))
                )
                _ <- log.info(s"$label: A is out of the group, members are now $members")
                // B really took over: it recovered (A committed nothing and wrote nothing), replayed from the
                // beginning, folded on and persisted. A's buffered state is stale from here on
                _ <- produce(inputTopic, partition, key, eventsAfterHandover)
                _ <- eventually(s"$label: B's snapshot", 150.seconds)(readSnapshots(stateTopic, partition))(
                  _.get(key) == utf8(newOwnerState)
                )
                _ <- log.info(s"$label: B persisted $newOwnerState; releasing A's revoke-time write")
                // A's revoke-time write is now a stale write against a partition it no longer owns
                _       <- stall.release.complete(())
                outcome <- stall.outcome.get.timeout(150.seconds)
                stored  <- readSnapshots(stateTopic, partition)
                _ <- log.info(
                  s"$label: A's revoke-time write ended with $outcome; " +
                    s"store holds ${stored.get(key).flatMap(_.decodeUtf8.toOption)}"
                )
              } yield (outcome, stored, key)
            }
          } yield result
        }
      }
    } yield result
  }

  /** Asserts the prevention: the new owner's snapshot is what a recovery would adopt, and the stale write was rejected
    * by the fence the arm isolates.
    */
  private def assertRejected(
    outcome: Either[Throwable, Unit],
    stored: BytesByKey,
    key: String,
    expected: Throwable => Boolean,
  ): Unit = {
    assertEquals(clue(stored.get(key)), utf8(newOwnerState))
    outcome match {
      case Left(e) =>
        val chain = causeChain(e)
        assert(
          chain.exists(expected),
          s"unexpected failure for the stale write: ${chain.map(_.getClass.getName)}: ${chain.map(_.getMessage)}",
        )
      case Right(()) => fail("expected the stale revoke-time write to be rejected, but it landed")
    }
  }

  test("an owner evicted inside its revoke callback cannot land its write (shared transactional id)") {
    val stateTopic = "revoke-write-eviction-state-topic"

    // both instances share the partition's stable transactional.id, so the takeover's init has already bumped
    // the producer epoch: the stale write dies there, before it reaches the offset commit
    evictionMidRevoke(
      label      = "shared-id",
      stateTopic = stateTopic,
      moduleOfA  = transactionalModuleOf(stateTopic),
      moduleOfB  = transactionalModuleOf(stateTopic),
    ).map {
      case (outcome, stored, key) =>
        assertRejected(
          outcome,
          stored,
          key,
          e => e.isInstanceOf[ProducerFencedException] || e.isInstanceOf[InvalidProducerEpochException],
        )
    }.unsafeRunSync()
  }

  test("an owner evicted inside its revoke callback cannot land its write (per-instance transactional ids)") {
    val stateTopic = "revoke-write-eviction-unshared-state-topic"

    // distinct transactional id prefixes: nothing ever inits A's producer id but A, so there is no epoch fence
    // to hide behind and the coordinator's validation of the generation A published, under a member it no longer
    // has, is the only thing left. This is the arm that isolates the published generation itself
    evictionMidRevoke(
      label      = "unshared-id",
      stateTopic = stateTopic,
      moduleOfA  = transactionalModuleOf(stateTopic, transactionalIdPrefix = s"$appId-a"),
      moduleOfB  = transactionalModuleOf(stateTopic, transactionalIdPrefix = s"$appId-b"),
    ).map {
      case (outcome, stored, key) =>
        assertRejected(outcome, stored, key, _.isInstanceOf[CommitFailedException])
    }.unsafeRunSync()
  }
}

object RevokeTimeWriteSpec {

  val Assigned = "assigned"
  val Revoked  = "revoked"
  val Lost     = "lost"

  /** Every record a fold saw, and the state it produced, per key. */
  final case class Observed(events: Ref[IO, Map[String, List[String]]], states: Ref[IO, Map[String, String]])

  object Observed {
    def of: IO[Observed] =
      (Ref.of[IO, Map[String, List[String]]](Map.empty), Ref.of[IO, Map[String, String]](Map.empty))
        .mapN(Observed(_, _))
  }

  /** One entry into or return from a rebalance callback. */
  final case class Callback(phase: String, at: FiniteDuration, partitions: Set[Int], returned: Boolean)

  final case class Rebalances(callbacks: Ref[IO, List[Callback]]) {
    def mark(phase: String, partitions: Set[Int], returned: Boolean): IO[Unit] =
      IO.monotonic.flatMap(now => callbacks.update(_ :+ Callback(phase, now, partitions, returned)))

    def entered(phase: String): IO[List[Callback]] =
      callbacks.get.map(_.filter(c => c.phase == phase && !c.returned))

    def returnedFrom(phase: String): IO[List[Callback]] =
      callbacks.get.map(_.filter(c => c.phase == phase && c.returned))
  }

  object Rebalances {
    def of: IO[Rebalances] = Ref.of[IO, List[Callback]](List.empty).map(Rebalances(_))
  }

  /** Blocks the first flush it sees: `reached` fires with the key being flushed, `release` lets it out and `outcome`
    * carries what the broker made of it.
    */
  final case class FlushStall(
    reached: Deferred[IO, KafkaKey],
    release: Deferred[IO, Unit],
    outcome: Deferred[IO, Either[Throwable, Unit]],
  )

  object FlushStall {
    def of: IO[FlushStall] =
      (Deferred[IO, KafkaKey], Deferred[IO, Unit], Deferred[IO, Either[Throwable, Unit]]).mapN(FlushStall(_, _, _))
  }
}
