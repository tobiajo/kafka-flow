package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
import cats.effect.unsafe.IORuntime
import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.Consumer
import com.evolutiongaming.kafka.flow.kafkapersistence.LiveRebalanceFencingSpec.Stall
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.flow.{
  ConsumerFlowOf,
  FoldOption,
  ForAllKafkaSuite,
  KafkaFlow,
  KafkaKey,
  PartitionFlowConfig,
  TickOption,
  TopicFlowOf
}
import com.evolutiongaming.retry.Retry
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, ConsumerRecord, IsolationLevel}
import com.evolutiongaming.skafka.producer.{ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Partition}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{CommitFailedException, CooperativeStickyAssignor}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Tests the assumption [[TransactionalKafkaPersistenceSpec]] rests on - that its simulated overlap "is
  * indistinguishable from the second flow being created while the first is still alive". Its fence tests inject the
  * stale consumer generation, so the fence is only ever asked to reject a hand-made token. Here the stale owner is made
  * stale by the broker, during a real rebalance, and is genuinely alive while the new owner writes.
  *
  * The overlap is produced the way production produces it: flow A stalls inside its `fold`, which runs on the poll
  * loop's thread of control, so the client's own heartbeat thread leaves the group past `max.poll.interval.ms` - the
  * eviction the design doc predicts to be fenced under every protocol and assignor ("A member evicted before the flush
  * is rejected under all three", Consumer rebalance protocols). Flow B then takes the partition over for real, recovers
  * A's snapshot, folds further events and persists. Only then is A let go, so its next periodic flush is a stale write
  * against a partition it no longer owns - with A still holding its flows, its buffered state and its per-partition
  * producer.
  *
  * The transactional arm asserts the stale write does not land and the new owner's snapshot survives; the plain arm is
  * the control, reproducing issue #732 under the same live rebalance.
  *
  * The flush under test is the *periodic* one, not the revoke-time one (`flushOnRevoke = false`): a stalled member's
  * next flush precedes the poll that would tell it anything was lost, and the revoke-time flush is already covered by
  * the flush-on-revoke pair in [[TransactionalKafkaPersistenceSpec]].
  */
class LiveRebalanceFencingSpec extends ForAllKafkaSuite {

  // an eviction is paced by the broker (a poll interval, then a join round) and each arm waits one out. Set above
  // the arm's own wait budget (285s) so those waits fail first - they name the step that hung, this only says "timed out"
  override def munitTimeout: Duration = 8.minutes

  // runtime is dominated by teardown, not by the scenario: closing an evicted consumer waits out Kafka's 30s default
  // close timeout on cleanup it can no longer complete, so the suite lands near 70s or near 160s depending on how many
  // of the three arms pay it

  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift
  // no retry: a fenced flow must surface its error to the test instead of being restarted under it
  implicit val retry: Retry[IO] = Retry.empty[IO]

  private val appId = "app-id"

  private val key                 = "key1"
  private val eventsBeforeStall   = (1 to 5).toList.map(i => s"e$i")
  private val stallEvent          = "e6"
  private val eventsAfterTakeover = (7 to 10).toList.map(i => s"e$i")

  /** A's state once it is let go: it folded everything up to and including the record it stalled on. */
  private val staleState = (eventsBeforeStall :+ stallEvent).mkString(",")

  /** B's state: A never committed the stalled record's offset, so B replays that record and folds on. */
  private val newOwnerState = (eventsBeforeStall ++ (stallEvent :: eventsAfterTakeover)).mkString(",")

  private def commonConfig = CommonConfig(bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers))

  private def producerConfig = ProducerConfig(common = commonConfig)

  private def producerOf = ProducerOf.apply1[IO]()

  private def consumerOf = ConsumerOf.apply1[IO]()

  /** The persistence module's own consumer: group-less, for the recovery read only. */
  private def persistenceConsumerConfig =
    ConsumerConfig(common = commonConfig, autoCommit = false, autoOffsetReset = AutoOffsetReset.Earliest)

  /** The flow-driving consumer. `CooperativeStickyAssignor` is the assignor under evaluation.
    *
    * Only the stalled flow needs to be evictable in seconds rather than the default five minutes, so only it gets the
    * tight timeouts: the session timeout is the smallest the broker accepts (`group.min.session.timeout.ms` is 6s) and
    * the poll interval sits just above it. The takeover keeps the defaults - it has to stay in the group through the
    * rest of the scenario, and a recovery slow enough to breach a 7s poll interval would evict it too.
    */
  private def drivingConsumerConfig(group: String, evictable: Boolean) = ConsumerConfig(
    common                      = commonConfig,
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
  private def readSnapshots(stateTopic: String): IO[BytesByKey] =
    KafkaPartitionPersistence.readSnapshots[IO](
      consumerOf     = consumerOf,
      consumerConfig = persistenceConsumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
      snapshotTopic  = stateTopic,
      partition      = Partition.min,
      stall = KafkaPartitionPersistence
        .Stall(KafkaPersistenceModule.TransactionalConfig.DefaultRecoveryStallTimeout, IO.monotonic)
        .some,
    )

  private def utf8(value: String): Option[ByteVector] = ByteVector.encodeUtf8(value).toOption

  private def transactionalModuleOf(
    stateTopic: String,
    transactionalIdPrefix: String = appId,
  ): Resource[IO, KafkaPersistenceModuleOf[IO, String]] =
    Resource.pure(
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
    )

  /** The control's persistence: a plain producer per instance and no offset binding - what the app looked like before
    * transactional snapshot writes.
    */
  private def plainModuleOf(stateTopic: String): Resource[IO, KafkaPersistenceModuleOf[IO, String]] =
    producerOf(producerConfig).map { producer =>
      KafkaPersistenceModuleOf.caching[IO, String](
        consumerOf     = consumerOf,
        producer       = producer,
        consumerConfig = persistenceConsumerConfig,
        snapshotTopic  = stateTopic,
      )
    }

  /** State is the comma-joined list of folded events, as a stand-in for a real aggregate. A `Stall` blocks the fold on
    * one named event: it reports itself blocked and waits to be released *before* folding, so nothing is appended and
    * nothing is flushed until the test lets it go.
    */
  private def fold(stall: Option[Stall]): FoldOption[IO, String, ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      val event = record.value.flatMap(_.value.decodeUtf8.toOption).getOrElse(sys.error("event payload missing"))
      val blockIfStalled = stall.traverse_ { stall =>
        (stall.reached.complete(()) *> stall.release.get).whenA(event == stall.on)
      }
      blockIfStalled.as(state.fold(event)(s => s"$s,$event").some)
    }

  /** One running instance of the app: a real consumer in `group`, the production flow wiring, state persisted and
    * offsets committed on every poll. Returns the flow's completion, so a fenced flow's error is observable.
    */
  private def instance(
    group: String,
    inputTopic: String,
    moduleOf: KafkaPersistenceModuleOf[IO, String],
    stall: Option[Stall],
  ): Resource[IO, IO[Unit]] =
    for {
      timersOf <- TimersOf.memory[IO, KafkaKey].toResource
      partitionFlowOf = kafkaEagerRecovery[IO, String](
        kafkaPersistenceModuleOf = moduleOf,
        applicationId            = appId,
        groupId                  = group,
        timersOf                 = timersOf,
        timerFlowOf = TimerFlowOf.persistPeriodically[IO](
          fireEvery     = 0.seconds,
          persistEvery  = 0.seconds,
          flushOnRevoke = false,
        ),
        fold                = fold(stall),
        tick                = TickOption.id[IO, String],
        partitionFlowConfig = PartitionFlowConfig(triggerTimersInterval = 0.seconds, commitOffsetsInterval = 0.seconds),
        registry            = EntityRegistry.empty[IO, KafkaKey, String],
      )
      consumer = consumerOf
        .apply[String, ByteVector](drivingConsumerConfig(group, evictable = stall.isDefined))
        .evalMap(Consumer.of[IO](_))
      completion <- KafkaFlow.resource(
        consumer = consumer,
        flowOf   = ConsumerFlowOf[IO](topic = inputTopic, flowOf = TopicFlowOf(partitionFlowOf)),
      )
    } yield completion

  private def produce(inputTopic: String, events: List[String]): IO[Unit] =
    producerOf(producerConfig).use { producer =>
      events.traverse_ { event =>
        producer.send(ProducerRecord[String, String](inputTopic, event.some, key.some, Partition.min.some)).flatten.void
      }
    }

  private def adminClient: Resource[IO, AdminClient] = {
    val props = new Properties
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)
    Resource.make(IO.delay(AdminClient.create(props)))(client => IO(client.close()))
  }

  /** The group's members as the coordinator sees them - the broker's own answer to "is A still an owner?" */
  private def groupMembers(client: AdminClient, group: String): IO[Set[String]] =
    IO.blocking(client.describeConsumerGroups(List(group).asJava).all().get(10, TimeUnit.SECONDS))
      .map(_.asScala.get(group).toList.flatMap(_.members().asScala.map(_.consumerId())).toSet)

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
    List.unfold(Option(e)) { current =>
      current.map(c => (c, Option(c.getCause).filter(_ ne c)))
    }

  /** Runs the scenario and returns what became of A's flow together with the snapshot store as a new owner would read
    * it. `None` means A's flow was still running when the store was read - which is what the control arm expects, since
    * a plain flow survives its own failed offset commit.
    */
  private def liveEvictionScenario(
    label: String,
    stateTopic: String,
    moduleOfA: Resource[IO, KafkaPersistenceModuleOf[IO, String]],
    moduleOfB: Resource[IO, KafkaPersistenceModuleOf[IO, String]],
  ): IO[(Option[Either[Throwable, Unit]], BytesByKey)] = {
    val inputTopic = s"input-$stateTopic"
    val group      = s"group-$stateTopic"

    // the two things a released A can do: die on the fence, or land its stale write. Whichever happens first
    // ends the wait; the store is then read and decides the arm.
    def awaitA(endedA: Deferred[IO, Either[Throwable, Unit]]): IO[Option[Either[Throwable, Unit]]] =
      IO.race(
        endedA.get,
        eventually(s"$label: A's stale write to land", 45.seconds)(readSnapshots(stateTopic))(
          _.get(key) == utf8(staleState)
        ),
      ).map(_.swap.toOption)
        .handleErrorWith(e => log.warn(s"$label: A neither ended nor wrote after release: $e").as(none))

    for {
      _       <- createTopic(inputTopic, 1)
      _       <- createTopic(stateTopic, 1)
      reached <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      endedA  <- Deferred[IO, Either[Throwable, Unit]]
      _       <- produce(inputTopic, eventsBeforeStall)
      result <- moduleOfA.use { moduleA =>
        instance(group, inputTopic, moduleA, Stall(stallEvent, reached, release).some).use { completionA =>
          completionA.attempt.flatMap(endedA.complete).void.background.use { _ =>
            for {
              // A owns the partition, folded the first events and persisted them
              _ <- eventually(s"$label: A's snapshot", 60.seconds)(readSnapshots(stateTopic))(
                _.get(key) == utf8(eventsBeforeStall.mkString(","))
              )
              // the stall: A blocks in the fold, so its poll loop stops making progress
              _ <- produce(inputTopic, List(stallEvent))
              _ <- reached.get.timeout(30.seconds)
              _ <- log.info(s"$label: A stalled on $stallEvent; waiting for the coordinator to drop it")
              // the broker removes A past its poll interval, while A keeps flows, state and producer
              _ <- adminClient.use { admin =>
                eventually(s"$label: the coordinator to drop A", 60.seconds)(groupMembers(admin, group))(_.isEmpty)
              }
              out <- moduleOfB.use { moduleB =>
                instance(group, inputTopic, moduleB, none).use { completionB =>
                  completionB.attempt.flatMap(o => log.warn(s"$label: B's flow ended: $o")).background.use { _ =>
                    for {
                      _ <- produce(inputTopic, eventsAfterTakeover)
                      // B really took over: it recovered A's snapshot, replayed the record A never committed,
                      // folded on and persisted. A's own state is stale from here on.
                      _ <- eventually(s"$label: B's snapshot", 90.seconds)(readSnapshots(stateTopic))(
                        _.get(key) == utf8(newOwnerState)
                      )
                      _ <- log.info(s"$label: B persisted $newOwnerState; releasing A")
                      // A's next flush is now a stale write against a partition it lost
                      _      <- release.complete(())
                      ended  <- awaitA(endedA)
                      stored <- readSnapshots(stateTopic)
                      _ <- log.info(
                        s"$label: A ended with $ended; store holds ${stored.get(key).flatMap(_.decodeUtf8.toOption)}"
                      )
                    } yield (ended, stored)
                  }
                }
              }
            } yield out
          }
        }
      }
    } yield result
  }

  /** Asserts the prevention: the new owner's snapshot is what a recovery would adopt, and the stale owner did not
    * survive its own fenced flush - the error propagated out of the flow rather than being lost.
    */
  private def assertFenced(
    ended: Option[Either[Throwable, Unit]],
    stored: BytesByKey,
    expected: Throwable => Boolean,
  ): Unit = {
    assertEquals(clue(stored.get(key)), utf8(newOwnerState))
    ended match {
      case Some(Left(e)) =>
        val chain = causeChain(e)
        assert(
          chain.exists(expected),
          s"unexpected failure for the fenced owner: ${chain.map(_.getClass.getName)}: ${chain.map(_.getMessage)}",
        )
      case other => fail(s"expected the fenced owner's flow to fail, got $other")
    }
  }

  test("a live evicted owner's stale snapshot flush is fenced (transactional)") {
    val stateTopic = "live-rebalance-tx-state-topic"

    liveEvictionScenario(
      label      = "transactional",
      stateTopic = stateTopic,
      moduleOfA  = transactionalModuleOf(stateTopic),
      moduleOfB  = transactionalModuleOf(stateTopic),
    ).map {
      // both instances share the partition's stable transactional.id, so the takeover's init has already
      // bumped the producer epoch: the stale flush dies there, before it reaches the offset commit
      case (ended, stored) =>
        assertFenced(
          ended,
          stored,
          e => e.isInstanceOf[ProducerFencedException] || e.isInstanceOf[InvalidProducerEpochException],
        )
    }.unsafeRunSync()
  }

  test("a live evicted owner's stale flush is fenced by member validation alone (transactional, unshared id)") {
    val stateTopic = "live-rebalance-tx-unshared-state-topic"

    // distinct transactional id prefixes: nothing ever inits A's producer id but A, so there is no epoch fence
    // to hide behind and the coordinator's validation of the evicted member is the only thing left. This is the
    // live counterpart of the isolated generation-fence tests in TransactionalKafkaPersistenceSpec, which reach
    // the same commit with a hand-made stale generation.
    liveEvictionScenario(
      label      = "transactional-unshared-id",
      stateTopic = stateTopic,
      moduleOfA  = transactionalModuleOf(stateTopic, transactionalIdPrefix = s"$appId-a"),
      moduleOfB  = transactionalModuleOf(stateTopic, transactionalIdPrefix = s"$appId-b"),
    ).map {
      case (ended, stored) => assertFenced(ended, stored, _.isInstanceOf[CommitFailedException])
    }.unsafeRunSync()
  }

  test("issue #732 reproduction: a live evicted owner's stale snapshot flush lands (plain)") {
    val stateTopic = "live-rebalance-plain-state-topic"

    liveEvictionScenario(
      label      = "plain",
      stateTopic = stateTopic,
      moduleOfA  = plainModuleOf(stateTopic),
      moduleOfB  = plainModuleOf(stateTopic),
    ).map {
      case (_, stored) =>
        // the corruption: nothing stops the evicted owner, so a recovery would now adopt its state and lose
        // everything the new owner persisted
        assertEquals(clue(stored.get(key)), utf8(staleState))
    }.unsafeRunSync()
  }
}

object LiveRebalanceFencingSpec {

  /** Blocks a fold on one event: `reached` fires when the fold is in, `release` lets it out. */
  final case class Stall(on: String, reached: Deferred[IO, Unit], release: Deferred[IO, Unit])
}
