package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptyMap}
import cats.effect.unsafe.IORuntime
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.kafka.flow.kafka.Consumer
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
import com.evolutiongaming.skafka.consumer.{
  AutoOffsetReset,
  ConsumerConfig,
  ConsumerGroupMetadata,
  ConsumerOf,
  ConsumerRecord,
  IsolationLevel
}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord, RecordMetadata}
import com.evolutiongaming.skafka.{
  ClientMetric,
  CommonConfig,
  OffsetAndMetadata,
  Partition,
  PartitionInfo,
  ToBytes,
  Topic,
  TopicPartition
}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Proves the transactional Kafka persistence fences a stale flush on the error-path TEARDOWN route, through a live
  * rebalance and the untouched production machinery (`KafkaFlow` + `kafkaEagerRecovery` +
  * `KafkaPersistenceModuleOf.cachingTransactional`).
  *
  * The route matters because it is not the one the existing #732 prevention tests cover. Those drive the revoke
  * callback with two `PartitionFlow`s over one partition and a fabricated stale generation
  * (`TransactionalKafkaPersistenceSpec`); here a real broker evicts a real member, and what flushes afterwards is
  * kafka-flow's teardown path rather than its revocation path. On the default last-write-wins persistence that flush
  * would land unfenced and overwrite the new owner's fresher snapshot - the corruption shape of
  * https://github.com/evolution-gaming/kafka-flow/issues/732. That exposure is documented and accepted
  * (`docs/persistence.md`, "Protecting against stale snapshot writes"), and already has a reproduction in
  * `TransactionalKafkaPersistenceSpec`, so it is not re-asserted here; this spec pins the mode that is supposed to be
  * safe on a route where its safety was previously inference.
  *
  * The mechanism, each step in source:
  *   1. member A's poll loop stalls in a fold (any stall - long processing, GC, blocked I/O) past
  *      `max.poll.interval.ms`, so the broker hands its partition to member B while A still holds buffered,
  *      never-persisted state (KafkaFlow's poll loop calls `TopicFlow.apply`, which runs the folds:
  *      `ConsumerFlow.scala`);
  *   1. B recovers, re-folds from the committed offset, processes further records, persists a FRESHER snapshot and
  *      commits a NEWER input offset;
  *   1. A's stall ends with an error escaping the poll loop; the stream tears down and `TopicFlow`'s release removes
  *      every cached `PartitionFlow` (`TopicFlow.scala`, `Resource.make`'s release: `cache.keys` + `removeAll`) -
  *      including the partition the broker already gave away, because teardown has no way to know it was kicked;
  *   1. releasing the partition flow releases its key flows, and `TimerFlowOf`'s flush-on-release flushes A's STALE
  *      buffered state (`TimerFlowOf.flushOnCancel`);
  *   1. the write path decides the outcome - here, a transaction the broker rejects.
  *
  * What the test asserts, beyond that the fresher snapshot survives:
  *   - the flush was ATTEMPTED and REJECTED, observed through A's own snapshot writer (`recording`). Without that, a
  *     passing prevention assertion could equally mean no flush ever happened;
  *   - which fence fires: the PRODUCER EPOCH, not the generation. Under the partition's shared stable
  *     `transactional.id` the new owner's `initTransactions` has already bumped the epoch, so the broker rejects the
  *     flush before it reaches the generation-gated offset commit (which would raise `CommitFailedException` instead);
  *   - the next owner's own durable outcome, which states the invariant positively: every record up to the offset it
  *     commits is in the snapshot it persists.
  *
  * A's teardown offset commit does not clobber B's either (the broker rejects a commit from a kicked member -
  * `CommitFailedException`, swallowed in `TopicFlow.commitPending`), so on the unfenced write path the surviving pair
  * would be a stale snapshot at a newer offset: silent loss. That is what the fence prevents here.
  *
  * One thing the route exposes as narrowly fixable, independently of the write path: teardown flushes EVERY cached
  * partition, where the revocation path (`TopicFlow.remove`) knows which partitions are gone. Not attempted here.
  */
class TeardownFlushFencingSpec extends ForAllKafkaSuite {
  implicit val ioRuntime: IORuntime = IORuntime.global
  implicit val logOf: LogOf[IO]     = LogOf.slf4j[IO].unsafeRunSync()
  implicit val log: Log[IO]         = logOf(this.getClass).unsafeRunSync()
  implicit val fromTry: FromTry[IO] = FromTry.lift

  // three sequential member lifecycles around a real broker rebalance run well past munit's 30s default
  override def munitTimeout: Duration = 5.minutes

  private val appId     = "app-id"
  private val poisonKey = "poison"
  private val key       = "key1"

  private def commonConfig(clientId: String) = CommonConfig(
    bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers),
    clientId         = clientId.some,
  )

  private def snapshotConsumerConfig(clientId: String) = ConsumerConfig(
    common          = commonConfig(clientId),
    autoCommit      = false,
    autoOffsetReset = AutoOffsetReset.Earliest,
  )

  /** Every member gets the partition's stable `transactional.id`, so a takeover fences the previous owner twice over:
    * the new owner's `initTransactions` bumps the shared id's producer epoch, and the generation the previous owner's
    * writes bind an offset commit to is no longer current (KIP-447). The epoch fence is the one that fires here - see
    * `writeOutcomes` and the test below.
    *
    * @param producerOf
    *   the module's snapshot-writer factory, so a member's writes can be observed (see `recording`).
    */
  private def transactionalModuleOf(
    clientId: String,
    stateTopic: String,
    producerOf: ProducerOf[IO],
  ): KafkaPersistenceModuleOf[IO, String] =
    KafkaPersistenceModuleOf.cachingTransactional[IO, String](
      consumerOf = ConsumerOf.apply1[IO](),
      producerOf = producerOf,
      config = KafkaPersistenceModule.TransactionalConfig(
        consumerConfig        = snapshotConsumerConfig(s"$clientId-snapshot-reader"),
        producerConfig        = ProducerConfig(common = commonConfig(s"$clientId-snapshot-writer")),
        transactionalIdPrefix = appId,
        snapshotTopic         = stateTopic,
      ),
    )

  /** The outcome of every broker-answered transactional call a member's snapshot writer made: the ack of each `send`,
    * each `sendOffsetsToTransaction` and each `commitTransaction`. Empty means the member never tried to write.
    */
  private type WriteOutcomes = Ref[IO, Vector[Either[Throwable, Unit]]]

  /** Wraps a `ProducerOf` so every write the module performs through it records its outcome. Used on the dying member
    * to assert that its teardown flush was actually ATTEMPTED and then REJECTED - without that, a passing prevention
    * assertion could just mean no flush ever happened.
    */
  private def recording(outcomes: WriteOutcomes): ProducerOf[IO] = { (config: ProducerConfig) =>
    ProducerOf.apply1[IO]().apply(config).map { underlying =>
      def record[A](fa: IO[A]): IO[A] = fa.attempt.flatTap(r => outcomes.update(_ :+ r.void)).rethrow

      new Producer[IO] {
        def initTransactions: IO[Unit]  = underlying.initTransactions
        def beginTransaction: IO[Unit]  = underlying.beginTransaction
        def commitTransaction: IO[Unit] = record(underlying.commitTransaction)
        def abortTransaction: IO[Unit]  = underlying.abortTransaction
        def sendOffsetsToTransaction(
          offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata],
          consumerGroupMetadata: ConsumerGroupMetadata
        ): IO[Unit] = record(underlying.sendOffsetsToTransaction(offsets, consumerGroupMetadata))
        // the ack, not the enqueue, is what the broker answers - so record the inner effect
        def send[K, V](record0: ProducerRecord[K, V])(
          implicit toBytesK: ToBytes[IO, K],
          toBytesV: ToBytes[IO, V]
        ): IO[IO[RecordMetadata]] = underlying.send(record0).map(ack => record(ack))
        def partitions(topic: Topic): IO[List[PartitionInfo]]   = underlying.partitions(topic)
        def flush: IO[Unit]                                     = underlying.flush
        def clientMetrics: IO[Seq[ClientMetric[IO]]]            = underlying.clientMetrics
        def clientInstanceId(timeout: FiniteDuration): IO[Uuid] = underlying.clientInstanceId(timeout)
      }
    }
  }

  /** The final compacted view of the snapshot topic for `key`, read `read_committed` the way the persistence's own
    * recovery reads it - so an observation can never see more than a recovering member would.
    */
  private def snapshotOf(stateTopic: String): IO[Option[String]] =
    KafkaPartitionPersistence
      .readSnapshots[IO](
        consumerOf = ConsumerOf.apply1[IO](),
        consumerConfig =
          snapshotConsumerConfig("snapshot-observer").copy(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopic = stateTopic,
        partition     = Partition.min,
        stall         = none,
      )
      .map(_.get(key).flatMap(_.decodeUtf8.toOption))

  private def committedOffset(group: String, inputTopic: String): IO[Option[Long]] =
    IO.blocking {
      val props = new Properties
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.container.bootstrapServers)
      val client = AdminClient.create(props)
      try {
        val offsets = client.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
        Option(offsets.get(new org.apache.kafka.common.TopicPartition(inputTopic, 0))).map(_.offset())
      } finally client.close()
    }

  private def eventually[A](hint: String, timeout: FiniteDuration = 90.seconds)(fa: IO[Option[A]]): IO[A] = {
    def loop(remaining: FiniteDuration): IO[A] = fa.flatMap {
      case Some(a)                     => a.pure[IO]
      case None if remaining > 0.nanos => IO.sleep(250.millis) *> loop(remaining - 250.millis)
      case None => IO.raiseError(new RuntimeException(s"condition not met within $timeout: $hint"))
    }
    loop(timeout)
  }

  /** What a member's fold observed: the key, the state BEFORE the record and the record's value. */
  private case class Seen(key: String, stateBefore: Option[String], value: String)

  /** State is the concatenation of consumed values; the poison record runs `onPoison` (member A blocks and then raises
    * there, other members pass through) and leaves no state.
    */
  private def foldOf(
    seen: Ref[IO, Vector[Seen]],
    onPoison: IO[Unit],
  ): FoldOption[IO, String, ConsumerRecord[String, ByteVector]] =
    FoldOption.of { (state, record) =>
      for {
        value <- IO(record.value.get.value.decodeUtf8.toOption.get)
        key    = record.key.get.value
        _     <- seen.update(_ :+ Seen(key, state, value))
        next <-
          if (key == poisonKey) onPoison.as(none[String])
          else IO.pure(state.fold(value)(_ + value).some)
      } yield next
    }

  /** A full production member: `KafkaFlow` over the given Kafka snapshot persistence. Yields the background completion
    * (raises if the member's stream died with an error).
    */
  private def member(
    group: String,
    clientId: String,
    inputTopic: String,
    stateTopic: String,
    maxPollInterval: FiniteDuration,
    timerFlowOf: TimerFlowOf[IO],
    partitionFlowConfig: PartitionFlowConfig,
    fold: FoldOption[IO, String, ConsumerRecord[String, ByteVector]],
    producerOf: ProducerOf[IO] = ProducerOf.apply1[IO](),
  ): Resource[IO, IO[Unit]] = {
    implicit val retry: Retry[IO] = Retry.empty[IO]
    for {
      moduleOf <- Resource.pure[IO, KafkaPersistenceModuleOf[IO, String]](
        transactionalModuleOf(clientId, stateTopic, producerOf)
      )
      timersOf <- Resource.eval(TimersOf.memory[IO, KafkaKey])
      partitionFlowOf = kafkaEagerRecovery[IO, String](
        kafkaPersistenceModuleOf = moduleOf,
        applicationId            = appId,
        groupId                  = group,
        timersOf                 = timersOf,
        timerFlowOf              = timerFlowOf,
        fold                     = fold,
        partitionFlowConfig      = partitionFlowConfig,
        tick                     = TickOption.id[IO, String],
        filter                   = none,
        registry                 = EntityRegistry.empty[IO, KafkaKey, String],
      )
      consumer = ConsumerOf
        .apply1[IO]()
        .apply[String, ByteVector](
          ConsumerConfig(
            common          = commonConfig(clientId),
            groupId         = group.some,
            autoCommit      = false,
            autoOffsetReset = AutoOffsetReset.Earliest,
            maxPollInterval = maxPollInterval,
          )
        )
        .evalMap(consumer => Consumer.of[IO](consumer))
      completion <- KafkaFlow.resource(
        consumer = consumer,
        flowOf   = ConsumerFlowOf[IO](topic = inputTopic, flowOf = TopicFlowOf(partitionFlowOf)),
      )
    } yield completion
  }

  /** Buffers state without ever persisting or committing (flush only on release) - the shape of an instance that relies
    * on flush-on-revoke.
    */
  private def bufferingTimerFlowOf: TimerFlowOf[IO] =
    TimerFlowOf.unloadOrphaned[IO](
      fireEvery           = 1.minute,
      maxOffsetDifference = 1000000,
      maxIdle             = 1.day,
      flushOnRevoke       = true,
    )

  /** Persists and commits after every poll - the new owner makes progress durable immediately. */
  private def eagerTimerFlowOf: TimerFlowOf[IO] =
    TimerFlowOf.persistPeriodically[IO](fireEvery = 0.seconds, persistEvery = 0.seconds, flushOnRevoke = false)

  private def eagerPartitionFlowConfig: PartitionFlowConfig =
    PartitionFlowConfig(triggerTimersInterval = 0.seconds, commitOffsetsInterval = 0.seconds)

  /** What the scenario observed once the dying member's teardown flush had run to completion, and then once the next
    * owner had built on whatever pair survived it.
    */
  private case class Observed(
    snapshot: Option[String],
    committedOffset: Option[Long],
    staleFlushOutcomes: Vector[Either[Throwable, Unit]],
    nextOwnerStateBefore: Option[String],
    nextOwnerSnapshot: Option[String],
    nextOwnerCommittedOffset: Long,
  )

  /** Member A buffers 1,2,3 and stalls; B takes the partition over, persists "12345" and commits offset 6; A then dies
    * of an injected error and its teardown flushes the stale "123" of a partition it no longer owns. Finally member C
    * recovers, to show what the surviving (snapshot, offset) pair means end to end.
    */
  private def teardownFlushScenario(stateTopic: String, group: String): IO[Observed] = {
    val inputTopic     = s"input-$stateTopic"
    val storedSnapshot = snapshotOf(stateTopic)

    val producerResource =
      ProducerOf.apply1[IO]().apply(ProducerConfig(common = commonConfig("input-producer")))

    def produce(producer: Producer[IO], key: String, value: String): IO[Unit] =
      producer.send(ProducerRecord[String, String](inputTopic, value.some, key.some)).flatten.void

    producerResource.use { producer =>
      for {
        _     <- createTopic(inputTopic, 1)
        _     <- createTopic(stateTopic, 1)
        seenA <- Ref.of[IO, Vector[Seen]](Vector.empty)
        seenB <- Ref.of[IO, Vector[Seen]](Vector.empty)
        seenC <- Ref.of[IO, Vector[Seen]](Vector.empty)
        // A's poison fold blocks on this gate; completing it left-makes the fold raise, i.e. an error escapes
        // the poll loop and A tears down
        gate <- Deferred[IO, Either[Throwable, Unit]]
        // every write A's snapshot writer makes, with its outcome: A never persists before teardown, so whatever
        // lands here IS the teardown flush - and the test can tell "fenced" from "never attempted"
        outcomesA <- Ref.of[IO, Vector[Either[Throwable, Unit]]](Vector.empty)
        _         <- List("1", "2", "3").traverse_(produce(producer, key, _))
        _         <- produce(producer, poisonKey, "x")

        // member A: short max.poll.interval, so the blocked fold gets it kicked from the group quickly; buffers
        // state without persisting - its only write is the teardown flush under test. A's release IS that flush,
        // so it cannot be `use`-scoped: it runs at the controlled point below, and the guarantee covers the rest
        allocatedA <- member(
          group               = group,
          clientId            = "member-a",
          inputTopic          = inputTopic,
          stateTopic          = stateTopic,
          maxPollInterval     = 5.seconds,
          timerFlowOf         = bufferingTimerFlowOf,
          partitionFlowConfig = PartitionFlowConfig(),
          fold                = foldOf(seenA, onPoison = gate.get.flatMap(IO.fromEither)),
          producerOf          = recording(outcomesA),
        ).allocated
        (completionA, releaseA) = allocatedA
        releasedA              <- Ref.of[IO, Boolean](false)
        releaseAOnce = releasedA.getAndSet(true).flatMap(alreadyReleased => releaseA.unlessA(alreadyReleased))
        observed <- (
          for {
            // A owns the partition, folded 1,2,3 into its (unpersisted) state and is now stalled on the poison record
            _ <- eventually("A folded 1,2,3 and stalled on the poison record") {
              seenA.get.map { seen =>
                Option.when(seen.count(_.key == key) == 3 && seen.exists(_.key == poisonKey))(())
              }
            }
            // member B: joins the same group; A - stalled past its max.poll.interval - is kicked and the broker
            // hands the partition to B, which re-folds from the committed offset (none -> earliest)
            afterTeardown <- member(
              group               = group,
              clientId            = "member-b",
              inputTopic          = inputTopic,
              stateTopic          = stateTopic,
              maxPollInterval     = 5.minutes,
              timerFlowOf         = eagerTimerFlowOf,
              partitionFlowConfig = eagerPartitionFlowConfig,
              fold                = foldOf(seenB, onPoison = IO.unit),
            ).use { _ =>
              for {
                _ <- eventually("B took the partition over and re-folded 1,2,3") {
                  seenB.get.map(seen => Option.when(seen.count(_.key == key) == 3)(()))
                }
                // B makes further progress than A ever saw...
                _ <- List("4", "5").traverse_(produce(producer, key, _))
                // ...and makes it durable: fresher snapshot, newer committed input offset
                _ <- eventually("B persisted the fresher snapshot")(storedSnapshot.map(_.filter(_ == "12345")))
                _ <- eventually("B committed the newer input offset") {
                  committedOffset(group, inputTopic).map(_.filter(_ == 6L))
                }
                // now the error escapes A's poll loop; A's stream tears down and its release flushes the
                // buffered state of a partition the broker gave away seconds ago - while B is still running
                _        <- gate.complete(Left(new Exception("boom: error escaping the poll loop")))
                outcomeA <- completionA.attempt
                _         = assert(outcomeA.isLeft, s"expected member A to die of the injected error, got $outcomeA")
                // once A's release has returned, its teardown flush has run to completion: the write path awaits
                // the broker's ack, so a write that was accepted is already durable and visible to the read below.
                // One read therefore decides the outcome - no settling window to get wrong in either direction
                _         <- releaseAOnce
                snapshot  <- storedSnapshot
                committed <- committedOffset(group, inputTopic)
                outcomes  <- outcomesA.get
              } yield (snapshot, committed, outcomes)
            }
            // end to end: the next owner recovers from the surviving (snapshot, offset) pair and builds on it
            _ <- produce(producer, key, "6")
            afterNextOwner <- member(
              group               = group,
              clientId            = "member-c",
              inputTopic          = inputTopic,
              stateTopic          = stateTopic,
              maxPollInterval     = 5.minutes,
              timerFlowOf         = eagerTimerFlowOf,
              partitionFlowConfig = eagerPartitionFlowConfig,
              fold                = foldOf(seenC, onPoison = IO.unit),
            ).use { _ =>
              for {
                stateBefore <- eventually("C recovered and processed record 6") {
                  seenC.get.map(_.collectFirst { case Seen(`key`, stateBefore, "6") => stateBefore })
                }
                // liveness gate only - the exact value is asserted by the tests. C is eager and persists before
                // committing, so once it has committed past record 6 its own snapshot is durable too
                offset <- eventually("C committed past record 6")(
                  committedOffset(group, inputTopic).map(_.filter(_ > 6L))
                )
                snapshot <- storedSnapshot
              } yield (stateBefore, snapshot, offset)
            }
          } yield Observed(
            snapshot                 = afterTeardown._1,
            committedOffset          = afterTeardown._2,
            staleFlushOutcomes       = afterTeardown._3,
            nextOwnerStateBefore     = afterNextOwner._1,
            nextOwnerSnapshot        = afterNextOwner._2,
            nextOwnerCommittedOffset = afterNextOwner._3,
          )
        ).guarantee(releaseAOnce.attempt.void)
      } yield observed
    }
  }

  test("issue #732 prevention: an error-path teardown flush is fenced (transactional)") {
    val test = teardownFlushScenario(
      stateTopic = "fenced-teardown-state-topic",
      group      = "fenced-teardown-group",
    ).map { observed =>
      // NOT VACUOUS: A did try to flush, and the broker rejected it. Without this the assertions below would also
      // hold if the teardown flush had simply never happened, which is the one way this test could rot into
      // proving nothing
      val errors = observed.staleFlushOutcomes.collect { case Left(e) => e }
      assert(
        clue(observed.staleFlushOutcomes).nonEmpty,
        "member A never attempted a snapshot write, so the fence was never exercised",
      )
      // under the shared stable id, B's init has already epoch-fenced A, so the broker rejects A's flush for its
      // stale producer epoch BEFORE it reaches the generation-gated offset commit (which would raise
      // CommitFailedException instead) - the same mechanism as the revoke-route prevention test. Either epoch
      // exception is accepted: which one surfaces depends on the transaction protocol version
      assert(
        errors.exists(e => e.isInstanceOf[InvalidProducerEpochException] || e.isInstanceOf[ProducerFencedException]),
        s"expected A's stale flush to be rejected for its producer epoch, got ${clue(errors)}",
      )

      // THE PROTECTION: the rejected flush did not land, so the new owner's fresher snapshot survived, paired with
      // the offset the new owner committed. A's release still succeeds - the rejection surfaces only as a
      // swallowed "scache: failed to release cache entry: ..." line
      assertEquals(clue(observed.snapshot), "12345".some)
      assertEquals(clue(observed.committedOffset), 6L.some)
      // and the next owner recovers the consistent pair - nothing is lost - then carries it forward: every
      // record up to the offset it commits is in the snapshot it persists
      assertEquals(clue(observed.nextOwnerStateBefore), "12345".some)
      assertEquals(clue(observed.nextOwnerSnapshot), "123456".some)
      assertEquals(clue(observed.nextOwnerCommittedOffset), 7L)
    }

    test.unsafeRunSync()
  }
}
