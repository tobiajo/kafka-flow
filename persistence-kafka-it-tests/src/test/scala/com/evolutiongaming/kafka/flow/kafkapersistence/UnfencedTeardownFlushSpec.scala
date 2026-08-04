package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.NonEmptyList
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
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, ConsumerRecord, IsolationLevel}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Partition}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Characterizes the DEFAULT (non-transactional) Kafka persistence on the error-path teardown route: the flush that
  * runs when the stream tears down is completely unfenced, so a dying instance can overwrite the new owner's fresher
  * snapshot AFTER the partition has already moved - the corruption shape of
  * https://github.com/evolution-gaming/kafka-flow/issues/732, produced here through a live rebalance and the untouched
  * production machinery (`KafkaFlow` + `kafkaEagerRecovery` + `KafkaPersistenceModuleOf`).
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
  *   1. the write path decides the outcome, which is what the two tests below separate.
  *
  * A's teardown offset commit does NOT clobber B's (the broker rejects a commit from a kicked member -
  * `CommitFailedException`, swallowed in `TopicFlow.commitPending`), which is exactly what makes the surviving pair
  * (stale snapshot, newer offset) silently corrupt in the unfenced case.
  *
  * The two tests run the same scenario and differ only in the persistence and the expected outcome, mirroring the
  * repro/prevention pair of `TransactionalKafkaPersistenceSpec` for the revoke-time flush. What this route adds over
  * that pair is that the ownership overlap is not simulated - a real broker evicts a real member - and that the
  * surviving (snapshot, offset) pair and the next owner's own durable outcome are both asserted, which is what shows
  * the loss to be unrecoverable rather than transient:
  *   - `caching` (the default): a plain `producer.send` with no fence of any kind (`KafkaSnapshotWriteDatabase.of`), so
  *     the stale snapshot lands after B's fresher one and, the snapshot topic being compacted, becomes the recovery
  *     value - while the committed input offset stays at B's newer one. The records between the two snapshots are never
  *     re-folded: durable data loss. That is documented, accepted behavior rather than a fix waiting to happen -
  *     last-write-wins stays exposed by design and the transactional mode is the protection (`docs/persistence.md`,
  *     "Protecting against stale snapshot writes"). Its assertions therefore characterize what the mode currently does;
  *     invert them to the transactional ones only if the default mode ever gains a fence.
  *   - `cachingTransactional`: the same flush is a transaction the broker rejects, so B's snapshot survives and the
  *     next owner loses nothing. On this route that was previously inference: the existing #732 prevention test covers
  *     the revoke callback, and the fence that fires here turns out to be the producer epoch, not the generation.
  *
  * One thing the route does expose as narrowly fixable, independently of the write path: teardown flushes EVERY cached
  * partition, where the revocation path (`TopicFlow.remove`) knows which partitions are gone. Not attempted here.
  */
class UnfencedTeardownFlushSpec extends ForAllKafkaSuite {
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

  /** The persistence a member runs on: the only difference between the two tests below.
    *
    * @param isolationLevel
    *   how the test observes the snapshot topic - the same level the persistence's own recovery reads with, so an
    *   observation cannot see more than a recovering member would.
    */
  private case class Persistence(
    moduleOf: (String, String) => Resource[IO, KafkaPersistenceModuleOf[IO, String]],
    isolationLevel: IsolationLevel,
  )

  /** The default persistence: a plain producer per member, snapshot writes are unfenced `producer.send`s. */
  private val cachingPersistence = Persistence(
    moduleOf = (clientId, stateTopic) =>
      ProducerOf
        .apply1[IO]()
        .apply(ProducerConfig(common = commonConfig(s"$clientId-snapshot-writer")))
        .map { producer =>
          KafkaPersistenceModuleOf.caching[IO, String](
            consumerOf     = ConsumerOf.apply1[IO](),
            producer       = producer,
            consumerConfig = snapshotConsumerConfig(s"$clientId-snapshot-reader"),
            snapshotTopic  = stateTopic,
          )
        },
    isolationLevel = IsolationLevel.ReadUncommitted,
  )

  /** The transactional persistence. Every member gets the partition's stable `transactional.id`, so a takeover fences
    * the previous owner twice over: the new owner's `initTransactions` bumps the shared id's producer epoch, and the
    * generation the previous owner's writes bind an offset commit to is no longer current (KIP-447). The epoch fence is
    * the one that fires here - see the test below.
    */
  private val transactionalPersistence = Persistence(
    moduleOf = (clientId, stateTopic) =>
      Resource.pure(
        KafkaPersistenceModuleOf.cachingTransactional[IO, String](
          consumerOf = ConsumerOf.apply1[IO](),
          producerOf = ProducerOf.apply1[IO](),
          config = KafkaPersistenceModule.TransactionalConfig(
            consumerConfig        = snapshotConsumerConfig(s"$clientId-snapshot-reader"),
            producerConfig        = ProducerConfig(common = commonConfig(s"$clientId-snapshot-writer")),
            transactionalIdPrefix = appId,
            snapshotTopic         = stateTopic,
          ),
        )
      ),
    isolationLevel = IsolationLevel.ReadCommitted,
  )

  /** The final compacted view of the snapshot topic for `key`, read the way recovery reads it. */
  private def snapshotOf(stateTopic: String, isolationLevel: IsolationLevel): IO[Option[String]] =
    KafkaPartitionPersistence
      .readSnapshots[IO](
        consumerOf     = ConsumerOf.apply1[IO](),
        consumerConfig = snapshotConsumerConfig("snapshot-observer").copy(isolationLevel = isolationLevel),
        snapshotTopic  = stateTopic,
        partition      = Partition.min,
        stall          = none,
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
    persistence: Persistence,
    group: String,
    clientId: String,
    inputTopic: String,
    stateTopic: String,
    maxPollInterval: FiniteDuration,
    timerFlowOf: TimerFlowOf[IO],
    partitionFlowConfig: PartitionFlowConfig,
    fold: FoldOption[IO, String, ConsumerRecord[String, ByteVector]],
  ): Resource[IO, IO[Unit]] = {
    implicit val retry: Retry[IO] = Retry.empty[IO]
    for {
      moduleOf <- persistence.moduleOf(clientId, stateTopic)
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
    nextOwnerStateBefore: Option[String],
    nextOwnerSnapshot: Option[String],
    nextOwnerCommittedOffset: Long,
  )

  /** Member A buffers 1,2,3 and stalls; B takes the partition over, persists "12345" and commits offset 6; A then dies
    * of an injected error and its teardown flushes the stale "123" of a partition it no longer owns. Finally member C
    * recovers, to show what the surviving (snapshot, offset) pair means end to end.
    */
  private def teardownFlushScenario(persistence: Persistence, stateTopic: String, group: String): IO[Observed] = {
    val inputTopic     = s"input-$stateTopic"
    val storedSnapshot = snapshotOf(stateTopic, persistence.isolationLevel)

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
        _    <- List("1", "2", "3").traverse_(produce(producer, key, _))
        _    <- produce(producer, poisonKey, "x")

        // member A: short max.poll.interval, so the blocked fold gets it kicked from the group quickly; buffers
        // state without persisting - its only write is the teardown flush under test. A's release IS that flush,
        // so it cannot be `use`-scoped: it runs at the controlled point below, and the guarantee covers the rest
        allocatedA <- member(
          persistence         = persistence,
          group               = group,
          clientId            = "member-a",
          inputTopic          = inputTopic,
          stateTopic          = stateTopic,
          maxPollInterval     = 5.seconds,
          timerFlowOf         = bufferingTimerFlowOf,
          partitionFlowConfig = PartitionFlowConfig(),
          fold                = foldOf(seenA, onPoison = gate.get.flatMap(IO.fromEither)),
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
              persistence         = persistence,
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
              } yield (snapshot, committed)
            }
            // end to end: the next owner recovers from the surviving (snapshot, offset) pair and builds on it
            _ <- produce(producer, key, "6")
            afterNextOwner <- member(
              persistence         = persistence,
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
            nextOwnerStateBefore     = afterNextOwner._1,
            nextOwnerSnapshot        = afterNextOwner._2,
            nextOwnerCommittedOffset = afterNextOwner._3,
          )
        ).guarantee(releaseAOnce.attempt.void)
      } yield observed
    }
  }

  // CHARACTERIZATION, NOT DESIRED BEHAVIOR: every assertion below asserts that the corruption HAPPENS. The default
  // mode is last-write-wins by design (see the scaladoc), so this is what it currently does; if it ever gains a
  // fence, invert these to the transactional test's assertions below.
  test("issue #732 shape (characterized): an error-path teardown flush lands unfenced after the new owner's snapshot") {
    val test = teardownFlushScenario(
      persistence = cachingPersistence,
      stateTopic  = "unfenced-teardown-state-topic",
      group       = "unfenced-teardown-group",
    ).map { observed =>
      // THE DEFECT: the plain-producer flush is not fenced, so the dying instance's stale snapshot landed
      // after - and thus over - the new owner's fresher one
      assertEquals(clue(observed.snapshot), "123".some)
      // while the committed input offset stays at B's newer one (A's teardown commit is rejected by the broker
      // and swallowed) - the silently corrupt (stale snapshot, newer offset) pair
      assertEquals(clue(observed.committedOffset), 6L.some)
      // so the next owner recovers state "123" at offset 6: records 4 and 5 are never re-folded, and with a
      // correct store this would be Some("12345") - durable data loss
      assertEquals(clue(observed.nextOwnerStateBefore), "123".some)
      // and it is not transient: the next owner persists the damaged aggregate and commits PAST record 6, so
      // the offsets that would have re-delivered 4 and 5 are gone for good
      assertEquals(clue(observed.nextOwnerSnapshot), "1236".some)
      assertEquals(clue(observed.nextOwnerCommittedOffset), 7L)
    }

    test.unsafeRunSync()
  }

  test("issue #732 prevention: an error-path teardown flush is fenced (transactional)") {
    val test = teardownFlushScenario(
      persistence = transactionalPersistence,
      stateTopic  = "fenced-teardown-state-topic",
      group       = "fenced-teardown-group",
    ).map { observed =>
      // THE PROTECTION: the same flush is a transaction the broker rejects, so the new owner's snapshot survived.
      // Under the shared stable id, B's init has already epoch-fenced A, so the broker rejects A's flush for its
      // stale producer epoch (logged by the run as "scache: failed to release cache entry:
      // InvalidProducerEpochException") before it ever reaches the generation-gated offset commit - the same
      // mechanism as the revoke-route prevention test, and the reason A's release still succeeds
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
