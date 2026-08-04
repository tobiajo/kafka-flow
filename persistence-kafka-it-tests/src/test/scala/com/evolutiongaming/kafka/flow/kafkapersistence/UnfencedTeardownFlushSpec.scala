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
import com.evolutiongaming.skafka.consumer.{AutoOffsetReset, ConsumerConfig, ConsumerOf, ConsumerRecord}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf, ProducerRecord}
import com.evolutiongaming.skafka.{CommonConfig, Partition}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import scodec.bits.ByteVector

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Pins a pre-existing defect of the DEFAULT (non-transactional) Kafka persistence: an error-path teardown flush is
  * completely unfenced, so a dying instance can overwrite the new owner's fresher snapshot AFTER the partition has
  * already moved - the corruption shape of https://github.com/evolution-gaming/kafka-flow/issues/732, produced here
  * through a live rebalance and the untouched production machinery (`KafkaFlow` + `kafkaEagerRecovery` +
  * `KafkaPersistenceModuleOf.caching`).
  *
  * The mechanism, each step in source:
  *   1. member A's poll loop stalls in a fold (any stall - long processing, GC, blocked I/O) past
  *      `max.poll.interval.ms`, so the broker hands its partition to member B while A still holds buffered,
  *      never-persisted state (KaflaFlow's poll loop calls `TopicFlow.apply`, which runs the folds:
  *      `ConsumerFlow.scala`);
  *   1. B recovers, re-folds from the committed offset, processes further records, persists a FRESHER snapshot and
  *      commits a NEWER input offset;
  *   1. A's stall ends with an error escaping the poll loop; the stream tears down and `TopicFlow`'s release removes
  *      every cached `PartitionFlow` (`TopicFlow.scala`, `Resource.make`'s release: `cache.keys` + `removeAll`) -
  *      including the partition the broker already gave away, because teardown has no way to know it was kicked;
  *   1. releasing the partition flow releases its key flows, and `TimerFlowOf`'s flush-on-release flushes A's STALE
  *      buffered state (`TimerFlowOf.flushOnCancel`);
  *   1. the non-transactional write path is a plain `producer.send` with no fence of any kind
  *      (`KafkaSnapshotWriteDatabase.of`), so the stale snapshot lands after B's fresher one and, the snapshot topic
  *      being compacted, becomes the recovery value - while the committed input offset stays at B's newer one. The
  *      records between the two snapshots are never re-folded: durable data loss.
  *
  * A's teardown offset commit does NOT clobber B's (the broker rejects a commit from a kicked member -
  * `CommitFailedException`, swallowed in `TopicFlow.commitPending`), which is exactly what makes the surviving pair
  * (stale snapshot, newer offset) silently corrupt.
  *
  * The assertions below PIN THE DEFECT: they assert the corrupted outcome. When the defect is fixed the assertions
  * must be flipped to the safe outcome (stale flush rejected or skipped; recovery sees B's snapshot). The
  * transactional mode (`KafkaPersistenceModuleOf.cachingTransactional`) is immune by construction: the same flush is
  * a generation-gated transaction the broker rejects (see `RevokeTimeFlushSpec`).
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

  private def commonConfig(clientId: String) = CommonConfig(
    bootstrapServers = NonEmptyList.one(kafka.container.bootstrapServers),
    clientId         = clientId.some,
  )

  private def snapshotConsumerConfig(clientId: String) = ConsumerConfig(
    common          = commonConfig(clientId),
    autoCommit      = false,
    autoOffsetReset = AutoOffsetReset.Earliest,
  )

  /** The final compacted view of the snapshot topic for `key`, read the way recovery reads it. */
  private def snapshotOf(stateTopic: String, key: String): IO[Option[String]] =
    KafkaPartitionPersistence
      .readSnapshots[IO](
        consumerOf     = ConsumerOf.apply1[IO](),
        consumerConfig = snapshotConsumerConfig("snapshot-observer"),
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
      case Some(a)                   => a.pure[IO]
      case None if remaining > 0.nanos => IO.sleep(250.millis) *> loop(remaining - 250.millis)
      case None                      => IO.raiseError(new RuntimeException(s"condition not met within $timeout: $hint"))
    }
    loop(timeout)
  }

  /** What a member's fold observed: the key, the state BEFORE the record and the record's value. */
  private case class Seen(key: String, stateBefore: Option[String], value: String)

  /** State is the concatenation of consumed values; the poison record runs `onPoison` (member A blocks and then
    * raises there, other members pass through) and leaves no state.
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
        next  <- if (key == poisonKey) onPoison.as(none[String])
                 else IO.pure(state.fold(value)(_ + value).some)
      } yield next
    }

  /** A full production member: `KafkaFlow` over the non-transactional Kafka snapshot persistence. Yields the
    * background completion (raises if the member's stream died with an error).
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
  ): Resource[IO, IO[Unit]] = {
    implicit val retry: Retry[IO] = Retry.empty[IO]
    for {
      producer <- ProducerOf.apply1[IO]().apply(ProducerConfig(common = commonConfig(s"$clientId-snapshot-writer")))
      moduleOf = KafkaPersistenceModuleOf.caching[IO, String](
        consumerOf     = ConsumerOf.apply1[IO](),
        producer       = producer,
        consumerConfig = snapshotConsumerConfig(s"$clientId-snapshot-reader"),
        snapshotTopic  = stateTopic,
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

  /** Buffers state without ever persisting or committing (flush only on release) - the shape of an instance that
    * relies on flush-on-revoke.
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

  // DEFECT PINNED, NOT DESIRED BEHAVIOUR: every assertion below asserts that the corruption HAPPENS.
  // When the unfenced flush is fixed, this test MUST be inverted to assert the safe outcome instead.
  test("DEFECT #732 (pinned): an error-path teardown flush lands unfenced after the new owner's fresher snapshot") {
    val stateTopic = "unfenced-teardown-state-topic"
    val inputTopic = s"input-$stateTopic"
    val group      = "unfenced-teardown-group"
    val key        = "key1"

    val producerResource =
      ProducerOf.apply1[IO]().apply(ProducerConfig(common = commonConfig("input-producer")))

    def produce(producer: Producer[IO], key: String, value: String): IO[Unit] =
      producer.send(ProducerRecord[String, String](inputTopic, value.some, key.some)).flatten.void

    val test = producerResource.use { producer =>
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
        // state without persisting - its only write is the teardown flush under test
        _ <- member(
          group               = group,
          clientId            = "member-a",
          inputTopic          = inputTopic,
          stateTopic          = stateTopic,
          maxPollInterval     = 5.seconds,
          timerFlowOf         = bufferingTimerFlowOf,
          partitionFlowConfig = PartitionFlowConfig(),
          fold                = foldOf(seenA, onPoison = gate.get.flatMap(IO.fromEither)),
        ).use { completionA =>
          for {
            // A owns the partition, folded 1,2,3 into its (unpersisted) state and is now stalled on the poison record
            _ <- eventually("A folded 1,2,3 and stalled on the poison record") {
              seenA.get.map { seen =>
                Option.when(seen.count(_.key == key) == 3 && seen.exists(_.key == poisonKey))(())
              }
            }
            // member B: joins the same group; A - stalled past its max.poll.interval - is kicked and the broker
            // hands the partition to B, which re-folds from the committed offset (none -> earliest)
            _ <- member(
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
                _ <- eventually("B persisted the fresher snapshot") {
                  snapshotOf(stateTopic, key).map(_.filter(_ == "12345"))
                }
                _ <- eventually("B committed the newer input offset") {
                  committedOffset(group, inputTopic).map(_.filter(_ == 6L))
                }
                // now the error escapes A's poll loop; A's stream tears down and its release flushes the
                // buffered state of a partition the broker gave away seconds ago
                _        <- gate.complete(Left(new Exception("boom: error escaping the poll loop")))
                outcomeA <- completionA.attempt
                _         = assert(outcomeA.isLeft, s"expected member A to die of the injected error, got $outcomeA")

                // THE DEFECT: the plain-producer flush is not fenced, so the dying instance's stale snapshot
                // lands after - and thus over - the new owner's fresher one
                _ <- eventually("A's stale teardown flush overwrote the fresher snapshot (the defect)") {
                  snapshotOf(stateTopic, key).map(_.filter(_ == "123"))
                }
                // while the committed input offset stays at B's newer one (A's teardown commit is rejected by
                // the broker and swallowed) - the silently corrupt (stale snapshot, newer offset) pair
                committed <- committedOffset(group, inputTopic)
                _          = assertEquals(committed, Some(6L))
              } yield ()
            }
          } yield ()
        }

        // end-to-end corruption: the next owner recovers from the corrupt pair - state "123" at offset 6 -
        // so records 4 and 5 are never re-folded: durable data loss
        _ <- produce(producer, key, "6")
        _ <- member(
          group               = group,
          clientId            = "member-c",
          inputTopic          = inputTopic,
          stateTopic          = stateTopic,
          maxPollInterval     = 5.minutes,
          timerFlowOf         = eagerTimerFlowOf,
          partitionFlowConfig = eagerPartitionFlowConfig,
          fold                = foldOf(seenC, onPoison = IO.unit),
        ).use { _ =>
          eventually("C recovered and processed record 6") {
            seenC.get.map(_.collectFirst { case Seen(`key`, stateBefore, "6") => stateBefore })
          }.map { stateBefore =>
            // with a correct store this would be Some("12345"); the defect makes it Some("123") - 4 and 5 lost
            assertEquals(stateBefore, Some("123"))
          }
        }
      } yield ()
    }

    test.unsafeRunSync()
  }
}
