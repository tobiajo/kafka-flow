package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.catshelper.{FromTry, Log}
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.{
  Consumer as SkafkaConsumer,
  ConsumerConfig,
  ConsumerOf,
  ConsumerRecord,
  ConsumerRecords,
  IsolationLevel,
  RebalanceListener1,
  WithSize
}
import munit.FunSuite
import scodec.bits.ByteVector

import java.util.regex.Pattern
import scala.concurrent.duration.*

/** Pins the recovery-read behaviour of the transactional mode: the target is the high watermark, not the read
  * consumer's own end offset, and a read making no progress for the whole stall deadline fails loudly instead of
  * hanging.
  */
class ReadSnapshotsSpec extends FunSuite {

  implicit val log: Log[IO]         = Log.empty[IO]
  implicit val fromTry: FromTry[IO] = FromTry.lift

  private val topic     = "state-topic"
  private val partition = Partition.min
  private val tp        = TopicPartition(topic, partition)

  private def record(offset: Long, key: String): ConsumerRecord[String, ByteVector] =
    ConsumerRecord(
      topicPartition   = tp,
      offset           = Offset.unsafe(offset),
      timestampAndType = none,
      key              = WithSize(key).some,
      value            = ByteVector.encodeUtf8(key).toOption.map(WithSize(_)),
    )

  test("a read_committed read drains to the read_uncommitted end offset, not its own") {
    // The read consumer's own endOffsets is the last-stable-offset, which an open transaction pins below
    // committed records (here: LSO = 1, records up to the high watermark 3) - a target taken from it
    // would stop after one record, silently missing the rest.
    val lso           = 1L
    val highWatermark = 3L
    val records       = List(record(0, "k0"), record(1, "k1"), record(2, "k2"))

    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = consumer(endOffset = lso, positionRef = positionRef, records = records)
      hwConsumer   = consumer(endOffset = highWatermark, positionRef = positionRef, records = Nil)
      stored <- KafkaPartitionPersistence.readSnapshots[IO](
        consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
        consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopic  = topic,
        partition      = partition,
        stall = KafkaPartitionPersistence.Stall(KafkaPartitionPersistence.defaultStallTimeout, IO.monotonic).some,
      )
    } yield assertEquals(stored.keys.toList.sorted, List("k0", "k1", "k2"), "the read must drain past its own LSO")

    test.unsafeRunSync()
  }

  test("a read stalled past the deadline fails loudly instead of hanging") {
    // The last-stable-offset pins the position at 1 while the high watermark stays 3: an open transaction
    // outlived the deadline, which the diagnosis must name - the log end still covers the target, so this
    // is not truncation.
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      readConsumer = consumer(endOffset = 1L, positionRef = positionRef, records = List(record(0, "k0")))
      hwConsumer   = consumer(endOffset = 3L, positionRef = positionRef, records = Nil)
      result <- KafkaPartitionPersistence
        .readSnapshots[IO](
          consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
          consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
          snapshotTopic  = topic,
          partition      = partition,
          stall          = KafkaPartitionPersistence.Stall(200.millis, IO.monotonic).some,
        )
        .attempt
    } yield result match {
      case Left(e: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        assertEquals(e.position, Offset.unsafe(1))
        assertEquals(e.targetOffset, Offset.unsafe(3))
        assert(e.diagnosis.contains("open transaction"), s"unexpected diagnosis: ${e.diagnosis}")
      case other => fail(s"expected RecoveryReadStalledError, got $other")
    }

    test.unsafeRunSync()
  }

  test("a read stalled past the deadline with a regressed log end is diagnosed as truncation") {
    // The high watermark is 3 at capture but 1 when the deadline fires (the log was truncated in
    // between): the diagnosis must name truncation.
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      hwRef       <- Ref.of[IO, Long](3L)
      readConsumer = consumer(endOffset = 1L, positionRef = positionRef, records = List(record(0, "k0")))
      hwConsumer   = consumer(endOffset = hwRef.getAndSet(1L), positionRef = positionRef, records = Nil)
      result <- KafkaPartitionPersistence
        .readSnapshots[IO](
          consumerOf     = consumerOf(readConsumer = readConsumer, hwConsumer = hwConsumer),
          consumerConfig = ConsumerConfig(isolationLevel = IsolationLevel.ReadCommitted),
          snapshotTopic  = topic,
          partition      = partition,
          stall          = KafkaPartitionPersistence.Stall(200.millis, IO.monotonic).some,
        )
        .attempt
    } yield result match {
      case Left(e: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        assert(e.diagnosis.contains("log truncation"), s"unexpected diagnosis: ${e.diagnosis}")
      case other => fail(s"expected RecoveryReadStalledError, got $other")
    }

    test.unsafeRunSync()
  }

  test("with no deadline a stalled read keeps waiting instead of failing") {
    // the non-transactional path passes no deadline; a stalled read must keep waiting, not fail
    val test = for {
      positionRef <- Ref.of[IO, Long](0L)
      result <- KafkaPartitionPersistence
        .readPartition[IO](
          consumer          = consumer(endOffset = 3L, positionRef = positionRef, records = List(record(0, "k0"))),
          snapshotPartition = tp,
          targetOffset      = Offset.unsafe(3),
          stall             = none,
          diagnose          = IO.pure(""), // unused without a deadline
        )
        .timeout(500.millis)
        .attempt
    } yield result match {
      case Left(_: KafkaPartitionPersistence.RecoveryReadStalledError) =>
        fail("expected no stall failure when no deadline is set")
      case Left(_)  => () // timed out from the outside while still polling - i.e. it kept waiting
      case Right(_) => fail("expected the read to keep waiting, not to complete")
    }

    test.unsafeRunSync()
  }

  // dispatches by isolation level: the read consumer for read_committed, the HW consumer for read_uncommitted
  private def consumerOf(
    readConsumer: SkafkaConsumer[IO, String, ByteVector],
    hwConsumer: SkafkaConsumer[IO, String, ByteVector],
  ): ConsumerOf[IO] = new ConsumerOf[IO] {
    def apply[K, V](
      config: ConsumerConfig
    )(implicit fromBytesK: FromBytes[IO, K], fromBytesV: FromBytes[IO, V]) =
      cats
        .effect
        .Resource
        .pure[IO, SkafkaConsumer[IO, K, V]](
          (if (config.isolationLevel == IsolationLevel.ReadUncommitted) hwConsumer else readConsumer)
            .asInstanceOf[SkafkaConsumer[IO, K, V]]
        )
  }

  // serves `records` one per poll from `position`, advancing it; on an empty poll it sleeps the poll timeout, as a
  // real blocking poll would, so elapsed wall time (the stall deadline) advances deterministically. endOffsets is
  // fixed per consumer (the isolation-dependent bound) unless an effect is passed (to move it between captures)
  private def consumer(
    endOffset: Long,
    positionRef: Ref[IO, Long],
    records: List[ConsumerRecord[String, ByteVector]],
  ): SkafkaConsumer[IO, String, ByteVector] =
    consumer(endOffset.pure[IO], positionRef, records)

  private def consumer(
    endOffset: IO[Long],
    positionRef: Ref[IO, Long],
    records: List[ConsumerRecord[String, ByteVector]],
  ): SkafkaConsumer[IO, String, ByteVector] =
    new SkafkaConsumer[IO, String, ByteVector] {
      private val delegate = SkafkaConsumer.empty[IO, String, ByteVector]

      override def endOffsets(partitions: NonEmptySet[TopicPartition]) =
        endOffset.map(end => Map(tp -> Offset.unsafe(end)))

      override def position(partition: TopicPartition) = positionRef.get.map(Offset.unsafe(_))

      override def poll(timeout: FiniteDuration) =
        positionRef
          .modify { pos =>
            records.find(_.offset.value == pos) match {
              case Some(record) => (pos + 1, record.some)
              case None         => (pos, none[ConsumerRecord[String, ByteVector]])
            }
          }
          .flatMap {
            case Some(record) => ConsumerRecords(Map(tp -> NonEmptyList.one(record))).pure[IO]
            case None         => IO.sleep(timeout).as(ConsumerRecords.empty[String, ByteVector])
          }

      def assign(partitions: NonEmptySet[TopicPartition])                         = delegate.assign(partitions)
      def assignment                                                              = delegate.assignment
      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]) = delegate.subscribe(topics, listener)
      def subscribe(topics: NonEmptySet[Topic])                                   = delegate.subscribe(topics)
      def subscribe(pattern: Pattern, listener: RebalanceListener1[IO])   = delegate.subscribe(pattern, listener)
      def subscribe(pattern: Pattern)                                     = delegate.subscribe(pattern)
      def subscription                                                    = delegate.subscription
      def unsubscribe                                                     = delegate.unsubscribe
      def commit                                                          = delegate.commit
      def commit(timeout: FiniteDuration)                                 = delegate.commit(timeout)
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) = delegate.commit(offsets)
      def commit(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata], timeout: FiniteDuration) =
        delegate.commit(offsets, timeout)
      def commitLater                                                          = delegate.commitLater
      def commitLater(offsets: NonEmptyMap[TopicPartition, OffsetAndMetadata]) = delegate.commitLater(offsets)
      def seek(partition: TopicPartition, offset: Offset)                      = delegate.seek(partition, offset)
      def seek(partition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
        delegate.seek(partition, offsetAndMetadata)
      def seekToBeginning(partitions: NonEmptySet[TopicPartition])     = delegate.seekToBeginning(partitions)
      def seekToEnd(partitions: NonEmptySet[TopicPartition])           = delegate.seekToEnd(partitions)
      def position(partition: TopicPartition, timeout: FiniteDuration) = positionRef.get.map(Offset.unsafe(_))
      def committed(partitions: NonEmptySet[TopicPartition])           = delegate.committed(partitions)
      def committed(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration) =
        delegate.committed(partitions, timeout)
      def clientInstanceId(timeout: FiniteDuration)         = delegate.clientInstanceId(timeout)
      def clientMetrics                                     = delegate.clientMetrics
      def partitions(topic: Topic)                          = delegate.partitions(topic)
      def partitions(topic: Topic, timeout: FiniteDuration) = delegate.partitions(topic, timeout)
      def topics                                            = delegate.topics
      def topics(timeout: FiniteDuration)                   = delegate.topics(timeout)
      def paused                                            = delegate.paused
      def pause(partitions: NonEmptySet[TopicPartition])    = delegate.pause(partitions)
      def resume(partitions: NonEmptySet[TopicPartition])   = delegate.resume(partitions)
      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset]) =
        delegate.offsetsForTimes(timestampsToSearch)
      def offsetsForTimes(timestampsToSearch: Map[TopicPartition, Offset], timeout: FiniteDuration) =
        delegate.offsetsForTimes(timestampsToSearch, timeout)
      def beginningOffsets(partitions: NonEmptySet[TopicPartition]) = delegate.beginningOffsets(partitions)
      def beginningOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration) =
        delegate.beginningOffsets(partitions, timeout)
      def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration) = endOffsets(partitions)
      def currentLag(partition: TopicPartition)                                        = delegate.currentLag(partition)
      def groupMetadata                                                                = delegate.groupMetadata
      def enforceRebalance                                                             = delegate.enforceRebalance
      def wakeup                                                                       = delegate.wakeup
    }
}
