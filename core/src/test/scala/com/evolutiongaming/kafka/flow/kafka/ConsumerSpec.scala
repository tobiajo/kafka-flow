package com.evolutiongaming.kafka.flow.kafka

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.kafka.flow.ExplodingRebalanceConsumer
import com.evolutiongaming.skafka.consumer.{
  Consumer as SkafkaConsumer,
  ConsumerGroupMetadata,
  RebalanceCallback,
  RebalanceConsumer,
  RebalanceListener1
}
import munit.FunSuite
import scodec.bits.ByteVector

import java.util.regex.Pattern
import scala.concurrent.duration.*
import scala.util.{Success, Try}

class ConsumerSpec extends FunSuite {

  // the pre-join unknown: negative generation, no member identity (the shape the publish guard must drop)
  private val unknown =
    ConsumerGroupMetadata(groupId = "group", generationId = -1, memberId = "", groupInstanceId = none)

  private def joined(generationId: Int) =
    ConsumerGroupMetadata(groupId = "group", generationId = generationId, memberId = "member", groupInstanceId = none)

  test("groupMetadata is None until a poll observes a joined generation") {
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](unknown)
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying))
      initial    <- consumer.groupMetadata
      _          <- consumer.poll(1.millis)
      preJoin    <- consumer.groupMetadata
      _          <- underlying.set(joined(1))
      beforePoll <- consumer.groupMetadata
      _          <- consumer.poll(1.millis)
      afterPoll  <- consumer.groupMetadata
    } yield {
      assertEquals(initial, none[ConsumerGroupMetadata], "no metadata before any poll")
      assertEquals(preJoin, none[ConsumerGroupMetadata], "the pre-join unknown generation must not be published")
      assertEquals(beforePoll, none[ConsumerGroupMetadata], "the value is read after a poll, not live")
      assertEquals(afterPoll, joined(1).some, "the poll after the join publishes the joined generation")
    }
    test.unsafeRunSync()
  }

  test("the refresh follows a generation bump that fires no callback") {
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](joined(1))
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying))
      _          <- consumer.poll(1.millis)
      // the silent bump: the generation advances with no rebalance callback anywhere in sight
      _     <- underlying.set(joined(2))
      _     <- consumer.poll(1.millis)
      after <- consumer.groupMetadata
    } yield assertEquals(after, joined(2).some, "the post-poll read must observe the bump")
    test.unsafeRunSync()
  }

  test("a negative generation is dropped, keeping the last joined one") {
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](joined(3))
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying))
      _          <- consumer.poll(1.millis)
      _          <- underlying.set(unknown)
      _          <- consumer.poll(1.millis)
      after      <- consumer.groupMetadata
    } yield assertEquals(after, joined(3).some, "a negative generation must not overwrite the last joined one")
    test.unsafeRunSync()
  }

  test("the refresh reads after the poll, not before: a join completing inside poll is visible right after it") {
    // pins the `poll <* refresh` order. The underlying metadata advances INSIDE poll (as a completing join
    // does, on the poll thread) and must be visible immediately after that same poll - a pre-poll read
    // (`refresh *> poll`) would still see the pre-join state and, on the first join, leave None published
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](unknown)
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying, onPoll = underlying.set(joined(7))))
      after      <- consumer.poll(1.millis) *> consumer.groupMetadata
    } yield assertEquals(after, joined(7).some, "the read must follow the poll that completed the join")
    test.unsafeRunSync()
  }

  test("generation 0 is published: the guard is a range check (>= 0), not a -1 test") {
    // 0 is never a live classic generation, but it is inside the guarded range: the coordinator's
    // compatibility skip fires only on negatives, so 0 reaches real validation and only self-fences
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](joined(0))
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying))
      _          <- consumer.poll(1.millis)
      after      <- consumer.groupMetadata
    } yield assertEquals(after, joined(0).some)
    test.unsafeRunSync()
  }

  test("the revoke callback publishes the generation the client has already moved to") {
    // `onJoinComplete` bumps the client's group metadata BEFORE invoking the revoke callback; without the read the
    // revoke-time commit would bind generation 1 while the coordinator is already on 2, and be fenced every time
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](joined(1))
      captured   <- Ref.of[IO, Option[RebalanceListener1[IO]]](none)
      seenByFlow <- Ref.of[IO, Option[ConsumerGroupMetadata]](none)
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying, onSubscribe = l => captured.set(l.some)))
      // stands in for the flow's revoke path: whatever it reads is what the transaction binds (KIP-447)
      _        <- consumer.subscribe(topics, onRevoked(consumer.groupMetadata.flatMap(seenByFlow.set)))
      _        <- consumer.poll(1.millis)
      listener <- captured.get
      _        <- listener.traverse_(_.onPartitionsRevoked(partitions).toF(rebalanceConsumer(joined(2))))
      seen     <- seenByFlow.get
      after    <- consumer.groupMetadata
    } yield {
      assertEquals(seen, joined(2).some, "the revoke path must see the generation the client already holds")
      assertEquals(after, joined(2).some, "and it must stay published for the rest of the poll")
    }
    test.unsafeRunSync()
  }

  test("a failing read in the revoke callback leaves the previous generation and does not fail the revocation") {
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](joined(1))
      captured   <- Ref.of[IO, Option[RebalanceListener1[IO]]](none)
      revoked    <- Ref.of[IO, Boolean](false)
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying, onSubscribe = l => captured.set(l.some)))
      _          <- consumer.subscribe(topics, onRevoked(revoked.set(true)))
      _          <- consumer.poll(1.millis)
      listener   <- captured.get
      _          <- listener.traverse_(_.onPartitionsRevoked(partitions).toF(new ExplodingRebalanceConsumer))
      delegated  <- revoked.get
      after      <- consumer.groupMetadata
    } yield {
      assert(delegated, "the wrapped listener must still run")
      assertEquals(after, joined(1).some, "a failed read must not disturb what was published")
    }
    test.unsafeRunSync()
  }

  test("the lost callback publishes nothing") {
    // the client never rewrites its group metadata on the lost path; the stub's newer generation is one this
    // member does not hold, so it must not leak into the snapshot
    val test = for {
      underlying <- Ref.of[IO, ConsumerGroupMetadata](joined(1))
      captured   <- Ref.of[IO, Option[RebalanceListener1[IO]]](none)
      consumer   <- Consumer.of[IO](kafkaConsumer(underlying, onSubscribe = l => captured.set(l.some)))
      _          <- consumer.subscribe(topics, RebalanceListener1.empty[IO])
      _          <- consumer.poll(1.millis)
      listener   <- captured.get
      _          <- listener.traverse_(_.onPartitionsLost(partitions).toF(rebalanceConsumer(joined(2))))
      after      <- consumer.groupMetadata
    } yield assertEquals(after, joined(1).some)
    test.unsafeRunSync()
  }

  private val topics     = NonEmptySet.of("topic")
  private val partitions = NonEmptySet.of(TopicPartition("topic", Partition.min))

  private def onRevoked(f: IO[Unit]): RebalanceListener1[IO] =
    new RebalanceListener1[IO] {
      def onPartitionsAssigned(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
        RebalanceCallback.api[IO].empty
      def onPartitionsRevoked(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
        RebalanceCallback.lift(f)
      def onPartitionsLost(partitions: NonEmptySet[TopicPartition]): RebalanceCallback[IO, Unit] =
        RebalanceCallback.api[IO].empty
    }

  private def rebalanceConsumer(meta: ConsumerGroupMetadata): RebalanceConsumer =
    new Consumer.NoopRebalanceConsumer {
      override def groupMetadata(): Try[ConsumerGroupMetadata] = Success(meta)
    }

  // only poll, subscribe and groupMetadata matter to Consumer.of; everything else delegates to the empty consumer.
  // `onPoll` runs inside poll, to simulate state (a join) advancing on the poll itself
  private def kafkaConsumer(
    meta: Ref[IO, ConsumerGroupMetadata],
    onPoll: IO[Unit]                                = IO.unit,
    onSubscribe: RebalanceListener1[IO] => IO[Unit] = _ => IO.unit,
  ): SkafkaConsumer[IO, String, ByteVector] =
    new SkafkaConsumer[IO, String, ByteVector] {
      private val delegate = SkafkaConsumer.empty[IO, String, ByteVector]

      def groupMetadata = meta.get

      def assign(partitions: NonEmptySet[TopicPartition]) = delegate.assign(partitions)
      def assignment                                      = delegate.assignment
      def subscribe(topics: NonEmptySet[Topic], listener: RebalanceListener1[IO]) =
        onSubscribe(listener) *> delegate.subscribe(topics, listener)
      def subscribe(topics: NonEmptySet[Topic])                           = delegate.subscribe(topics)
      def subscribe(pattern: Pattern, listener: RebalanceListener1[IO])   = delegate.subscribe(pattern, listener)
      def subscribe(pattern: Pattern)                                     = delegate.subscribe(pattern)
      def subscription                                                    = delegate.subscription
      def unsubscribe                                                     = delegate.unsubscribe
      def poll(timeout: FiniteDuration)                                   = onPoll *> delegate.poll(timeout)
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
      def position(partition: TopicPartition)                          = delegate.position(partition)
      def position(partition: TopicPartition, timeout: FiniteDuration) = delegate.position(partition, timeout)
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
      def endOffsets(partitions: NonEmptySet[TopicPartition]) = delegate.endOffsets(partitions)
      def endOffsets(partitions: NonEmptySet[TopicPartition], timeout: FiniteDuration) =
        delegate.endOffsets(partitions, timeout)
      def currentLag(partition: TopicPartition) = delegate.currentLag(partition)
      def enforceRebalance                      = delegate.enforceRebalance
      def wakeup                                = delegate.wakeup
    }
}
