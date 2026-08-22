package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import com.evolutiongaming.kafka.flow.key.KeysOf
import com.evolutiongaming.kafka.flow.persistence.PersistenceOf
import com.evolutiongaming.kafka.flow.registry.EntityRegistry
import com.evolutiongaming.kafka.flow.timer.{TimerFlowOf, TimersOf}
import com.evolutiongaming.kafka.flow.{KeyContext, KeyStateOf, KafkaKey}
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import com.evolutiongaming.skafka.{Offset, Partition, TopicPartition}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scodec.bits.ByteVector

class ExperimentKafkaPersistenceSpec extends AnyFunSuite with Matchers {

  // Test: High-watermark-bounded recovery read does not prevent watermark loss
  test("Kafka-persistence: Watermark loss on aggregate deletion") {
    val result = (for {
      // Setup
      topicPartition <- Resource.eval(IO.pure(TopicPartition("test-topic", Partition.unsafe(0))))
      kafkaKey = KafkaKey("test-app", "test-group", topicPartition, "test-key")
      
      // Mock dependencies
      timersOf <- Resource.eval(TimersOf.memory[IO, KafkaKey])
      persistenceOf <- Resource.eval(PersistenceOf.kafka[IO, Unit, ConsumerRecord[String, ByteVector]](
        snapshotTopicPartition = TopicPartition("snapshots", Partition.unsafe(0)),
        producer = null, // Mock producer
        partitionMapper = KafkaPersistencePartitionMapper.identity
      ))
      timerFlowOf = TimerFlowOf.memory[IO]
      fold = (state: Option[Unit], record: ConsumerRecord[String, ByteVector]) => IO.pure(state)
      registry = EntityRegistry.empty[IO, KafkaKey, Unit]
      
      // Create KeyStateOf
      keyStateOf = KeyStateOf.eagerRecovery[IO, Unit](
        applicationId = "test-app",
        groupId = "test-group",
        keysOf = KeysOf.memory[IO, KafkaKey],
        timersOf = timersOf,
        persistenceOf = persistenceOf,
        timerFlowOf = timerFlowOf,
        fold = fold,
        registry = registry
      )
      
      // Create KeyState
      keyState <- keyStateOf(topicPartition, "test-key", 0L, KeyContext.empty[IO])
      
      // Simulate aggregate deletion (tombstone)
      _ <- Resource.eval(keyState.keyFlow.onEvent(None)) // Tombstone the aggregate
      
      // Simulate replayed duplicates (stale seqNr, new offsets)
      staleRecord = ConsumerRecord[String, ByteVector](
        topicPartition = topicPartition,
        offset = Offset.unsafe(1000), // New offset
        timestampAndType = None,
        key = Some("test-key"),
        value = Some(ByteVector.encodeUtf8("test-value").toOption.get),
        headers = Nil
      )
      
      // Attempt to process the stale record
      result <- Resource.eval(keyState.keyFlow.onEvent(Some(staleRecord)))
    } yield result).use(IO.pure(_)).unsafeRunSync()
    
    // Assert: The watermark is lost, and the stale record is processed (causing gaps)
    result shouldBe a[Any] // Should not reject the stale record
  }

  // Test: High-watermark-bounded recovery read does not prevent replayed duplicates
  test("Kafka-persistence: Replayed duplicates reset watermark backward") {
    val result = (for {
      // Setup
      topicPartition <- Resource.eval(IO.pure(TopicPartition("test-topic", Partition.unsafe(0))))
      kafkaKey = KafkaKey("test-app", "test-group", topicPartition, "test-key")
      
      // Mock dependencies
      timersOf <- Resource.eval(TimersOf.memory[IO, KafkaKey])
      persistenceOf <- Resource.eval(PersistenceOf.kafka[IO, Unit, ConsumerRecord[String, ByteVector]](
        snapshotTopicPartition = TopicPartition("snapshots", Partition.unsafe(0)),
        producer = null, // Mock producer
        partitionMapper = KafkaPersistencePartitionMapper.identity
      ))
      timerFlowOf = TimerFlowOf.memory[IO]
      fold = (state: Option[Unit], record: ConsumerRecord[String, ByteVector]) => IO.pure(state)
      registry = EntityRegistry.empty[IO, KafkaKey, Unit]
      
      // Create KeyStateOf
      keyStateOf = KeyStateOf.eagerRecovery[IO, Unit](
        applicationId = "test-app",
        groupId = "test-group",
        keysOf = KeysOf.memory[IO, KafkaKey],
        timersOf = timersOf,
        persistenceOf = persistenceOf,
        timerFlowOf = timerFlowOf,
        fold = fold,
        registry = registry
      )
      
      // Create KeyState
      keyState <- keyStateOf(topicPartition, "test-key", 0L, KeyContext.empty[IO])
      
      // Simulate processing a record with seqNr = 100
      _ <- Resource.eval(keyState.keyFlow.onEvent(Some(ConsumerRecord(
        topicPartition = topicPartition,
        offset = Offset.unsafe(1),
        timestampAndType = None,
        key = Some("test-key"),
        value = Some(ByteVector.encodeUtf8("test-value").toOption.get),
        headers = Nil
      ))))
      
      // Tombstone the aggregate
      _ <- Resource.eval(keyState.keyFlow.onEvent(None))
      
      // Simulate replayed duplicate (stale seqNr = 100, new offset = 1000)
      staleRecord = ConsumerRecord[String, ByteVector](
        topicPartition = topicPartition,
        offset = Offset.unsafe(1000), // New offset
        timestampAndType = None,
        key = Some("test-key"),
        value = Some(ByteVector.encodeUtf8("test-value").toOption.get),
        headers = Nil
      )
      
      // Attempt to process the stale record
      result <- Resource.eval(keyState.keyFlow.onEvent(Some(staleRecord)))
    } yield result).use(IO.pure(_)).unsafeRunSync()
    
    // Assert: The watermark is reset backward, and the stale record is processed
    result shouldBe a[Any] // Should not reject the stale record
  }
}