package com.evolutiongaming.kafka.flow.kafkapersistence

import com.evolutiongaming.skafka.Partition
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner

import java.nio.charset.StandardCharsets.UTF_8

/** Maps partitions of source Kafka topics into persistence topics.
  *
  * Please be careful when using this with `com.evolutiongaming.kafka.flow.RemapKey`. Only the identity mapper is
  * guaranteed to work properly with an arbitrary `RemapKey`, for other combinations you have to manually ensure that
  * the `isStateKeyOwned` implementation is correct and will not allow duplicate KeyFlows.
  *
  * If the aggregate key depends on the record's contents, then only the identity mapper can be used.
  */
trait KafkaPersistencePartitionMapper {

  /** Called after rebalance or initial partition assignment.
    * @param sourcePartition
    *   partition of the input stream, i.e. the kafka-journal topic.
    * @return
    *   partition of the persistence topic that has snapshots for aggregates built by events from the `sourcePartition`.
    */
  def getStatePartition(sourcePartition: Partition): Partition

  /** Checks if the aggregate in the state partition should be initialized as a
    * `com.evolutiongaming.kafka.flow.KeyFlow`.
    *
    * If the aggregate is initialized, it will have timers and ticks started. This is not desirable if the aggregate is
    * actually sourced from a different partition, which will also be started concurrently.
    * @param stateKey
    *   the aggregate's key.
    * @param sourcePartition
    *   partition of the input stream.
    * @return
    *   `true` if the aggregate is built from events in `sourcePartition`.
    */
  def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean
}

object KafkaPersistencePartitionMapper {
  def identity: KafkaPersistencePartitionMapper = Identity

  /** Reproduces the **Java** client's default partitioner - murmur2 over the UTF-8 key bytes - to decide ownership, so
    * it fits an input topic written by a Java or Scala producer with default partitioning. A librdkafka-based producer
    * (Python, Go, C#, node) instead defaults to `consistent_random`, i.e. CRC32, and would disagree on nearly every key
    * while still claiming a plausible-looking share, so ask which client produced the topic before using this.
    *
    * `sourcePartitions` must be the input topic's real partition count. Nothing validates it, and any other value
    * misplaces at least half of all keys: a misplaced key is credited to a partition other than the one whose events
    * build it, so its owner recovers nothing for it and rebuilds from empty state - and where the crediting partition
    * reads the same state partition, it recovers the snapshot into a second flow.
    *
    * The count is therefore a deployment invariant: expanding the input topic moves keys between partitions (a state
    * migration under any mapping, identity included) and `sourcePartitions` has to be raised in the same roll - a stale
    * number keeps recovering against the old partitioning.
    */
  def modulo(sourcePartitions: Int, statePartitions: Int): KafkaPersistencePartitionMapper =
    new Modulo(sourcePartitions, statePartitions)

  private object Identity extends KafkaPersistencePartitionMapper {
    override def getStatePartition(sourcePartition: Partition): Partition = sourcePartition

    override def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean = true
  }

  private class Modulo(sourcePartitions: Int, statePartitions: Int) extends KafkaPersistencePartitionMapper {
    override def getStatePartition(sourcePartition: Partition): Partition =
      Partition.unsafe(sourcePartition.value % statePartitions)

    // UTF-8: the bytes skafka's ToBytes[String] handed the producer to hash
    override def isStateKeyOwned(stateKey: String, sourcePartition: Partition): Boolean =
      BuiltInPartitioner.partitionForKey(stateKey.getBytes(UTF_8), sourcePartitions) == sourcePartition.value
  }
}
