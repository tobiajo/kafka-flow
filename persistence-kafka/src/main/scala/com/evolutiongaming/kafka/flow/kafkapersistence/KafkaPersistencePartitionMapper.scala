package com.evolutiongaming.kafka.flow.kafkapersistence

import com.evolutiongaming.skafka.Partition
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner

import java.nio.charset.StandardCharsets.UTF_8

/** Maps partitions of source Kafka topics into persistence topics.
  *
  * Please be careful when using this with `com.evolutiongaming.kafka.flow.RemapKey`. Only the identity mapper is
  * guaranteed to work properly with an arbitrary `RemapKey`, for other combinations you have to manually ensure that
  * the `isStateKeyOwned` implementation is correct: it must claim each key for the source partition whose events build
  * the aggregate, and for no other.
  *
  * If the aggregate key depends on the record's contents, then only the identity mapper can be used.
  *
  * A non-identity mapper is sound only where the input topic's partitioning is a deterministic function of the key. A
  * producer that spreads one key over several partitions leaves `isStateKeyOwned` nothing correct to answer - no single
  * partition builds the aggregate - and where those partitions share a state partition, their snapshots of that key
  * overwrite each other. Under identity the same input keeps them in separate state partitions, so it does not arise.
  */
trait KafkaPersistencePartitionMapper {

  /** Consulted on every snapshot write, and once per partition at recovery - so it must be pure, and stable across
    * restarts and releases. The mapping is part of the snapshot topic's layout: changing it over existing snapshots is
    * a state migration, never a config change - see `modulo` and the persistence docs.
    *
    * Routing is per partition, not per key: every key of `sourcePartition` is persisted in the one partition returned
    * here, so the state topic is never written on more partitions than the input topic has. The partition returned must
    * exist in the state topic - a number beyond its partition count surfaces only as a failed snapshot send.
    * @param sourcePartition
    *   partition of the input stream, i.e. the kafka-journal topic.
    * @return
    *   partition of the persistence topic that has snapshots for aggregates built by events from the `sourcePartition`.
    */
  def getStatePartition(sourcePartition: Partition): Partition

  /** Checks if the aggregate in the state partition should be initialized as a
    * `com.evolutiongaming.kafka.flow.KeyFlow`.
    *
    * Only recovery consults this - writes are not filtered. Of the source partitions sharing a state partition, at most
    * one may return `true` for a given key, the one whose events build the aggregate: a second one starts a duplicate
    * flow for that key, with its own timers and ticks. If none does, the persisted snapshot is not recovered.
    *
    * Which partition that is was decided by whatever produced the input topic, so this is not a free choice - an
    * implementation has to reproduce that assignment, as `modulo` reproduces the Java client's default partitioner.
    *
    * Two things an implementation that hashes the key has to get right, neither of which the signature can express. The
    * bytes: hash `stateKey.getBytes(UTF_8)`, because UTF-8 is what skafka's `ToBytes[String]` handed the producer, and
    * a bare `getBytes` reads the JVM's default charset - identical on a UTF-8 JVM, silently different elsewhere. And
    * the key: under `com.evolutiongaming.kafka.flow.RemapKey` this receives the *remapped* key, not the record's
    * original one, so ownership must be derivable from whatever the remap produces.
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

  /** Recovers input partition N from state partition N and claims every key it reads. It never recomputes where a key
    * belongs - state follows the partition the record was delivered on - so unlike `modulo` it holds whatever
    * partitioner the producer uses.
    */
  def identity: KafkaPersistencePartitionMapper = Identity

  /** Reproduces the **Java** client's default partitioner - murmur2 over the UTF-8 key bytes - to decide ownership, so
    * it fits an input topic written by a Java or Scala producer with default partitioning. A librdkafka-based producer
    * (Python, Go, C#, node) instead defaults to `consistent_random`, i.e. CRC32, and would disagree on nearly every key
    * while still claiming a plausible-looking share, so ask which client produced the topic before using this.
    *
    * `sourcePartitions` must be the input topic's real partition count: any other value misplaces at least half of all
    * keys - a misplaced key is credited to a partition other than the one whose events build it, so its owner recovers
    * nothing for it and rebuilds from empty state, and where the crediting partition reads the same state partition, it
    * recovers the snapshot into a second flow.
    *
    * The count is a deployment invariant: expanding the input topic moves keys between partitions (a state migration
    * under any mapping, identity included) and `sourcePartitions` has to be raised in the same roll - a stale number
    * keeps recovering against the old partitioning.
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
