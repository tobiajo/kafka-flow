package com.evolutiongaming.kafka.flow.kafkapersistence

import com.evolutiongaming.skafka.Partition
import munit.FunSuite

class KafkaPersistencePartitionMapperSpec extends FunSuite {

  // The only test that pins ownership to the producer's own partitioning: the mapper tests elsewhere derive their
  // expected keys from `isStateKeyOwned`, so they follow any hash it adopts. These partitions are hardcoded, and
  // the keys are non-ASCII, so the bytes are pinned too - a bare `getBytes` reads the JVM's default charset instead,
  // UTF-8 only since JDK 18 (skafka's ToBytes[String] is UTF-8, and those are the bytes the producer hashed).
  // It records the intent more than it enforces it: where the default charset IS UTF-8, both spellings agree and
  // this cannot fail. It fails where the bug bites - a JVM whose default is a legacy charset.
  test("modulo ownership reproduces the producer's partitioning, over UTF-8 bytes") {
    val mapper = KafkaPersistencePartitionMapper.modulo(sourcePartitions = 2, statePartitions = 1)
    // "naïve": UTF-8 -> 1, ISO-8859-1 -> 0
    assert(mapper.isStateKeyOwned("naïve", Partition.unsafe(1)))
    // "åäö": UTF-8 -> 0, US-ASCII -> 1
    assert(mapper.isStateKeyOwned("åäö", Partition.min))
  }

  // where the snapshots land, which nothing else pins: every other mapper test maps onto a single state partition,
  // and `% 1` makes any wrong formula - a constant included - indistinguishable from the right one
  test("modulo routes a source partition to source % statePartitions") {
    val mapper = KafkaPersistencePartitionMapper.modulo(sourcePartitions = 6, statePartitions = 2)
    assertEquals(mapper.getStatePartition(Partition.min), Partition.min)
    assertEquals(mapper.getStatePartition(Partition.unsafe(3)), Partition.unsafe(1))
    assertEquals(mapper.getStatePartition(Partition.unsafe(4)), Partition.min)
  }
}
