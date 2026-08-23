# Kafka Floor-Based Safe Deletes

## Overview

This document describes the implementation of floor-based safe deletes for kafka-flow's Kafka backend, equivalent to Cassandra's compare-and-set + tombstone approach. This would eliminate the need for preserved `seenSeqNr` in SPA (DRGN-2219).

## Background

### Current Situation

- **Cassandra backend:** Uses offset compare-and-set + tombstone with offset for safe deletes
- **Kafka backend:** Uses transactional offset binding + generation fencing (KIP-447)
- **SPA (DRGN-2219):** Uses preserved `seenSeqNr` to prevent false gaps from MirrorMaker duplicates

### The Problem

Kafka's current implementation does NOT track per-snapshot offsets in the compacted topic. This means:
- ✅ Safe from stale writers (via generation fencing)
- ❌ Cannot use floor filter for duplicate detection
- ❌ Needs preserved `seenSeqNr` as workaround

### The Solution

Add offset tracking to Kafka snapshots, enabling floor-based duplicate detection.

## Implementation

### 1. Schema Change (Required)

The compacted Kafka topic currently stores:
```
Key: AggregateId
Value: Serialized[S]
```

Needs to store:
```
Key: AggregateId
Value: Serialized[KafkaSnapshot[S]] = Serialized[(Offset, S)]
```

**Migration strategy:**
1. Create new compacted topic with new schema
2. Backfill existing snapshots with `Offset.Min` or actual offsets
3. Update consumers to read from new topic
4. Decommission old topic

### 2. Read Database Changes

**File:** `KafkaSnapshotReadDatabase.scala`

**New method:**
```scala
def of[F[_]: Monad, S: FromBytes[F, *]](
  snapshotTopic: Topic,
  getState: String => F[Option[ByteVector]]
): SnapshotReadDatabase[F, KafkaKey, S] = {
  key =>
    for {
      state      <- getState(key.key)
      maybeState <- state.traverse(bytes => FromBytes[F, KafkaSnapshot[S]].apply(bytes.toArray, snapshotTopic))
    } yield maybeState.map(snapshot => Stored.Live(snapshot.value, Some(snapshot.offset)))
}
```

**Legacy method (unchanged):**
```scala
def ofLegacy[F[_]: Monad, S: FromBytes[F, *]](
  snapshotTopic: Topic,
  getState: String => F[Option[ByteVector]]
): SnapshotReadDatabase[F, KafkaKey, S] = {
  key =>
    for {
      state      <- getState(key.key)
      maybeState <- state.traverse(bytes => FromBytes[F, S].apply(bytes.toArray, snapshotTopic))
    } yield maybeState.map(value => Stored.Live(value, none))
}
```

### 3. Write Database Changes

**File:** `KafkaSnapshotWriteDatabase.scala`

**Modified `apply` method:**
```scala
private def apply[F[_], S](
  snapshotTopicPartition: TopicPartition,
  partitionMapper: KafkaPersistencePartitionMapper,
  send: ProducerRecord[String, S] => F[Unit],
): SnapshotWriteDatabase[F, KafkaKey, S] = new SnapshotWriteDatabase[F, KafkaKey, S] {
  override def write(key: KafkaKey, stored: Stored[S]): F[Unit] = {
    // For floor-based mode, we would write KafkaSnapshot with offset
    // For now, this remains unchanged (legacy mode)
    produce(key, stored.value)
  }
  
  private def produce(key: KafkaKey, snapshot: Option[S]): F[Unit] = {
    val targetPartition = partitionMapper.getStatePartition(key.topicPartition.partition)
    val record = new ProducerRecord(
      topic     = snapshotTopicPartition.topic,
      partition = targetPartition.some,
      key       = key.key.some,
      value     = snapshot
    )
    send(record)
  }
}
```

**Future enhancement:** Add `applyWithFloor` method that writes `KafkaSnapshot[S]` instead of `S`.

### 4. Serialization Instances

**Required:** `ToBytes[F, KafkaSnapshot[S]]` and `FromBytes[F, KafkaSnapshot[S]]` instances.

**Implementation:**
```scala
// In KafkaSnapshot companion object or separate file
implicit def kafkaSnapshotToBytes[F[_], S: ToBytes[F, *]]: ToBytes[F, KafkaSnapshot[S]] = ???
implicit def kafkaSnapshotFromBytes[F[_], S: FromBytes[F, *]]: FromBytes[F, KafkaSnapshot[S]] = ???
```

These would serialize/deserialize the `(Offset, S)` tuple.

### 5. Module Changes

**File:** `KafkaPersistenceModule.scala`

**New methods:**
```scala
// Non-transactional with floor support
def cachingWithFloor[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
  consumerOf: ConsumerOf[F],
  producer: Producer[F],
  consumerConfig: ConsumerConfig,
  snapshotTopicPartition: TopicPartition,
  metrics: FlowMetrics[F],
  partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
)(
  implicit fromBytesKey: FromBytes[F, String],
  fromBytesState: FromBytes[F, KafkaSnapshot[S]],
  toBytesState: ToBytes[F, KafkaSnapshot[S]]
): Resource[F, KafkaPersistenceModule[F, S]] = ???

// Transactional with floor support
def cachingTransactionalWithFloor[F[_]: LogOf: Async: Parallel: Runtime, S](
  consumerOf: ConsumerOf[F],
  producerOf: ProducerOf[F],
  config: TransactionalConfig,
  assignment: PartitionAssignment[F],
  metrics: FlowMetrics[F] = FlowMetrics.empty[F],
)(
  implicit fromBytesKey: FromBytes[F, String],
  fromBytesState: FromBytes[F, KafkaSnapshot[S]],
  toBytesState: ToBytes[F, KafkaSnapshot[S]]
): Resource[F, KafkaPersistenceModule[F, S]] = ???
```

### 6. SPA Integration

**File:** `JournalFoldOption.scala` (in slots-provider-api)

**Changes:**
```scala
case class GameRoundState(
  // existing fields...
  floor: SeqNr  // NEW: floor from tombstone offset
)

def hasGap(event: GameRoundEventWithMetadata): Boolean = {
  if (event.seqNr <= state.floor) false  // Drop duplicate below floor
  else if (event.seqNr > state.seenSeqNr + 1) true  // Real gap
  else false  // Expected
}

// On delete: set floor to current seenSeqNr
def onDelete: GameRoundState = this.copy(floor = seenSeqNr)
```

## Current Status

### Completed
- ✅ Added `of` and `ofLegacy` methods to `KafkaSnapshotReadDatabase`
- ✅ Updated comments to reflect floor-based approach
- ✅ Branch `tj/kafka-floor-safe-deletes` created

### Remaining Work
- ❌ Implement `ToBytes[F, KafkaSnapshot[S]]` and `FromBytes[F, KafkaSnapshot[S]]`
- ❌ Add `applyWithFloor` method to `KafkaSnapshotWriteDatabase`
- ❌ Complete `cachingWithFloor` and `cachingTransactionalWithFloor` methods
- ❌ Schema migration for compacted topics
- ❌ SPA integration (remove preserved `seenSeqNr`)
- ❌ Tests

## Benefits

1. **Eliminates preserved `seenSeqNr`:** Floor filter handles duplicates automatically
2. **More robust:** Handles all duplicate scenarios, not just MirrorMaker
3. **Consistent with Cassandra:** Same abstraction across both backends
4. **Formally verified:** kafka-flow TLA+ models already prove this works

## Testing

### Unit Tests
- Test `KafkaSnapshotReadDatabase.of` with offset-carrying snapshots
- Test `KafkaSnapshotWriteDatabase.applyWithFloor` writes correct format
- Test floor filter in `SnapshotFold`

### Integration Tests
- Test with actual Kafka cluster
- Verify schema migration
- Verify backward compatibility

### TLA+ Verification
- Run existing kafka-flow models (already prove floor works)
- Add specific tests for floor-based safe deletes

## Migration Path

1. **Phase 1:** Implement serialization instances and write database changes
2. **Phase 2:** Create new compacted topics with new schema
3. **Phase 3:** Backfill existing data
4. **Phase 4:** Update consumers to use new format
5. **Phase 5:** Remove preserved `seenSeqNr` from SPA
6. **Phase 6:** Decommission old topics

## References

- Cassandra implementation: `tj/address-partition-ownership-overlap-possiblity-cassandra`
- kafka-flow models: `models/SingleWriterStore/`
- TLA+ proof: `SeenSeqNrPreservedComparison.tla`
- DRGN-2219: Preserved seenSeqNr investigation

## Files Modified

- `persistence-kafka/src/main/scala/.../KafkaSnapshotReadDatabase.scala`
- `persistence-kafka/src/main/scala/.../KafkaSnapshotWriteDatabase.scala`
- `persistence-kafka/src/main/scala/.../KafkaPersistenceModule.scala`

## Files to Create

- `core/src/main/scala/.../KafkaSnapshotSerialization.scala` (serialization instances)
- `docs/kafka-floor-safe-deletes.md` (this document)
