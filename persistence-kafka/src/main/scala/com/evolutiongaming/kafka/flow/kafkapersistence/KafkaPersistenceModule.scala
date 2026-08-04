package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Parallel
import cats.effect.{Async, Clock, Concurrent, Resource}
import cats.syntax.all.*
import com.evolution.scache.Cache
import com.evolutiongaming.catshelper.{FromTry, Log, LogOf, Runtime}
import com.evolutiongaming.kafka.flow.kafka.ScheduleCommit
import com.evolutiongaming.kafka.flow.key.{Keys, KeysOf}
import com.evolutiongaming.kafka.flow.metrics.syntax.*
import com.evolutiongaming.kafka.flow.persistence.{PersistenceOf, SnapshotPersistenceOf}
import com.evolutiongaming.kafka.flow.snapshot.{SnapshotDatabase, SnapshotWriteDatabase, SnapshotsOf}
import com.evolutiongaming.kafka.flow.{FlowMetrics, KafkaKey, PartitionAssignment}
import com.evolutiongaming.skafka.consumer.{ConsumerConfig, ConsumerOf, IsolationLevel}
import com.evolutiongaming.skafka.producer.{Producer, ProducerConfig, ProducerOf}
import com.evolutiongaming.skafka.{FromBytes, Partition, ToBytes, Topic, TopicPartition}
import com.evolutiongaming.sstream.Stream
import scodec.bits.ByteVector
import com.evolutiongaming.skafka.consumer.ConsumerRecord

import scala.concurrent.duration.*

/** A module, necessary to create a Kafka snapshot persistence.
  */
trait KafkaPersistenceModule[F[_], S] {
  def keysOf: KeysOf[F, KafkaKey]
  def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]]

  /** A `ScheduleCommit` that overrides the default way input offsets are committed (by the consumer), binding them to
    * the snapshot writes for single-writer offset safety. `None` means the module does not override it and offsets are
    * committed the default way. See `KafkaPersistenceModule.cachingTransactional`.
    */
  def scheduleCommit: Option[ScheduleCommit[F]]
}

object KafkaPersistenceModule {

  /** Settings for [[cachingTransactional]].
    *
    * @param consumerConfig
    *   config for the snapshot-reading consumers; recovery forces `read_committed` and clears `groupId` and
    *   `autoCommit` (the ephemeral readers never join a group or commit offsets) regardless of this value, and suffixes
    *   `clientId` per consumer it opens: `-snapshot-<input partition>` for the recovery read, `-snapshot-<input
    *   partition>-hw` for the high-watermark capture, and `-snapshot-<input partition>-src` for the partition-count
    *   check a declaring `partitionMapper` triggers. All three matter to client-id-scoped ACLs and log filters
    * @param producerConfig
    *   base config for the snapshot producer; `transactionalId` and `idempotence` are overridden per producer and
    *   `clientId` is suffixed with `-snapshot-<input partition>`
    * @param transactionalIdPrefix
    *   prefix for `transactional.id` (suffixed `-snapshot-<input partition>`; stable per partition). It does not affect
    *   fencing of stale writers (that is by consumer generation) - it is a readable label and, on an ACL-secured
    *   cluster, the `transactional.id` prefix the producer principal must be authorized for. Because the id is stable
    *   per partition, the prefix must be unique per flow: use your `applicationId`, and an application running several
    *   flows must append a per-flow discriminator (e.g. the input topic) or the flows share ids and fence each other's
    *   producers - an `"<applicationId>*"` prefixed ACL still covers it.
    * @param snapshotTopic
    *   snapshot topic name (should be configured as a 'compacted' topic) to read/write snapshots
    * @param maxWritesPerTransaction
    *   upper bound of snapshot writes group committed in one per-partition, serialized transaction, see
    *   `KafkaSnapshotWriteDatabase.transactional`
    * @param recoveryStallTimeout
    *   how long a snapshot recovery read may make no progress before it fails with `RecoveryReadStalledError` instead
    *   of hanging. Keep it below the driving consumer's `max.poll.interval.ms` - including one diagnostic re-read on
    *   failure, bounded by the snapshot consumer's `default.api.timeout.ms` - and above the open-transaction wait,
    *   which the *pinning* producer's `transaction.timeout.ms` bounds (plus the broker's abort scan): your own
    *   `producerConfig.transactionTimeout` for a co-owner, but not for a producer outside this flow. Under identity
    *   normally nothing to wait for - a takeover aborts its predecessor's transaction. A many-to-one `partitionMapper`
    *   adds co-owners of the shared state partition, whose live transactions no takeover reaches.
    */
  final case class TransactionalConfig(
    consumerConfig: ConsumerConfig,
    producerConfig: ProducerConfig,
    transactionalIdPrefix: String,
    snapshotTopic: Topic,
    maxWritesPerTransaction: Int         = TransactionalConfig.DefaultMaxWritesPerTransaction,
    recoveryStallTimeout: FiniteDuration = TransactionalConfig.DefaultRecoveryStallTimeout,
  )

  object TransactionalConfig {

    /** Default for [[TransactionalConfig.maxWritesPerTransaction]] - group-committed snapshot writes per transaction,
      * see `KafkaSnapshotWriteDatabase.transactional`.
      */
    private[kafkapersistence] val DefaultMaxWritesPerTransaction: Int = 256

    /** Default for [[TransactionalConfig.recoveryStallTimeout]] - the recovery read's no-progress deadline; 3 minutes
      * clears both configuration bounds at Kafka defaults.
      */
    private[kafkapersistence] val DefaultRecoveryStallTimeout: FiniteDuration = 3.minutes
  }

  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopicPartition: TopicPartition
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] =
    caching(consumerOf, producer, consumerConfig, snapshotTopicPartition, FlowMetrics.empty[F])

  /** Creates an instance of [[KafkaPersistenceModule]] for state recovery from a specific partition of a snapshot Kafka
    * 'compacted' ([[https://kafka.apache.org/documentation/#compaction official documentation]]) topic. The exposed
    * `keysOf` and `persistenceOf` implementations will perform cached reading of all the snapshot data in that
    * partition to the end without committing offsets. This implementation is to be used only with `eagerRecovery`
    * strategy since it relies on the order of state recovery actions during partition assignment. See the
    * implementation details below.
    *
    * Implementation details:
    *
    * During state recovery kafka-flow internally first fetches the list of all keys, recovering the state for each key
    * separately afterwards. To avoid redundant reads from a snapshot topic, a per-partition cache of ''String (key) ->
    * ByteVector (value)'' is created. This cache only serves the purpose of avoiding duplicate reading from a snapshot
    * topic during initialization process.
    *
    * It is populated during fetching of all keys when KeysOf.all is called internally (which effectively reads all the
    * data from the snapshot topic). Later, when a state is being recovered for a specific key, the value is obtained
    * from the cache and then removed from it. Removing a value for a specific key from a cache is safe at that point
    * since state recovery is performed only once - either during initialization when a partition is assigned (and there
    * is a snapshot for a key) or when the journal record is first seen (no snapshot for a key previously).
    *
    * @param consumerOf
    *   Kafka consumer factory to create snapshot reading consumers
    * @param producer
    *   Kafka producer for saving snapshots
    * @param consumerConfig
    *   Kafka consumer config for snapshot reading consumers
    * @param snapshotTopicPartition
    *   the snapshot topic, paired with the INPUT partition being recovered - not the state partition, which
    *   `partitionMapper` derives from it (under the identity default the two are the same number)
    * @param metrics
    *   instance of `FlowMetrics` to customize metrics of internally created snapshot database
    * @see
    *   com.evolutiongaming.kafka.flow.PartitionFlow.of for implementations details of keys fetching and state recovery
    *   for a partition
    * @see
    *   com.evolutiongaming.kafka.flow.KeyStateOf.eagerRecovery for implementation details of constructing
    *   com.evolutiongaming.kafka.flow.KeyState for a specific key
    * @see
    *   com.evolutiongaming.kafka.flow.KeyFlow.of for implementation details of state recovery for a specific key
    */
  def caching[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producer: Producer[F],
    consumerConfig: ConsumerConfig,
    snapshotTopicPartition: TopicPartition,
    metrics: FlowMetrics[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift
    of(
      consumerOf      = consumerOf,
      consumerConfig  = consumerConfig,
      snapshotTopic   = snapshotTopicPartition.topic,
      inputPartition  = snapshotTopicPartition.partition,
      metrics         = metrics,
      partitionMapper = partitionMapper,
      writeDatabase   = KafkaSnapshotWriteDatabase.of[F, S](snapshotTopicPartition, producer, partitionMapper),
      stall           = none, // transactional only
    ).map {
      // non-transactional: offsets are committed the default way (by the consumer), see package.scala
      case (keysOf, persistenceOf) => module(keysOf, persistenceOf, commit = None)
    }
  }

  /** EXPERIMENTAL - use at your own risk: design-verified but not yet proven in production operation, and unknown
    * defects may remain. No compatibility guarantee: configuration, API, and behavior may change in any release,
    * without deprecation.
    *
    * Variant of `caching` protecting the snapshot topic from stale writers by binding the input-offset commit into the
    * snapshot transaction. Each assigned partition gets a transactional producer with a stable per-partition
    * `transactional.id`, whose `initTransactions` aborts any transaction a crashed previous owner left open; snapshot
    * writes run in group-committed transactions (see `KafkaSnapshotWriteDatabase.transactional`) that also commit the
    * input offset. A stale consumer generation is rejected by the broker (KIP-447), aborting the transaction, so a
    * stale owner can neither advance offsets nor overwrite a newer snapshot. Recovery reads with `read_committed`,
    * bounded by the high watermark so an open transaction the takeover does not reach is waited out; output stays
    * at-least-once. See the "Protecting against stale snapshot writes" persistence docs for guarantees, limitations,
    * costs and rollout, and `docs/kafka-single-writer-design.md` for the mechanism.
    *
    * The `assignment` must describe the input partition of the SAME consumer that drives this flow (its `groupMetadata`
    * generation is what fences a stale owner); `assignedAt` seeds the offset-to-commit so even the first write is
    * generation-gated, and the snapshot partition is derived from the input partition number through `partitionMapper`
    * (with the identity default they are equal).
    *
    * @param partitionMapper
    *   maps the input partition to the state partition snapshots are written to and recovered from, as in `caching`.
    *   The `transactional.id` and the offset commit stay keyed to the input partition, so a non-identity mapping makes
    *   safety conditional on `isStateKeyOwned` claiming every key for the input partition whose events build it - read
    *   [[KafkaPersistencePartitionMapper]] before writing one. The mapping is part of the snapshot topic's layout, not
    *   a setting: changing it over existing snapshots needs a fresh topic and an input replay, never a rolling swap -
    *   see the persistence docs' limitations.
    */
  def cachingTransactional[F[_]: LogOf: Async: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    producerOf: ProducerOf[F],
    config: TransactionalConfig,
    assignment: PartitionAssignment[F],
    metrics: FlowMetrics[F]                          = FlowMetrics.empty[F],
    partitionMapper: KafkaPersistencePartitionMapper = KafkaPersistencePartitionMapper.identity,
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
    toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaPersistenceModule[F, S]] = {
    for {
      log <- Resource.eval(LogOf[F].apply(KafkaPersistenceModule.getClass))
      // a declaring partitionMapper is checked against the live input topic by the KafkaPersistenceModuleOf
      // factories, not here: this constructor has the input topic but its `caching` sibling does not, and one
      // rule that holds for both beats two that differ
      transactional <- transactionalWriteDatabase[F, S](producerOf, config, assignment, partitionMapper, log)
      // records of aborted transactions (e.g. of a fenced previous owner) must not be recovered as snapshots
      parts <- of(
        consumerOf      = consumerOf,
        consumerConfig  = config.consumerConfig.copy(isolationLevel = IsolationLevel.ReadCommitted),
        snapshotTopic   = config.snapshotTopic,
        inputPartition  = assignment.topicPartition.partition,
        metrics         = metrics,
        partitionMapper = partitionMapper,
        writeDatabase   = transactional.writeDatabase,
        stall           = KafkaPartitionPersistence.Stall(config.recoveryStallTimeout, Clock[F].monotonic).some,
      )
    } yield {
      val (keysOf, persistenceOf) = parts
      // transactional mode binds the input-offset commit into the snapshot transaction (see package.scala)
      module(keysOf, persistenceOf, transactional.scheduleCommit.some)
    }
  }

  /** Builds the partition's transactional producer (stable per-partition `transactional.id`) and the group-committing
    * write database that binds the input-offset commit into each snapshot transaction. See [[cachingTransactional]].
    */
  private def transactionalWriteDatabase[F[_]: Async, S](
    producerOf: ProducerOf[F],
    config: TransactionalConfig,
    assignment: PartitionAssignment[F],
    partitionMapper: KafkaPersistencePartitionMapper,
    log: Log[F],
  )(
    implicit toBytesState: ToBytes[F, S]
  ): Resource[F, KafkaSnapshotWriteDatabase.Transactional[F, S]] = {
    implicit val fromTry: FromTry[F] = FromTry.lift
    import config.{maxWritesPerTransaction, producerConfig, snapshotTopic, transactionalIdPrefix}
    import assignment.{assignedAt, groupMetadata, topicPartition as inputTopicPartition}

    // a state-partition-keyed id would be shared by live owners of different input partitions, fencing each
    // other on every initTransactions
    val inputPartition  = inputTopicPartition.partition
    val transactionalId = s"$transactionalIdPrefix-snapshot-${inputPartition.value}"
    val transactionalProducerConfig = producerConfig.copy(
      transactionalId = transactionalId.some,
      idempotence     = true,
      common = producerConfig
        .common
        .copy(clientId = producerConfig.common.clientId.map(cid => s"$cid-snapshot-${inputPartition.value}"))
    )

    for {
      producer <- producerOf(transactionalProducerConfig)
      // required before KafkaSnapshotWriteDatabase.transactional (below) can open transactions; it also aborts any
      // transaction a crashed predecessor left open (no client-visible signal - the logged duration is the
      // abort's only trace)
      _ <- Resource.eval(Clock[F].timed(producer.initTransactions).flatMap {
        case (took, _) => log.info(s"transactional producer $transactionalId initialized in ${took.toMillis} ms")
      })
      transactional <- Resource.eval(
        KafkaSnapshotWriteDatabase.transactional[F, S](
          snapshotTopicPartition  = TopicPartition(snapshotTopic, inputPartition),
          producer                = producer,
          inputTopicPartition     = inputTopicPartition,
          groupMetadata           = groupMetadata,
          assignedOffset          = assignedAt,
          maxWritesPerTransaction = maxWritesPerTransaction,
          partitionMapper         = partitionMapper,
        )
      )
    } yield transactional
  }

  /** Builds the cached `keysOf` + `persistenceOf` for a partition; the module's `scheduleCommit` is attached by the
    * caller via [[module]] (transactional binds the offset, caching defers to the consumer).
    */
  private def of[F[_]: LogOf: Concurrent: Parallel: Runtime, S](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    inputPartition: Partition,
    metrics: FlowMetrics[F],
    partitionMapper: KafkaPersistencePartitionMapper,
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
    stall: Option[KafkaPartitionPersistence.Stall[F]],
  )(
    implicit fromBytesKey: FromBytes[F, String],
    fromBytesState: FromBytes[F, S],
  ): Resource[F, (KeysOf[F, KafkaKey], SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]])] =
    for {
      partitionDataCache <- Cache.loading[F, String, ByteVector]
      keysOf <- Resource.eval(
        makeKeysOf(
          partitionDataCache,
          consumerOf,
          consumerConfig,
          snapshotTopic,
          inputPartition,
          partitionMapper,
          stall,
        )
      )
      persistenceOf <- Resource.eval(
        makeSnapshotPersistenceOf(keysOf, partitionDataCache, snapshotTopic, metrics, writeDatabase)
      )
    } yield (keysOf, persistenceOf)

  private def module[F[_], S](
    keys: KeysOf[F, KafkaKey],
    persistence: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]],
    commit: Option[ScheduleCommit[F]],
  ): KafkaPersistenceModule[F, S] =
    new KafkaPersistenceModule[F, S] {
      override def keysOf: KeysOf[F, KafkaKey] = keys

      override def persistenceOf: SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]] =
        persistence

      override def scheduleCommit: Option[ScheduleCommit[F]] = commit
    }

  private def makeKeysOf[F[_]: LogOf: Concurrent](
    cache: Cache[F, String, ByteVector],
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    inputPartition: Partition,
    partitionMapper: KafkaPersistencePartitionMapper,
    stall: Option[KafkaPartitionPersistence.Stall[F]],
  )(implicit fromBytesKey: FromBytes[F, String]): F[KeysOf[F, KafkaKey]] =
    LogOf[F].apply(classOf[KeysOf[F, KafkaKey]]).map { implicit log =>
      val statePartition = partitionMapper.getStatePartition(inputPartition)
      val reader =
        KafkaPartitionPersistence.SnapshotReader(TopicPartition(snapshotTopic, statePartition), inputPartition)

      def readPartitionData: F[BytesByKey] =
        KafkaPartitionPersistence
          .readSnapshots[F](
            consumerOf     = consumerOf,
            consumerConfig = consumerConfig,
            reader         = reader,
            stall          = stall,
          )
          .flatMap { snapshots =>
            val owned = snapshots.view.filterKeys(key => partitionMapper.isStateKeyOwned(key, inputPartition)).toMap
            val ratio = s"$reader owns ${owned.size} of ${snapshots.size} keys read"
            // weak signal: a merely-wrong mapper keeps a plausible share, so only owning none of a non-empty partition
            // is loud enough to warn - still not proof, and unreachable under identity. See the persistence docs
            if (owned.isEmpty && snapshots.nonEmpty)
              log
                .warn(s"$ratio - no key it holds is claimed by this input partition; recovering all state as empty")
                .as(owned)
            else log.info(ratio).as(owned)
          }

      new KeysOf[F, KafkaKey] {
        def apply(key: KafkaKey): Keys[F] =
          Keys.empty[F]

        def all(applicationId: String, groupId: String, topicPartition: TopicPartition): Stream[F, KafkaKey] = {
          Stream.fromF {
            readPartitionData
              .map(_.map { case (key, value) => KafkaKey(applicationId, groupId, topicPartition, key) -> value })
              .flatTap(_.toList.traverse_ { case (k, v) => cache.put(k.key, v) })
              .map(_.keys)
          }
        }
      }
    }

  private def makeSnapshotPersistenceOf[F[_]: LogOf: Concurrent, S](
    keysOf: KeysOf[F, KafkaKey],
    cache: Cache[F, String, ByteVector],
    snapshotTopic: Topic,
    metrics: FlowMetrics[F],
    writeDatabase: SnapshotWriteDatabase[F, KafkaKey, S],
  )(
    implicit fromBytesState: FromBytes[F, S]
  ): F[SnapshotPersistenceOf[F, KafkaKey, S, ConsumerRecord[String, ByteVector]]] =
    LogOf[F].apply(classOf[KafkaPersistenceModule[F, S]]).map { implicit log =>
      val read =
        KafkaSnapshotReadDatabase.of[F, S](snapshotTopic, getState = key => cache.remove(key).flatten)

      val snapshotDatabase = SnapshotDatabase(
        read  = read,
        write = writeDatabase
      ).withMetricsK(metrics.snapshotDatabaseMetrics)

      PersistenceOf.snapshotsOnly[F, KafkaKey, S, ConsumerRecord[String, ByteVector]](
        keysOf      = keysOf,
        snapshotsOf = SnapshotsOf.backedBy[F, KafkaKey, S](snapshotDatabase)
      )
    }
}
