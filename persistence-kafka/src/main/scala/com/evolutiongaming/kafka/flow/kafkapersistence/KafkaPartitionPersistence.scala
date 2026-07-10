package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.implicits.*
import cats.{FlatMap, MonadThrow, data}
import com.evolutiongaming.catshelper.{BracketThrowable, Log}
import com.evolutiongaming.kafka.flow.kafka.Codecs.*
import com.evolutiongaming.skafka.*
import com.evolutiongaming.skafka.consumer.AutoOffsetReset.Earliest
import com.evolutiongaming.skafka.consumer.{
  Consumer => SkafkaConsumer,
  ConsumerConfig,
  ConsumerOf,
  ConsumerRecord,
  WithSize
}
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object KafkaPartitionPersistence {

  private case class MissingOffsetError(topicPartition: TopicPartition) extends NoStackTrace

  /** The recovery read made no progress for far longer than any transient hiccup explains, so waiting cannot fix it:
    * every record below the target is decided and fetchable (the target is the read's own end offset; with the stable
    * per-partition `transactional.id` no open transaction can pin the position below it), so the likely cause is a log
    * end that regressed below the captured target - log truncation after an unclean leader election, i.e. the cluster
    * lost acknowledged snapshot records. Failing loudly beats hanging: recovery runs on the poll thread inside the
    * rebalance callback, so a hang does not crash anything - `max.poll.interval.ms` silently evicts the member from the
    * group while the thread stays stuck, invisible to process-level health checks.
    */
  final case class RecoveryReadStalledError(
    topicPartition: TopicPartition,
    position: Offset,
    targetOffset: Offset,
  ) extends RuntimeException(
        s"recovery read of $topicPartition made no progress at offset $position, short of target $targetOffset, " +
          "far beyond any transient broker hiccup; the log end has likely regressed below the captured target " +
          "(log truncation after an unclean leader election) - failing rather than hanging or silently recovering " +
          "possibly incomplete state"
      )
      with NoStackTrace

  // each stalled poll takes at least the 10ms poll timeout, so this many consecutive no-progress polls is at
  // least ~2 minutes - deliberately below the 5-minute max.poll.interval.ms default, so a stuck recovery fails
  // loudly and leaves the group cleanly BEFORE the broker evicts the member around its stuck poll thread. A
  // slow-but-progressing read never comes near it: the count resets on every advance. The cost: a broker
  // outage longer than this turns recovery into a visible, self-healing crash-restart loop instead of an
  // in-place wait - during an outage, visibility is the better default
  private[kafkapersistence] val maxStalledPolls = 12000L

  // ~5s of empty 10ms polls between "no progress" log lines: a single empty poll is normal at this poll
  // rate, a run of them is worth a line so a stuck recovery is visible long before the tripwire
  private val logEveryStalledPolls = 500L

  private[kafkapersistence] def readPartition[F[_]: MonadThrow: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset
  ): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      FlatMap[F]
        .tailRecM[(BytesByKey, Option[Offset], Long), BytesByKey]((BytesByKey.empty, none, 0L)) {
          case (acc, lastPosition, stalledPolls) =>
            consumer
              .position(snapshotPartition)
              .flatMap {
                case offset if offset >= targetOffset =>
                  acc.asRight[(BytesByKey, Option[Offset], Long)].pure[F]
                case offset =>
                  val stalled = if (lastPosition.contains(offset)) stalledPolls + 1 else 0L
                  val logStalled = Log[F]
                    .info(s"Snapshot topic read making no progress at offset $offset, target $targetOffset")
                    .whenA(stalled > 0 && stalled % logEveryStalledPolls == 0)
                  // waiting cannot fix a stall this long - fail loudly (see the error's scaladoc)
                  val failStalled = RecoveryReadStalledError(snapshotPartition, offset, targetOffset)
                    .raiseError[F, Unit]
                    .whenA(stalled >= maxStalledPolls)
                  failStalled *> logStalled *>
                    consumer
                      .poll(10.millis) // TODO: make poll timeout configurable
                      .map { records =>
                        val read = records.values.values.flatMap(_.toIterable).foldLeft(acc)(processRecord)
                        (read, offset.some, stalled).asLeft
                      }
              }
        }

  private[kafkapersistence] def processRecord(
    map: BytesByKey,
    record: ConsumerRecord[String, ByteVector]
  ): BytesByKey = record match {
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), Some(WithSize(value, _)), _) => map + (key -> value)
    case ConsumerRecord(_, _, _, Some(WithSize(key, _)), None, _)                     => map - key
    case _ => map // ignore records with no key for now
  }

  private[kafkapersistence] def readSnapshots[F[_]: BracketThrowable: Log](
    consumerOf: ConsumerOf[F],
    consumerConfig: ConsumerConfig,
    snapshotTopic: Topic,
    partition: Partition,
  )(implicit fromBytes: FromBytes[F, String]): F[BytesByKey] = {
    val snapshotsPartition         = TopicPartition(topic = snapshotTopic, partition = partition)
    val snapshotPartitionSingleton = data.NonEmptySet.of(snapshotsPartition)

    consumerOf
      .apply[String, ByteVector](
        consumerConfig.copy(
          autoOffsetReset = Earliest,
          common = consumerConfig
            .common
            .copy(clientId = consumerConfig.common.clientId.map(cid => s"$cid-snapshot-$partition"))
        )
      )
      .use { consumer =>
        for {
          _ <- consumer.assign(snapshotPartitionSingleton)
          // The read target is this consumer's own end offset. Under read_committed that is the last stable
          // offset - the horizon below which every transaction is decided - which sits below the log end
          // exactly while a transaction is open. That cannot hide a committed snapshot here: the topic is
          // written by a single producer lineage (the stable per-partition transactional.id), and a producer
          // must initTransactions - aborting its predecessor's open transaction - before it can write, so no
          // committed record ever sits above an open transaction. What this bound does not defend against is
          // a foreign producer's transaction on the snapshot topic, a deployment the docs exclude (sharing a
          // snapshot topic mixes state on recovery regardless of any read bound). Contrast Kafka Streams,
          // which must bound its restore at the high watermark instead (KAFKA-10167): its per-process
          // transactional ids leave a crashed instance's transaction unabortable, pinning the last stable
          // offset below committed records until the broker times it out - the stable per-partition id is
          // what spares this read that wait. See docs/kafka-single-writer-design.md.
          targetOffset <- consumer.endOffsets(snapshotPartitionSingleton).flatMap { endOffsets =>
            BracketThrowable[F].fromOption(endOffsets.get(snapshotsPartition), MissingOffsetError(snapshotsPartition))
          }
          bytesByKey <- readPartition(
            consumer,
            snapshotsPartition,
            targetOffset
          )
          _ <- Log[F].info(
            s"Snapshot topic $snapshotTopic partition $partition read complete at offset $targetOffset, ${bytesByKey.size} keys read"
          )
        } yield bytesByKey
      }
  }
}
