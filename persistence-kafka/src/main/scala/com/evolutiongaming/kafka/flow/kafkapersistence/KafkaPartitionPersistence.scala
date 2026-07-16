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
  IsolationLevel,
  WithSize
}
import scodec.bits.ByteVector

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object KafkaPartitionPersistence {

  private case class MissingOffsetError(topicPartition: TopicPartition) extends NoStackTrace

  /** A snapshot recovery read made no progress until the stall deadline. `diagnosis` names the cause, re-read when it
    * fires: the log end regressed below the captured target (truncation - acknowledged records lost), or an open
    * transaction outlived the deadline. See the design doc's stalled-read section.
    */
  final case class RecoveryReadStalledError(
    topicPartition: TopicPartition,
    position: Offset,
    targetOffset: Offset,
    diagnosis: String,
  ) extends RuntimeException(
        s"recovery read of $topicPartition made no progress at offset $position, short of target $targetOffset, " +
          s"past the stall deadline - failing rather than hanging or silently recovering possibly incomplete state; $diagnosis"
      )
      with NoStackTrace

  /** Default no-progress deadline for the transactional recovery read. It must sit below `max.poll.interval.ms` (or the
    * broker evicts the stuck member first) and above the self-healing wait for an open transaction outside this
    * partition's `transactional.id` (its `transaction.timeout.ms` plus the broker's abort scan); 2 minutes clears the
    * defaults. Override via `TransactionalConfig.recoveryStallTimeout`; the non-transactional read passes none.
    */
  val defaultStallTimeout: FiniteDuration = 2.minutes

  /** How long past a producer's `transaction.timeout.ms` the broker may take to actually abort a hung transaction: the
    * abort scan runs every `transaction.abort.timed.out.transaction.cleanup.interval.ms`, 10 seconds by default.
    * Together they bound the self-healing recovery wait that a stall deadline must sit above.
    */
  private[kafkapersistence] val brokerAbortScanInterval: FiniteDuration = 10.seconds

  // at most one "no progress" log line per 5s while stalled, so a stuck recovery is visible long before the deadline
  private val logStallEvery: FiniteDuration = 5.seconds

  /** Arms the recovery-read stall deadline: a read making no progress for [[timeout]] fails, measured by elapsed wall
    * time read from [[monotonic]]. Present only in the transactional mode; the non-transactional read passes `None` and
    * waits as long as it takes.
    */
  private[kafkapersistence] final case class Stall[F[_]](timeout: FiniteDuration, monotonic: F[FiniteDuration])

  /** `diagnose` is run only if the deadline fires, to tag the failure with its cause (an unbounded read never touches
    * it); the caller provides it because explaining a stall needs a high-watermark re-read only the caller can do.
    */
  private[kafkapersistence] def readPartition[F[_]: MonadThrow: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset,
    stall: Option[Stall[F]],
    diagnose: F[String],
  ): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      stall.fold(drain(consumer, snapshotPartition, targetOffset))(
        drainWithDeadline(consumer, snapshotPartition, targetOffset, _, diagnose)
      )

  // poll once (waiting up to the poll timeout) and fold the batch into the accumulator
  private def pollFold[F[_]: FlatMap](
    consumer: SkafkaConsumer[F, String, ByteVector],
    acc: BytesByKey,
  ): F[BytesByKey] =
    consumer
      .poll(10.millis) // TODO: make poll timeout configurable
      .map(_.values.values.flatMap(_.toIterable).foldLeft(acc)(processRecord))

  // non-transactional read: drain to the target, waiting as long as it takes (the long-shipped behaviour)
  private def drain[F[_]: MonadThrow](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset,
  ): F[BytesByKey] =
    FlatMap[F].tailRecM(BytesByKey.empty) { acc =>
      consumer.position(snapshotPartition).flatMap {
        case offset if offset >= targetOffset => acc.asRight[BytesByKey].pure[F]
        case _                                => pollFold(consumer, acc).map(_.asLeft[BytesByKey])
      }
    }

  private final case class Last(position: Offset, progressAt: FiniteDuration, logAt: FiniteDuration)
  private final case class ReadState(acc: BytesByKey, last: Option[Last])

  // transactional read: the same drain, but fails loudly (tagged with `diagnose`) once no progress has been made for
  // the whole deadline - see the error's scaladoc. A stall is elapsed wall time since the position was reached, not a
  // poll count.
  private def drainWithDeadline[F[_]: MonadThrow: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset,
    stall: Stall[F],
    diagnose: F[String],
  ): F[BytesByKey] =
    FlatMap[F].tailRecM(ReadState(BytesByKey.empty, none)) {
      case ReadState(acc, last) =>
        consumer.position(snapshotPartition).flatMap {
          case offset if offset >= targetOffset =>
            acc.asRight[ReadState].pure[F]
          case offset =>
            stall.monotonic.flatMap { now =>
              last.filter(_.position == offset) match {
                case None =>
                  // first poll, or advanced to a new offset: (re)start the stall clock and the log throttle
                  pollFold(consumer, acc)
                    .map(read => ReadState(read, Last(offset, now, now).some).asLeft[BytesByKey])
                case Some(stuck) =>
                  // still at the same offset: fail once the deadline has passed, else log at the throttle
                  val stalledFor = now - stuck.progressAt
                  val logDue     = now - stuck.logAt >= logStallEvery
                  val failOrLog =
                    diagnose
                      .flatMap(d =>
                        RecoveryReadStalledError(snapshotPartition, offset, targetOffset, d).raiseError[F, Unit]
                      )
                      .whenA(stalledFor >= stall.timeout) *>
                      Log[F]
                        .info(
                          s"Snapshot topic read making no progress at offset $offset, target $targetOffset, " +
                            s"stalled for ${stalledFor.toSeconds}s"
                        )
                        .whenA(logDue)
                  failOrLog *> pollFold(consumer, acc).map { read =>
                    ReadState(read, stuck.copy(logAt = if (logDue) now else stuck.logAt).some).asLeft[BytesByKey]
                  }
              }
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
    stall: Option[Stall[F]],
  )(implicit fromBytes: FromBytes[F, String]): F[BytesByKey] = {
    val snapshotsPartition         = TopicPartition(topic = snapshotTopic, partition = partition)
    val snapshotPartitionSingleton = data.NonEmptySet.of(snapshotsPartition)

    def suffixed(suffix: String): ConsumerConfig =
      consumerConfig.copy(
        common = consumerConfig.common.copy(clientId = consumerConfig.common.clientId.map(cid => s"$cid-$suffix"))
      )

    def endOffset(consumer: SkafkaConsumer[F, String, ByteVector]): F[Offset] =
      consumer.endOffsets(snapshotPartitionSingleton).flatMap { endOffsets =>
        BracketThrowable[F].fromOption(endOffsets.get(snapshotsPartition), MissingOffsetError(snapshotsPartition))
      }

    // the true log end, read through a short-lived read_uncommitted view; a read_committed consumer sees only the
    // last-stable-offset, which a crashed writer's open transaction pins below records committed after it
    val highWatermark: F[Offset] =
      consumerOf
        .apply[String, ByteVector](
          suffixed(s"snapshot-$partition-hw").copy(isolationLevel = IsolationLevel.ReadUncommitted)
        )
        .use(endOffset)

    // The read target must be that high watermark, not this consumer's own end offset: a target bounded at the
    // last-stable-offset would silently miss the records above it. Targeting the high watermark instead makes the
    // read wait the open transaction out, completing only once everything below the target is decided (see the
    // design doc's "Recovery read"). Under read_uncommitted the consumer's own end offset already is the high
    // watermark, so no extra capture is needed.
    val capturedHighWatermark: F[Option[Offset]] =
      if (consumerConfig.isolationLevel == IsolationLevel.ReadCommitted) highWatermark.map(_.some)
      else none[Offset].pure[F]

    capturedHighWatermark.flatMap { captured =>
      consumerOf
        .apply[String, ByteVector](suffixed(s"snapshot-$partition").copy(autoOffsetReset = Earliest))
        .use { consumer =>
          for {
            _ <- consumer.assign(snapshotPartitionSingleton)
            targetOffset <- captured match {
              case None => endOffset(consumer)
              case Some(highWatermark) =>
                endOffset(consumer).flatMap { lastStableOffset =>
                  Log[F]
                    .warn(
                      s"Snapshot topic $snapshotTopic partition $partition recovery waits for open transaction(s): " +
                        s"last-stable-offset $lastStableOffset below target $highWatermark; resolves within the " +
                        s"pinning producer's transaction.timeout.ms plus the broker's abort scan"
                    )
                    .whenA(lastStableOffset.value < highWatermark.value)
                    .as(highWatermark)
                }
            }
            // a stall's cause, re-read lazily only if the deadline fires (see readPartition): a log end below the
            // captured target means truncation - an unclean leader election dropped acknowledged records, which
            // waiting can never recover; at or above it, an open transaction outlived the deadline. Best-effort: a
            // re-read that itself fails leaves the cause undetermined rather than masking the stall.
            diagnose = highWatermark
              .map { logEnd =>
                if (logEnd.value < targetOffset.value)
                  s"the log end regressed to $logEnd, below the captured target: log truncation after an unclean " +
                    "leader election - acknowledged snapshot records are lost"
                else
                  s"the log end $logEnd still covers the target: an open transaction outlived the deadline - its " +
                    "producer's transaction.timeout.ms likely exceeds the stall deadline"
              }
              .handleError(_ => "the cause could not be determined: the high-watermark re-read failed")
            bytesByKey <- readPartition(consumer, snapshotsPartition, targetOffset, stall, diagnose)
            _ <- Log[F].info(
              s"Snapshot topic $snapshotTopic partition $partition read complete at offset $targetOffset, ${bytesByKey.size} keys read"
            )
          } yield bytesByKey
        }
    }
  }
}
