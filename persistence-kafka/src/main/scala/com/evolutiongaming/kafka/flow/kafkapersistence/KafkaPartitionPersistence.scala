package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.implicits.*
import cats.{FlatMap, Monad, data}
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

  private[kafkapersistence] def readPartition[F[_]: Monad: Log](
    consumer: SkafkaConsumer[F, String, ByteVector],
    snapshotPartition: TopicPartition,
    targetOffset: Offset
  ): F[BytesByKey] =
    Log[F].info(s"Snapshot topic read started up to offset $targetOffset") *>
      FlatMap[F]
        .tailRecM[BytesByKey, BytesByKey](BytesByKey.empty) { acc =>
          consumer
            .position(snapshotPartition)
            .flatMap {
              case offset if offset >= targetOffset =>
                acc.asRight[BytesByKey].pure[F]
              case _ =>
                consumer
                  .poll(10.millis) // TODO: make poll timeout configurable
                  .map(
                    _.values
                      .values
                      .flatMap(_.toIterable)
                      .foldLeft(acc)(processRecord)
                      .asLeft
                  )
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

    def suffixed(suffix: String): ConsumerConfig =
      consumerConfig.copy(
        common = consumerConfig.common.copy(clientId = consumerConfig.common.clientId.map(cid => s"$cid-$suffix"))
      )

    def endOffset(consumer: SkafkaConsumer[F, String, ByteVector]): F[Offset] =
      consumer.endOffsets(snapshotPartitionSingleton).flatMap { endOffsets =>
        BracketThrowable[F].fromOption(endOffsets.get(snapshotsPartition), MissingOffsetError(snapshotsPartition))
      }

    // The read target must be the high watermark, not this consumer's own end offset: under read_committed
    // that is the last-stable-offset, which a crashed writer's open transaction pins below records
    // committed after it - a read bounded there would silently miss them. A target captured through a
    // short-lived read_uncommitted consumer instead makes the read wait such a transaction out, completing
    // only once everything below the target is decided (see the design doc's "Recovery read"). Under
    // read_uncommitted the consumer's own end offset already is the high watermark, so no extra capture is needed.
    val capturedHighWatermark: F[Option[Offset]] =
      if (consumerConfig.isolationLevel == IsolationLevel.ReadCommitted)
        consumerOf
          .apply[String, ByteVector](
            suffixed(s"snapshot-$partition-hw").copy(isolationLevel = IsolationLevel.ReadUncommitted)
          )
          .use(endOffset)
          .map(_.some)
      else
        none[Offset].pure[F]

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
}
