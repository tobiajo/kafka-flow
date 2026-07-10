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

    def suffixed(config: ConsumerConfig, suffix: String): ConsumerConfig =
      config.copy(
        autoOffsetReset = Earliest,
        common          = config.common.copy(clientId = config.common.clientId.map(cid => s"$cid-$suffix"))
      )

    // The read target is the high watermark, taken with read_uncommitted. This consumer's own end offset
    // under read_committed is the last-stable-offset, which an open transaction (e.g. of a hard-crashed
    // previous owner, for up to its transaction.timeout.ms) pins BELOW records committed after it - a read
    // bounded by it completes silently missing those committed snapshots. Bounding by the high watermark
    // makes the read wait such transactions out instead: the read_committed position cannot pass the
    // last-stable-offset until the broker resolves them, so the read completes only once everything below
    // the target is decided. For a read_uncommitted config the two bounds coincide.
    val highWatermark: F[Offset] =
      consumerOf
        .apply[String, ByteVector](
          suffixed(consumerConfig, s"snapshot-$partition-hw").copy(isolationLevel = IsolationLevel.ReadUncommitted)
        )
        .use { consumer =>
          consumer.endOffsets(snapshotPartitionSingleton).flatMap { endOffsets =>
            BracketThrowable[F].fromOption(
              endOffsets.get(snapshotsPartition),
              MissingOffsetError(snapshotsPartition)
            )
          }
        }

    consumerOf
      .apply[String, ByteVector](suffixed(consumerConfig, s"snapshot-$partition"))
      .use { consumer =>
        for {
          _            <- consumer.assign(snapshotPartitionSingleton)
          targetOffset <- highWatermark
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
