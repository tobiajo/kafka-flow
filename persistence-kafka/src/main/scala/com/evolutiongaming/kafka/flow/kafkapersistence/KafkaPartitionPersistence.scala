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
