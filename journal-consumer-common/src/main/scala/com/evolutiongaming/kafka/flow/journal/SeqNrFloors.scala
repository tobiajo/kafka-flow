package com.evolutiongaming.kafka.flow.journal

import cats.effect.{Ref, Sync}
import cats.syntax.all._
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.skafka.TopicPartition
import scache.Cache

/** Tracks the highest `seqNr` seen per key, scoped to the current assignment. */
trait SeqNrFloors[F[_]] {
  def update(key: KafkaKey, seqNr: SeqNr): F[Unit]
  def get(key: KafkaKey): F[Option[SeqNr]]
}

object SeqNrFloors {
  def of[F[_]: Sync](cache: Cache[F, KafkaKey, SeqNr]): SeqNrFloors[F] = new SeqNrFloors[F] {
    override def update(key: KafkaKey, seqNr: SeqNr): F[Unit] =
      cache.put(key, seqNr)

    override def get(key: KafkaKey): F[Option[SeqNr]] =
      cache.get(key)
  }
}