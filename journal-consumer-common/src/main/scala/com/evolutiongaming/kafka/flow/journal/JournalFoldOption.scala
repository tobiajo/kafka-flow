package com.evolutiongaming.kafka.flow.journal

import cats.Monad
import cats.syntax.all._
import com.evolutiongaming.skafka.consumer.ConsumerRecord
import scodec.bits.ByteVector

/** A fold that replays events to rebuild state. */
trait JournalFold[F[_], S] {
  def apply(state: Option[S], record: ConsumerRecord[String, ByteVector]): F[Option[S]]
}

object JournalFold {
  def of[F[_]: Monad, S](f: (Option[S], ConsumerRecord[String, ByteVector]) => F[Option[S]]): JournalFold[F, S] =
    (state, record) => f(state, record)
}

/** Uses the floor as a fallback for `seenSeqNr` when no aggregate exists. */
object JournalFoldOption {
  def apply[F[_]: Monad](
    floor: SeqNrFloors[F],
    delegate: JournalFold[F, S]
  ): JournalFold[F, S] = new JournalFold[F, S] {
    override def apply(state: Option[S], record: ConsumerRecord[String, ByteVector]): F[Option[S]] = {
      floor.get(record.key).flatMap { floorSeqNr =>
        val seenSeqNr = state.flatMap(_.seenSeqNr).orElse(floorSeqNr)
        delegate.apply(state, record.copy(seenSeqNr = seenSeqNr))
      }
    }
  }
}