package com.evolutiongaming.kafka.flow.kafkapersistence

import cats.Monad
import cats.syntax.all.*
import com.evolutiongaming.kafka.flow.KafkaKey
import com.evolutiongaming.kafka.flow.snapshot.{KafkaSnapshot, SnapshotReadDatabase, Stored}
import com.evolutiongaming.skafka.{FromBytes, Offset, Topic}
import scodec.bits.ByteVector

object KafkaSnapshotReadDatabase {
  /** Reads snapshots from a compacted topic that stores KafkaSnapshot[S] (with offset).
    * This enables floor-based safe deletes.
    *
    * NOTE: This requires the snapshot topic to store KafkaSnapshot[S] instead of just S.
    * Use `ofLegacy` for the traditional format (without offset tracking).
    */
  def of[F[_]: Monad, S: FromBytes[F, *]](
    snapshotTopic: Topic,
    getState: String => F[Option[ByteVector]]
  ): SnapshotReadDatabase[F, KafkaKey, S] = {
    key =>
      for {
        state      <- getState(key.key)
        maybeState <- state.traverse(bytes => FromBytes[F, KafkaSnapshot[S]].apply(bytes.toArray, snapshotTopic))
        // the compacted-topic backend now tracks per-snapshot offset for floor-based safe deletes
      } yield maybeState.map(snapshot => Stored.Live(snapshot.value, Some(snapshot.offset)))
  }

  /** Reads snapshots from a compacted topic that stores just S (legacy format, without offset).
    * This is the traditional format that does not support floor-based safe deletes.
    */
  def ofLegacy[F[_]: Monad, S: FromBytes[F, *]](
    snapshotTopic: Topic,
    getState: String => F[Option[ByteVector]]
  ): SnapshotReadDatabase[F, KafkaKey, S] =
    key =>
      for {
        state      <- getState(key.key)
        maybeState <- state.traverse(bytes => FromBytes[F, S].apply(bytes.toArray, snapshotTopic))
        // the compacted-topic backend tracks no per-snapshot offset; it is generation-fenced, so the buffer is
        // unfenced (offset None) - see SnapshotsOf.backedBy without offsetOf
      } yield maybeState.map(value => Stored.Live(value, none))
}
