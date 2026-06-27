package com.evolutiongaming.kafka.flow.snapshot

import cats.effect.Ref
import cats.mtl.Stateful
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.evolutiongaming.catshelper.Log
import com.evolutiongaming.kafka.flow.LogPrefix
import com.evolutiongaming.kafka.flow.effect.CatsEffectMtlInstances.*
import com.evolutiongaming.skafka.Offset

trait Snapshots[F[_], S] extends SnapshotReader[F, S] with SnapshotWriter[F, S]

/** Allows to read a previously saved snapshot */
trait SnapshotReader[F[_], S] {

  /** Restores a snapshot */
  def read: F[Option[S]]

}

/** Provides a persistence for a specific key */
trait SnapshotWriter[F[_], S] {

  def append(snapshot: S): F[Unit]

  def initPersisted(snapshot: S): F[Unit]

  def flush: F[Unit]

  def delete(persist: Boolean, offset: Offset): F[Unit]

}
object Snapshots {

  // MG-PROBE (Candidate A): the DB speaks the unified Stored/read/write contract, but the buffer keeps its OWN ADT
  // (the read-contract vs buffer-state layer boundary preserved). The buffer translates Stored <-> Buffered at the
  // database edge (read, flush, delete).
  private[flow] sealed trait Buffered[+S]
  private[flow] object Buffered {
    final case class Live[S](value: S, persisted: Boolean) extends Buffered[S]
    final case class Deleted(offset: Offset) extends Buffered[Nothing]
    case object Empty extends Buffered[Nothing]
  }

  private[flow] def of[F[_]: Ref.Make: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    offsetOf: Option[S => Offset],
  )(implicit log: Log[F]): F[Snapshots[F, S]] =
    Ref.of[F, Buffered[S]](Buffered.Empty).map(state => Snapshots(key, database, state.stateInstance, offsetOf))

  private[snapshot] def apply[F[_]: Monad, K: LogPrefix, S](
    key: K,
    database: SnapshotDatabase[F, K, S],
    state: Stateful[F, Buffered[S]],
    offsetOf: Option[S => Offset],
  )(implicit log: Log[F]): Snapshots[F, S] = new Snapshots[F, S] {
    private val prefixLog: Log[F] = log.prefixed(LogPrefix[K].extract(key))

    private def highWater(current: Buffered[S]): Option[Offset] = current match {
      case Buffered.Live(value, _) => offsetOf.map(_(value))
      case Buffered.Deleted(o)     => o.some
      case Buffered.Empty          => none
    }

    // translate the DB's Stored read into the buffer's own cell, seeding the replay-window floor for a tombstone
    def read =
      database.read(key).flatMap {
        case Some(Stored.Live(snapshot, _)) => state.set(Buffered.Live(snapshot, persisted = true)).as(snapshot.some)
        case Some(Stored.Tombstone(offset)) => state.set(Buffered.Deleted(offset)).as(none[S])
        case None                           => none[S].pure[F]
      }

    def append(snapshot: S) =
      state.modify { current =>
        val below = (offsetOf.map(_(snapshot)), highWater(current)).mapN(_ < _).getOrElse(false)
        if (below) current
        else
          current match {
            case Buffered.Live(existing, _) if existing == snapshot => current
            case _                                                  => Buffered.Live(snapshot, persisted = false)
          }
      }

    def initPersisted(snapshot: S) =
      state.set(Buffered.Live(snapshot, persisted = true))

    // translate the buffer's live cell into the DB's Stored.Live write
    def flush =
      state.get.flatMap {
        case Buffered.Live(value, false) =>
          database.write(key, Stored.Live(value, offsetOf.map(_(value)))) *>
            state.set(Buffered.Live(value, persisted = true))
        case _ => ().pure[F]
      }

    def delete(persist: Boolean, offset: Offset) =
      state.get.flatMap { current =>
        val fenceOffset = highWater(current).fold(offset)(offset max _)
        // translate a delete into a Stored.Tombstone write
        val delete =
          if (persist) database.write(key, Stored.Tombstone(fenceOffset)) *> prefixLog.info("deleted snapshot")
          else ().pure[F]
        state.set(Buffered.Deleted(fenceOffset)) *> delete
      }

  }

  def empty[F[_]: Applicative, S]: Snapshots[F, S] = new Snapshots[F, S] {
    def read                                     = none[S].pure[F]
    def append(event: S)                         = ().pure[F]
    def initPersisted(event: S)                  = ().pure[F]
    def flush                                    = ().pure[F]
    def delete(persist: Boolean, offset: Offset) = ().pure[F]
  }

}
