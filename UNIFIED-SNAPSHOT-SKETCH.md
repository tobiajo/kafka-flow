# SKETCH — unify the snapshot store around "offset + optional payload" (do not merge)

Throwaway design sketch. Goal: collapse the three coordinated-but-separate sites that today carry
the replay-window fence into **one type and one monotonic write path**, so the delete path is not a
parallel universe of the persist path.

## The root of the entanglement

A delete is modeled as an **absence**, so it needs a parallel representation at every layer:

| concern | persist (has a value) | delete (absence) — the parallel site |
|---|---|---|
| store write | `persist(key, snapshot)` | `delete(key, offset)` |
| store read | `get → Option[S]` | `recover → Recovered.Deleted(offset)` (offset can't ride inside an absent value) |
| buffer cell | `Buffered.Live(value, persisted)` | `Buffered.Deleted(offset)` |
| offset discipline | monotonic `append` drops a regress | `delete` lifts to `max(offset, highWater)` |

Four pairs. Each looks local; together they are one idea (per-key monotonic durability) smeared
across four places. If the stored unit always carried its offset *and* an optional payload, the
right column disappears.

## The unified core types

```scala
/** What the store holds for a key: an offset always, a payload that is absent for a tombstone. */
final case class Stored[S](offset: Offset, value: Option[S], metadata: String = "")

trait SnapshotDatabase[F[_], K, S] {
  /** Upsert. `value = Some` persists a snapshot; `value = None` writes a tombstone.
    * A compare-and-set store gates the write on `stored.offset`. */
  def write(key: K, stored: Stored[S]): F[Unit]

  /** `None` = no row; `Some(Stored(o, None))` = tombstone at `o`; `Some(Stored(o, Some(v)))` = live. */
  def read(key: K): F[Option[Stored[S]]]
}
```

`Recovered` is **deleted**. `persist` / `delete` collapse into `write`; `get` / `recover` collapse
into `read`. The three-way outcome that `Recovered` encoded is now just the shape of
`Option[Stored[S]]`.

## The unified buffer — one monotonic `append`, persist and delete both flow through it

```scala
private final case class Cell[S](stored: Stored[S], persisted: Boolean)
// buffer state: Ref[F, Option[Cell[S]]]   (None = nothing buffered yet)

def read: F[Option[S]] =
  database.read(key).flatMap {
    case Some(stored) => state.set(Cell(stored, persisted = true).some).as(stored.value)
    case None         => none[S].pure[F]
  }

// THE single write site. The persist-drop vs delete-lift asymmetry — the whole replay-window
// subtlety — lives here, as two arms of one match, instead of split across append + delete.
def append(stored: Stored[S]): F[Unit] =
  state.update {
    case Some(cur) if monotonic && stored.offset < cur.stored.offset =>
      stored.value match {
        case Some(_) => cur                                  // persist regress → DROP (redundant re-derivation)
        case None    => Cell(stored.copy(offset = cur.stored.offset),  // delete → LIFT to high-water, still applies
                              persisted = false).some
      }
    case _ => Cell(stored, persisted = false).some
  }

def flush: F[Unit] =
  state.get.flatMap {
    case Some(Cell(stored, false)) => database.write(key, stored) *> markPersisted
    case _                         => ().pure[F]             // nothing dirty (live OR tombstone): one path
  }

/** High-water offset, for stamping a value-less (timer) delete. */
def offset: F[Option[Offset]] = state.get.map(_.map(_.stored.offset))
```

A **persist** is `append(Stored(offset, Some(v)))`. A **delete** is `append(Stored(offset, None))`.
Same site, same monotonicity. There is no `delete(offset)` method and no `max(offset, highWater)`
buried in it — the lift is one arm of `append`, visible next to the drop it contrasts with.

## What each caller becomes

```scala
// Persistence.replaceState  (was snapshots.append(state))
snapshots.append(Stored(offset = record.offset, value = state.some))

// Persistence.delete  (was snapshots.delete(persist, current.offset) with the max() inside)
snapshots.append(Stored(offset = current.offset, value = none))
//   → append lifts a replay-window delete to the high-water on its own; the caller passes the
//     plain processing offset and stops caring about the fence.
```

The `TickToState` tick→None path is unchanged except it now appends a value-less `Stored` like any
other write.

## How the three sites collapse — before / after

| today | after |
|---|---|
| `Recovered{Present,Deleted,Absent}` + `recover` default + Cassandra `recover` override | gone — `read: F[Option[Stored[S]]]` |
| `Buffered{Live,Deleted,Empty}` ADT | one `Cell(stored, persisted)` over `Option` |
| `delete(persist, offset)` + `max(offset, highWater)` fence | `append(Stored(_, None))` — the lift is one arm of `append` |
| `offsetOf: Option[S => Offset]` (extract offset from the value) | offset is intrinsic to `Stored`; a `monotonic: Boolean` flag replaces the function |
| metrics wrapper MUST delegate `recover` or silently downgrades a tombstone | only `read`/`write` to delegate — the downgrade footgun is gone |

## Ripple (blast radius)

- **core** — `SnapshotDatabase.scala` (drop `Recovered`, new `write`/`read`), `Snapshots.scala`
  (rewrite as above), `SnapshotsOf.scala` (`Some(_.offset)` → `monotonic`), `Persistence.scala`
  (`delete`/`replaceState` construct `Stored`), `KafkaSnapshot` stays as the *fold state* type.
- **persistence-cassandra** — `CassandraSnapshots`: `persist`+`delete` merge into one `write` that
  matches on `stored.value` (Some → the CAS `UPDATE … value=:value`, None → the tombstone
  `UPDATE … value=null`); `recover` becomes `read` returning `Option[Stored]`. The decode already
  reads offset + optional value, so this side is *small*.
- **persistence-kafka** — `KafkaSnapshotWriteDatabase` write path constructs `Stored`.
- **tests** — `SnapshotsSpec`, `SnapshotsOfSpec`, `SnapshotReplayFencingSpec`, Cassandra
  `SnapshotSpec`/`FlowSpec` migrate to `write`/`read` and `Cell`. Mechanical but not tiny.

## Decision points / risks (call these before committing)

1. **LWW stores and monotonicity.** Keep a `monotonic: Boolean` (default `false` for non-CAS) so
   last-write-wins stores keep LWW semantics; `true` only for the CAS/offset-carrying wiring. Don't
   silently make every store monotonic.
2. **`KafkaSnapshot` vs `Stored`.** This sketch keeps `KafkaSnapshot[S]` (offset + value) as the
   fold's state and introduces `Stored[S]` (offset + Option value) as the storage/buffer unit; a
   live one maps `Stored(o, Some(v))`. They overlap but serve different layers — acceptable.
3. **The deeper variant (out of scope).** Make the *fold* yield `KafkaSnapshot[Option[S]]` so a
   delete carries the driving record's offset end-to-end and `SnapshotFold`'s filter gains a floor
   for tombstones too (today the buffer Cell carries the deleted-key case because the fold's
   `Option[S]` loses the offset on `None`). This also fixes the tombstone fold-filter gap — but it
   touches the user-facing `Fold` API and is a much wider change. Recommend NOT doing it now.
4. **No codec change.** The `None` payload is the Cassandra `NULL` value, handled at the SQL bind in
   `write` — `ToBytes`/`FromBytes` are untouched.

## Verdict

The storage/buffer-level unification (this sketch) collapses all four parallel sites into one type
+ one `append`, removes `Recovered` and the metrics-wrapper footgun, and keeps the Cassandra change
small. The essential asymmetry (persist-drop vs delete-lift) does not vanish — it can't — but it
ends up as two arms of one `match`, which is exactly the "not smeared across the codebase" property
we want. The fold-level variant (point 3) is a bridge too far for the payoff.
