# TEMP / scratch — Research: snapshot-store design, middle-ground investigation

> Throwaway research notes (do not merge). Investigates whether a middle ground exists between the
> shipped `#834` design and the `cas-unify` refactor, grounded in the actual code at each point.
> Companion: `TEMP-REPORT-snapshot-store-design.md`.

## 0. Framing

All designs below are **behaviourally identical** — same compare-and-set / tombstone semantics, same
replay-window fix, all green on the same unit + IT suites (Cassandra `SnapshotSpec` 13/13, `FlowSpec`
4/4; Kafka `Transactional…` 8/8, `StatefulProcessing` 3/3; full core suite incl.
`SnapshotReplayFencingSpec` 7/7). So this is a **structure / API-surface** question, not correctness.

Reference points (commit → what it is):

| commit | role |
|---|---|
| `8b85ad8` | "offsetOf explicit (Option, not Offset.min sentinel)" — buffer = **two Refs** (live `Snapshot[S]` + separate `Option[Offset]` floor) |
| `3288c65` | explore: collapse buffer into one `Buffered` ADT (`Live(Snapshot[S])`/`Deleted`/`Empty`) |
| `92ef452` | explore: inline value into `Buffered.Live(value, persisted)` — **this is `#834`'s code** (`57785d3` is a doc-only commit on top) |
| `cas-unify` | unify: one `Stored` ADT, `read`/`write`, buffer `Cell(Stored, persisted)` |
| `mg-probe` `956501d` | **this research**: Candidate A (unify DB contract + `Buffered` buffer) |

## 1. The design factors into TWO independent decisions

Examining the code, the differences are not one axis but two orthogonal ones:

**Decision A — the public `SnapshotDatabase` contract**
- A0 (`8b85ad8`…`92ef452` = `#834`): `get: Option[S]` **+** `recover: Recovered[S]` read pair, `persist` **+** `delete(offset)` write pair. `Recovered = Present|Deleted|Absent`.
- A1 (`cas-unify`): `read: Option[Stored]` **+** `write(Stored)`. `Stored = Live|Tombstone`.

**Decision B — the in-memory buffer representation (internal, not public)**
- B0 (`8b85ad8`): two Refs — a live cell and a side-channel floor offset.
- B1 (`3288c65`/`92ef452` = `#834`, and Candidate A): the buffer's own `Buffered` ADT.
- B2 (`cas-unify`): reuse the DB's `Stored` via `Cell(Stored, persisted)`.

The shipped `#834` is **(A0, B1)**. `cas-unify` is **(A1, B2)**. The fact that these are independent is
the whole basis for a middle ground: you can pick **(A1, B1)** — unify the public contract *without*
collapsing the buffer into the wire type.

## 2. The one concrete defect class: the `get`/`recover` footgun

Under A0 the read is two methods, and `recover` has a default derived from `get`:

```scala
def recover(key: K)(implicit F: Functor[F]): F[Recovered[S]] =
  get(key).map(_.fold(Recovered.Absent: Recovered[S])(Recovered.Present(_)))  // never reports Deleted
```

A **wrapper** (e.g. metrics) that overrides `get` but forgets `recover` inherits this default, which
calls the wrapper's `get`; a tombstone's `get` returns `None` → `Absent` → no replay-window floor →
the legitimate owner self-fences → **livelock**. In `#834` this is **mitigated, not absent**: the
metrics wrapper *does* override `recover` (with a comment explaining why). So it is a **latent trap for
any future wrapper/implementor**, not an active bug.

Under A1 there is a single `read`, so the trap **cannot be expressed** — there is no second read path
to diverge from. Removing this footgun is the single most concrete, defensible win of moving off A0.

## 3. Middle grounds identified

### MG-A — unify the contract (A1), keep the buffer's own ADT (B1). *Prototyped: `mg-probe` `956501d`.*

Public DB API = `cas-unify`'s (`Stored`, `read`/`write`) — so it **kills the footgun** and unifies the
contract — but the buffer keeps `Buffered` (B1), translating `Stored ⇄ Buffered` at exactly three
edges:

```scala
// read:   Some(Stored.Live(s,_)) -> Buffered.Live(s, persisted=true)   ; Some(Stored.Tombstone(o)) -> Buffered.Deleted(o)
// flush:  Buffered.Live(v,false) -> database.write(Stored.Live(v, offsetOf.map(_(v))))
// delete: -> database.write(Stored.Tombstone(fenceOffset))
```

**Evidence:** `core/Compile/compile` is **green**; the change is **isolated to `Snapshots.scala`**
(SnapshotDatabase/Cassandra/metrics/kafka are byte-identical to `cas-unify`). The translation is three
one-line maps. It preserves the **read-contract (`Stored`) vs buffer-state (`Buffered`) layer boundary**
— the main structural objection to `cas-unify`'s B2 — at the cost of keeping two ADTs and the 3-edge
translation. (Tests not migrated in the probe; the buffer-level `SnapshotsSpec` would revert to the
`#834` `Buffered` shape — i.e. its test cost ≈ `#834`'s.)

### MG-B — keep persist/delete, collapse the read to a single method (drop `get`)

Smallest possible footgun fix while staying close to A0: **remove `get`**, make `recover` (or a renamed
`read`) the **sole, abstract** read returning the ADT. No `get` ⇒ nothing for a wrapper to diverge
from. Write stays `persist`/`delete`; buffer stays `Buffered`.

- Pro: minimal change; kills the footgun; no write-side churn.
- Con: still a (small) public API change — implementors lose `get` and must define the read explicitly;
  keeps the `Recovered`-vs-`Buffered` duplication and the persist/delete duality. It is a **partial**
  cleanup behind a **partial** API break.

### Non-starters

- **(A0, B2)** — reuse `Stored` in the buffer while the DB still exposes `get`/`recover`/`persist`/
  `delete`: there is no `Stored` in the A0 contract to reuse, so this cell is unnatural.
- **`8b85ad8` (B0)** — the two-Ref buffer (live cell + side-channel floor) is strictly worse than B1
  on cohesion (two states to keep coherent; `highWater` must `orElse`-combine them). Superseded by
  `3288c65`. Not a candidate.

## 4. Tradeoff matrix

| | `#834` (A0,B1) | MG-B (A0′,B1) | MG-A (A1,B1) | unify (A1,B2) |
|---|---|---|---|---|
| Public API break | none (baseline) | small (drop `get`) | yes (`read`/`write`) | yes (`read`/`write`) |
| `get`/`recover` footgun | latent | **gone** | **gone** | **gone** |
| # ADTs for live/tomb/absent | 2 (`Recovered`+`Buffered`) + write-pair | 2 + write-pair | 2 (`Stored`+`Buffered`) | **1** (`Stored`, reused) |
| read-contract vs buffer-state boundary | separate | separate | **separate** | merged |
| illegal states representable | none | none | none | none |
| offset of a live value | inside `S` | inside `S` | top-level (`Stored.Live`) | top-level (`Stored.Live`) |
| blast radius vs `#834` | — | tiny | DB API + ~3-line buffer xlate | DB API + buffer rewrite |
| behaviour | identical | identical | identical | identical |

## 5. Honest reading

- The **footgun removal** is the only concrete quality defect addressed, and it is shared by MG-B, MG-A
  and unify. It is *latent* in `#834`, not active.
- **MG-A vs unify** is a narrow call: unify is slightly less code (one ADT, no translation); MG-A keeps
  the read-contract/buffer-state boundary at the cost of three trivial maps. Both kill the footgun and
  unify the public contract. The difference is taste (one-type-everywhere vs layered DTOs), not
  substance — the `Stored`/`Buffered` shapes are near-identical today, so unify's "coupling" is benign
  and MG-A's "decoupling" is mostly future-proofing.
- **MG-B** has the worst ratio: it pays a public API break (however small) for only a partial cleanup.
- All A1 options (**MG-A, unify**) are a **public API break** for custom `SnapshotDatabase` implementors
  and require updating `persistence.md`'s "Custom snapshot storage" contract (already done on
  `cas-unify`).
