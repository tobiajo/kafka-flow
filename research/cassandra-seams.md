# Seam analysis — attack hypotheses and verdicts

*Cassandra arm of the #732 single-writer study — adversarial seam analysis (S1–S14): attack hypothesis and verdict per boundary. Corpus index: [`README.md`](README.md).*

Each seam: what could break there, how I attacked it, verdict. Status: OPEN until closed by
code-reading + test/model/experiment evidence.

## S1 — TTL reconfiguration ⇒ poison row (null `offset` cell) — **FINDING — CONFIRMED, fixed (F-1)**

**Attack.** Cassandra TTLs are per-cell (pending ext (4) confirmation). Every CAS write stamps the
statement's TTL onto the cells *it* writes; it never rewrites `created`/`metadata` cells it doesn't
touch. (A *persist* is the exception: it writes all of `created, metadata, value, offset`, refreshing
all four.) But the CAS **delete** writes only `value=null, offset=:offset`. So:

1. Deploy without TTL; persist key K (4 cells, no TTL).
2. Redeploy with `ttl = T` (the documented recommendation for delete workloads!).
3. Delete K: tombstone writes `value=null` (deletion marker) + `offset` with TTL T.
   `created`/`metadata` cells keep **no TTL** — they live forever.
4. At `t+T` the `offset` cell expires. Row still visible via live `created`/`metadata` cells:
   `value=null`, `offset=null`.

Consequences (from code reading):
- `CassandraSnapshots.read` (persistence-cassandra …/CassandraSnapshots.scala:133): tombstone branch
  does `row.decode[Offset]("offset")` **non-optionally** ⇒ decode of null ⇒ exception ⇒ **every
  recovery of K fails, forever** (cells never expire). Same non-optional decode on the live branch
  (line 301) for the analogous value-alive/offset-expired shape (TTL *shortening* variant).
- Write path: `IF offset <= :offset` against null offset ⇒ not applied (pending ext (3));
  `resolveConditional` sees null stored offset ⇒ "row absent" ⇒ `INSERT IF NOT EXISTS` ⇒ row exists ⇒
  not applied ⇒ retry `UPDATE` ⇒ not applied ⇒ `SnapshotWriteConflict` — **every write to K
  perma-conflicts** until all cells expire (never, in the TTL-enable scenario).

Variants: (a) TTL enabled between persist and delete (permanent poison); (b) TTL shortened
(poison until the longest old TTL); (c) TTL disabled after tombstones written (tombstone `offset`
cell still carries old TTL; on expiry, `value`'s deletion marker is gone after gc_grace… row likely
fully disappears — benign); (d) same-TTL steady state — offset cell is always the last written ⇒
always outlives siblings ⇒ **unreachable in steady state** (matches the code comment's claim).

**Verdict (initial, pre-experiment — superseded; the S14 experiment / F-1 *refuted* the decode-crash
prediction: a null offset decodes as 0, a silent floor loss, not an exception): reachable only across a
TTL reconfiguration, but the recommended rollout (“configure a TTL for workloads that delete keys”) is
exactly that reconfiguration when applied to an existing deployment.** Plan: (1) ext (4) confirms cell-TTL semantics; (2) reproduce against real
Cassandra (two `CassandraSnapshots` instances over one table, second with `ttl=1s`, sleep past expiry,
then read + persist); (3) fix candidates: delete statement also nulls `created`/`metadata` (tombstone
becomes self-contained: row dies exactly when `offset` expires); `read` treats null-offset rows
defensively (decode `Option[Offset]`, absent ⇒ treat row as absent/reaped); optionally the write path
already treats null-offset as absent — the failure is only `INSERT IF NOT EXISTS` colliding with the
half-dead row, which the `created`/`metadata`-nulling fix removes after expiry.

## S2 — `flushCell` is not atomic vs concurrent `put` — ASSUMPTION-CRITICAL, not a defect

`flushCell`: `state.get` → `database.write` → `markPersisted` (modify current cell). If an `append`
replaced the cell between the DB write and `markPersisted`, the *new* cell would be marked persisted
without having been written (lost write). Requires intra-key concurrency, which kafka-flow's
poll-thread serialization forbids (ticks, folds, flushes of one key are serialized; models README
states it as an assumption). Same shape existed pre-branch. **Verdict: correct under stated
serialization assumption; assumption now surfaced in claims A4. No fix; now stated as the design doc's
4th Assumption (and the models README).**

## S3 — First-write compound interleavings

`UPDATE`(absent) → `INSERT IF NOT EXISTS` → retry `UPDATE`. Attacks: two first-writers (covered:
model `casfw_3w`, IT race test); reap between INSERT-loss and retry ⇒ spurious conflict (covered:
`casfw_reap`, `casfw_spurious`, absorbed in `cassandra_firstwrite_spurious`); zombie DELETE between
loser-INSERT and retry — retry sees absent ⇒ conflict(none) ⇒ teardown/recover — absorbed the same
way. **Verdict: closed by models + code; the spurious path's "recovers on next flush" is checked as
convergence in `cassandra_firstwrite_spurious`.**

## S4 — Snapshot-store delete vs journal delete: no cross-store atomicity — pre-existing gap

`Buffers.delete(persist=true, o)`: `snapshots.delete` then `journals.delete` then `keys.delete`.
Crash after the first: tombstone at `o` durable, journal intact. Snapshot-recovery mode: recovers
tombstone floor, replays input — correct. Events-recovery mode: fold over the *intact journal*
resurrects the pre-delete state in memory; a later flush persists it above `X` (CAS admits: offset
grew) — the delete is lost. Record-driven deletes re-issue on replay (offset uncommitted ⇒ record
reprocessed); **tick-driven deletes have no such guarantee** (wall-clock condition may not re-fire
deterministically, though expiry-style ticks usually re-fire). Same hazard exists under LWW (and
pre-branch); CAS neither causes nor fixes the cross-store *atomicity*. **Verdict: OVERTURNED — this was
under-graded.** Filed originally as an out-of-scope, in-memory-only gap. Modelling the journal as a
second state source showed the resurrected state becomes *durable* (findings **F-6**); the review
committee then showed the first fix re-admitted it at the second recovery (findings **F-7**). Now
fixed: recovery folds only journal rows above the fenced store's floor onto the store base
(`ReadState`), so residue below a tombstone is never folded, at every recovery — modelled faithfully
(row-set journal, `cassandra_events_*`) and pinned end-to-end (`FlowSpec` revive IT). The residual — a
delete that never became durable (a crash *before* the tombstone write) — is inherent to the two-store
design, which is why `Buffers.delete` writes the tombstone before clearing the journal (design doc).

## S5 — Equal-offset tombstone → live resurrection at the same offset

Tick creates state at offset `X` for a key tombstoned at `X` (buffer: `put(Live(v, X))` over
`Tombstone(X)`: not below ⇒ replaces; store: `IF offset <= X` applies). Legitimate owner acting at its
stored offset — by design (equal-offset admission). A *zombie* at exactly `X` could do the same — the
documented equal-offset gap; contents equal under determinism (its fold at `X` is the same events).
**Verdict: closed by design argument E1/E2 + determinism assumption; model should exercise an
equal-offset zombie (MG2, model-audit).**

## S6 — `Snapshots.read` tombstone sets floor but returns `None` — downstream interplay

`Persistence.read` sees `None` ⇒ no `initPersistedState`, no `Timestamps.onPersisted` ⇒ `persistedAt`
stays unset ⇒ K3's buffer-only delete holds. But: the tombstone cell is set with `persisted=true`, so
a subsequent `flush` (with no new append) writes nothing — correct. A subsequent append above floor
replaces cell (`persisted=false`) ⇒ flush persists — correct. Append *below* floor: dropped, cell
stays tombstone/persisted ⇒ flush no-op — correct (this is K1's fix). **Verdict: closed by code
reading; covered by SnapshotReplayFencingSpec (per test-audit).**

## S7 — Double-read on recovery (events mode): `ReadState` floor read then journal fold

`ReadState(journals, fold, snapshots)`: `snapshots.read.void` (fenced only) then fold. The floor read
*itself* recovers a live snapshot's value and discards it (`.void`) — no state leak; tombstone sets
buffer cell (side-effect wanted); live snapshot **does not** set the buffer (read returns value
without seeding cell — `initPersistedState` is only called by `Persistence.read` on `Some`... but
here the *outer* read result is the fold result, not the snapshot). Attack: events-mode, key has live
snapshot at X (journal intact). Floor read returns `Some(v)` (no cell seeded — Live branch of
`Snapshots.read` is pure). Fold rebuilds from journal ⇒ `Persistence.read` returns fold result ⇒
`initPersistedState(foldState)` seeds buffer at the fold's offset. Consistent. But subtle: the
`Snapshots.read` Live branch does NOT seed the floor — for a *live* key in events mode the floor
comes from the fold result (journal intact ⇒ reconstructs to X̃ = journal's top offset). If the
journal was truncated/TTL-reaped *below* the snapshot offset X, fold yields state at X̃ < X with no
floor at X ⇒ replay-window self-fence possible for a *live* key in events mode — the design says
events-recovery pairing with CAS "buys no stale-writer safety" and only the deleted-key livelock is
removed. A live key with journal-TTL < snapshot presence could still self-fence. **Attack refined:**
events mode + journal TTL reaping + CAS snapshots (an odd but constructible pairing — snapshots
written but never read for state). The floor read DOES run (`fenced`) but its Live result seeds
nothing. **Verdict: CLOSED — fixed; and this seam correctly anticipated the live-snapshot arm of F-7.**
The candidate fix named here ("`Snapshots.read`'s Live branch should also seed the floor") is exactly
the adopted fix: `read` now seeds the live cell so `Snapshots.floor` carries the live snapshot's offset,
and `ReadState` folds only journal rows above that floor onto the live snapshot as the base — so a
journal reaped/polluted below a live snapshot recovers the snapshot, not a truncated fold. The review
committee's F-7 is the general form of this exact hazard (both the tombstone and this live arm),
reached across two recoveries; the live arm is pinned by `ReadStateFloorGateSpec`'s
"live key's post-snapshot journal suffix folds onto the store base" second-recovery case. Credit where
due: the seam analysis flagged the live arm and left it OPEN with the right fix — it was the *tombstone*
arm's re-entry, and the model's vacuity, that the self-audit then under-verified (F-7).

## S8 — Unfenced buffer receiving a `Stored.Tombstone` from a downgraded store

CAS→LWW downgrade leaves tombstone rows; LWW-mode `read` still surfaces `Stored.Tombstone` ⇒ buffer
(fenced under current wiring — see S9) floors at X. Replays below X dropped. Benign under
determinism; key readable (None) and re-persistable above X. With the S9 fix (LWW wired unfenced),
`Snapshots.read`'s tombstone branch still sets the floor cell (offset carried in the `Stored`), so
behavior is the same — acceptable either way. **Verdict: benign; document the downgrade residue
(tombstone rows persist until TTL/manual cleanup; they floor the buffer even in LWW mode).**

## S9 — Fenced buffer wired for LWW-mode Cassandra — **FINDING, cost regression + doc mismatch**

`CassandraPersistence` builds one `PersistenceModule`; its `restoreEvents`/`restoreSnapshots`/
`snapshotsOnly` call `snapshots.snapshotsOf` — the `KafkaSnapshot` extension that always passes
`Some(_.offset)` ⇒ `fenced=true` **for both write modes**. Consequences for `compareAndSet=false`
users after upgrading:
1. `restoreEvents` now performs a per-key snapshot-store read on every key recovery (the `fenced`
   gate is true) — a pure cost regression vs master for a mode that can never recover a tombstone
   floor (LWW deletes remove rows). Eager recovery ⇒ one extra point-read per key per rebalance.
2. Monotonic buffer semantics (lower-offset appends dropped) replace master's last-write-wins
   buffer — behavior delta, benign under determinism (A2), arguably a robustness improvement
   (prevents transient durable regression during replay under LWW), but undocumented: the design doc
   says the fence is live "only for the offset-carrying KafkaSnapshot / compare-and-set wiring",
   which conflates snapshot-type with write-mode.
**Fix candidate F2:** wire by mode — `compareAndSet=false` ⇒ `SnapshotsOf.backedBy(db)` (exact master
behavior); `true` ⇒ `db.snapshotsOf`. Needs a wiring seam in `PersistenceModule` (overridable
`snapshotsOf` member) or a mode-aware module in `CassandraPersistence`. **Verdict: FINDING → fixed
(F-2): mode-scoped `snapshotsOf` hook; `CassandraPersistenceWiringSpec` pins fenced-per-mode.**

## S10 — Determinism violated (A2 broken): blast radius

If folds are non-deterministic: (i) equal-offset replacement can change contents (E2's records-equal
argument still bounds the *events*, but tick state diverges — doc already says this); (ii) monotonic
buffer drops a replay re-derivation that would have *differed* — the durable keeps the older
derivation; recovery point unaffected, no committed events lost — still safe by the offset argument;
(iii) LWW comparison: master would have overwritten with the new derivation. Net: CAS mode under
broken A2 degrades to "first derivation at an offset wins", never loses offsets. **Verdict: closed —
safety does not rest on A2; only value-level reproducibility does. Worth one sentence in doc.**

## S11 — `Offset` boundary values

`Offset.min` = 0 sentinel eliminated by `deef301` (explicit `Option`) ✅. Tombstone at `Offset.min`:
`put` lift uses `max` — fine. `Handover c ∈ 0..offset` covers 0. **Closed.**

## S12 — `SnapshotWriteConflict` swallowed anywhere?

Grep: raised in `resolveConditional`/`deleteCompareAndSet`; no `recover`/`handleError` on the persist
path in core/persistence-cassandra main code; `GroupCommit` is Kafka-only. Flow teardown semantics
per doc. **Closed (grep + reading; FlowSpec IT exercises the teardown).**

## S13 — `initPersisted` bypasses the monotonic cell discipline — **FINDING, code/comment contradiction**

`Snapshots` class doc: "Every write (`append`, `delete`) and recovery (`read`) flows through that one
cell, kept monotonic"; `put` is called "the single monotonic write site". But `initPersisted` does
`state.set(Cell(Live(...), persisted=true))` **unconditionally** — a second, non-monotonic write site.
Reachable clobber: events-recovery seeds a tombstone floor `X` via `Snapshots.read`; the journal fold
then returns a state (journal not cleared — e.g. S4 partial delete failure, or journal written before
a crash) at offset `X̃ < X`; `Persistence.read`'s `flatTap` calls `initPersistedState` ⇒ cell reset to
`Live(X̃)` ⇒ floor lost ⇒ replay-window writes below `X` conflict again — the exact livelock K1/K5
fixed, resurrected through the back door. In snapshot-recovery mode it is harmless (init follows read
of the same snapshot at the same offset). **Fix candidate F3: route `initPersisted` through the
monotonic `put` (drop a below-floor init, preserving the floor cell), or floor-guard the `set`.**
**Verdict: FINDING → fixed (F-3): `initPersisted` routes through `put`; unit-tested and model-checked
(`MonotonicInit` / `cassandra_init_clobber`).**

## S14 — Poison-row handling (S1 follow-through, post ext(4) confirmation) — **Closed (fixed, F-1)**

Ext (4) CONFIRMED per-cell TTL + row-marker semantics (see external-semantics.md), with one addition
that reshapes the fix space: the first write is an `INSERT` — if executed by a no-TTL deployment it
leaves an **immortal row marker**, so even a delete that tombstones `created`/`metadata` cannot make
the row disappear when `offset` expires. Prevention inside the delete statement is therefore
insufficient; the read and write paths must tolerate `offset = null`:
- read: tombstone branch must decode `offset` as `Option`; null ⇒ treat as absent (guard expired ≡
  reaped) instead of throwing on decode.
- write: distinguish "row absent" from "row present, `offset` null" in the not-applied result if the
  driver exposes it (ext (2) pending; the experiment below settles it empirically); a null-guard
  repair write (`IF offset = null`) is the Paxos-safe resurrection path, else the key perma-conflicts
  on flush even after the read fix.
- docs: recommend configuring the TTL from the first deployment; enabling it later leaves immortal
  markers/cells for keys deleted after the switch.
**Experiment planned** (`SnapshotTtlEdgeSpec`, research branch): no-TTL instance persists k@10; ttl=1s
instance deletes k@11; wait past expiry; assert row visible with value=null, offset=null; record
current `read` (expected: decode failure) and `persist` (expected: SnapshotWriteConflict) behavior;
log the LWT not-applied result shape for the poison row vs a truly absent row (settles ext (2)/(3)
empirically).

**Experiment outcome (closes S1/S14).** Run against real Cassandra. The poison row is reachable
exactly as constructed, and the perma-conflict prediction CONFIRMED — but the read prediction was
**REFUTED, and reality is worse**: `row.decode[Offset]` on the null cell yields **0**, not an
exception, so the tombstone floor silently collapses to `Offset.min` — the deleted-key replay fence
is disarmed with no error signal at all. The not-applied result shape DOES discriminate the poison
row from a truly absent one (condition column present-with-null vs absent from the metadata),
settling ext(2)/(3) empirically and later re-confirmed at Cassandra source level
(external-semantics.md). Fixed as findings **F-1** (`6053bb5`): guard-expired row reads as absent,
delete on it is an idempotent no-op, persist claims it via the Paxos-safe `IF offset = null` repair;
`SnapshotTtlEdgeSpec` (now on the cassandra branch) pins the state and the repair end to end.
**Closed (fixed).**
