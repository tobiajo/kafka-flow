# TLA+ models

Formal models backing the single-writer snapshot protections for
[#732](https://github.com/evolution-gaming/kafka-flow/issues/732) (a stale partition owner
overwriting a newer snapshot during a rebalance overlap). They are developer/reviewer artifacts; the
prose rationale is in [`docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md)
and [`docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md).

The suite is organised as **one abstract specification with everything else a refinement of it**
(Specifying Systems, Sec. 5.8): correctness of a backend *means* `Backend ⇒ SingleWriterStore` —
checked in TLC by a refinement mapping. Each backend is a self-contained, faithful spec that models
its real mechanism (so its hazards are reachable, not abstracted away — Sec. 7.6); a rejected design
is a spec whose refinement theorem is *false*; a genuinely finer grain of one operation is a
grain-of-atomicity refinement underneath a backend (Sec. 7.3).

## The tower

```
SingleWriterStore — THE spec
  the durable store is always a correct, non-stale fold
  (INV_DurableCorrect, SafeSpec, LIVE_Progress)
  │
  │   a backend is correct iff  Backend ⇒ SingleWriterStore  (checked by a refinement mapping)
  │
  ├─ Cassandra  ✓   offset compare-and-set + offset-carrying tombstone + replay-window monotone buffer
  │    └─ CasFirstWrite   the non-atomic first-write compound ⇒ one atomic CAS (grain of atomicity)
  │
  ├─ Kafka      ✓   consumer-generation fence + capture-coupling + offset seed + atomic offset binding
  │    └─ GroupCommit     write orchestration: termination + offset ordering
  │
  └─ Epoch      ✗   REJECTED: producer epoch / stable transactional.id (the theorem is false)
```

`✓` marks a refinement theorem that holds; `✗` one that *fails* (TLC returns a counterexample). The
backends differ only in the fence — Cassandra a per-key offset compare-and-set, Kafka the consumer
generation (KIP-447) — and each models its mechanism faithfully enough that removing it is a
reachable refinement violation. `CasFirstWrite` and `GroupCommit` are the finer-grained models, under
Cassandra and Kafka respectively.

The **replay window** (after a handover the new owner resumes at a committed offset below the durable
snapshot, then replays up) is a *shared* kafka-flow mechanism — **both** backends have it. Each has a
*different* protection, and removing each produces a *different* failure:

- **Cassandra** can't bind the snapshot and the consumer offset atomically (they live in different
  stores), so the window is unavoidable. Its offset-CAS rejects the legitimate replay flush → the flow
  tears down, re-recovers the same snapshot, resumes below it and conflicts again: a
  conflict→recover→retry **livelock** (`cassandra_replay_fixoff`). The monotone buffer (`Fix`) holds the
  high-water so the replay write is dropped instead of conflicting.
- **Kafka** has no offset gate (and its monotone buffer is inert), so a replay flush below the snapshot
  would **regress** it — silent data loss, *worse* than the livelock. Its protection is the atomic
  binding: the snapshot write and the input-offset commit ride one transaction. `AtomicBind` models
  that binding **faithfully, commit lag included**: a flush's transaction commits the offset scheduled
  *before* it (`scheduled` = `offsetToCommit`), so the committed offset genuinely trails the newest
  snapshot by up to the in-flight round and the replay window is *real* — what holds is the honest
  pair: committed never **leads** the snapshot (`INV_CommittedNeverAhead`) and the offset-only marker
  lane closes the lag (`LIVE_CommitCatchesUp`), with the window's replayed events dropped by the
  recovered-snapshot filter (`SnapshotFold` — the `ReplayFilter` knob). Three paired controls:
  `kafka_lag_nofilter` (filter off — the owner re-folds events already inside its recovered base and
  flushes double-folded contents below the snapshot), `kafka_replay_unbound` (no binding — a plain
  produce is gated by nothing, a zombie's write regresses the topic), and `kafka_replay_unbound_gap`
  (the idealized `INV_NoReplayGap`, kept as a named negative rather than a silent overclaim).

Same shared window, two protections, two failure modes — which is why the `offset max highWater` buffer
lives in shared snapshot code (it's Cassandra's protection) while Kafka leans on `sendOffsetsToTransaction`.

## Map

| Model | Role | What it abstracts (code) |
|---|---|---|
| [`SingleWriterStore`](#singlewriterstore) | the spec | a per-key durable cell that only advances and always equals the correct fold |
| [`Cassandra`](#cassandra--kafka) | refinement | the offset-gated LWT + offset-carrying tombstone; the owner folds onto its recovered base, and replays a recovered snapshot through the monotone buffer (`offset max highWater`); with `EventsRecovery` the owner recovers from the *journal* — a second, unfenced state source (`restoreEvents`) modelled as a **row set** (`journalRows`, so a stale residue below a tombstone is representable), folded onto the fenced store's snapshot with rows at or below the store's offset filtered out (`FloorFilter` ↔ the offset-vs-floor skip in `ReadState`) — the guard that holds at every recovery, not just the first |
| [`Kafka`](#cassandra--kafka) | refinement | the generation-fenced transaction, with capture coupled to teardown, the offset seed, and the input-offset commit bound atomically into the snapshot write — which closes the replay window (without it the owner's re-flush regresses the snapshot) |
| [`Epoch`](#epoch) | rejected | producer-epoch fencing **as the fence** (a stable `transactional.id` with nothing else) — the theorem is false |
| [`TokenSync`](#tokensync) | equivalence (Kafka) | the owner's published generation token under two bump kinds (callback-firing vs silent) and two sync mechanisms (capture vs post-poll refresh): refresh subsumes capture |
| [`RecoveryRead`](#recoveryread) | semantics (Kafka) | the recovery read's bound against transactional log semantics (LSO vs high watermark; unique vs stable `transactional.id`): finding F-10 and its two remedies |
| [`CasFirstWrite`](#casfirstwrite) | grain of atomicity | the non-atomic first-write compound (`UPDATE` → `INSERT IF NOT EXISTS` → retry) ⇒ one atomic CAS |
| [`GroupCommit`](#groupcommit) | finer (Kafka) | the write orchestration (lock + queue + per-item `Deferred`): termination + offset ordering |
| [`FlushCell`](#flushcell) | assumption control | the non-atomic `flushCell` compound (read → `database.write` → `markPersisted`): the paired control for the per-key serialization assumption (A4) |

## Running

Needs a JRE. `tla2tools.jar` is downloaded to this folder on first run if missing (git-ignored),
pinned to **v1.7.0 = TLC 2.16** — the version the suite is verified against, and the one the outcome
matchers target (for 2.16 `run.sh` accepts the unnamed temporal-violation report when a config declares
exactly one temporal property). Newer TLC (v1.8.0 / 2.18) is not a drop-in — its output tripped every
matcher — so bump `JAR_VERSION` in `run.sh` only alongside a full suite re-run. Then:

```sh
./run.sh             # check every config; one PASS/FAIL line each, non-zero exit on any failure
./run.sh refines     # only configs whose name contains the filter
```

The layout is **flat**, but cross-module: the three backends `EXTENDS SnapshotFlow`, a base module
holding the parts that are *identical* across them — the durable cell type (`Absent`/`Snap`/`Tomb`),
the correct fold (`CorrectContents`), and the `SingleWriterStore` refinement mapping (`AbsCell`, the
`SWS` instance, the `Ref*` aliases) over the shared `op`/`store` variables. Each backend adds only its
own variables, fence and actions — which is where they genuinely differ, so those stay per-backend.
Each config declares its module and expected outcome inline:

```
\* spec: Cassandra                       -- the module to run
\* expect: HOLDS                          -- model checking completes with no error
\* expect: VIOLATES INV_Some               -- a state invariant is violated (a safety control)
\* expect: VIOLATES-TEMPORAL Prop          -- a temporal property is violated (a liveness control)
\* expect: VIOLATES-REFINEMENT Prop        -- the refinement (step simulation) fails: Impl does NOT
                                             imply the spec (a rejected design, or a removed fence)
\* flags:  -deadlock                       -- optional; for a HOLDS run that reaches a terminal state
```

What makes the suite trustworthy is **pairing**: almost every theorem/invariant that should hold has a
sibling config that flips one knob and makes it *fail* (a removed guard / tombstone / fence / fix /
fairness), and the refinement check has its own control (`casfw_refines_vacuous`: a deliberately
mismatched impl/spec pair must fail the mapping). The suite is **61 configs, 35 of them expected
failures**. The most recent additions — the `tokensync_*` capture-vs-refresh 2×2 (+ equivalence), the
`gclanes_*` two-lane GroupCommit controls, the two `*_mo4`
higher-bound events configs, the `flowsalive_*` teardown-coupling controls, and the `recoveryread_*`
recovery-bound corners (F-10) — are grouped under
*Additional coverage* below but are full members of the suite.

## What each model checks

### `SingleWriterStore`

The abstract spec. State a reader observes: a per-key durable snapshot `durable[k]` and the offset it
was folded to, `hwm[k]`. One action `Commit` jumps `hwm` to a later offset and sets `durable` to the
correct fold. `INV_DurableCorrect == durable[k] = CorrectFor(k, hwm[k])` captures both hazards at
once — a stale overwrite regresses `hwm` (fails `SafeSpec`), corrupt contents make
`durable ≠ CorrectFor` (fails `INV_DurableCorrect`). `LIVE_Progress`: every key's whole log
eventually becomes durable.

| Config | Shows | Outcome |
|---|---|---|
| `sws_holds` | the spec is self-consistent (correct fold + progress) | HOLDS |

### `Cassandra` / `Kafka`

Self-contained specs that fold the input into a snapshot and write through their fence, with a
refinement mapping to `SingleWriterStore` (`RefSafeSpec` = step simulation, `RefDurableOK` = mapped
invariant, `RefLive` = mapped liveness). Each models its mechanism faithfully, so its hazards are
reachable: `Cassandra` folds onto its *recovered base* (so a row-removing delete + revive corrupts
contents), gates on the stored offset, and replays a recovered snapshot below the committed offset
(the replay window — the `Fix` monotone buffer, or a conflict→recover livelock without it); `Kafka` *captures* the
live generation in a rebalance callback (coupled to teardown), seeds the offset, and binds the
input-offset commit atomically into the snapshot write (`AtomicBind`) — which closes the replay window
that `Kafka` otherwise has too (without the binding the owner's re-flush below the snapshot regresses it).

| Config | Shows | Outcome |
|---|---|---|
| `cassandra_refines` | offset CAS + tombstone + replay monotone buffer implements the spec (safety + durable + liveness) | HOLDS |
| `cassandra_unguarded` | the offset guard removed | VIOLATES-REFINEMENT `RefSafeSpec` |
| `cassandra_notomb` | a row-removing delete: zombie revives, owner folds onto the stale base | VIOLATES `INV_NoCorruptDurable` |
| `cassandra_skiptomb` | the F-9 never-persisted-delete resurrection (`SkipTomb=TRUE`): a key created and deleted before it was ever durably persisted skipped the tombstone write (deferred flush / `persist=false`) while the consumer offset committed past the delete, so a revoked zombie flushes its buffered pre-delete snapshot onto the un-fenced absent row — resurrecting the deleted key below the committed delete offset. Invisible to the `store.offset`-keyed invariants (the revived cell is a self-consistent fold at its own offset), so `INV_NoResurrection` keys off `committed`. Closed in code by always writing the offset-carrying tombstone on a fenced delete (`SkipTomb=FALSE` — the paired positive `cassandra_refines`, where `INV_NoResurrection` HOLDS) | VIOLATES `INV_NoResurrection` |
| `cassandra_replay_fixoff` | the replay monotone buffer removed: a legitimate owner livelocks (conflict→recover→retry, never commits; the zombie is still correctly rejected) | VIOLATES-TEMPORAL `RefLive` |
| `cassandra_replay_fixoff_safe` | the safety half of the same claim: with the buffer removed nothing unsafe becomes durable — the livelock is purely a liveness failure | HOLDS |
| `cassandra_reap` | the TTL boundary, stated: a whole-row reap removes the guard with the row, so a later lower-offset write is accepted — the guarantee holds only within the TTL | VIOLATES-REFINEMENT `RefSafeSpec` |
| `cassandra_tombstone_replay` | the same livelock for a *deleted* key: recovery does not surface the tombstone's offset as the buffer floor (`TombFloor` off), so the legitimate owner re-derives below it and self-fences (the zombie is still correctly rejected) | VIOLATES-TEMPORAL `RefLive` |
| `cassandra_init_clobber` | the same livelock through the post-read *init*: the journal fold is init'd as an already-persisted cell without the monotonic put (`MonotonicInit` off), so a journal trailing the snapshot store regresses the just-seeded floor (`Snapshots.initPersisted` routed through `put` closes it) | VIOLATES-TEMPORAL `RefLive` |
| `cassandra_firstwrite_spurious` | the first-write compound's spurious conflict fed into the conflict/recover loop: leaves the row absent → no replay window → converges (the contrast to the livelock above) | HOLDS |
| `cassandra_events_refines` | events-recovery (`restoreEvents`) composed with the fence still implements the spec: the journal modelled as a **row set** folded onto the fenced store base, the fenced floor read (`TombFloor`), the journal-first flush ordering (`JournalOrder`), and the **offset floor filter** (`FloorGuard`+`FloorFilter` ↔ `ReadState`'s skip of rows ≤ the store offset) | HOLDS |
| `cassandra_events_journal_revive` | **the journal revive**, unguarded (`FloorGuard` off): the journal is unfenced, so a zombie's delete racing a not-yet-fenced owner's replayed appends leaves pre-delete rows in the journal; the next events-recovery folds them back to life and persists forward — durable resurrection with correct-looking offsets. Exists under last-write-wins too (there unfixable — no trustworthy comparator); the fence can guard it | VIOLATES `INV_NoCorruptDurable` |
| `cassandra_events_revive_reentry` | **the revive re-entry** (F-7) — the negative control for the fold-*result* comparison (`FloorGuard` on, `FloorFilter` off) the first fix attempted: it discards a fold that trails the store, but once legitimate post-delete events advance the journal to/past the store offset the polluted fold no longer trails and sails through, resurrecting at the *second* recovery. Offset is not provenance; the structural offset filter (`cassandra_events_refines`) is what holds at every recovery | VIOLATES `INV_NoCorruptDurable` |
| `cassandra_events_nofloor` | the deleted-key floor read dropped from events-recovery (`TombFloor` off): the fold yields nothing, the owner re-derives below the tombstone and self-fences — the `cassandra_tombstone_replay` lasso reached through the events composition | VIOLATES-TEMPORAL `RefLive` |
| `cassandra_events_unordered` | the `journals.flush *> snapshots.flush` ordering broken (guard also off): a live key's journal lags its own durable snapshot, the owner recovers below it and self-fences. The guard subsumes the ordering for correctness (with `FloorGuard` on this converges) — the ordering is what makes events-recovery worth having, the guard is what makes it safe | VIOLATES-TEMPORAL `RefLive` |
| `cassandra_events_unordered_guarded` | the same broken ordering but with the floor filter on (`FloorGuard`+`FloorFilter`): a lagging journal falls back onto the fenced snapshot base and converges — the guard subsumes the ordering, checked not asserted | HOLDS |
| `kafka_refines` | the generation fence (coupled + seeded) implements the spec | HOLDS |
| `kafka_decoupled` | capture decoupled from teardown — a stale flow keeps flushing | VIOLATES-REFINEMENT `RefSafeSpec` |
| `kafka_decoupled_coupling` | the same cause at the coupling invariant | VIOLATES `INV_CaptureCoupled` |
| `kafka_unseeded` | the offset-to-commit left unseeded (first flush ungated) | VIOLATES-REFINEMENT `RefSafeSpec` |
| `kafka_replay` | the binding, faithfully — the one-round commit lag makes the replay window real; committed never leads the snapshot, the marker lane closes the lag, the filter drops the replayed events | HOLDS (`INV_CommittedNeverAhead`, `LIVE_CommitCatchesUp`) |
| `kafka_lag_nofilter` | the recovered-snapshot filter removed: the owner re-folds events already inside its recovered base and flushes double-folded contents below the snapshot | VIOLATES-REFINEMENT `RefSafeSpec` |
| `kafka_replay_unbound` | the atomic binding removed: the window opens and the owner's re-flush below the snapshot regresses it (no offset gate) — silent data loss | VIOLATES-REFINEMENT `RefSafeSpec` |
| `kafka_replay_unbound_gap` | the same cause pinned at the invariant: without the binding `INV_NoReplayGap` itself is violated (so it cannot pass vacuously) | VIOLATES `INV_NoReplayGap` |
| `kafka_genlag` | the owner-side token lag without the post-poll refresh (`Refresh` off): a no-assignment generation bump (cooperative assignor) fires no callback, the owner's token lags, the broker spuriously fences the legitimate owner, and the teardown/recover re-captures nothing — a livelock. The post-poll refresh closes it (`kafka_refines`, `Refresh=TRUE`); lag is the safe direction, so the fence is untouched | VIOLATES-TEMPORAL `RefLive` |

### `Epoch`

The rejected design — producer-epoch fencing **as the fence**: a stable `transactional.id` whose epoch
order is the only thing gating writes. Epochs are assigned in `initTransactions` arrival order,
independent of ownership, so a late-initialising stale owner wins the epoch and its stale write lands.
Its refinement theorem is *false*. Note what is rejected: epoch order as the *safety* mechanism. The
adopted design does use a stable per-partition `transactional.id` — for takeover-abort of a crashed
owner's dangling transaction (`RecoveryRead`, finding F-10) — but safety still rests on the generation
bound into every commit; the late-init race this model exhibits is then availability-only (the fenced
true owner crashes, restarts, re-inits) because the stale write still dies at the generation fence.

| Config | Shows | Outcome |
|---|---|---|
| `epoch_refines` | `Epoch ⇒ SingleWriterStore` does not hold (the stale write lands) | VIOLATES-REFINEMENT `RefSafeSpec` |

### `RecoveryRead`

The snapshot-topic recovery read against Kafka's transactional log semantics (finding F-10): the log as
one-record transactions (committed / open / aborted), the LSO as the offset before the first open
record (what `read_committed` `endOffsets` returns), the read's completion blocked by any open record
below its bound (the `read_committed` position cannot pass one), and the broker's timeout abort. The
double-handover scenario: owner A commits S1, opens a transaction, hard-crashes; owner B (whose
**mandatory** `initTransactions` aborts A's transaction iff the id is stable — same partition, same id)
commits the newer S3 and crashes leaving its own transaction open; reader C recovers. Isolated from the
refinement tower, like `TokenSync`; the correctness bar is `INV_ReadsAllCommitted` — the completed read
returned every committed record.

| Config | Shows | Outcome |
|---|---|---|
| `recoveryread_lso_unique` | F-10 as found: unique per-assignment ids (nobody aborts A's transaction) + the read bounded at its own `read_committed` `endOffsets` — the LSO, pinned below the committed S3; the read completes early, silently missing it (live-replicated: an 85 ms under-read) | VIOLATES `INV_ReadsAllCommitted` |
| `recoveryread_hw_unique` | remedy (1): the high-watermark bound (an uncommitted-isolation lens) makes the read *wait* the open transaction out — Kafka Streams' restore shape (KAFKA-10167, ext(K6)), mandatory under an id scheme that cannot takeover-abort | HOLDS |
| `recoveryread_lso_stable` | remedy (2), adopted: with a stable per-partition id, B's mandatory init aborts A's transaction *before B writes*, so a committed record above an open transaction is unreachable in the lineage (`INV_LineageSerialized`) — the plain LSO bound is complete with no wait and no reader-side ordering assumption (C completes at B's dangling transaction, missing nothing) | HOLDS |
| `recoveryread_lso_foreign` | remedy (2)'s residual as a negative control: an open transaction from *outside* the id lineage (the shared-snapshot-topic misconfiguration the docs exclude) re-pins the LSO below committed records — the one-topic-one-flow discipline is load-bearing, and no read-bound choice absorbs it (the HW bound would only turn silent into slow) | VIOLATES `INV_ReadsAllCommitted` |

### `CasFirstWrite`

The one place a persist is *not* one atomic CAS: the `UPDATE` → `INSERT IF NOT EXISTS` → retry
compound. A finer grain of atomicity (Sec. 7.3), discharged by refinement to the atomic CAS
`CasFirstWriteAtomic`. The refinement leaves exactly one deviation — a *spurious* conflict (the retry
finds the row gone) mapping to a give-up — whose recovery this module cannot check (a conflict is
terminal here). That recovery is checked in `Cassandra`'s `cassandra_firstwrite_spurious`: fed into the
conflict/recover loop the spurious conflict leaves the row absent, so no replay window arises and it
converges — confirming the "recovers on its next flush" claim rather than asserting it.

| Config | Shows | Outcome |
|---|---|---|
| `casfw_guarded` | the compound, guarded | HOLDS |
| `casfw_unguarded` | ungated UPDATEs | VIOLATES `INV_NoStaleOverwrite` |
| `casfw_reap` | a TTL reap mid-protocol | HOLDS |
| `casfw_spurious` | the spurious-conflict path is reachable (liveness-only) | VIOLATES `INV_NeverSpurious` |
| `casfw_3w` | three concurrent first-writers | HOLDS |
| `casfw_refines` | the compound refines the atomic CAS | HOLDS (`RefinesAtomic`) |
| `casfw_refines_vacuous` | the refinement check itself is non-vacuous: the ungated compound against the *guarded* atomic spec (`SpecGuarded` decoupled from `Guarded`) must fail the mapping — a mapping that let this pass would be accepting anything | VIOLATES-REFINEMENT `RefinesAtomic` |

### `GroupCommit`

Under Kafka: the write orchestration (one-permit lock + FIFO queue + per-item `Deferred`). It
terminates (every write's outcome delivered, no deadlock) and the committed offset never leads the
durable write prefix.

| Config | Shows | Outcome |
|---|---|---|
| `gc_cap1` / `gc_cap2` / `gc_cap3` | no batching / partial batch / full drain | HOLDS (`Termination` + `INV_OffsetWithinDurable`) |
| `gc_ungated` | the flush-blocks-then-schedule coupling dropped | VIOLATES `INV_OffsetWithinDurable` |
| `gc_nofair` | the safety-only spec, no fairness (engine control) | VIOLATES-TEMPORAL `Termination` |

### `FlushCell`

The paired control for the **serialization** assumption (A4 below) — the one load-bearing assumption
that otherwise had no checked sibling. `Snapshots.flushCell` is a non-atomic compound of three separate
register ops: read the cell → `database.write` the value → `markPersisted` the buffer. A concurrent
`Append` landing *between* the write and the mark rebinds the buffer to a value that `markPersisted`
then marks persisted though the durable store never received it — a **lost write**. The poll-thread
serialization is what forbids that interleaving (`Serialized ⇒` an `Append` only runs when the flush is
not mid-compound). This does **not** discharge A4 (TLC cannot see JVM threads); like every pairing in
the suite it proves the hazard the assumption rules out is *real*, so the assumption is a checked choice
rather than a silent article of faith.

| Config | Shows | Outcome |
|---|---|---|
| `serial_holds` | serialized (`Serialized=TRUE`): no `Append` interleaves the compound, so it is effectively atomic | HOLDS (`INV_NoLostWrite`) |
| `serial_race` | serialization removed (`Serialized=FALSE`): an `Append` lands between the write and the mark, and `markPersisted` marks a value durable that was never written | VIOLATES `INV_NoLostWrite` |

## Additional coverage

These close residuals the study named but had earlier only argued. Grouped separately because they
arrived after the core suite, but full members of it (counted in the 61 above, run by `run.sh`).

### `GroupCommitLanes` — the two-lane write orchestration (closes G1/G2)

Sibling to `GroupCommit` modelling the writer's *two* real lanes — `writes` (bounded by `Cap` =
`maxWritesPerTransaction`) and the **unbounded** offset-only `markers` — over the shared `offsetToCommit`,
plus the abort path. Turns the previously argued-unverified G1/G2 into checked properties: **G1** — markers
never steal a write slot (`INV_NoSlotSteal`) and a marker after the last write batch is not stranded
(`LIVE_MarkersNotStarved`); **G2** — the committed offset never leads the committed-durable prefix
(`INV_OffsetWithinDurable`), even under the marker/write race and an abort.

| Config | Shows | Outcome |
|---|---|---|
| `gclanes_holds` / `gclanes_cap1` | both lanes, gated, atomic durability | HOLDS (`INV_OffsetWithinDurable`, `INV_NoSlotSteal`, `Termination`, `LIVE_MarkersNotStarved`) |
| `gclanes_shared` | markers share the write budget (`SharedBudget`) | VIOLATES `INV_NoSlotSteal` (a marker cuts the write batch) |
| `gclanes_starve` | markers lack their own trigger (`MarkerSelfCommit=FALSE`) | VIOLATES-TEMPORAL `LIVE_MarkersNotStarved` (a post-last-write marker stranded) |
| `gclanes_ungated` | the flush-before-schedule coupling dropped | VIOLATES `INV_OffsetWithinDurable` |
| `gclanes_abort_holds` | aborts enabled, durability observed atomically | HOLDS |
| `gclanes_abort_race` | aborts enabled + durability observed *eagerly* (`AtomicDurable=FALSE`) | VIOLATES `INV_OffsetWithinDurable` (a marker commits an offset scheduled against a write that then aborts) |

### `cassandra_events_refines_mo4` / `cassandra_events_revive_reentry_mo4` — higher bound (MaxOffset=4)

`MaxOffset=4` companions to the `cassandra_events_*` F-7/F-9 pair. Finding B-1 showed the *genuine*
delete-residue revive is reachable only at `MaxOffset≥4` (at 3 only its journal-loss dual is), so these
confirm the FloorFilter fix is non-vacuous at the bound where the flagship hazard actually appears:
`cassandra_events_refines_mo4` HOLDS, `cassandra_events_revive_reentry_mo4` VIOLATES `INV_NoCorruptDurable`.
Costlier than the `MaxOffset=3` configs, so they complement rather than replace them.

### `FlowsAlive` — the cross-partition teardown coupling (closes the flows-alive residual)

The Kafka fence (KIP-447) validates member + generation but does **no per-partition ownership check**,
so the only thing stopping a lingering flow for a no-longer-owned partition from committing under a
fresh token is that revoke *tears the flow down before the node acts in the new generation*. In code
that is `TopicFlow.remove` awaiting `cache.remove(_).flatten` inside the (synchronous, pre-assign)
revoke callback. The model makes teardown an interleavable action gated by one knob, `AwaitTeardown`,
and checks the invariant **as safety** (`INV_FlowsAlive == live ⊆ owned`) — because a single un-owned
commit corrupts, *eventual* removal is not enough.

| Config | Shows | Outcome |
|---|---|---|
| `flowsalive_holds` | `AwaitTeardown=TRUE` — revoke awaits teardown before the new generation | HOLDS (`INV_FlowsAlive`) |
| `flowsalive_race` | `AwaitTeardown=FALSE` — fire-and-forget teardown lingers past the reassignment | VIOLATES `INV_FlowsAlive` (reassign `{p1}→{}` leaves the p1 flow alive-but-un-owned) |

The negative shows the awaited coupling is load-bearing, not incidental — a future fire-and-forget refactor
would reintroduce the exact race. In-code complement: a unit test (`TopicFlowSpec`, "remove awaits the flow
teardown", on the Kafka branch) adds then removes a partition whose flow release completes a `Deferred` and
asserts it is completed by the time `remove` returns, so a fire-and-forget refactor fails the build.
Modelled here, pinned by the test there.

*(Related artifacts outside `models/`: `.github/workflows/models.yml` runs this suite on TLC 2.16 in CI.
The JVM counterpart of the `FlushCell`/A4 model — `FlushCellConcurrencySpec` — and the F-9
replay-of-a-reaped-tombstone IT ship with the code they protect on the cassandra branch, not here.)*

### `TokenSync` — capture vs. refresh (why the refresh subsumes the assignment capture)

A standalone model (it does not extend `SnapshotFlow`; it tracks only the owner's published generation
token) added by the `group.protocol=consumer` experiment to settle whether assignment-callback **capture**
and the post-poll **refresh** are equivalent. The coordinator's generation advances by two free actions —
`AssigningBump` (a callback fires) and `SilentBump` (none — the KIP-848 case where the member keeps its
partitions) — so, like `Kafka.tla`'s `GenBump`, the advance is a background one, not poll-synchronous. Two
knobs sync the token: capture (callback-driven, so it can act only when a callback is owed) and refresh (a
read, which sees the current generation regardless). The liveness target is `Synced == <>[](token = liveGen)`.
(`TokenNeverLeads == token <= liveGen` holds in every config, but that is structural — `token` is only ever
assigned the current `liveGen` — so it is a sanity check of this model, not a safety *result*; the real
stale-write safety lives in `Kafka.tla`, where a zombie can actually land a write. The capture/refresh
asymmetry the outcomes turn on is *assumed* here, faithful to the code, not derived — so this model checks the
reasoning's consistency, it does not independently prove the Kafka premise.)

The 2×2 over {capture, refresh}, all with a silent bump reachable (the KIP-848 regime):

| Config | Mechanism(s) | Outcome |
|---|---|---|
| `tokensync_capture` | only capture | VIOLATES-TEMPORAL `Synced` |
| `tokensync_refresh` | only refresh | HOLDS |
| `tokensync_both` | both | HOLDS |
| `tokensync_neither` | neither (control) | VIOLATES-TEMPORAL `Synced` |

and the boundary case — capture alone, but with *no* silent bump (every bump fires a callback):

| Config | Mechanism(s) | Outcome |
|---|---|---|
| `tokensync_capture_assigning` | only capture, callbacks always fire | HOLDS — capture ≡ refresh here |

Capture-on-finished-rebalance and refresh are **equivalent when every bump fires a callback** (eager, or a
581-fixed classic cooperative one); refresh **subsumes** capture, because only the read catches a silent bump.
That is the model-level complement to removing capture-on-assign (F-8 corollary) — the empirical/code evidence
is what carries it: `Consumer.scala` (capture removed) with the models-branch unit suites green (core 121/121,
persistence-kafka 14/14; 82 + 12 on the experiment branch), and the `Kip848ConsumerProtocolSpec` silent-bump
case against a real 4.3.0 broker. `Kafka.tla` retains capture as the shipped design of record.

## Assumptions (verified *under*, not proven)

Per-key linearizable writes (each backend's primitive is one atomic register op — the first-write
compound is the one place this is modelled, `CasFirstWrite`); deterministic, replayable folds (a
user contract, so a value is a function of its offset); poll-thread serialization of rebalance
callbacks and a key's processing — **A4**, the basis of `Kafka`'s capture-coupling and of treating
`flushCell`'s read→write→mark compound as atomic (its paired control is `FlushCell`: remove
serialization and the compound loses a write, `serial_race`); and the KIP-447 broker fence. The models
verify behaviour under these; they do not re-derive them.

## Triggers are abstracted, not modelled

A write or delete in kafka-flow can be *triggered* many ways — per record, a periodic timer, an
idle-unload timer, or flush-on-revoke (`TimerFlowOf`). The models never model the trigger, only the
**effect**: which writes/deletes can become durable, at what offset, in what order. Every effect is
reachable by the nondeterministic enabling of the write / delete / `OwnerRecover` / `Handover`
actions, so any concrete trigger schedule is just one path through them. This rests on the
serialization and determinism assumptions above: a key's timer and its processing never run
concurrently, and a re-fold reproduces the same value. A mechanism earns its own modelling only when
it **widens that effect envelope** (a write at a new offset / content / ordering) or **breaks an
assumption** — not merely because it is a distinct code path. Three cases worth naming, all covered:

- *flush-on-revoke* (`flushOnCancel`: `persistence.flush *> context.remove`) — the revoked owner
  writing during the handover overlap. This is the zombie write / capture-coupling, modelled directly.
- *idle-unload* (`unloadOrphaned`: `persistence.flush *> context.remove`, where `KeyContext.remove`
  drops the key from memory with **no** database delete) — evicts a *live* snapshot, re-recovered on
  next access. It adds no reachable behaviour and is left out deliberately, not overlooked: resuming
  at the flushed offset is `Handover` with `c = stored.offset` (the no-gap case, whose live-snapshot
  reseed `cassandra_refines` already exercises), and the evict→recover gap with a concurrent zombie
  write is already reachable through the delete-eviction path — the *more* hazardous version, since it
  also writes a tombstone. Idle-unload is strictly dominated by it.
- *skip-tombstone delete* (`Persistence.delete` with `persist = false`, or `deleteCompareAndSet` on an
  absent row — clear the buffer, no DB write, but the consumer offset is **still committed**) — the
  **shipped** design always writes the offset-carrying tombstone (`Tomb(o)`, the real CAS-mode delete),
  which is what the default model does: the refinement mapping `hwm = stored.offset` needs it so an
  offset regression is caught as #732, and it also fences the delete against a zombie. The pre-fix
  no-op (F-9) is not a benign abstraction — it was a real resurrection hazard, kept here as a paired
  negative control behind the `SkipTomb` knob: with `SkipTomb=TRUE` a delete of a never-persisted key
  is the no-op (row stays Absent, offset committed past it), so a revoked zombie flushes its buffered
  pre-delete snapshot onto the un-fenced absent row and resurrects the deleted key — `INV_NoResurrection`
  fires (`cassandra_skiptomb`). With `SkipTomb=FALSE` (the fix) `INV_NoResurrection` HOLDS
  (`cassandra_refines`). The no-op would also let `stored.offset` lag `committed` and falsely fail
  `RefLive` for an absent-result key (e.g. all-deletes) — which is why the *refinement* configs keep
  `SkipTomb=FALSE` and the resurrection is checked by its own committed-keyed invariant, not `RefLive`.

The one thing that would force a trigger into the model is doubting serialization — e.g. a timer
firing *mid-flush* on another thread. That is a concurrency hazard (model the race), not a clock.
