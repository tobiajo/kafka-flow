# Model–code fidelity audit

*Shared apparatus — covers all implementations, sectioned by implementation: TLA+ model↔code fidelity audit and coverage gaps. The suite itself and how to run it: [`../models/README.md`](../models/README.md). Corpus index: [`README.md`](README.md).*

Method: every model action/definition mapped to the code construct it abstracts and verified by
reading both; every config's constants/expectations checked; pairing discipline audited. Run results:
all configs replicated (25 at this study's start; 38 after its additions; 61 now, with the later
`gclanes_*`/`*_mo4`/`flowsalive_*`/`tokensync_*` additions — see the suite ledger in `findings.md`), TLC
2.16, matching declared outcomes. The audit below merges an independent full-read audit with the orchestrator's
adversarial verification of its findings; dispositions state what was done.

This file is shared across implementations and its sections are thematic (faithful / findings / gaps /
addenda), so each implementation's model work is spread across them. Scope map for a reader who cares
about only one:

- **Cassandra models** (`Cassandra`, `CasFirstWrite`(`Atomic`), `FlushCell`): "Verified faithful"
  bullets 1 & 3; findings C1/C2, 7b, pairing holes #1/#2/#4, stale refs; coverage gaps
  events-recovery / `flushCell` / equal-offset / unfenced-wiring; both events-recovery addenda (F-6,
  F-7); and the F-9, A4, B-1 items of the advisory review.
- **Kafka models** (`Kafka`, `GroupCommit`, `GroupCommitLanes`, `Epoch`, `TokenSync`): "Verified faithful"
  bullets 2 (`GroupCommit`) & 4 (generation fencing); finding **K1** and pairing hole #3
  (`kafka_replay_unbound_gap`); coverage gap **G1/G2** (`GroupCommitLanes`); the **Epoch** rejected-design
  section; the "**K1 resolved**" and "**TokenSync**" addenda; and the −1-gap and group-commit×fence×lag
  items of the advisory review.
- **Shared / root**: `SingleWriterStore` + the `SnapshotFlow` mapping; the method note above and the
  advisory review's "Foundation held" paragraph.

## Verified faithful (spot list)

- `CasFirstWrite` ↔ `persistCompareAndSet`'s UPDATE → INSERT IF NOT EXISTS → retry-once compound,
  including equality admission and the reap/spurious knobs; the refinement to the atomic CAS
  (`casfw_refines`) is sound.
- `GroupCommit` ↔ the lock+queue+Deferred orchestration, including the batch-stranding subtlety its
  `Termination` property exists for; `co := oc` inside the masked transaction.
- `Cassandra` conflict semantics ↔ uncaught `SnapshotWriteConflict` ⇒ teardown ⇒ re-recover ⇒ resume
  from committed; `Handover` reaches the replay window rather than positing it.
- Kafka generation fencing ↔ `sendOffsetsToTransaction` with commit-time `groupMetadata` (equivalent
  to the model's captured generation **under the poll-thread serialization assumption**, which the
  README states).

## Findings and dispositions

**K1 — `AtomicBind`/`INV_NoReplayGap` overstated the code (confirmed against code).** A transaction
commits `offsetToCommit.get`, which holds the offset scheduled *before* this batch's writes (offsets
are scheduled only after a flush returns), so the committed offset can trail the newest snapshot by
one in-flight round: the replay window is *narrowed*, not closed; `Handover`'s `AtomicBind ⇒ c =
store.offset` is an idealization. What the binding really guarantees: the offset and the snapshot
move together or not at all (no committed-ahead gap; a stale generation lands neither). The residual
trailing window's re-flush is prevented by `SnapshotFold`'s recovered-snapshot filter — the mechanism
the Cassandra model *does* carry. **Disposition:** `Kafka.tla` and README now label the idealization
and state the real guarantee + residual protection (models commit "the binding's real strength").
Modelling the one-round commit lag explicitly was done subsequently — see the "K1 resolved — the
binding modelled faithfully" addendum below.

**C1/C2 — the model's `dropped` branch exempts two real code behaviours (confirmed).** The code drops
replayed live appends only *strictly* below the floor (re-writes at equality; the model's `<=` never
does), and a replayed delete is *persisted lifted to the floor* while the model drops it. Both
exemptions are sound only under the determinism/idempotence contract, and both are observationally
invisible to any later read/recovery (the lifted tombstone + replay filter ≡ the dropped write; the
equal-offset re-write differs only in unmodelled tick state). A literal model of the lift would fail
`INV_DurableCorrect` on a state no reader can distinguish — an artifact of mapping wall-clock deletes
onto the event log. **Disposition:** the exemptions and their soundness argument are now stated in
`Cassandra.tla` at the `dropped` definition instead of being silent.

**7b — no flow-level TTL reap (confirmed gap).** The documented "monotonicity only within the TTL"
boundary was checked nowhere above the first-write compound. **Disposition:** `ReapTTL` knob +
`cassandra_reap` config — the boundary as a checked *expected refinement failure* (a reap removes the
guard; a later lower-offset write is accepted; hwm regresses). Complements `casfw_reap` (in-protocol
safety).

**Pairing holes (confirmed).** (#2) "safety still holds" under `Fix=FALSE` was asserted in a comment,
never checked → `cassandra_replay_fixoff_safe` (HOLDS, safety-only). (#3) `INV_NoReplayGap`'s
binding-dependence was never pinned → `kafka_replay_unbound_gap` (VIOLATES). (#4) the
VIOLATES-REFINEMENT matcher accepted *any* action-property violation → run.sh now matches the
abstract module the declared alias instantiates. (#1) no negative control for `casfw_refines`
(a vacuous mapping would pass): originally accepted as low-value, **since closed** —
`CasFirstWrite`'s abstract instance takes its own `SpecGuarded` (normally = `Guarded`), and
`casfw_refines_vacuous` runs the ungated compound against the guarded atomic spec: the mapping must
fail (VIOLATES-REFINEMENT `RefinesAtomic`), so `casfw_refines` passing is evidence, not vacuity.

**Stale references (confirmed, fixed).** `Recovered.Deleted`/`SnapshotDatabase.recover` (pre-`Stored`
ADT) in `Cassandra.tla` and `cassandra_tombstone_replay.cfg`; `offsetOf = Offset.min` (removed
sentinel) in `Kafka.tla`.

## Accepted coverage gaps (with rationale)

- **The recovery read's bound (Kafka)** — *gap since closed; see the RecoveryRead addendum.* The tower
  models recovery as an atomic read of the modeled store, so the read's bound (LSO vs. high watermark
  under `read_committed`) did not exist at that abstraction and read-*completeness* held by
  construction — unfalsifiable, and it hid a real defect (F-10, the under-read past an open
  transaction). The lesson is recorded in F-10's detection post-mortem: an abstraction boundary is a
  coverage decision, and it belongs in this list at the time it is made, not after it bites.
  `RecoveryRead.tla` backfills the corner as a semantics model (the tower's store stays atomic — the
  bound question is separable and cleaner standalone).
- **Events-recovery (journal fold)** — *gap since closed; see the second addendum below.* Originally
  not modelled as a second state source; only the floor side was covered (`TombFloor`, and the study's
  `MonotonicInit`/`ReseedFloor` paired with code fix F-3), with the `journals.flush *>
  snapshots.flush` ordering verified by code reading only. Modelling it (the `EventsRecovery` /
  `JournalOrder` / `FloorGuard` knobs) both discharged the ordering claim as a checked negative
  control and surfaced a genuine defect the code-reading pass had missed (F-6, the journal revive).
  The `fenced` gate remains covered by a unit test (`ReadStateFloorGateSpec`), the right layer for a
  wiring fact.
- **`flushCell`/`markPersisted` non-atomicity** (7c): safe only under per-key serialization; the
  README names mid-flush timers as exactly what would force this into a model. Stated as assumption
  A4 in the claim inventory.
- **Equal-offset divergent contents** (7d): unrepresentable by construction (zombies write the
  deterministic fold); benignity rests on the stated user contract — the same contract the code rests
  on, so the model is not weaker than reality's guarantee.
- **Unfenced wiring as a surface** (7e): the mis-wiring hazard (fencing store behind an unfenced
  buffer) exists as knob analogies only; after fix F-2 the wiring is mode-scoped and pinned by an
  integration test (`CassandraPersistenceWiringSpec`), which is the right layer for it.
- **GroupCommit marker lane & abort paths** (G1/G2): were **argued-unverified** — the standing
  `GroupCommit` spec checks termination + offset ordering over a *single* lane, so it depends on
  neither the marker/write lane split nor the abort path, and no test was cited for either. **Now
  modelled** by `GroupCommitLanes.tla` (a sibling that splits the queue into the real
  bounded-`writes` / unbounded-`markers` lanes over the shared `offsetToCommit` and adds the abort
  action), which turns both into checked properties: **G1** markers never steal a write slot
  (`INV_NoSlotSteal`, paired negative `gclanes_shared`) and a post-last-write marker is not stranded
  (`LIVE_MarkersNotStarved`, paired negative `gclanes_starve`); **G2** the committed offset never
  leads the committed-durable prefix even under the marker/write race and on abort
  (`INV_OffsetWithinDurable`, paired negatives `gclanes_ungated` and `gclanes_abort_race`). The
  positives `gclanes_holds` / `gclanes_cap1` / `gclanes_abort_holds` HOLD; the configs are part of the
  suite. What the model does *not* prove is its own fidelity to `KafkaSnapshotWriteDatabase`: the lane
  mechanism is faithful by construction and reading, not by a refinement proof against the code.

## Epoch (rejected design) over-approximation

After `ZombieInit` bumps the epoch, the model's owner keeps writing where the real broker would fence
it — tolerable for a theorem meant to fail (the counterexample is reachable in the rejected design),
noted for precision. Scope note after the id-scheme change (F-10): what this model rejects is producer-epoch
order **as the fence**. The adopted design also uses a stable per-partition `transactional.id` — for
takeover-abort, not safety — so the negative control outlives the change: safety still never rests on epoch
order, and the late-init inversion exhibited here is availability-only under the generation fence (the
stale write dies at the offset commit).

## Addendum: events-recovery modelled as a second state source (F-6 found)

`Cassandra.tla` now carries events-recovery genuinely: under `EventsRecovery` the owner's recovered
state is the *journal fold* (`journalAt`/`foldedAt` — the fold's reach, separate from the buffer floor
`recoveredAt`, a distinction the first refinement failure forced), journal appends are **unfenced**
(they advance on a zombie's write even when the snapshot CAS rejects it — exactly the code's plain
inserts), a delete clears the journal, and `JournalOrder` is the `journals.flush *> snapshots.flush`
ordering. Composing this with the fence made TLC find a real defect the seam analysis had graded
"in-memory only" (D-2): the **journal revive** — a zombie's delete racing a not-yet-fenced owner's
replayed appends leaves pre-delete events in the journal; the next events-recovery folds them back to
life and persists forward durably (`cassandra_events_journal_revive`, VIOLATES `INV_NoCorruptDurable`;
reproduced against the code — findings F-6). The guard the model shape first suggested was
`Snapshots.reconcile` (a fold the fenced store provably leads is discarded) — **which was insufficient
and is superseded; see the F-7 addendum below.** This first model was a *scalar* journal (`journalAt`
folded by `CorrectContents`), and that is precisely why it could not catch F-7 — it encoded "the
journal is a correct prefix" by construction. The paired controls pin the floor read
(`cassandra_events_nofloor`) and the flush ordering (`cassandra_events_unordered`).

## Addendum: the journal modelled as a row set (F-7 — the review committee's finding)

The F-6 addendum's model, and the `reconcile` fix it certified, were both wrong at the second recovery.
A fresh-context review committee found **F-7**: `reconcile` compared the fold's *result* to the store
and discarded a trailing fold — but once legitimate post-delete events advance the journal to/past the
store offset the polluted fold no longer trails and the residue re-enters (durable corruption at the
second recovery). The scalar model could not exhibit this: `CorrectContents(journalAt)` *is* a correct
prefix by definition, so `cassandra_events_refines` HOLDING was vacuous w.r.t. a residue-carrying
journal — the abstraction assumed away exactly the defect.

`Cassandra.tla` now models the journal as a **row set** (`journalRows ⊆ Offsets`, `FoldRowsOnto`): a
zombie's unfenced persist inserts a row even below a durable tombstone (the residue), a delete clears
the rows, and recovery folds the rows under a three-mode knob — `FloorGuard=FALSE` (no guard, revive at
recovery #1), `FloorGuard=TRUE ∧ FloorFilter=FALSE` (the fold-result comparison — F-7 re-entry), and
`FloorGuard=TRUE ∧ FloorFilter=TRUE` (the adopted fix — fold only rows above the fenced floor onto the
store base). The new control `cassandra_events_revive_reentry` VIOLATES `INV_NoCorruptDurable` on the
fold-compare mode; `cassandra_events_refines` HOLDS under the filter — a **non-vacuous** pair, since the
reentry control proves the polluted state reachable in the shared state space. Reachability needs a
second recovery over an advanced store, so **M4** was taken up in full: Cassandra `Handover` is now a
`0..2` counter, and (Kafka side, same M4) the single-zombie scalars became functions over a bounded
zombie set with `Rebalance`/`GenBump`/`Handover` as `0..2` counters — two concurrent stale generations,
the fence still holds. Suite: 38 configs / 23 expected failures, all as declared (TLC 2.16). The lesson
is the study's own, turned on itself: a model that assumes the property it is meant to check certifies
nothing — the row-set remodel is what makes the events-recovery HOLDS mean something.

## Addendum: K1 resolved — the binding modelled faithfully

`Kafka.tla` no longer idealizes `AtomicBind`. The commit pipeline is modelled as the code has it: a
flush's transaction commits the previously **scheduled** offset (`scheduled` = `offsetToCommit`) and
schedules its own; the offset-only marker lane closes the residual lag; `Handover` resumes at
`committed` (which genuinely trails the snapshot by up to one round); the owner folds **batches**
(a single-event fold would understate the window to one offset and falsely make the filter look
redundant); the recovered-snapshot filter (`SnapshotFold`) is its own knob. Checked: `kafka_replay`
HOLDS with the honest pair (`INV_CommittedNeverAhead` — committed never leads the snapshot;
`LIVE_CommitCatchesUp`); `kafka_lag_nofilter` shows the filter is load-bearing (without it the owner
flushes double-folded contents below the snapshot — refinement fails); the ungated-produce reality of
the caching backend replaces the old over-gated zombie (`kafka_replay_unbound` now fails through the
faithful mechanism); the idealized `INV_NoReplayGap` survives only as a named negative
(`kafka_replay_unbound_gap`). Two grain notes stated in the model: a deleted key's recovery floor
stands in for the transient the code exhibits there (the same wall-clock/event-log exemption as the
Cassandra `dropped` branch), and replayed event-driven deletes are filter-dropped like persists.

## Addendum: the models advisory review (six axes + refutation)

The models themselves were then put under an adversarial review — the layer everything else rests on —
across six axes, each run as if the suite were a paper's artifact under re-review, with a refutation
pass on every candidate before it was recorded (the guard against the §10.1 over-rotation). What the
review found, and what survived it:

- **Foundation held.** The root spec + refinement mapping genuinely encode #732 (a step regressing the
  mapped `hwm`, or corrupting contents, fails `RefSafeSpec` — verified by re-running the controls and
  reading the counterexamples). Six of seven defended HOLDS configs were shown **non-vacuous** by
  witness invariants (the hazard is reached at the config's own bound with the defense on). All 23
  negative controls fail for their **intended** reason (each counterexample inspected raw). The four
  README assumptions are honestly dispositioned — LWT linearizability, poll-thread serialization and the
  KIP-447 fence discharged at source in `external-semantics.md`; determinism is a correctly-asserted user
  contract.
- **F-9 (Axis F, "skip-tombstone") — a real, confirmed reachability gap, now fixed.** The always-tombstone
  delete in `Cassandra.tla` modelled the *fix* before the code had it, masking the never-persisted-delete
  resurrection (findings **F-9**). Closed at code level (fenced always-tombstone) and given a paired
  negative: **`SkipTomb`** reintroduces the pre-fix no-op window and `cassandra_skiptomb` VIOLATES the new
  **`INV_NoResurrection`** (committed-keyed, because the resurrection is invisible to the `store.offset`-keyed
  invariants); `cassandra_refines` HOLDS with it — a non-vacuous pair. The ~L199 comment now states the
  always-tombstone is faithful to the adopted fix, with `SkipTomb` as the pre-fix control.
- **A4 pairing closed (Axis E).** A4 (per-key serialization) was the one load-bearing assumption without
  the suite's signature negative control. `FlushCell.tla` now models the `flushCell` compound with a
  concurrent `Append`: `serial_race` VIOLATES `INV_NoLostWrite`, `serial_holds` HOLDS. This proves the
  hazard is real and load-bearing; it does not discharge A4's truth (out of TLC's reach — see D-1).
- **B-1 (Axis B) — a labelling correction, not a bound change.** The F-7 pair runs at MaxOffset=3, where
  the reentry negative fires via the journal-under-representation *loss* dual, not the delete-*revive* its
  comment narrates (the revive needs MaxOffset≥4 and is not even the counterexample TLC reports there).
  Both are the same F-7 family, both closed by the same fold-onto-fenced-base fix, so the pair is genuinely
  non-vacuous; `cassandra_events_revive_reentry.cfg` now carries a note saying so. Bound not raised
  (raising it does not change which trace TLC reports).
- **Refuted / out of scope.** The Kafka `-1` unknown-generation gap (Axis D) is broker semantics the models
  rightly *assume* (like the KIP-447 fence) — source-verified in `external-semantics.md` ext(K3), claim-pinned
  KF5, IT-covered; a model knob would be redundant and violate the suite's scope boundary. The one residue
  — `docs/kafka-single-writer-design.md` framed −1 as a freshness matter, not the fence-*bypass* it is —
  was a cross-branch doc nit, **since fixed on the Kafka branch (#843)**: the doc now states publishing
  `generationId < 0` lands the offset commit unfenced, which is why `Consumer.publish` refuses it. The
  `run.sh` `VIOLATES-REFINEMENT` matcher
  keys on the abstract module only (Axis C): sound today (one action property in `SingleWriterStore`),
  documented in the runner, left as-is. The group-commit×fence×lag composition (Axis D) is a sound modular
  factoring (the fence is a whole-transaction abort, orthogonal to batching; the marker/write race is
  safety-subsumed by `GroupCommit`'s `INV_OffsetWithinDurable`) — and the previously "argued, unverified"
  G1/G2 pieces are now **modelled** in `GroupCommitLanes.tla` (the two-lane split + abort
  path; `INV_NoSlotSteal` / `LIVE_MarkersNotStarved` / `INV_OffsetWithinDurable`, each with a paired
  negative). The Cassandra guard-expired repair and the idle-unload/Epoch
  abstractions are honest, adequately-disclosed boundaries.

Suite after the round: **41 configs / 25 negative controls**, all as declared (TLC 2.16).

## Addendum: TokenSync — capture vs. refresh equivalence (the capture-removal experiment)

`TokenSync.tla` is a small, tower-isolated model (it does not extend `SnapshotFlow`; it models only the
owner's published generation token, not the store) added to answer the question the capture-removal
experiment raised: are assignment-callback **capture** and the post-poll **refresh** equivalent, and where
do they differ? The coordinator's generation advances by two free actions — an `AssigningBump` (a callback
fires) and a `SilentBump` (none, the KIP-848 case) — so, like `Kafka.tla`'s `GenBump`, it models the
background-thread epoch advance rather than a poll-synchronous one. Configs (`tokensync_*`), each as
declared: the 2×2 over {capture, refresh} with a silent bump reachable — `refresh` and `both` **HOLD**,
`capture` and `neither` **VIOLATE-TEMPORAL `Synced`** (capture cannot catch a silent bump; refresh subsumes
it, and adds nothing when combined) — plus the boundary case `capture_assigning` (capture alone but every bump
fires a callback) which **HOLDS**, giving the capture ≡ refresh equivalence. Two honesty caveats on this
model: (1) `TokenNeverLeads` holds by construction (`token` is only ever assigned the current `liveGen`), so
it is a structural sanity check, **not** a checked safety result — the real stale-write safety is in
`Kafka.tla`; (2) the capture/refresh asymmetry the outcomes turn on is *assumed* in the action guards (capture
is callback-gated, refresh is not), faithful to the code but not derived — so this model **demonstrates the
reasoning's consistency**, it does not independently prove the Kafka premise (the IT suite does). It is the
model-level complement to dropping capture-on-assign (F-8 corollary), not the primary evidence. Adds **5
configs (3 HOLDS + 2 negative controls)**. `Kafka.tla` retains capture as the design of record it was written against; note
it conflates capture with teardown (its `Poll(z)` is both), so it cannot cleanly show the capture-removed
variant's zombie-safety — that rests on the code (teardown-on-revoke) and `FlowsAlive`, not on this model.

## Addendum: RecoveryRead — the recovery read's bound (F-10 and its remedies)

`RecoveryRead.tla` is a semantics model isolated from the tower (like `TokenSync`): it checks the
*reasoning* about Kafka's transactional log semantics against the recovery read's bound, with the
external facts assumed as modeled, not derived. Fidelity notes: the LSO is defined structurally (the
offset before the first open record — the `read_committed` `endOffsets` semantics of ext(K2)); the
read's wait is the `Complete` guard (a `read_committed` position cannot pass an open record); a
takeover-abort is folded into `BInit` (mandatory init aborts the same-lineage open transaction —
the *contract* half of ext(K5); the marker-replication visibility half is not modeled and not needed,
since `BInit` precedes `BWriteS3` in the script exactly as init-before-produce is mandatory in the
client). The double-handover scenario is a fixed cast (A crashes mid-transaction, B commits above it
and crashes mid-transaction, C reads), so this is a scripted scenario check, not an exhaustive
protocol exploration — deliberately: the four configs are the four corners of the design space
(bound × id scheme), and `TimeoutAbort` is left free to interleave so TLC also covers the pin
resolving before/after the capture. Non-vacuity: the HOLDS configs carry `PROPERTY Terminates`
(completion is reached, the read is not forever blocked) and the stable config additionally checks
`INV_LineageSerialized` — the structural fact (committed-above-open unreachable within a lineage) that
makes the LSO bound complete, checked rather than asserted. Adds **4 configs (2 HOLDS + 2 negative
controls)**; run in CI with the rest of the suite (TLC 2.16).
