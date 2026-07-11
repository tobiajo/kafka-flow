# Test audit — coverage matrix and gap closures

*Cassandra arm of the #732 single-writer study — test coverage matrix and the closures this study added. Corpus index: [`README.md`](README.md).*

Method: all snapshot/persistence test files read in full and mapped against the claims. The `C1..C19`
labels below are this audit's **own** coverage-item numbering (one per behaviour a test should pin),
*not* claims.md identifiers — claims.md numbers claims `M/D/R/K/E/N/T/A/X`. The two are related by
topic, not by index (e.g. C1 stale-persist ↔ claim M1/M4; C8 tombstone read ↔ D5/K4); the C-list is a
test-side checklist, deliberately finer-grained than the claim list. Strength graded direct/indirect;
quality problems flagged. Below: the matrix verdicts, then what the study changed.

## Matrix summary (pre-existing suite)

Strong: stale-persist rejection (C1), equal-offset admission (C2), no-resurrection (C6), monotonic
buffer drop (C9), delete lift to high-water — three layers, unit/flow/IT (C10), no re-persist below
floor (C11), buffer-only delete after absent recovery (C14), TTL on insert+update (C15), the #732
A/B reproduction/prevention through real PartitionFlow machinery with only `compareAndSet` varying
(C17), unfenced-stays-LWW (C18).

Medium: first-write fallback (C3, exercised by necessity, not isolated), first-write race (C4,
probabilistic — conflicts not guaranteed under serialization; retry branch not deterministically
forced), tombstone row-kept/value-null (C5, inferred not selected), replayed-delete idempotence (C7 —
equal-offset half only), tombstone floor seeding (C12 — core-direct, store mimicked).

**Gaps found (each could mask a real regression):**
1. **C8 — the real tombstone read was never asserted** (the IT adapter collapsed `Stored` to its
   value): a `read` regressing tombstone→None would pass the entire suite while re-arming the
   deleted-key livelock. *The highest-value finding of the audit.*
2. **C7 (absent half)** — the "deleting an absent key" test actually deleted an equal-offset
   tombstone; the true row-absent branch never executed.
3. **C16** — nothing asserted the tombstone carries the TTL (probe was `TTL(value)`, null after
   delete).
4. **C13 (gate half)** — the `fenced` gate on the events-recovery floor read untested in either
   direction.
5. **C19** — the metrics wrapper's `Stored` delegation untested; a wrapper collapsing the tombstone
   would silently disarm the floor.

## Closures (this study)

All five gaps closed — commit "Close the audited verification gaps…" (`384d139` on the cassandra
branch): SnapshotSpec asserts `Stored.Tombstone(offset)` from the real store, adds the
never-written-key delete (no row created) and the `TTL(offset)` tombstone probe;
`ReadStateFloorGateSpec` pins read-iff-fenced; `SnapshotDatabaseMetricsSpec` pins verbatim
pass-through. Additionally the study's fix commits carry their own regression tests
(`SnapshotTtlEdgeSpec` poison-row repair E2E, `CassandraPersistenceWiringSpec` fenced-per-mode,
two `initPersisted`-floor cases in `SnapshotsSpec`).

## Noted, not changed

- **C4 probabilistic race**: making the first-write retry deterministic needs fault injection at the
  session layer; the race test remains valuable as-is (asserts no corruption + highest-wins under
  real contention). Accepted.
- **Mimic drift risk** (`SnapshotReplayFencingSpec` hand-models CAS gating in two doubles; one mock
  deletes by row-removal, diverging from tombstone semantics): mitigated by the new C8 IT assertion
  anchoring the real store's behaviour; flagged for a comment if the doubles are reused.
- **Undocumented behaviours with tests**: conflict-on-revoke is swallowed by scache (FlowSpec pins
  it; persistence.md describes it); revoke-time `scheduleCommit` failure swallowed (PartitionFlowSpec);
  `AdditionalPersistSpec`'s feature is outside the studied docs. No action for this study.
- **Impure counter in a `State` program** (`SnapshotsSpec.countingSnapshotDb` uses a `var`): safe
  under single `runS`; would double-count if the `Eval` were forced twice. Cosmetic; left.

## Suite results (study end — superseded by the findings.md ledger)

core 104/104 · persistence-cassandra IT 33/33 (incl. SnapshotSpec 15, FlowSpec 4, TtlEdge 1,
Wiring 1) · persistence-kafka IT 12/12 · persistence-kafka unit 9/9 · metrics 6/6 · TLA+ 29/29
(16 negative controls). Counts certified by the study-end full run; current authoritative totals are the
findings.md ledger's latest column (post-models-review: core 121, Cassandra IT 36, TLA+ 57), which supersedes
the intermediate post-review figures (116 / 35 / 38).
