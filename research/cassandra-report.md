# The Cassandra single-writer snapshot mode under replication:
# an adversarial self-audit with mechanized verification

*Cassandra arm of the #732 single-writer study — the full narrative: subject, method, results, what didn't hold, threats, review committee. **Start here for Cassandra.** Corpus index: [`README.md`](README.md).*

> **Framing (read first).** This is **not** an independent replication in the arm's-length sense: the
> subject design, this audit, and every fix were produced in one continuous, AI-assisted engineering
> effort — same lineage, not a separate team. What the study actually is, and what makes it worth
> trusting, is an *adversarial self-audit backed by mechanized checks*: paired TLA+ negative controls,
> executable regression tests, and — the decisive hedge — a fresh-context review committee that treated
> the finished artifact as a paper under review. That committee found a real defect the self-audit had
> certified (**F-7**, §10), which is the strongest evidence both for the method's value and for why the
> "independent" label would have been an overclaim. The prose below is edited toward that honest
> framing; where an older paragraph still reads as arm's-length replication, the correction is in §6.
>
> **Errata (post-review).** The abstract's original three-defect tally is superseded. A **fourth** and
> **fifth** defect were later found — F-6 (the journal revive) and
> F-7 (its re-entry, a durable-corruption defect in the mode the fence exists to protect). The core
> *offset fence* held; the *events-recovery composition* did not, until F-7. See §9–§10.

## Abstract

We replicated and adversarially examined the correctness of kafka-flow's
compare-and-set snapshot mode (branch `tj/address-partition-ownership-overlap-possiblity-cassandra`,
addressing issue #732: stale partition owners overwriting newer snapshots during rebalance overlap).
The study re-derived every claim in the design documentation from the artifacts; replicated the test
suites (unit + integration against real Cassandra and Kafka) and the TLA+ model suite (25 configs at
study start, 38 now); verified the Cassandra semantics the design rests on against primary sources down to
Cassandra's source code; and attacked fourteen seams. The offset fence, the
offset-carrying tombstone, the replay-window monotonic buffer, and the tombstone-floor recovery are
correct as designed, and every stated negative control fails exactly as declared. The study found
**three defects at first pass** — one empirically reproduced data-plane defect (a TTL-reconfiguration "poison row"
whose guard expires, silently disarming the deleted-key replay fence and permanently conflicting all
writes to the key), one wiring regression (last-write-wins deployments inheriting the fence and a
per-key recovery read), and one protocol back door (`initPersisted` bypassing the monotonic cell) —
all three fixed with regression tests, and the third also model-checked with a new paired negative
control. **Two further defects surfaced later** in the events-recovery composition — F-6 (the journal
revive) and, after the review committee, F-7 (its second-recovery re-entry, which the first fix and its
single-recovery tests/model had missed) — both now fixed, with the model rebuilt to represent the
journal faithfully as a row set. The study further found the Kafka model's `AtomicBind` to be an idealization of the code
(the binding closes the committed-ahead gap but leaves a one-round trailing window, protected by the
recovered-snapshot filter); recorded the merged Kafka mode's generation-lag spurious fence (F-8, an
availability defect fixed by the post-poll generation refresh); corrected the model's claims, closed
the verification pairing holes and test-coverage holes, and documented the mode's operational preconditions (Cassandra version
floor, Paxos v2, timeout-unknown semantics, LWT/plain-write exclusivity).

## 1. Subject and stance

Subject: the full fence (persists **and** deletes gated) as designed in
`docs/cassandra-single-writer-design.md`, implemented in `persistence-cassandra` +
`core`'s snapshot buffer, tested in core/IT suites, and modelled in `models/` (pulled onto this
branch from the models branch). Stance: adversarial self-audit (see the framing note above — same
lineage as the subject, so not arm's-length replication, but run *as if* the design doc were a paper
under review): accept nothing on authority; prefer executable evidence (test, model, experiment) over
argument; where the design depends on Cassandra behaviour, verify against primary sources; where the
study's own predictions differ from reality, report the discrepancy (§4.1 contains one). The load-bearing
hedge against self-review bias is external: a fresh-context review committee (§10).

### 1.1 What is merged upstream: the persist-only subset

The studied subject is the *full* fence; the artifact merged upstream is a deliberate subset —
**persist-only** (`tj/…-cassandra-persist-only`). It gates every `persist` (`IF offset <= :offset`, the
first-write compound, and the F-1 guard-expired repair) but leaves `delete` a plain last-write-wins
`DELETE`. So #732 is closed **for persists** — a stale owner cannot overwrite a newer snapshot — and
left **open for deletes**: during an overlap a stale writer can still erase a newer snapshot or
resurrect a just-deleted key, and with events-recovery a crash alone can revive one (the same revive
that exists under plain last-write-wins). That residual is a documented boundary, not an oversight, and
it is pinned as such — modelled here (`cassandra_notomb` = `Guarded ∧ ¬Tombstone`, VIOLATES
`INV_NoCorruptDurable`; claims X1) and integration-tested on the persist-only branch itself
(`SnapshotSpec`, "a delete is unguarded … persist-only gap"; that IT is not on this research branch). Fencing the delete is what pulls in the offset-carrying tombstone, the monotonic buffer,
tombstone-floor recovery and the events-recovery floor (the F-3/F-6/F-7 machinery; introducing it also
surfaced the F-2 wiring regression) — a breaking API and a liveness trap on each recovery path — so the
full design was verified here but **deferred**
(upstream #834), deferred. The rest of this report audits that deferred design; persist-only is the
conservative first step whose cost/benefit the audit is what justifies.

## 2. Method

1. **Claim inventory** (`claims.md`): 40+ claims extracted from the docs and load-bearing comments,
   each mapped to evidence class (code / test / model / external / argument-only) and verdict.
2. **Test replication and audit** (`test-audit.md`): all suites run (JDK 21, sbt; testcontainers with
   real Cassandra and Kafka); every snapshot/persistence test read and graded against the inventory.
3. **Model replication and audit** (`model-audit.md`): all TLC configs run (TLC 2.16, now pinned via
   tla2tools v1.7.0 after a newer-TLC matcher incompatibility was found); every model action mapped to
   the code construct it abstracts; configs audited for pairing (each positive theorem should have a
   knob-flipped negative control).
4. **External semantics** (`external-semantics.md`): four independent research passes over Apache
   docs, JIRA, and the Cassandra source (LWT linearizability + caveats; conditional-result shape and
   null-condition semantics; per-cell TTL and tombstone semantics; serial-vs-commit consistency and
   timestamp tie-breaking).
5. **Seam analysis** (`seams.md`): fourteen attack hypotheses at the boundaries (buffer/store,
   store/DB semantics, recovery modes, TTL, consistency, wiring, cross-store atomicity), each driven
   to a verdict by code reading plus, where load-bearing, an experiment, a test, or a model.
6. **Fixes** applied to the subject, each with tests; model changes on the
   models branch (restacked on the new tip); everything re-verified end to end.

## 3. Results: what held

- **The fence.** `IF offset <= :offset` per key, the first-write compound
  (UPDATE → INSERT IF NOT EXISTS → retry once), conflict-as-teardown, and equal-offset admission all
  behave as documented — at unit, integration (real Cassandra, including an 8-writer first-write
  race), and model level. LWT linearizability per partition and its clock-independence for safety are
  confirmed by primary sources, subject to the preconditions now documented (§5).
- **The tombstone.** Row-keeping deletes with the offset carried; no resurrection; idempotent
  replays; the row-absent no-op (now actually exercised by a test — it wasn't before).
- **The replay window.** The monotonic buffer, the delete lift to the high-water, and the flush
  no-op below the floor: verified at three layers, and the two livelocks they prevent (live-key,
  deleted-key) are reachable in the model exactly when the respective mechanism is removed.
- **The models.** All 25 original configs replicate: 12 positive theorems hold, 13 negative controls
  fail precisely as declared. The refinement-tower structure is faithful at the audited grain, with
  the exemptions and idealizations now stated in the models rather than silent (§4.4).
- **The docs.** The design document's claims are, after this study's earlier editorial pass and the
  fixes below, individually evidenced; persistence.md's user guidance matches the code.

## 4. Results: what did not hold

### 4.1 F-1: the guard-expired poison row (fixed; empirically characterized)

Cassandra TTLs are per cell; only the delete writes a column subset. Enabling/shortening `ttl`
between a key's persist and its delete lets the `offset` guard cell expire while a longer-lived cell
(or the first write's INSERT row marker, immortal only when first-written without a `ttl` and no table
`default_time_to_live`) keeps the row visible — unreachable under a uniform `ttl` from the first write,
where the delete co-writes `offset` with `value` so a visible row always carries a live guard.
Predicted consequence was a decode crash;
**the experiment refuted the prediction and revealed worse**: the null offset decodes as 0, so the
tombstone floor silently collapses (the deleted-key fence is disarmed with no error), and every
persist to the key conflicts forever. Fixed: guard-expired rows read as absent, delete as no-op, and
persists claim the row via a Paxos-safe `IF offset = null` repair that reinstates the guard —
possible because Cassandra's not-applied result provably distinguishes "row absent" from "row
present, guard null" (verified in `ModificationStatement`/`ColumnCondition` source; the table has no
static columns). End-to-end regression test reproduces the state against real Cassandra and asserts
the repair and the restored fence. The repair is palliative for an immortal-marker row: being an
`UPDATE` it re-arms the guard but never recreates the marker, so such a row re-poisons after each
`ttl` until reaped.

### 4.2 F-2: fence wired by snapshot type, not write mode (fixed)

`compareAndSet=false` deployments inherited the fenced buffer and, under events-recovery, a new
per-key store read per recovery that can never find a tombstone floor under last-write-wins. Fixed
with a mode-scoped wiring hook; pinned by an integration test. The design doc's "fence live only for
the compare-and-set wiring" now holds by construction.

### 4.3 F-3: `initPersisted` bypassed the monotonic cell (fixed; model-checked)

The buffer's contract promises one monotonic write site; `initPersisted` was a second,
non-monotonic one. Events-recovery seeds the tombstone floor and then inits the journal fold's
result; a journal trailing the snapshot store clobbers the floor and reopens the deleted-key
livelock. Fixed by routing the init through the monotonic `put`; unit-tested; and the model gained
`MonotonicInit`/`ReseedFloor` with a paired negative control (`cassandra_init_clobber`) that exhibits
the livelock lasso when the fix is off.

### 4.4 Model findings (claims corrected, holes closed)

- **K1**: the Kafka model's `AtomicBind` forces `committed = store.offset`; the code commits the
  offset scheduled *before* a transaction's writes, so the committed offset can trail the newest
  snapshot by one in-flight round. The binding's real guarantee — offset and snapshot move together
  or not at all, and a stale generation lands neither — plus the residual protection (the
  recovered-snapshot filter) are now stated in `Kafka.tla`/README. (Affects the merged Kafka work's
  models, not its code.)
- **Grain exemptions stated**: the model drops replayed deletes (the code persists them lifted) and
  equal-offset re-writes (the code re-writes tick state); both are observationally equivalent under
  the determinism contract — the argument is now in `Cassandra.tla` instead of implicit.
- **New configs**: `cassandra_reap` (the TTL scope boundary as a checked expected refinement
  failure), `cassandra_replay_fixoff_safe` (the "safety still holds during the livelock" half,
  verified rather than asserted), `kafka_replay_unbound_gap` (pins `INV_NoReplayGap` as
  binding-dependent). Suite at the post-audit stage: 29 configs / 16 negative controls, all as
  declared (current totals in the findings.md ledger).
- **Tooling**: run.sh's temporal-violation matcher was TLC-2.18-specific (silently failing three
  controls under 2.16) and its refinement matcher accepted any action-property violation; both fixed.

### 4.5 Test-coverage holes (closed)

Five audited gaps, each capable of masking a real regression silently — most notably the real
tombstone read was never asserted against Cassandra (the IT adapter collapsed `Stored` to its value,
so a read regressing tombstone→None would have passed the entire suite while re-arming the
deleted-key livelock). All five closed; see `test-audit.md`.

## 5. Operational preconditions (now documented in the design doc)

Cassandra ≥ 3.0.24 / 3.11.10 / 4.0 (CASSANDRA-12126 broke exactly this mode's non-applying LWT
shape); the unsafe serial-reads flag unset; Paxos v2 recommended on 4.1+ (linearizability across
range movements); LWT timeouts are unknown outcomes — the flow's teardown-and-recover plus the
`SnapshotFold` replay filter make the redo convergent independent of the fence bound (a committed
write's replays are dropped, an uncommitted one re-folds to a strictly higher offset — so `<` and `<=`
converge alike; the equal-offset admission is not what saves it); only LWT
statements may touch the table; quorum read/write consistency is not defaulted; TTL preferably
configured from the first deployment (else the F-1 repair path handles the residue).

## 6. Threats to validity

- **Tool version**: TLC 2.16 (pinned via tla2tools v1.7.0); the three temporal-violation
  verdicts were disambiguated manually (single declared property per config) with a version-tolerant
  classifier. State spaces are small (MaxOffset=3) and no liveness result is *believed* to depend on
  2.16-specific behaviour. The `models.yml` CI job runs `run.sh` on this same pinned 2.16. Open: a re-run
  on **newer TLC (2.18)**, unreachable in the sandbox and tripping `run.sh`'s 2.16-targeted matchers on
  the one CI attempt — so it needs matcher work plus a reachable toolchain. "CI covers it" holds *for
  2.16*, not 2.18.
- **Serialization assumption (A4)**: `flushCell`'s read→write→mark sequence and Kafka's commit-time
  generation read are safe only under kafka-flow's per-key serialization; now stated in the design
  doc's Assumptions, but still **argument-only, unverified** — the study did not attempt to violate it
  experimentally, and the model assumes rather than checks it (re-graded post-F-7, findings D-1).
- **Un-modelled remainder, re-graded**: events-recovery's journal is now modelled faithfully as a row
  set (F-7 remodel), so it is no longer on this list — and it is precisely the seam that, while
  "covered by code reading instead", hid F-6/F-7. GroupCommit's marker lane/abort paths (G1/G2) were
  labelled *argued, unverified* here; they are now **modelled** by `GroupCommitLanes.tla`
  (two-lane split + abort, each guarantee with a paired negative — see §10.3).
  (The Kafka flows-alive invariant was swept in here too, then corrected by advisory review: it holds by
  Kafka's documented rebalance contract + awaited teardown, not code reading — only a regression
  test/model is missing; see kafka-generation-study.md, §10 and model-audit.md.)
- **Race test determinism**: the first-write race IT exercises contention probabilistically; the
  retry branch is not deterministically forced (would need session-level fault injection).
- **Same-lineage authorship (the honest statement of the self-review threat)**: subject, audit, and
  fixes share one lineage — this is a self-audit, not an independent replication (see the framing note
  at the top and the retitle). §6's original "study self-review" bullet disclosed only that the *fixes*
  were self-verified; the larger fact is that the *whole study* shares authorship with the design. The
  hedges are the mechanized controls and, decisively, the fresh-context review committee — which found
  F-7 (§10), demonstrating both that the threat is real and that the hedge works.

## 7. Artifacts

| Branch | Content |
|---|---|
| `tj/…-cassandra` | subject + this study's 4 fix/test commits (`796ab0d`, `f1b1865`, `6053bb5`, `384d139`), the post-study F-6 guard (`bda56f3`, §9), and the post-review F-7 fix + doc corrections (`a8aca58`, `7b6f3a1`, §10) |
| `tj/…-models` = `tj/…-research` | the cassandra branch + the models restacked on its tip (study commits `71cb0e8`, `7c22267`; post-study events-recovery + vacuity work; the post-review faithful-journal + M4 remodel) + `research/` (this report, claims, seams, audits, external semantics, findings) |

History note: after the study, the subject branches' histories were rewritten twice — a
commit-granularity melt for reviewability (trees byte-identical at the tip) and a re-sign — and the
`research/` directory moved onto the models branch (whose working alias, `…-research`, is kept
identical). Hashes above are the post-rewrite ones; the study's original working history, including
the poison-row before-picture, was superseded by that reorganization (the reproduction lives in
`SnapshotTtlEdgeSpec`). Hashes cited later in the record (e.g. §9/§10) are similarly post-rewrite.

Verification at this report's writing (post-review): core 116/116 · Cassandra IT 35/35 (real Cassandra) ·
Kafka IT 12/12 · persistence-kafka 9/9 · metrics 6/6 · TLA+ 38/38. These have since advanced — the single
reconciled count ledger in findings.md (study-end / post-audit / post-review / post-models-review columns,
current core 121 · Cassandra IT 36 · TLA+ 57) is authoritative over any intermediate count elsewhere in the
record, including this line.

## 8. Future work

Most of §8's original list is done (§9–§10): the Kafka commit lag, the `casfw_refines` vacuity
control, events-recovery as a genuine second state source, and the D-2 journal/snapshot gap (now F-6/
F-7). What genuinely remains: **fault-injected first-write retry determinism** (the race IT is still
probabilistic); the **A4 per-key serialization assumption** — now exercised both ways (the `FlushCell`
model and, on the cassandra branch, `FlushCellConcurrencySpec`), though A4's *truth* stays a JVM-threading
property those pin rather than derive (D-1); executable or
modelled evidence for **GroupCommit's marker lane / abort paths** (since modelled, §10.3); a
**regression test/model pinning the Kafka flows-alive invariant**'s synchronous-await (the invariant
itself holds by Kafka's documented rebalance contract — an earlier over-grade, corrected by advisory
review; the teardown coupling is since modelled (§10.3) and pinned by a unit test — `TopicFlowSpec`
"remove awaits the flow teardown" on #843); and an independent **Jepsen-class check of Cassandra
LWTs**, beyond this repo's scope but the foundation the version-floor guidance rests on.

## 9. Addendum (post-study continuation)

Four of §8's items have since been done, with one changing the study's conclusions. The commit lag
is now modelled faithfully (model-audit.md, K1 addendum: `kafka_replay` + `kafka_lag_nofilter` +
`kafka_replay_unbound_gap`); the generation-refresh delta got its own study
(kafka-generation-study.md); and the `casfw_refines` vacuity hole is closed (`SpecGuarded` decoupled
from `Guarded`, `casfw_refines_vacuous` — the ungated compound against the guarded atomic spec fails
the mapping, so the refinement check is shown capable of failing). Most significantly, **modelling events-recovery as a second state source
found a fourth code defect** the seam analysis had under-graded as in-memory-only (D-2): the *journal
revive* — the unfenced journal, re-populated by a not-yet-fenced owner's replayed appends racing a
zombie's delete, folds pre-delete state back to life on the next recovery and the flow persists it
forward durably. Reclassified as **F-6** (findings.md), reproduced against the code, fixed on the
subject branch (`Snapshots.reconcile` — a fold the fenced store provably leads is discarded for the
store's view; the model's `FloorGuard`) — **a fix the review committee later found insufficient at the
second recovery (F-7, §10); the adopted fix is the offset floor filter, not this fold comparison** — and paired in the model
(`cassandra_events_journal_revive` / `cassandra_events_refines`, with `cassandra_events_nofloor` and
`cassandra_events_unordered` pinning the floor read and the `journals.flush *> snapshots.flush`
ordering). Under last-write-wins the same revive is unfixable (no trustworthy comparator) — now a
documented persistence.md limitation. A methodological note §6 anticipated: the un-modelled remainder
was covered "by code reading instead", and code reading is precisely what missed this — the defect
surfaced only when the seam was given a mechanized model.

## 10. Review committee, F-7, and the revisions

A fresh-context review committee (four reviewers — formal methods, empirical evidence, methodology,
adversarial correctness) audited the finished artifact as a paper under review. Two of them, working
independently, found the same defect the self-audit had certified: **F-7**, the journal revive's
*re-entry*. The F-6 fix (`Snapshots.reconcile`) compared the recovery fold's *result* to the store and
discarded a fold that trailed it — sound at the first recovery, but once legitimate post-delete events
advance the journal to or past the store's offset the polluted fold no longer trails and the pre-delete
residue is folded back to life at the second recovery (durable corruption, reachable by a plain crash
or journal TTL, no zombie). Offset is not provenance. The self-audit missed it because both its unit
tests and its model exercised only one recovery, and the model folded `CorrectContents(journalAt)` —
encoding "the journal is a correct prefix" *by construction*, the very property F-7 violates, so
`cassandra_events_refines` HOLDING was vacuous.

This is F-6's own thesis turned back on the study: the seam certified by single-scope evidence is where
the defect hid. The revisions accordingly went past the one fix:

- **F-7 fixed** (`a8aca58`): `ReadState` folds only journal events above the fenced floor
  (`Snapshots.floor`) onto the store base — an offset filter that holds at every recovery — replacing
  the fold-result comparison. Pinned by `ReadStateFloorGateSpec` second-recovery cases (both arms) and
  a flow-level `FlowSpec` IT across three recoveries that fails when the filter is reverted.
- **Model rebuilt faithfully**: the journal is now a row set (`journalRows`), so a residue below a
  tombstone is representable; the new control `cassandra_events_revive_reentry` VIOLATES
  `INV_NoCorruptDurable` on the fold-compare mode while `cassandra_events_refines` HOLDS under the
  filter — a non-vacuous pair. `cassandra_events_refines` HOLDING now means something.
- **Structural bounds raised (M4)**: Cassandra `Handover` is a two-count (the second recovery is the
  F-7 regime); Kafka's single-zombie scalars became functions over a bounded zombie set with two
  rebalances / gen-bumps, so the fence is checked against two concurrent stale generations (still holds).
- **Every code-reading-only verdict re-graded** (D-1/A4, GroupCommit G1/G2) to *argued, unverified*
  with the risk stated, instead of left at pre-F-7 confidence; A4 went into the design doc's
  Assumptions. (The Kafka flows-alive invariant was initially swept in here too, then corrected by
  advisory review — it rests on Kafka's documented rebalance contract, not code reading; only a
  regression test/model is missing.)
- **Framing corrected**: retitled to *adversarial self-audit with mechanized verification*; the
  same-lineage authorship stated in §6; the abstract's superseded claims marked with errata; the six-way
  count drift reconciled into one dated ledger (findings.md); the reproducibility gap closed (the test
  Cassandra image pinned at/above the study's own version floor).

Suite after the revisions: **core 116/116 · Cassandra IT 35/35 · Kafka IT 12/12 · TLA+ 38/38**
(23 negative controls), all as declared. The committee's verdict was *major revisions*; the point of
record is that the committee — not the authors — is what caught F-7, which is exactly the argument for
why an audit of this shape needs a fresh-context adversary and cannot certify itself.

### 10.1 The per-branch advisory round — the debatable and hard parts

The committee closed F-7; a subsequent per-branch adversarial pass (Kafka #843, persist-only, full
Cassandra — each a fresh-context refutation of *that branch's* claims) continued the audit. It is worth
recording because what it surfaced was less new defects than the **hard, debatable seams where a
confident correction can still be wrong** — including two in the audit's own re-grading.

- **The re-grading heuristic over-rotated — twice.** F-7's lesson ("a code-reading-only verdict is
  where a defect hides") was applied as a blanket downgrade of such verdicts to *argued, unverified*.
  Two of those downgrades were themselves wrong:
  - **Kafka flows-alive** ("every flow alive after a poll is owned in the refreshed generation") was
    graded argued/unverified as if an in-house assumption. It is not: synchronous revoke/lost-*before*-
    assign, completing before `poll` returns, is a documented `ConsumerRebalanceListener` contract, and
    `TopicFlow.remove` awaits the teardown on both paths — present correctness holds by construction, and
    the only genuine residual was that no test/model pinned it against a future async refactor (now
    modelled, §10.3: `FlowsAlive` shows the awaited coupling load-bearing, and pinned by a unit test —
    `TopicFlowSpec` "remove awaits the flow teardown" on #843). A documented
    external contract is not the same evidence class as a bespoke code-reading argument; the heuristic
    conflated them.
  - **The persist-only `<=` rationale** took three passes to state correctly: "re-flush of the buffered
    high-water snapshot" → over-corrected to "LWT timeout-redo at the same offset" (wrong — a timed-out
    persist tears down and recovers, and the `SnapshotFold` filter converges the redo whether the fence
    is `<` or `<=`) → finally E1's **equal-offset tick re-flush** (a timer mutates a key's *value* without
    advancing its *offset*, so the next flush re-persists at the stored offset, which a strict `<` would
    self-fence). A subtle liveness argument with several plausible-but-wrong explanations; single-pass
    reading mis-attributed it each time.

  Both were caught only by an adversary reading *against* the correction — F-7's shape, one level up: the
  audit's own re-grading needed auditing.

- **The full-Cassandra refutation broke nothing.** An independent pass re-ran every `cassandra_*` config
  (all pass as declared) and attacked the five load-bearing claims — the F-7 floor filter at repeated
  recoveries, the tombstone, the monotonic buffer, the tombstone-floor decode — finding no defect and no
  mis-grade. The remaining findings were stale *prose*, not correctness: a config comment still crediting
  the pre-F-7 "correct-by-construction" vacuity, A4-landing references, a superseded `reconcile` mention.

Debatable calls left flagged rather than resolved: whether flows-alive should read "verified (by
contract)" or "unverified (no regression guard)" is a labelling judgment, not a fact; whether
persist-only's events-recovery revive is in scope for a mode that keeps master's recovery is arguable; and
the "code-reading ⇒ argued/unverified" heuristic stays double-edged — it caught F-7, and it over-fired
twice. The honest summary: the correctness held up to every adversarial pass; the residual difficulty
lived in the *record* — in calibrating confidence — not in the design.

### 10.2 The models advisory review — auditing the foundation

The models are the layer all of the above rests on, so they were audited last, on their own terms:
six adversarial axes (root spec & mapping soundness; non-vacuity of every HOLDS; harness soundness;
model↔code fidelity; the four assumptions; abstraction/effect-envelope), each with a mandate to *break*
its axis by running TLC experiments, and each candidate then put through a refutation pass before it
was recorded. The foundation largely held: the refinement mapping genuinely encodes #732, six of seven
defended HOLDS configs are demonstrably non-vacuous (witness invariants show the hazard reached at the
config's own bound), all 23 negative controls fail for their intended reason (every counterexample read
raw), and the four assumptions are honestly dispositioned (three discharged at source in
`external-semantics.md`, determinism a user contract).

Two findings survived refutation and produced real changes. **F-9** (the "skip-tombstone" gap): the
model's always-tombstone delete modelled the *fix* before the code had it, masking a full-mode
resurrection of a never-persisted, just-deleted key by a paused zombie — the same "assume the property"
trap as F-7, but caught by the review, not in production. It is now **fixed in code** (a fenced delete
always writes the offset-carrying tombstone; `deleteCompareAndSet` INSERTs it `IF NOT EXISTS` on an
absent row), validated against real Cassandra, and given a paired model control (`SkipTomb` /
`INV_NoResurrection`, committed-keyed because the hazard is invisible to the store-offset invariants).
The refutation is what kept it honest: it graded the defect **low-severity** (a create-and-delete inside
one flush interval overlapping a held-snapshot zombie) rather than letting the review inflate it, and it
**rejected** the sibling Kafka `-1` proposal as out of scope — that fence-bypass is broker semantics the
models rightly *assume*, already source-verified and IT-pinned, so a model knob would have been
redundant machinery. Second, the A4 per-key-serialization assumption — the one load-bearing premise
without the suite's signature negative control — now has one (`FlushCell`: `serial_race` violates
`INV_NoLostWrite`, `serial_holds` holds), closing the *pairing* gap while leaving A4's *truth* where it
honestly sits (a JVM-threading property TLC cannot see). A third item (**B-1**) resolved to a labelling
correction: the F-7 negative fires at the adopted bound via the loss *dual* of the revive it narrates —
same family, same fix, non-vacuous — so the config comment was corrected rather than the bound raised.

The pattern of §10 held once more: the correctness survived, and the review's own value was as much in
its refutation pass — sizing F-9 honestly, refusing the redundant Kafka knob — as in the finding itself.
Suite after the round: 41 configs / 25 negative controls, all as declared.

### 10.3 Closing the named residuals

Residuals the study *named* but had not *exercised* were closed and folded into the
suite: (1) `GroupCommitLanes.tla` + seven
`gclanes_*` configs turn the *argued-unverified* **G1/G2** (marker-lane liveness / no-slot-steal; offset
≤ durable prefix under the two-lane race and abort) into checked properties, each with a paired negative;
(2) `cassandra_events_*_mo4` re-check the F-7/F-9 revive pair at `MaxOffset=4`, the bound where the
genuine delete-residue revive (not just its loss dual) is reachable — closing **B-1**'s bounded-MC
caveat; (3) `core/.../FlushCellConcurrencySpec.scala` is the JVM counterpart of the `FlushCell`/**A4**
model — a cats-effect race that loses a write unserialized (200/200) and loses none under `Semaphore(1)`
+ sequential phases; (4) the `persistence-cassandra-it-tests` replay-of-a-reaped-tombstone case pins the
**F-9** residual against real Cassandra; and (5) `.github/workflows/models.yml` runs `run.sh` (pinned to
TLC 2.16, the verified version) in CI, not only locally; and (6) `FlowsAlive.tla` +
`flowsalive_holds` / `flowsalive_race` pin the cross-partition **flows-alive** invariant by modelling
teardown as a separate interleavable action gated by an `AwaitTeardown` knob and checking `live ⊆ owned`
as *safety* — the awaited revoke-teardown coupling HOLDS, a fire-and-forget refactor VIOLATES. (This was
the residual first judged intractable *as a timing test*; modelling it deterministically sidesteps the
flakiness — a real-rebalance liveness harness was the wrong shape, an exhaustive model the right one.)
The model artifacts (1, 2, 5, 6) are now part of the suite — **57 configs** (incl. the Kafka
`tokensync_*` capture-vs-refresh set), run in CI. The two Scala
tests (3, 4) ship with the code they protect on the **cassandra branch** (#6), not on this research
branch, so they run in the normal build regardless of whether the study merges. The flows-alive
invariant's *in-code* complement — a unit test on the **Kafka branch** (#843), `TopicFlowSpec` "remove
awaits the flow teardown" — adds then removes a partition whose flow release completes a `Deferred` and
asserts it is completed by the time `remove` returns, so a fire-and-forget refactor fails the build.
