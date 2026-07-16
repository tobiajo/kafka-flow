# Single-writer snapshot correctness in kafka-flow — a verification report

*Status: **in-progress.** The design and its verification are complete and internally consistent (TLA+
suite 75/75) **modulo two disclosed generality residuals (C2/C3, [§6](#6-open-work))**; the two
open-issue fixes exist as open upstream drafts but **unmerged**; the #850 remedy is decided in
principle ([`850-remedy-decision.md`](850-remedy-decision.md): **A required for full safety, B
optional for post-crash speed**), a combined implementation being downstream work; and a
human arm's-length review is **outstanding**. Open items are tracked in
[§6](#6-open-work), not hidden. **This file is the report**; the detailed files under `research/` and
[`../models/`](../models/) are its sections and evidence, indexed in [§8](#8-sources). Each stands on its
own; the report synthesizes and routes — it does not restate their detail.*

## Abstract

kafka-flow binds each partition's durable snapshot to the consumer that owns the partition, so that a
**single writer** controls every key's state. Issue
[#732](https://github.com/evolution-gaming/kafka-flow/issues/732) is the failure of that guarantee:
during a rebalance overlap the previous owner can overwrite the new owner's *newer* durable snapshot.
This report presents (a) the **design** that fences #732 across three backends; (b) the **method** by
which it was verified — a TLA+ refinement tower, tests against real Cassandra and Kafka, primary-source
semantic pins, and fresh-context adversarial review; (c) the **results** — the fence holds across all
three arms, and eleven defects were found during verification, each carrying a transferable lesson; (d)
**what a conforming implementation must carry** (code, tests, docs, code-comments, design document); and
(e) the **open work**. Verification is mechanized but same-lineage — an *adversarial self-audit*, not an
independent replication — and that limit is stated throughout.

## 1. Introduction

A kafka-flow partition is processed by exactly one consumer at a time, and that consumer persists a
durable snapshot of each key's folded state. During a rebalance the broker can briefly leave two
consumers believing they own the same partition (an *overlap*). **#732** is the corruption that overlap
allows: the outgoing owner, still finishing its work, writes a snapshot that overwrites the incoming
owner's newer one — a stale writer clobbering fresh durable state. Closing that gap — a *single-writer*
guarantee that survives rebalance overlaps, crashes, and recovery — is the subject of this report.

The fence is realized in three backends — **Kafka transactional**, **Cassandra full CAS**, and the
**Cassandra persist-only** subset merged upstream. All three close #732; they share one invariant (no
stale owner overwrites a newer durable snapshot) and differ only in the fencing mechanism. Verifying it
surfaced eleven defects, presented in [§4](#4-findings-and-lessons) with the lessons they teach; two, on
the recovery path, remain open work ([§6](#6-open-work)).

## 2. The design

Depth across the arms is deliberately uneven, and this report does not hide it: the Cassandra full mode
has a full narrative and a review committee, the Kafka mode a thorough study set, and persist-only is a
strict subset of the Cassandra design. This is the mechanism per arm; the exact CQL/API and the rationale
live in the design docs ([`../docs/kafka-single-writer-design.md`](../docs/kafka-single-writer-design.md),
[`../docs/cassandra-single-writer-design.md`](../docs/cassandra-single-writer-design.md)) and the arm
narratives.

| Implementation | The fence (mechanism) | Guarantee (and residual) |
|---|---|---|
| **Kafka transactional** | the snapshot write and the input-offset commit ride one producer transaction, fenced by the consumer **generation** (KIP-447); a stale generation's commit is rejected and its transaction aborts. `read_committed` recovery, group-committed batches, a post-poll generation refresh. The recovery-read remedy for F-10 is **decided in principle** (A required for safety, B optional for speed — [§6](#6-open-work)), adoption downstream. | a stale owner's commit is rejected and aborts; residual: the cross-partition flows-alive invariant rests on the documented rebalance contract (modelled, pinned by a teardown-coupling test). |
| **Cassandra full** (verified, deferred upstream #834) | an **offset-conditional CAS** on `persist` (with a first-write compound and a guard-expiry repair) **plus** an offset-carrying tombstone delete (always written in fenced mode), a replay-window monotonic buffer, tombstone-floor recovery, and an events-recovery offset floor. | #732 closed for persists **and** deletes. |
| **Cassandra persist-only** (merged upstream) | the persist CAS above; `delete` left a plain last-write-wins `DELETE`. | #732 closed **for persists**; documented residual: a stale writer can resurrect a *deleted* key. |

The arm narratives are [`cassandra-report.md`](cassandra-report.md) (the primary subject, **start here for
Cassandra**) and [`kafka-generation-study.md`](kafka-generation-study.md) (**start here for Kafka**).

## 3. Method

The design is treated *as a paper under review*: every claim is re-derived from the artifacts (code,
tests, TLA+ models, primary-source Cassandra/Kafka semantics), every seam is attacked, every external
fact is pinned, and verdicts are recorded with evidence — nothing on authority. Four instruments carry
the weight:

- **A refinement tower** (`Backend ⇒ SingleWriterStore`) is the spine: each fence is modelled so that
  *removing* it is a reachable refinement violation. Read-side correctness is itself a checked
  refinement (`RecoveryRead ⇒ RecoveryReadAtomic`), not an assumed atomic step.
- **The fact-knob discipline**: a load-bearing platform fact whose primary source admits two readings is
  held as a swept `CONSTANT` spanning both, and the design is checked under each; if the verdict flips,
  the fact is pinned by source + a precondition-asserted experiment before the passing half is believed.
- **Paired negative controls + non-vacuity**: every HOLDS carries a knob-flip control that fails for the
  intended reason, and each HOLDS is shown to reach its hazard at its own bound.
- **Primary-source pins and tests**: external semantics verified against Apache docs / JIRA / source
  ([`external-semantics.md`](external-semantics.md)); unit and integration tests against real Cassandra
  and Kafka.

**Stance, stated honestly.** This is an *adversarial self-audit with mechanized verification*, **not** an
independent replication: subject, audit, and fixes share one lineage. The hedge against self-review bias
is a **fresh-context adversarial review** that treats the finished artifact as a submission — and it
earned its keep (it caught F-7, which the self-audit had certified). Its limit is also stated: that
review is itself fresh-context **AI**, a rung below a human adversary ([`advisory-review.md`](advisory-review.md)
H1); a human arm's-length pass is outstanding. **Toolchain:** sbt + JDK 21; TLC 2.15 (rev eb3ff99) via
`tla2tools.jar` release v1.7.0 (suite run in CI); Docker + testcontainers for the integration tests. Model
fidelity — every model action mapped to the code it abstracts, and the accepted coverage gaps — is
audited in [`model-fidelity.md`](model-fidelity.md); the suite itself is [`../models/`](../models/).

## 4. Findings and lessons

**What holds.** #732 is verified across all three arms — each fence modelled so that removing it is a
reachable refinement violation. The refinement tower is faithful at the audited grain; the suite is
**75/75** (positive theorems hold, negative controls fail as declared); the recovery read is tied into
the tower as a checked refinement, not an assumed atomic step.

**The recurring lessons.** The learnings are the interesting part of this study, and four patterns recur
across the findings — the forest before the trees:

- **Single-scope evidence is where the surviving defect lives.** A claim survives exactly as far as its
  evidence reaches; the defect hid wherever the evidence was single-scope — one recovery cycle (F-7), one
  state source (F-6), one tool version (F-5), one reading of a platform fact (F-10). Widen the scope
  before trusting green.
- **The "assume the property" trap.** A model or test that bakes in the property under test passes
  *vacuously* — the journal modelled as a "correct prefix" (F-6/F-7), an IT whose hazardous precondition
  had already resolved (F-10). Model a component as what it physically is; give every HOLDS a knob-flip
  control that fails for the intended reason; a pin that passes with the fix removed is not a pin.
- **Platform facts are knobs until pinned.** A fact whose source admits two verdict-flipping readings is
  load-bearing (the recovery bound — LSO vs high-watermark, F-10); an atomic action is a compressed
  assumption (F-10 lived inside a one-step recovery); an omitted environment action is the "never happens"
  reading (truncation, F-11). Sweep it as a `CONSTANT`, pin it by source + experiment, keep the label true.
- **A self-audit cannot certify itself.** The load-bearing hedge is a fresh-context adversary (it caught
  F-7 after the self-audit's tests *and* model passed) — with the honest limit that this one is AI, a rung
  below a human. Grade evidence by its true provenance, and let a refutation pass keep severity honest.

**The findings.** Eleven defects, each with the rule it teaches; the full dispositions, the evidence
anchors (tests/configs), and the suite ledger are in [`findings.md`](findings.md).

| # | Arm | Defect | The rule it teaches |
|---|---|---|---|
| **F-1** | Cassandra | TTL-reconfiguration "poison row": the guard cell expires and the deleted-key fence silently collapses to the floor | *Know the primitive's granularity, and reproduce a predicted failure against the real system — reality can be quieter and worse* (the null offset decoded to 0, a silent floor loss, not the predicted crash). |
| **F-2** | Cassandra | fence wired by snapshot *type*, so last-write-wins deployments inherited it plus a useless per-recovery read | *Gate a behaviour on the capability it belongs to, never a correlated proxy.* |
| **F-3** | Cassandra | `initPersisted` bypassed the monotonic cell, reopening the deleted-key livelock | *One writer means one write site: a second, non-monotonic path defeats the invariant.* |
| **F-6** | Cassandra | events-recovery *journal revive*: a not-yet-fenced owner's replayed appends fold pre-delete state back to life durably | *A seam covered by code-reading alone is where the defect hides — model the second state source.* |
| **F-7** | Cassandra | the revive's **second-recovery** re-entry, which a single-recovery fix and its tests/model missed; caught by fresh-context review | *Verify a fix at the next cycle, not the first; and offset is not provenance — a floor filter, not a comparison.* |
| **F-9** | Cassandra | never-persisted-delete resurrection, invisible to `store.offset`-keyed invariants | *Key the invariant to the hazard's own observable* (committed-keyed). |
| **F-4/F-5** | apparatus | a wiring gap and the TLC matcher/version mislabel — a green harness silently misclassifying | *Pin the verification toolchain too: a green run under the wrong tool version proves nothing, and a drifted label survives every self-audit.* |
| **F-8** | Kafka | generation-lag *spurious* fence (availability); closed by the post-poll refresh, after which capture-on-assign proved redundant | *A token that lags but never leads only ever fences — the question is availability, not safety; and a dead defensive mechanism is dead weight.* |
| **F-10** (#850) | Kafka | recovery read silently under-reads past a crashed writer's open transaction (LSO vs high-watermark) — **A required, B optional (decided in principle)** | *A platform fact with two verdict-flipping readings is load-bearing; new prose forces the source lookup settled code never triggers* (found by chasing a fresh sentence to the `endOffsets` javadoc). |
| **F-11** (#849) | Kafka | recovery read hangs → silent member eviction when its target outlives the log — **remedy on a draft branch** | *A silent failure is first-class severity: tripwire on no-progress (not duration), budgeted against the platform's timeouts; and an omitted environment action needs a knob.* |

*Where the design admits more than one remedy (the two F-10/#850 mechanics), the whole decision matrix is proven — including the out-of-lineage asymmetry that makes A required and B optional,
not a presumed winner — see [§6 Open work](#6-open-work).*

## 5. What the implementations require

A design is not "verified" until the obligations it implies are written down and each is tied to the
evidence that defines it *and* the artifact that discharges it. The normative register
([`implementation-requirements.md`](implementation-requirements.md)) states, per arm — Kafka (K-\*, plus the
open R-850/R-849), Cassandra full (C-\*), persist-only (P-\*), custom stores (X-\*), and shared (S-\*) —
what a conforming implementation MUST carry, each obligation derived mechanically from a red config, a
finding fix, or a model assumption, and discharged by:

- **code** — the mechanism itself (e.g. the mode-scoped fence wiring; the offset floor filter; the
  no-progress recovery tripwire);
- **tests** — a *non-vacuous* pin: a test that would fail if the code regressed to the config the model
  marks red (the F-10 lesson — a green test that cannot fail on the defect closes nothing);
- **docs** — the operational preconditions (Cassandra version floor, Paxos v2, TTL-from-first-deploy) and
  the user-facing contract in [`../docs/persistence.md`](../docs/persistence.md);
- **code comments** — the *why* at the fence sites (why the captured generation must lag, why the delete
  keeps the row, why the recovery bound matters);
- **design document** — the arm's design doc kept in step with the mechanism.

An item is closed only when its model (here), its code, and its non-vacuous test (on the implementation)
are all green. The register also carries the not-yet-merged status and the cross-branch integration gaps.

## 6. Open work

This report is **in-progress**; what remains is tracked, not hidden. The design and record are complete
and internally consistent, modulo two disclosed generality residuals (C2/C3 below). Forward items:

- **The F-10/#850 remedy — decided in principle, adoption downstream.** The remedy 2×2 proves both
  candidates sufficient and composable (neither → the read violates; either alone → holds; both →
  compose), but *not* equivalent: **A** (the high-watermark read bound) holds **unconditionally**,
  while **B** (the stable per-partition `transactional.id`) leaves a silent foreign-pin residual
  (`recoveryread_lso_foreign` VIOLATES). So the ranking is forced —
  [`850-remedy-decision.md`](850-remedy-decision.md): **A is required for full safety; B is optional,
  buying sub-second post-crash recovery in place of A's ~70 s wait tail.** A+B (with the #849 stall
  deadline) is the end-state when that speed is wanted; A alone is the floor; B alone is ruled out.
- **The remedies exist as open upstream drafts, unmerged; a combined implementation is downstream
  work.** A (#852), B (#853), and the #849 stall deadline (#851) each carry code + a non-vacuous test
  on their own branch. This models branch carries `research/` + `models/` and merges first; the
  models already cover the combined corner (`recoveryread_both`, at the disclosed one-handover cast —
  C2 below), so A / B / the deadline drop in without new modelling. What a combined A+B+deadline
  implementation must additionally carry — the deadline's lower-bound check, the wait×deadline
  coexistence test, and the capture-before-init orphan-signal design question — is the register's
  R-850-C ([`implementation-requirements.md`](implementation-requirements.md)).
- **A human arm's-length review is outstanding.** Both arms have had fresh-context **AI** review at parity
  (the Cassandra committee that caught F-7; the Kafka-arm pass over `RecoveryRead ⇒ RecoveryReadAtomic`,
  `RecoveryDeadline`, and the register — both in [`advisory-review.md`](advisory-review.md)); a human
  adversary is the rung above, which neither has had.
- **Disclosed residuals (not #732 safety gaps):** **C2** — the remedy models check a single A→B handover,
  not inductively for *n* concurrent stale writers; **C3** — trip-abort latency is abstracted to zero.
  Both are recorded in [`model-fidelity.md`](model-fidelity.md). Also open as a toolchain refresh: a TLC
  2.18 re-run (the suite is verified on the pinned 2.15).

**Cross-reference hygiene.** Commit messages on this branch avoid `#`-issue/PR references (they would
create GitHub cross-reference backlinks — spam on the referenced issues); issue/PR references live only in
these files' prose, which does not generate backlinks.

## 7. Conclusion

The #732 single-writer guarantee holds across all three arms, and the eleven findings are as much of the
contribution as the fence: each is a worked example of a transferable rule (§4), and the recurring one —
*a claim survives only as far as its evidence reaches, and the surviving defect lives wherever the
evidence was single-scope* — is what a fresh-context adversary, not the self-audit, had to surface. What
remains is forward work, not gaps: the #850 A-vs-B decision, the merges, and a human review (§6).

## 8. Sources

The detailed files behind this report. Each **stands on its own and is the single source of truth for its
facet** — read one cold and it holds up; the report above synthesizes and routes, it does not restate a
file's detail, and the file is authoritative where they overlap. Grouped by role (the reading order); each
opens with a role banner.

| File | Role | What it holds |
|---|---|---|
| [`cassandra-report.md`](cassandra-report.md) | Narrative (Cassandra) | the Cassandra arm's full narrative, incl. the test-coverage audit (§11) and seam analysis (§12). **Start here for Cassandra.** |
| [`kafka-generation-study.md`](kafka-generation-study.md) | Narrative (Kafka) | the Kafka arm's study, incl. the KIP-848 realized experiment and the PR-review dispositions. **Start here for Kafka.** |
| [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) | Narrative (Kafka) | primary-source pin of rebalance mechanics; the KIP-848 addendum. |
| [`findings.md`](findings.md) | Evidence | the defect ledger (F-1..F-11) with anchors, and the single reconciled suite-count ledger. |
| [`claims.md`](claims.md) | Evidence | every design claim → evidence class → verdict (Cassandra families; Kafka KF-series). |
| [`external-semantics.md`](external-semantics.md) | Evidence | primary-source verification of external facts (Cassandra ext(1)–(X2), ext(C-F9); Kafka ext(K1)–(K14)). |
| [`model-fidelity.md`](model-fidelity.md) | Apparatus | TLA+ model↔code fidelity, non-vacuity, accepted coverage gaps. |
| [`../models/`](../models/) (+ [`../models/README.md`](../models/README.md)) | Apparatus | the TLA+ suite: the refinement tower, the configs, `run.sh`. |
| [`implementation-requirements.md`](implementation-requirements.md) | Forward | the normative register (§5) + the not-yet-merged backlog and cross-branch integration gaps. |
| [`850-remedy-decision.md`](850-remedy-decision.md) | Forward | the #850 remedy comparison (A vs B vs composed): decision rule, criteria, matrix, recommendation with staged path and flip conditions; its external pins are homed as ext(K9)–(K13), its §6 routes to them. |
| [`advisory-review.md`](advisory-review.md) | Review | the external reviews (corpus-wide advisory pass + the Kafka-arm models/register pass). |
| [`850-implementation-review.md`](850-implementation-review.md) | Review | advisory review of the combined A+B+deadline implementation (fork PRs #14/#15) against the register and models, under the initial-implementation stance (v9.0.0 the only released cut); merge-shape recommendation and the on-merge corpus flips. |

**Where each arm lives inside the shared files.** Cassandra: `cassandra-*.md`; register S-/C-/P-/X-\*;
findings F-1..F-7, F-9; ext(1)–(X2), ext(C-F9); claims Mechanism/Delete/Replay/Deleted-key/Consistency/
TTL/Rejected; models `Cassandra`, `CasFirstWrite`, `FlushCell`, `SnapshotFlow`, `SingleWriterStore`.
Kafka: `kafka-*.md`; findings F-8/F-10/F-11; ext(K1)–(K14); claims KF1–KF16; register S-/K-\*, R-850/R-849;
models `Kafka`, `GroupCommit`, `GroupCommitLanes`, `Epoch`, `FlowsAlive`, `TokenSync`, `RecoveryRead ⇒
RecoveryReadAtomic`, `RecoveryDeadline`.

*Snapshot date: 2026-07-14 (keep in step with the [`implementation-requirements.md`](implementation-requirements.md)
status snapshot; the [`advisory-review.md`](advisory-review.md) date records the last review pass —
2026-07-12 — and moves only with a new review).*
