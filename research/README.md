# Single-writer snapshot correctness (#732) — research corpus

Verification of kafka-flow's stale-writer fence for
[#732](https://github.com/evolution-gaming/kafka-flow/issues/732) (a rebalance-overlap stale owner
overwriting a newer snapshot), across the implementations that address it. **This file is the corpus
front door** — what each document is, its implementation scope, and where a given fact lives. Each
implementation has its own narrative; the Cassandra one, `cassandra-report.md`, is the deepest and is
the primary subject.

**Stance.** Adversarial self-audit with mechanized verification — **not** an independent replication in
the arm's-length sense (subject, audit, and fixes share one lineage; see `cassandra-report.md` §1/§6).
Run *as if* the design were a paper under review: re-derive every claim from the artifacts (code, tests,
TLA+ models, external Cassandra/Kafka semantics), attack every seam, record verdicts with evidence, take
nothing on authority. The hedge against self-review bias is external — a fresh-context review committee
(`cassandra-report.md` §10) that found a defect the self-audit had certified (F-7).

## Scope: the implementations

All close #732; they share one invariant (no stale owner overwrites a newer durable snapshot) and differ
in the fence. **Depth is deliberately uneven and this corpus does not hide it:** the Cassandra full mode
got a full report plus a review committee; the Kafka mode a thorough study set; persist-only is the *subset*
of the Cassandra design merged upstream, not a separate arm.

| Implementation | What it is | Guarantee (and residual) | Where verified |
|---|---|---|---|
| **Kafka transactional** (the transactional snapshot mode) | snapshot write + input-offset commit bound in one producer transaction, fenced by the consumer **generation** (KIP-447 `sendOffsetsToTransaction`), stable per-partition `transactional.id` (takeover-abort), `read_committed` recovery, group-committed batches, post-poll generation refresh | a stale owner's offset commit is rejected (`ILLEGAL_GENERATION`) and aborts the transaction; residual: the cross-partition flows-alive invariant rests on the documented rebalance contract (modelled in `FlowsAlive`; pinned by `TopicFlowSpec` "remove awaits the flow teardown") | `docs/kafka-single-writer-design.md`; the **Kafka arm** files below; `external-semantics.md` ext(K1)–(K7); claims KF1–KF14; findings F-8, F-10; models `Kafka`/`GroupCommit`/`GroupCommitLanes`/`Epoch`/`FlowsAlive`/`TokenSync`/`RecoveryRead` |
| **Cassandra persist-only** (**merged upstream**) | `persist` offset-fenced (`IF offset <= :offset`, first-write compound, `IF offset = null` repair); `delete` a plain last-write-wins `DELETE` | #732 closed **for persists**; residual (documented, accepted): a stale writer can resurrect a *deleted* key | the **Cassandra arm** files (persist-only framing in `cassandra-report.md` §1.1); model `cassandra_notomb` (VIOLATES `INV_NoCorruptDurable`); `SnapshotSpec` IT *on the persist-only branch*; claims X1 |
| **Cassandra full** (**verified, deferred** — upstream #834) | the above **plus** an offset-carrying tombstone delete (always written in fenced mode, even for a never-persisted key — F-9), the replay-window monotonic buffer, tombstone-floor recovery, and the events-recovery offset floor | #732 closed for persists **and** deletes | `cassandra-report.md` §3–§10; findings F-1..F-9; the full model suite |

## How this corpus is organized

One naming rule, so a filename tells you its scope:

- **Prefixed = one implementation.** `cassandra-*` files are the Cassandra arm; `kafka-*` files are the
  Kafka arm. Each arm has a narrative/verification study plus its supporting analyses.
- **Unprefixed = genuinely shared**, and sectioned *inside* by implementation: `findings`, `claims`,
  `external-semantics`, `model-audit`. These stay unified on purpose — `findings.md` is the single
  reconciled suite ledger and `external-semantics.md`/`claims.md` are single source tables; splitting
  them per implementation would fragment those records. The
  [cross-cutting map](#cross-cutting-map-which-file-holds-what) shows which section is which.

Every file opens with an italic **role banner** stating its scope and pointing back here, so a document
opened cold self-describes.

## File index

Scope column: **Cassandra** / **Kafka** = single-implementation; **Shared** = all implementations,
sectioned inside.

| File | Scope | Role | Read it when |
|---|---|---|---|
| [`cassandra-report.md`](cassandra-report.md) | Cassandra | The Cassandra arm's narrative: subject, method, what held/didn't, threats, the review committee. | You want the Cassandra story. **Start here for Cassandra.** |
| [`cassandra-seams.md`](cassandra-seams.md) | Cassandra | Adversarial seam analysis (S1–S14): attack hypothesis + verdict per boundary. | "Could Cassandra boundary X break?" |
| [`cassandra-test-audit.md`](cassandra-test-audit.md) | Cassandra | Test coverage matrix and the closures this study added. | "What's tested on the Cassandra side, what isn't?" |
| [`kafka-generation-study.md`](kafka-generation-study.md) | Kafka | The Kafka arm's verification study: load-bearing claims, seam attacks, model check, backport map. | You want the Kafka story. **Start here for Kafka.** |
| [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md) | Kafka | Primary-source pin of rebalance mechanics: classic verdicts, the KIP-848 addendum, the consumer-protocol guide. | "Where does the generation move, when do callbacks fire, what does the broker validate?" |
| [`kafka-pr-dispositions.md`](kafka-pr-dispositions.md) | Kafka | Closure record for the PR-review discussions — every argued thread with its terminal disposition. | "Was argument X settled, and how — so it isn't relitigated." |
| [`kafka-consumer-protocol-experiment.md`](kafka-consumer-protocol-experiment.md) | Kafka | The realized `group.protocol=consumer` (KIP-848) experiment: a vendored skafka fork, IT-proven under both protocols on Kafka 4.3.0, plus the capture-redundancy finding. Additive/experimental — not part of the current design. | "What does it take to support the consumer rebalance protocol — and what did building it teach?" |
| [`findings.md`](findings.md) | Shared | Consolidated defects and dispositions (F-1..F-10; F-8/F-10 are the Kafka ones) + the single reconciled suite ledger. | "What went wrong, what was fixed, what's accepted-as-is." |
| [`claims.md`](claims.md) | Shared | Every design claim → evidence class → verdict (Cassandra families; Kafka KF-series). | "Is claim X actually grounded, and in what?" |
| [`external-semantics.md`](external-semantics.md) | Shared | Primary-source verification of external facts: Cassandra ext(1)–(X2), Kafka-broker ext(K1)–(K7). | "What does LWT / the group coordinator *actually* guarantee?" |
| [`model-audit.md`](model-audit.md) | Shared | TLA+ model↔code fidelity, non-vacuity, coverage gaps (per-implementation scope map at its top). | "Is a `HOLDS` result faithful and non-vacuous?" (pair with `../models/README.md`) |
| [`../models/`](../models/) (+ `models/README.md`) | Shared | The TLA+ suite itself: the refinement tower, the configs, how to run (`run.sh`). | "I want to run or read the models." (`model-audit.md` is the audit *of* them.) |

## Reading paths

- **New here** → this index, then the arm you care about (`cassandra-report.md` or
  `kafka-generation-study.md`), then skim `findings.md`.
- **Checking one claim** → `claims.md`, find the claim, follow its evidence citation into the apparatus.
- **Trusting the models** → `model-audit.md` + `../models/README.md`; then `cassandra-seams.md` /
  `external-semantics.md` for the reading behind a verdict.
- **Picking up after a gap** → go to the pinned record for the fact (protocol →
  `kafka-rebalance-semantics.md`; DB/broker → `external-semantics.md`) and cite it; do not re-derive from
  memory (that drift is what these records exist to stop).

## Cross-cutting map: which file holds what

The Shared files mix implementations; this is where each arm's content lives inside them.

- **Cassandra** (persist-only + full): `cassandra-*.md` (narrative, seams, tests); findings F-1..F-7,
  F-9; `external-semantics.md` ext(1)–(X2), ext(C-F9); `claims.md` Mechanism / Delete / Replay /
  Deleted-key / Consistency / TTL / Rejected sections; models `Cassandra`, `CasFirstWrite(Atomic)`,
  `FlushCell`, `SnapshotFlow`, `SingleWriterStore`.
- **Kafka**: `kafka-*.md` (study, rebalance semantics, dispositions, consumer-protocol experiment); findings
  F-8 (+ its capture-redundancy corollary) and F-10; `external-semantics.md` ext(K1)–(K7); `claims.md`
  KF1–KF14; models `Kafka`, `GroupCommit`, `GroupCommitLanes`, `Epoch`, `FlowsAlive`, `TokenSync`,
  `RecoveryRead`.

## Toolchain

sbt + JDK 21 (tests); TLC 2.16 via `tla2tools.jar` (models; `run.sh` pins v1.7.0/TLC 2.16, verified in
CI — see finding F-5 for matcher handling); Docker + testcontainers (Cassandra / Kafka integration
tests). Provenance note: this corpus lives on the models branch (its `models/` shared with the Cassandra
work). The study began at a pre-rewrite tip; the history was later melted and re-signed (see
`cassandra-report.md` §7).
