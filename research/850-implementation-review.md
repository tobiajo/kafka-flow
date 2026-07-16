# Implementation review — the combined #850/#849 remedies as the initial implementation

*Review (Kafka) — advisory review of the combined A+B+deadline implementation (fork PRs
[tobiajo#14](https://github.com/tobiajo/kafka-flow/pull/14) and
[tobiajo#15](https://github.com/tobiajo/kafka-flow/pull/15)) against the register
([`implementation-requirements.md`](implementation-requirements.md)), the models, and the corpus.
Corpus index: [`README.md`](README.md).*

## Standing and stance

Release facts, checked against tags: **v9.0.0** (2026-06-30, cut an hour after #833 merged) is the
only release carrying the transactional mode — the earliest EXPERIMENTAL form (#833 + #840): unique
per-assignment ids, the LSO-bounded read (F-10 live), the pre-#842 config surface. #842 (the
config-surface reshape) and #854 (the generation refresh) are unreleased, as is everything reviewed
here.

Stance taken by this review: the mode is EXPERIMENTAL (no-compatibility banner) and its only released
form is already superseded on master, so the two PRs are reviewed as **completing the initial
implementation** ahead of its first proper release. Findings are graded as gaps against the intended
design, not regressions against shipped behavior; "upgrade trap"/"migration" framings are demoted
accordingly (a v9.0.0 user upgrading must already re-integrate across #842's break and re-read the
docs).

## What was reviewed, and how

- **#14** (`caa7310`, one commit): the A+B composition — high-watermark read bound + stable
  per-partition `transactional.id`. **#15** (`7043084`, one commit, stacked on #14): the no-progress
  stall deadline with a fired-deadline diagnosis.
- Method: two independent adversarial correctness passes (one per PR, full-file reads, Kafka
  semantics checked against client/broker behavior), plus a register/model cross-check
  (R-850/R-850-C/R-849, `RecoveryRead ⇒ RecoveryReadAtomic`, `RecoveryDeadline`, claims
  KF12/KF15/KF16, [`850-remedy-decision.md`](850-remedy-decision.md) §5).
- Verification grade: CI green on both PRs × both Scala versions, and CI **executes the real-broker
  IT suite** (`persistence-kafka-it-tests` is in the root aggregate; `ForAllKafkaSuite` starts a
  Kafka testcontainer with no skip path) — so the B-test (takeover-abort), the A-test (bounded wait
  with the pin precondition asserted), and the R-b coexistence test have all run against a real
  broker, not merely compiled.

## Verdict

**No blocking defect.** Every register obligation applying to the combined implementation is met in
code and pinned by a test, including the two integration gaps the register recorded as open across
the upstream drafts: **R-a** (the deadline's lower-bound warning, absent on #851 — implemented at
module acquisition alongside the upper bound) and **R-b** (the wait×deadline coexistence test — the
IT arms a 30 s deadline over the ~15 s legitimate wait). Model fidelity holds at every load-bearing
shape: frozen captured target (no recompute on stall — `recoveryread_trunc_recompute`'s red shape is
absent), the two-clock deadline structure (no-progress resets on advance; R-849.1), init-before-read
ordering, and fail-raised-before-the-next-poll.

## Merge shape (R-c re-answered under the stance)

#14 alone re-opens #849 in a new form: the high-watermark bound is what makes the target *waitable*,
and #14 has no deadline — a hanging transaction, a truncated log, or a foreign producer with
`transaction.timeout.ms` above `max.poll.interval.ms` (the broker cap defaults to 15 min > 5 min)
hangs the poll thread into silent eviction. #15 is the corpus-mandated bound ("required under
either" remedy, §5 of the decision report).

Under the standing above, that coupling is **sequencing hygiene, not a safety requirement**: no
released cut carries the waitable target, so an inter-merge window exposes no user.

**Decided 2026-07-16: joined into one PR** (tobiajo#14, retitled "Recover transactional snapshots
completely, with a bounded read"; tobiajo#15 closed into it). What settled it is overlap, not
safety: 6 of each PR's 7 files were shared; the deadline's rework of
`KafkaPartitionPersistence.scala` (+149/−40) exceeded the base PR's own change to it (+59/−28); the
R-b coexistence check lived inside the base PR's wait-out IT test; and the design doc's three new
sections are one narrative (the termination ladder). The split preserved the archaeology of the
three upstream drafts (#851/#852/#853), not two separable changes — exactly what the
initial-implementation stance retires. R-c's actual concern (don't parallel-merge conflicting
independent drafts) is satisfied: the combination was built stacked and merges as one. The hazard
above survives as a constraint on any future re-split: the high-watermark bound must never ship
without the deadline.

## Findings (all advisory; ordered by residual weight)

**#14 — high-watermark bound + stable id**

1. **B2 at the call site** (medium). `TransactionalConfig.transactionalIdPrefix`'s scaladoc keeps the
   pre-stable-id phrasing — the prefix's "only roles" are a label and an ACL floor, several flows
   "**can** append any per-flow discriminator" — while register B2 and `docs/persistence.md` make
   per-flow uniqueness mandatory: flows sharing a prefix now fence each other's producers into a
   mutual crash loop (loud, availability-only; `initTransactions` epoch-bumps the other's producer,
   the loser fails at its next flush, restarts, fences back). Non-enforcement is a recorded corpus
   decision (a documented obligation, not a runtime guard); the stance removes the upgrade-trap
   reading. Actionable: one spliced clause in the scaladoc ("must be unique per flow — flows sharing
   a prefix fence each other's producers").
2. **Wait-warn overclaim** (low). The LSO-below-target warn says the wait "resolves within the
   pinning producer's transaction.timeout.ms plus the broker's abort scan" — true only for the
   self-healing case; a hanging transaction (pre-KIP-890) or truncation never resolves, and a
   foreign timeout may legally exceed the poll interval. Mitigated in-stack by #15 (the stall log
   and the diagnosed error name exactly those cases); "normally resolves within" would make the warn
   exact.
3. **A-test pin fragility** (low). The wait-out IT's active-pin precondition is timing-fragile both
   ways: a cold runner whose setup outruns the 5 s crashed-producer timeout plus a 10 s abort-scan
   tick fails the `lso < hw` assert hard; conversely the pin can resolve between the assert and the
   read's capture, silently degrading the run to the already-aborted path. Green on CI today; a
   longer crashed-producer timeout (e.g. 15 s, still ≪ the 60 s outer bound) widens both margins.
4. **Suite-hang on guarded regressions** (low). `ReadSnapshotsSpec`'s fake never advances position on
   an empty poll and test bodies run through `unsafeRunSync()`, so some pinned regressions (an
   isolation flip; `>=` weakened to `>`) would wedge the suite instead of failing red — an
   `IO.timeout` around the bodies converts them. The headline pin (target from the reader's own
   `endOffsets`) does fail red. Also stated: that `initTransactions` is *called* is pinned only by
   the IT (the unit spec's `Producer.empty` cannot observe it).

**#15 — stall deadline**

5. **Eviction-bound proxy** (medium). The upper-bound warning compares `recoveryStallTimeout` with
   the **snapshot** consumer config's `maxPollInterval` — inert for the group-less `assign()`
   readers — while the party actually evicted at `max.poll.interval.ms` is the **driving** consumer,
   whose config the module cannot see. Concrete miss: driving consumer tuned to 60 s, snapshot
   config left at the 5 min default, deadline at the 2 min default → no warning, and eviction beats
   the deadline. R-849.2a is warn-grade (a SHOULD), so this is warning quality, not a register
   violation. Actionable: name the proxy in the warn text and the `recoveryStallTimeout` scaladoc
   ("the snapshot consumer config's `maxPollInterval`, assumed to mirror the driving consumer's").
6. **A position regression resets the clock** (low). `last.filter(_.position == offset)` treats any
   change as progress; after truncation the `Earliest` reset can re-grant a full deadline per
   truncation. A single truncation converges (≤ one extra period, then correctly diagnosed); only
   pathological repeated truncation postpones indefinitely. This is the second benign model
   divergence — `RecoveryDeadline` models progress as advance-only.
7. **Diagnose latency stacks after the deadline** (low). The fail path blocks on the high-watermark
   re-read (bounded by the client's `default.api.timeout.ms`, 60 s default) before the error
   surfaces: 120 s + 60 s still clears the 300 s default eviction, but tight tunings could invert —
   a headroom clause in the guidance covers it. The `handleError` wrapping means a failed re-read
   can only soften the diagnosis, never mask the stall error.
8. **Two-way diagnosis** (low). Every non-truncation stall is labeled an outliving transaction
   (fetch-path-only stalls — quota throttling, an unfetchable partition — would be mislabeled); the
   "likely" hedge keeps this acceptable as shipped.
9. **Boundary, stated** (not a defect). The deadline is checked between returning client calls; a
   wedged call that never returns is outside it. The residual is narrow — `position()` fails loudly
   after its own API timeout, `poll(10ms)` returns — and the advertised stall shapes (LSO pin,
   truncation) keep polls returning, so the design doc's "only client-side bound" claim holds where
   it applies.

Also confirmed clean, condensed: drain termination after abort resolution (control records advance
the position past filtered data, so the captured target is reached); resource lifecycles under
`Resource.use` on all error/cancellation paths; the throttled stall log (one line per ≥5 s window, no
starvation of the fail branch); diagnose laziness (never evaluated on the happy or unarmed path);
the non-transactional `drain` semantically identical to master with `caching`'s public signatures
byte-stable; empty-topic and exact-target edges; 2.13/3 cross-compilation of the changed code.

### Disposition (2026-07-16, applied in the joint PR)

Findings 1–5 and 7 are applied: the `transactionalIdPrefix` scaladoc carries the uniqueness
obligation (mirroring `persistence.md`); the wait-warn says "normally resolves"; the wait-out IT's
crashed producer uses a 15 s transaction timeout with the deadline armed at 45 s (both flake margins
widened); `ReadSnapshotsSpec` bodies run under a 10 s timeout, so a hanging regression fails red;
the poll-interval warning and the `recoveryStallTimeout` scaladoc name the snapshot-consumer-config
stand-in; `persistence.md` says "well below" and "at module acquisition". Findings 6, 8 and 9 are
accepted as recorded.

## Corpus reconciliation (edits owed to the corpus, not the code)

1. **Report vs register on capture-before-init.** [`850-remedy-decision.md`](850-remedy-decision.md)
   §5 lists the orphan-state log (capture the `read_uncommitted` end offset **before**
   `initTransactions`) under "Required when B is adopted"; the register (R-850-C) records it as an
   open design question with the init-duration proxy as fallback — the fallback the implementation
   uses, since capture-before-init conflicts with the acquisition-opens-no-consumer pin
   (`KafkaPersistenceModuleSpec`). Reconcile the report to the register, or decide the capture.
2. **`docs/persistence.md` wording**: the bounds are "warned at startup" → at module acquisition
   (the design doc states it correctly).
3. **On merge**: flip KF16 ⏳→✅; update the register's status snapshot and "Integration gaps" (R-a
   and R-b are carried by the combined implementation, not open); re-home the A-test/B-test/
   R-849-test citations from the upstream draft branches to the merged code.
4. **Release standing**: the register's status table records the mode as "merged, #833,
   EXPERIMENTAL" without release standing; note that v9.0.0 is the only released cut (pre-#842
   surface, F-10 live) so a reader does not assume the remedies shipped.

*Snapshot date: 2026-07-16.*
