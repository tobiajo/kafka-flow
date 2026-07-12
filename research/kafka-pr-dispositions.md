# Dispositions: the generation-refresh PR discussions

*Kafka arm of the #732 single-writer study — closure record for the PR-review discussions; every argued thread with its terminal disposition. Corpus index: [`README.md`](README.md).*

Closure record for the design discussions on the Kafka generation-refresh change (upstream #843).
Every thread that was argued during
review is listed with its terminal disposition and the evidence that settles it, so none needs
relitigating. Protocol facts cite `kafka-rebalance-semantics.md` (primary-source-verified, 3-vote
adversarial); test facts cite the named specs; empirical claims cite the measured experiments.
Anything not closed is listed under Open at the end — everything else is SETTLED.

## A. Design-doc framing

**A1. "The generation — authoritative for partition ownership" (SETTLED: dropped).** The generation is
the group's *member-level* authority; the broker validates member + generation and never which
partitions the member holds (semantics verdict 5, classic coordinator, 3-0). Per-partition
single-writer is a composition — assignment grants, the fence revokes group-wide, the revoke-time
teardown stops a former owner locally — so the doc no longer asserts the token alone is authoritative
for ownership.

**A2. Refresh altitude (SETTLED: mechanism out of the doc).** *Under the classic protocol* the post-poll
refresh reads as a workaround for skafka issue 581 (its typed listener drops the empty `onPartitionsAssigned`
kafka-clients emits on every completed rebalance; semantics verdict 7), so a workaround gets a workaround's
footprint: the doc states the durable requirement — the captured generation must follow every
generation bump, not just an assignment that changes this member's partitions — and the words
"refresh"/"after each poll" appear nowhere in it; the mechanism lives in `Consumer.scala` with the
issue linked. Note this "581 fix would let one code site change" framing is classic-only: under
`group.protocol=consumer` a silent bump fires no callback at all, so the read is the **permanent** mechanism,
not a workaround (D2; `kafka-consumer-protocol-experiment.md` §5). The experiment further showed the
capture half of it is redundant even under classic (F-8 corollary, KF11).

**A3. The "Single-writer ownership" section (SETTLED: deleted).** A 23-line ownership set-piece was
added mid-review and produced a coherence clash with the doc's "what is missing by default is only the
link" framing. Four reconciliation drafts (minimal patch / single arc / rescope+retitle / coordinated),
each independently reviewed (scores 8–9/10), were all rejected — correctly: the section itself was the
accretion. It was deleted; the doc returned to master's shape plus the refresh. The lesson is recorded
in the report's method notes: when every arrangement of an addition fights the document, remove the
addition.

**A4. The lock/lease/fencing-token analogy (SETTLED: kept, with one key point).** Challenged twice. The
analogy is a safety story and holds within that scope: the token fences a stale member; binding the
write to the fenced commit extends the broker's verdict to the store. The genuine gap — the safety
story is not self-contained, because a *still-valid* member could commit a partition it just lost —
is stated as a Key point (the fence is per member + generation, not per partition; closed client-side
by the awaited revoke-time teardown). With that key point present, "only the link is missing" stays
honest: the teardown is pre-existing behavior the mode relies on, not something the feature adds.

**A5. Wiring-paragraph self-contradiction (SETTLED: fixed).** The paragraph said the metadata is
"captured on each partition assignment" — the exact insufficiency the PR fixes — four review angles
independently flagged it; now "captured on each partition assignment and kept current thereafter".

**A6. "The offset advances on the periodic tick or on revoke" (SETTLED: qualified).** Unconditional as
written, wrong under a cooperative assignor, where the revoke-time commit never lands (B2). Now states
the revoke-time commit is best-effort, with the cooperative behavior and its consequence (the new
owner replays; `flushOnRevoke` does not shrink the replay window there) in the doc, `persistence.md`,
and the `Consumer.scala` comment — three tellings, one wording.

## B. Mechanism semantics (pinned in kafka-rebalance-semantics.md)

**B1. "Callbacks and the generation move only inside poll" (SETTLED, verdicts 1–2).** Under the classic
protocol all generation movement and every listener callback run on the application thread inside
`poll()` (plus `close()`/`unsubscribe()` for the leave path). The heartbeat thread can only *reset* the
internal generation, never advance it, and never touches the public `groupMetadata()`, which is
assigned solely in the constructor and `onJoinComplete`. The "could it advance between polls?" worry is
closed for classic; it is real only under KIP-848 (B5).

**B2. Eager vs cooperative revoke (SETTLED, verdicts 3–5).** Eager revokes in `onJoinPrepare`, before
the join, with the still-current generation — the revoke-time flush lands in a graceful rebalance.
Cooperative revokes in `onJoinComplete`, after the member has already adopted the new generation
(stamped before the callback), still inside the same poll — so the flush, carrying the held
generation, is always fenced and its transaction aborts. That is the safe direction: the partition
already belongs to the new owner, and since the broker never checks partition ownership, capturing the
advanced generation inside the revoke callback would let the late flush overwrite the new owner's
snapshot. Fenced-is-correct; the cost is availability only (full replay of the revoked partitions),
now documented rather than promised away. **Since runtime-pinned** by `RevokeTimeFlushSpec`
(real second-member rebalance, no simulated generation): cooperative-sticky flush fenced with the
callback observing the already-advanced live generation, eager-sticky control commits — see the
runtime-pin note under the verdicts table in `kafka-rebalance-semantics.md`.

**B3. Live `groupMetadata` reads (SETTLED: rejected, now for a pinned reason).** Beyond the original
objections (the serialized consumer cannot be called inside the callback; lag is the safe direction),
a live read inside the cooperative revoke callback would observe the *new* generation and un-fence the
late flush — the corruption case. The captured token's lag at revoke time is load-bearing, not an
implementation accident.

**B4. The −1 sentinel (SETTLED, verdicts 2 and 6).** Reported only until the first join completes;
after fall-out the client keeps the last joined generation (a comment claiming otherwise was corrected
once, lost in a history squash, and re-applied — the drift that motivated pinning this study). The
sentinel cannot reach a transaction in this design (flows exist only after assignment, and capture runs
before the listener), so `publish`'s `generationId >= 0` guard is defensive; its rationale is stronger
than previously stated — on the transactional path the classic coordinator accepts the sentinel
unfenced even against a non-empty group.

**B5. KIP-848 (SETTLED: audited — the addendum in kafka-rebalance-semantics.md).** The re-audit the
docs called for has been done (kafka-clients 4.3.0 bytecode + broker/coordinator sources; addendum
verdicts C1–C5, B1–B5). Outcome: the design stays safe under `group.protocol=consumer` — the captured
epoch still lags and never leads, offset-commit validation fences any lag (`STALE_MEMBER_EPOCH`) — exact
equality ≤4.2.0, and from 4.3.0 a lagging commit for a *still-owned* partition is instead accepted
(KIP-1251), which only removes spurious fences — the
revoke-time flush is accepted at the current epoch with reassignment deferred to the acknowledgement,
and the flows-alive coupling survives (reconciliation waits for the callback). What changes versus
classic: the epoch advances on the background thread between polls (and even mid-callback), and a silent
bump has no callback at all — so a `groupMetadata()` *read* (the post-poll refresh, or a live read) is
the only thing that tracks it, though no read fully closes the read-to-commit window; and the
transactional sentinel skip survived into the new coordinator (KAFKA-18060), so the publish guard stays
load-bearing. The *failure mode is unchanged* — a transactional fence is an abortable
`CommitFailedException` under both protocols (the coordinator translates the stale-epoch error to
`ILLEGAL_GENERATION` on the TxnOffsetCommit path; an earlier draft calling it "fatal/harsher" is
corrected). The design keeps the classic protocol as its default and verified contract; the
consumer protocol is now *selectable and runtime-tested* via the vendored skafka fork
([`kafka-consumer-protocol-experiment.md`](kafka-consumer-protocol-experiment.md)) — the earlier "skafka's
`ConsumerConfig` cannot select the new one" is superseded — but the residual read-to-commit window plus the
addendum's open items keep it a deliberate, experimental step.

## C. Code and tests

**C1. The teardown pin was not a pin (SETTLED: rewritten, verified both ways).** scache's `remove`
eagerly forks the entry release, so the original assertion — check the release Deferred right after
`remove` — passed 99.8–100% of 3 000 measured runs *with the await removed*. Rewritten: the finalizer
is gated on a latch and the program runs under `TestControl`, where virtual time advances only when no
fiber can progress — "the sleep won the race" now proves `remove` was blocked on the teardown, not
merely slow. Verified empirically in both directions: passes as written; 3/3 deterministic failures
with `.flatten` dropped.

**C2. Teardown test redundancy (SETTLED: both needed).** `PartitionFlowSpec`'s on-release tests pin
that a flow's release flushes/commits; `TopicFlowSpec`'s pin covers the coupling — that `remove`
*awaits* that release before the consumer proceeds. Dropping the await breaks no `PartitionFlowSpec`
test; only the coupling pin catches it. The whole-`TopicFlow` resource release is the shutdown path
(member leaving), not the cross-partition safety path, and needs no separate pin.

**C3. Per-poll refresh cost (SETTLED: off the instrumented path).** In the standard wiring the refresh
re-read went through the metrics+logging wrappers (~100×/s at the default poll timeout: an extra
serialized `assignment()` call, per-topic metric samples, a debug line). The consumer is now built
uninstrumented, the same wrappers applied for the operator surface (`ConsumerOf` with metrics wraps via
the identical `withMetrics1`, bytecode-checked), and the refresh reads through the raw consumer;
`publish` writes the Ref only on change. Binary-compatible (added overload; MiMa green).

**C4. Review findings ledger (SETTLED: all addressed or refuted).** The adversarial review of the PR
produced, besides C1/C3/A5/A6: the IT's `createTopic` hoisted into the suite (blocking-safe,
idempotent); the skafka-issue canary named in the IT scaladoc (a dependency upgrade fixing 581 turns
the no-callback premise red — the unwinding signal, not a regression); `pollUntil` returning the
witnessed value (dead error branch deleted); minor cleanups. Two candidates were REFUTED and are not
defects: "`poll <* refresh` drops a fetched batch on refresh failure" (the retry re-acquires the
consumer from committed offsets — duplicates, not loss) and any legitimate value being refused by the
`generationId >= 0` guard (member generations are ≥ 1).

**C5. The refresh's own safety (SETTLED, long since).** The token is only ever the member's own
generation — it can lag but never lead, so keeping it current removes the spurious fence without
weakening the real one. Model-checked earlier (`kafka_genlag` violates liveness with refresh off,
safety holds throughout; `kafka_refines` holds with it on) and reproduced against a real broker by
`ConsumerGroupMetadataSpec` (zero callbacks on the retained member; the generation advances anyway).

## D. Open (deliberately, and only these)

1. **KIP-848 residuals** — the audit is done (B5) and the consumer-protocol path is now *built and
   runtime-tested* (the realized experiment, [`kafka-consumer-protocol-experiment.md`](kafka-consumer-protocol-experiment.md):
   a vendored skafka fork, the stale-writer fence proven under both protocols on Kafka 4.3.0). Two items
   once listed here are settled: the stale-transactional-commit wire error (`ILLEGAL_GENERATION` → graceful
   abort — addendum C4), and the lagging-epoch relaxation, which **shipped for consumer groups in Kafka 4.3.0
   via KIP-1251** (KAFKA-19779 is the StreamsGroup-only sibling). The async-epoch behaviour is modelled
   (`Kafka.tla`'s `GenBump` is a free action; `TokenSync.tla` proves refresh subsumes capture) — so the only
   residual before `consumer` graduates from experimental to supported is judgement/soak, not a missing model;
   the surgical removal of capture from `Kafka.tla` (retiring its now-moot `Coupled` hazard) is optional cleanup.
2. **skafka issue 581 — a dead end for a consumer future (reframed).** The experiment settles this: under
   `group.protocol=consumer` a silent epoch bump fires **no** callback at all (not one skafka drops — one
   kafka-clients never emits), so a `groupMetadata` *read* is the only mechanism and there is nothing a 581
   fix could unwind. The post-poll refresh is therefore the **permanent** currency mechanism under `consumer`,
   not a 581 workaround, and the "581 fix makes capture sufficient" canary premise holds only for classic.
   Relatedly, the experiment proved **capture-on-assign redundant** even under classic (F-8 corollary, claim
   KF11): the refresh alone suffices, since nothing reads the generation `Ref` before the end-of-poll refresh —
   proven at the model level by `TokenSync.tla` and empirically (whole suite green with capture removed).
3. **Where the review line lands** — the review fixes live on a working branch off the PR head,
   pending the maintainer's fold (append or squash) into the PR.
4. **Cooperative flush-on-revoke** — always fenced (B2) is documented and safe; *skipping* the flush
   under a cooperative assignor (saving a doomed transaction round-trip) would be an availability
   optimization, considered and not pursued.

Everything above the Open list is settled; a future edit that touches these areas should cite this
record and `kafka-rebalance-semantics.md` rather than re-derive from memory.
