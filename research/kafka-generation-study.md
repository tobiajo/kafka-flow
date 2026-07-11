# Study: the generation refresh-on-poll change (the Kafka capture/refresh delta)

*Kafka arm of the #732 single-writer study — verification of the generation-capture fence: load-bearing claims, seam attacks, model check, backport map. Corpus index: [`README.md`](README.md).*

Subject: the changes between master and the rebased stack's base — deriving the transactional
config from the flow, `PartitionAssignment`, **the post-poll generation refresh**, and
the callback-ordering note. Method as in `cassandra-report.md`: claims re-derived, seams attacked, externals
verified at Kafka source level (3.9.0 + trunk), the fix model-checked.

## What the change is

Assignment-time capture alone misses rebalances that assign the member nothing new (cooperative
assignor; another member joins): the generation bumps, the captured token lags, and the next
transactional flush of a *retained* partition is spuriously fenced (`CommitFailedException`) — an
availability defect in the fail-safe direction. The fix refreshes the published token after every
poll (`poll <* refresh`), guarded so the pre-join unknown sentinel (−1) is never published.

## Verdicts on the load-bearing claims

| Claim | Verdict | Evidence |
|---|---|---|
| Cooperative rebalance can bump the generation with no observable callback on a member | **CONFIRMED, mechanism refined** | kafka-clients *does* invoke `onPartitionsAssigned` with an **empty** delta on every completed rebalance (Javadoc-guaranteed; `ConsumerCoordinator.onJoinComplete` invokes unconditionally — unlike cooperative revoked and the lost paths, which are empty-gated; eager revoked has no such guard, see kafka-rebalance-semantics.md). It is the typed listener layer (skafka `NonEmptySet`) that cannot forward an empty set. The branch's new IT (`ConsumerGroupMetadataSpec`) pins the end-to-end premise against a real broker — zero callbacks on the retained member, generation advances only via refresh. Doc/comment now attribute the mechanism precisely. |
| "Rebalances complete within a poll, so the refresh always observes the post-rebalance generation" | **PARTIALLY CONFIRMED → corrected** | Callbacks and the `groupMetadata` update do run on the poll thread; but since KIP-266, `poll(Duration)` does **not** block on the join — a round can span polls, and `groupMetadata()` after a poll reflects the last *completed* join. The refresh therefore converges on the poll after the round finishes; the interim lag self-fences (safe direction). Availability fix intact; wording corrected in code comment + doc. |
| Publish guard: "-1 before the first join **and after falling out of the group**" | **HALF-REFUTED → corrected** | Source-verified: `ConsumerCoordinator` assigns the public `groupMetadata` only in the constructor and `onJoinComplete`; `resetStateAndGeneration` resets the *internal* generation, not the public token. After fall-out the client deliberately keeps the stale joined token — which is exactly what gets a zombie fenced. The guard is still load-bearing (every startup polls before the first join completes), but its comment claimed the wrong second scenario; corrected, along with the doc's live-read-rejection bullet (whose other two reasons stand). |
| −1 + empty memberId skips coordinator validation (would land unfenced) | **CONFIRMED** | `GroupCoordinator.validateOffsetCommit` (and the KRaft `ClassicGroup` twin): the transactional branch falls through to accept exactly that shape — the pre-KIP-447 compatibility path. The guard is what stands between a pre-join refresh and an unfenceable commit. Since source-verified for KIP-848 too: the new coordinator accepts exactly this sentinel shape unfenced from 4.0 (KAFKA-18060 deliberately restored it after an initial rejection), so the guard is load-bearing under both protocols (kafka-rebalance-semantics.md, KIP-848 addendum B3). Gate shapes since pinned: the classic skip fires on *any* negative generation (not only −1), the consumer-group type on −1 exactly and only from 4.0.0 — full per-version span table in external-semantics.md ext(K3). |
| Refresh cannot weaken the fence ("every flow alive after a poll is owned in the refreshed generation") | **CONFIRMED — holds by the documented rebalance contract + awaited teardown; only a regression test/model is missing (see Residual risks)** | Two halves: (a) TxnOffsetCommit validates member+generation but **no per-partition ownership** (source-verified; the validators take no partition arguments) — so this invariant is the *only* thing preventing a lingering foreign-partition flow from committing under a fresh token, and refresh removes the incidental stale-token second net; (b) the invariant holds by construction: `TopicFlow.remove` awaits the cache release (`cache.remove(_).flatten`, `parTraverse_`) inside the revoked/lost callback, and the client invokes revoked/lost before assigned, with the refresh after the poll. Release errors are swallowed but the entry is still removed. The construction is bounded by the rebalance timeout: teardown stalling past it gets the member evicted and the partition reassigned over still-live flows — outside the invariant, but an evicted member is *removed* from the group, so its commits fail member validation (`UNKNOWN_MEMBER_ID`, before any generation comparison — same abortable `CommitFailedException`). Normal path closed by teardown, eviction path by the broker's rejection; neither relies on the timeout never firing. |
| Lag-safe / lead-unsafe asymmetry ("the token never leads") | **CONFIRMED** | The token is only ever a generation the member actually joined (capture on assignment; refresh publishes the member's own current membership). A leading token is unrepresentable through this path. |

## Model check

`Kafka.tla` extended with the owner's side of the token (it previously modelled only the zombie's):
`oCapturedGen` gates `OwnerFold` at the broker (rejected ⇒ teardown/recover, like any failed flush);
`GenBump` is the no-assignment generation bump (fires no capture); `OwnerRefresh` is the post-poll
refresh (`Refresh` knob; weakly fair — polls keep happening). Results:

- **`kafka_genlag`** (`Refresh=FALSE`, the pre-fix code): the spurious-fence **livelock** — reject →
  teardown → recover (no re-capture: no assignment happened) → reject… TLC exhibits the lasso;
  `RefLive` VIOLATED. Safety holds throughout (a rejected commit writes nothing).
- **`kafka_refines`** (`Refresh=TRUE`, with `GenBump` reachable): HOLDS — the refresh re-syncs and the
  owner completes.
- The zombie side is untouched by `Refresh` (the model's `Poll` already captured the live generation —
  the refresh made the model *more* faithful, not less); `kafka_decoupled` remains the control for the
  now-solely-load-bearing teardown coupling.

Suite (as of this sub-study): 30 configs, 17 negative controls, all as declared.

## Residual risks / notes

- **KIP-848**: since audited — the addendum in `kafka-rebalance-semantics.md` pins it. The two
  suspicions this note originally raised were both confirmed: the member epoch does advance on the
  background heartbeat thread (the poll-boundary reasoning dissolves), and the −1 transactional
  compatibility skip does survive into the new coordinator (KAFKA-18060 — deliberately restored for
  4.0, so the publish guard stays load-bearing). The design's fail direction stays fenced/crash; see
  the addendum's consequences section for the full picture. **Since built and runtime-tested** — the
  `group.protocol=consumer` experiment (`kafka-consumer-protocol-experiment.md`) enables it through a
  vendored skafka fork and proves the stale-writer fence under *both* protocols on a real 4.3.0 broker
  (`Kip848ConsumerProtocolSpec`); the audit's predictions (background-thread epoch advance, no-callback
  silent bump) were observed, not only reasoned. Experimental; classic stays the default and the
  verified contract.
- **Capture-on-assign is redundant with the refresh** (a corollary the experiment surfaced; findings F-8
  corollary, claim KF11). Nothing reads the captured generation between the assign callback and the
  end-of-poll refresh, so the refresh alone is the currency mechanism; removing capture kept the affected
  suites green (82 unit + 12 IT on the experiment branch; 121 core + 14 persistence-kafka unit on the models
  branch), and `TokenSync.tla` complements it at the model level (refresh subsumes
  capture — equivalent only when every bump fires a callback, which the `consumer` silent bump does not).
  Under `consumer` the refresh is the *permanent* mechanism, not a skafka-581 workaround.
- **The flows-alive invariant is the sole fence support** for the cross-partition case (no broker
  per-partition ownership validation, no stale-token second net). **Evidence grade (corrected by
  advisory review): present correctness established, not in doubt.** It holds by Kafka's documented
  `ConsumerRebalanceListener` contract — `onPartitionsRevoked`/`onPartitionsLost` run synchronously on
  the poll thread, before `onPartitionsAssigned` and before `poll` returns (javadoc; the
  revoked/lost-before-assigned ordering is source-verified above) — together with `TopicFlow.remove`
  awaiting the teardown inside that callback (`cache.remove(_).flatten` under `parTraverse_`, on both
  the revoked and lost paths). An earlier post-F-7 pass over-graded it *argued, unverified*, as if an
  in-house code-reading assumption; a documented external contract is not that. The genuine residual is narrower: nothing *pins* the synchronous-await, so a
  future refactor to fire-and-forget teardown would break it silently, and `Kafka.tla` assumes the
  teardown coupling rather than checking it. **Both candidate closures are now done.** (1) `FlowsAlive.tla`
  makes teardown a separate, interleavable action gated by an `AwaitTeardown` knob, and
  `INV_FlowsAlive == live ⊆ owned` is checked as *safety* (a single un-owned commit corrupts, so eventual
  removal is not enough) — `flowsalive_holds` HOLDS with the awaited coupling, `flowsalive_race` VIOLATES
  it under fire-and-forget (a reassignment leaves the old flow alive-but-un-owned), so the coupling is
  shown load-bearing, not incidental. (2) A unit test on the Kafka branch pins it in code: `TopicFlowSpec`
  "remove awaits the flow teardown" adds then removes a partition whose flow release completes a
  `Deferred`, and asserts it is completed by the time `remove` returns — so a fire-and-forget refactor
  fails the build rather than only logging at runtime.
- The rebase left `persistence-cassandra-it-tests/FlowSpec` uncompilable against the new
  `PartitionAssignment` API (found by the stack re-verification; fixed — cassandra-branch backport).

## Backport map from this study

- → the refresh-on-poll change: the two claim corrections (Consumer.scala publish/refresh
  comments; kafka-single-writer-design.md capture bullet + live-read rejection bullet).
- → cassandra branch: the FlowSpec `PartitionAssignment` compile fix.
- → models branch: `Kafka.tla` owner-token extension + `kafka_genlag` + README row.
