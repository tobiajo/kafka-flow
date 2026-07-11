# The `group.protocol=consumer` (KIP-848) experiment

*Kafka arm of the #732 single-writer study — a **realized experiment** (built, cross-compiled 2.13/3, and
integration-tested against a real Kafka 4.3.0 broker) for running the transactional single-writer mode under
the KIP-848 consumer rebalance protocol. Additive and experimental — it does **not** change the
classic-protocol design; classic stays the default and the verified contract. Corpus index:
[`README.md`](README.md).*

This began as a forward-looking sketch and was then implemented. The code was developed on a
standalone experiment branch and consolidated onto this branch as its final state. It is
grounded in the KIP-848 audit in [`kafka-rebalance-semantics.md`](kafka-rebalance-semantics.md), and it
**runtime-confirms** that audit (previously source-only): the predicted background-thread epoch behaviour was
observed against a real broker.

## 1. Scope

Enable kafka-flow's Kafka transactional mode (snapshot write + input-offset commit bound in one producer
transaction, fenced by the consumer generation/member-epoch) under `group.protocol=consumer`, targeting
**Kafka 4.3.0+**. Dynamic members only (no static membership). Not Kafka Streams (KIP-1071); the plain
consumer protocol (KIP-848).

## 2. What did *not* change — the mechanism is already protocol-agnostic

No epoch/generation plumbing was rearchitected. The transactional bind
(`KafkaSnapshotWriteDatabase.commitOffsets` → `sendOffsetsToTransaction(offsets, meta)`, flowing
`Consumer.groupMetadata` → `PartitionAssignment.groupMetadata` → `TopicFlow.add`) passes through whatever
metadata the consumer reports; the broker maps a stale epoch to `ILLEGAL_GENERATION` → abortable
`CommitFailedException` → the transaction aborts, the same graceful path as classic (addendum C4). The
rebalance listener + awaited teardown (`TopicFlow.remove` awaits each flow's teardown inside the revoke
callback before the next poll) relies only on callbacks running on the poll thread, which KIP-848 preserves
for assignment-*changing* rebalances (addendum C3). The `publish` `generationId >= 0` guard stays
load-bearing (the broker accepts the `−1` + empty-member-id transactional commit unfenced, B3).

The generation-currency approach did not need a callback either: it is a **post-poll read** (`refresh`). The
experiment went further and showed the *capture-on-assign* half of the original approach is redundant (§5).

## 3. The enabling change — a vendored skafka fork (the target architecture)

**Blocker:** skafka 20.2.0's `ConsumerConfig` exposes no `group.protocol` field and no raw-properties
passthrough, and it always emits classic-only keys (`partition.assignment.strategy`, `session.timeout.ms`,
`heartbeat.interval.ms`) that the consumer protocol rejects. kafka-clients 4.3.0 (already resolved) supports
KIP-848 — so the client is not the blocker, skafka's typed config surface is.

**First attempt (abandoned): a kafka-flow-side shim.** Build a raw `KafkaConsumer` with `group.protocol` set
and wrap it via skafka's `Consumer.fromConsumerJ1` (which applies skafka's serialization semaphore), plus a
hand-written `Deserializer[ByteVector]`, behind a `groupProtocol` switch in `KafkaModule`. It works and needs
no upstream change, but it bypasses skafka's typed config (two consumer-build paths, a maintenance seam) and
pushes protocol logic into kafka-flow.

**Chosen: fork skafka in-tree so kafka-flow is unchanged.** skafka 20.2.0 is vendored under `lib/skafka` as a
source-fork build subproject (sources verbatim from the v20.2.0 tag; it cross-builds 2.13.18/3.3.7 from a
single, non-version-specific source tree — matching kafka-flow). The **only** fork change is `ConsumerConfig`:
add `groupProtocol: Option[GroupProtocol]` and `groupRemoteAssignor: Option[String]` (+ HOCON parsing) and a
`GroupProtocol` type (`Classic` / `Consumer`); `bindings` then emit `group.protocol` / `group.remote.assignor`
and, under `group.protocol=consumer`, **omit** the three classic-only keys. `core` depends on the vendored
skafka instead of the published artifact; the managed coordinate is dropped; `lib/skafka` is excluded from
scalafmt so the fork stays a minimal diff.

With that, **kafka-flow itself needs no change**: the protocol is selected by setting
`groupProtocol = GroupProtocol.Consumer` on the `ConsumerConfig` passed to `KafkaModule.of`, and
`consumerOf`'s `config.copy(...)` preserves it. Vendoring (rather than depending on an unreleased skafka)
makes the experiment reproducible from source. **Cleaner long-term:** contribute the knob upstream and retire
the fork.

## 4. Assignor

Under KIP-848 partition assignment is server-side, so `partition.assignment.strategy` is not set (the fork
omits it under the consumer protocol). The default `group.remote.assignor` is `uniform`, which is sticky — the
silent-bump test (§8) relies on that stickiness to keep this member's partition in place when a co-tenant
joins. `group.remote.assignor` is exposed on the forked config but left at the broker default here.

## 5. The refresh is the sole currency mechanism — and capture is redundant

The post-poll `refresh` (a `groupMetadata()` read into a `Ref`, run after every poll) is the KIP-848-safe
generation-currency mechanism, and the one that keeps working here:

- Under `consumer`, a **silent bump** (a co-tenant joins, this member keeps its partitions) advances the
  member epoch **on the background thread with no rebalance callback at all** — not one skafka drops (that is
  the classic-protocol issue 581), one kafka-clients never emits. So only a *read* observes it; no callback
  can. This settles a reframing: the refresh is not a workaround for skafka 581, it is the **permanent**
  currency mechanism under `consumer`, and 581 is a dead end for a consumer-bound future (fixing it would let
  capture cover the *classic* silent bump, but `consumer` needs the read regardless).

- **Capture-on-assign is redundant** (the experiment's main code finding). `poll` is
  `consumer.poll(timeout) <* refresh`, so records reach the flow only *after* `refresh` has run; nothing reads
  the generation `Ref` between the assign callback (where capture ran) and that end-of-poll refresh — recovery
  on assignment only reads, transactional commits run in the post-poll flow phase and read the refreshed value,
  and the flush-on-revoke runs *before* the assign callback and reads the prior poll's refresh. So capture's
  write is never observed. Removing it and subscribing the listener directly kept the suite green — **82 core
  unit + 12 integration** on the experiment branch (the 12 include `Kip848ConsumerProtocolSpec`), **121 core +
  14 persistence-kafka unit** on the models branch — including the full #732 transactional suite under classic
  and both zombie fences under `consumer`. The original "capture before the listener so the recovery and the
  flushes it triggers are gated" rationale described a reader that does not exist on the transactional path.
  (One nuance the removal
  makes moot: under `consumer` capture and refresh could read *different* epochs — the background thread can
  bump between them — but since capture's write is never read, the difference never mattered.)

- **KIP-1251 (Kafka 4.3.0)** relaxes the broker: `ConsumerGroup.validateOffsetCommit` accepts a *lagging*
  epoch for a *still-owned* partition, so the spurious-fence (liveness) risk the refresh guards against is also
  handled broker-side. On a guaranteed-4.3.0+ fleet the refresh is belt-and-suspenders (droppable as an
  optimization; a live read would also serve); on 4.0–4.2 it is strictly required, and under `consumer` a read
  is the only way to get it.

Safety is unchanged on every version: a commit for a partition the member no longer owns is fenced throughout
(≤4.2 by exact epoch; ≥4.3.0 also by the per-partition assignment epoch). The liveness property at stake — a
still-valid owner eventually commits — is what the refresh (client) and KIP-1251 (broker) protect.

## 6. Broker floor

KIP-848 is GA from Kafka **4.0**; the experiment recommends and tests **4.3.0** so KIP-1251 minimises spurious
(graceful) transaction aborts on silent bumps.

## 7. Contract (relaxed)

`persistence-kafka/.../KafkaPersistenceModule.scala` (mirrored in `docs/persistence.md` and the design doc's
Forward-looking section) previously stated *"Only the classic consumer rebalance protocol is supported."* It
now reads: classic is the default and verified contract; `group.protocol=consumer` is supported
**experimentally** (set `groupProtocol = GroupProtocol.Consumer`; Kafka 4.3.0+ recommended). The integration
test below is what earned the relaxation.

## 8. Testing (runtime evidence)

Against a real broker, one build serving both protocols:

- **`ForAllKip848KafkaSuite`** pins `apache/kafka:4.3.0` (KIP-848 GA + KIP-1251) and builds a `KafkaModule`
  per protocol via the forked typed config.
- **`Kip848ConsumerProtocolSpec`** (3 tests, green):
  - *silent bump* — a co-tenant joins, this member keeps its partition; **no rebalance callback fires**, yet
    the post-poll refresh **advances the reported member epoch**. (This run also surfaced, and now guards, the
    KIP-848 ordering property that the epoch can be *read* before the initial assign callback drains on the
    poll thread — the baseline is gated on the callback for that reason.)
  - *zombie fence — classic* and *— consumer*: a reassigned-away writer's transactional commit is rejected
    (`CommitFailedException`) and its transaction aborts; recovery reads nothing. The #732 invariant, end to
    end, on both protocols on the same 4.3.0 broker.
- **`Kip848ConfigSpec`** (deterministic, no broker): pins the forked `ConsumerConfig` bindings — `group.protocol`
  set and classic-only keys omitted under `consumer`; retained under classic.

The KIP-848 addendum's "analytical until the IT runs" caveat is discharged for these cases: the
background-thread epoch advance and the no-callback silent bump are now observed, not only reasoned.

*Honest scope of the IT (per the advisory review):* the zombie fence is **synthetic** — it fabricates a
stale generation (`current.generationId - 1`) on a still-joined consumer rather than reassigning the partition
to another member, so it tests "a commit below the partition's assignment epoch is rejected," not an
end-to-end reassigned-away owner. Case A soundly exercises the refresh path (reaching its assertion requires an
observed epoch advance with zero callbacks) but does not *prove* the co-tenant's join was the specific cause of
the bump. Both are valid demonstrations of the load-bearing properties, not exhaustive scenario coverage.

## 9. Residuals / open

- **The async-epoch behaviour is modelled** (correcting an earlier overstatement in this study that called
  the models "synchronous"). `Kafka.tla`'s `GenBump` is a *free* action — it interleaves anywhere, not only
  at a poll — so the background-thread epoch advance was already represented, with `OwnerRefresh` the read
  that observes it (`kafka_genlag` fails without it, `kafka_refines` holds with it). `TokenSync.tla` (added by
  this experiment, in [`../models/`](../models/)) then checks the capture-vs-refresh question directly:
  under a modeled (code-faithful, not derived) capture/refresh asymmetry, **refresh subsumes capture** —
  equivalent when every bump fires a callback, strictly better on a silent bump (`tokensync_*`, the 2×2 over
  {capture, refresh} plus the equivalence boundary; `model-audit.md` addendum). What remains is surgical, not a
  correctness gap: `Kafka.tla` still carries capture as the design of record it was written against, so ripping it out there
  (and retiring its `Coupled` hazard) is optional cleanup.
- **Capture removal has no safety cost — it only constrains recovery-time writes (a caveat, not a current
  defect).** Capture is redundant given today's readers: nothing reads the generation `Ref` between the assign
  callback and the post-poll refresh (recovery is read-only; commits run post-poll, or on revoke read a prior
  poll's refresh). If a future change made recovery *persist* (an eager snapshot-on-recovery, a fold that flushes
  during `init`), a first-assignment commit would hit the `None` branch and **fail loud** (`IllegalStateException`)
  and a re-assignment would read a stale generation and be **broker-fenced** — both fail-safe, never an ungated
  commit; capture would instead have let them succeed. A "no commit before the first refresh" guard/test would pin
  it if the mode graduates.
- **Upstream skafka** should gain `group.protocol` / `group.remote.assignor` (ideally a raw-properties
  passthrough); then the vendored fork retires. `journal` (via kafka-journal) still pulls the published skafka
  transitively, so a root-level aggregate build sees two skafkas — harmless for the experiment modules, which
  resolve only the vendored one, but another reason to prefer the upstream route.
- `group.remote.assignor` choice (uniform vs range) and its interaction with the single-writer goal.
- Static membership (`group.instance.id`) is out of scope.
