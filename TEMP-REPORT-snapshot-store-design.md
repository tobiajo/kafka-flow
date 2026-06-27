# TEMP / scratch — Decision report: snapshot-store design

> Throwaway report (do not merge). Principal-engineer recommendation on which snapshot-store design to
> adopt. Evidence and the design-space analysis are in `TEMP-RESEARCH-snapshot-store-design.md`.

## TL;DR

1. **Merge `#834` (`92ef452`) as-is, now.** It is the bug fix; it is reviewed and tested; the only
   quality wart (the `get`/`recover` footgun) is *latent*, not active.
2. **Do the unification as a separate follow-up PR — adopt `cas-unify` (the unify branch).** Not because
   it is materially safer than `#834` (behaviour is identical), but because it removes the footgun and
   collapses three type families to one, at a small, self-contained, fully-tested cost.
3. **Do not ship MG-B** (drop-`get`-only): it pays an API break for a partial cleanup.
4. **MG-A is the fallback** if, and only if, review objects to the buffer reusing the DB's `Stored`.

## Why not block `#834` on the refactor

`#834` fixes a real production-grade defect (`#732` stale-writer corruption + the replay-window
self-fence/livelock). It is correct, tested against real Cassandra/Kafka, and reviewed. The
unification changes **zero behaviour**. Coupling a no-behaviour-change refactor — which is also a
**public API break** — to a shipping bug fix is bad sequencing: it re-opens review, widens the blast
radius, and delays the fix for no functional gain. Ship the fix; refactor on its own clock.

The footgun does **not** raise the urgency: in `#834` the one wrapper that could trip it (metrics)
already overrides `recover` correctly. It is a trap for *future* code, which a follow-up removes in
time.

## Why `cas-unify` for the follow-up (over MG-A and MG-B)

The follow-up exists to (a) remove the footgun and (b) stop encoding live/tombstone/absent in three
separate type families. All three candidates do (a). They differ on (b) and on cost:

- **MG-B** keeps `Recovered` + `Buffered` + `persist`/`delete` and only drops `get`. It breaks the
  public API (however slightly) for a *partial* cleanup — the worst effort-to-payoff ratio. If you are
  going to break the contract at all, land the clean end state. **Rejected.**
- **`cas-unify`** reaches one `Stored` ADT spanning read + write + buffer: the smallest end state,
  least code, no translation layer. Its cost is collapsing the read-contract/buffer-state boundary —
  but today `Stored` and `Buffered` are the same shape, so that coupling is benign, and the ADT admits
  no illegal states (the earlier two-`Option` record, which did, is discarded).
- **MG-A** keeps that layer boundary (`Stored` at the DB, `Buffered` in the buffer) for a 3-line
  translation. It is *equally* sound and equally footgun-free; the decoupling it buys is mostly
  future-proofing against `Stored` and `Buffered` diverging.

**Recommendation: `cas-unify`.** Prefer the simplest correct end state. Adopt **MG-A** only if a
reviewer makes a concrete case that the DB wire type and the in-memory buffer state will diverge (e.g.
a planned `Stored` field the buffer should not carry) — then the boundary earns its keep. Absent that
argument, the boundary is ceremony.

This is a low-stakes decision: `cas-unify` ↔ MG-A is a near-tie, both behaviour-identical and
test-green. Do not spend review capital litigating it — pick `cas-unify`, and treat a reviewer's
preference for MG-A as cheap to honour (the probe at `mg-probe` shows it is isolated to one file).

## Consequences of the follow-up (either A1 option)

- **Public API break.** `SnapshotDatabase` implementors migrate `get`/`recover`/`persist`/`delete` →
  `read`/`write`. In-tree implementors and the metrics wrapper are already migrated on `cas-unify`.
  External custom stores (the `persistence.md` "Custom snapshot storage" extension point) must update;
  that doc is updated on `cas-unify`.
- **Restack.** Landing it changes `#834`'s tip ⇒ `#835` (models) must restack onto it (low conflict —
  TLA+/docs only).
- **No data/format change, no migration.** Wire format (Cassandra rows, Kafka records) is untouched.
- **Net production code shrinks** (`cas-unify`: main `+160/−170`).

## What would change this recommendation

- If `#834` had **not** yet been reviewed/shipped, I would still ship the bug fix first, but the cost
  of folding the refactor in would be lower — adopting `cas-unify` *as* `#834`'s design would be
  defensible to save one review cycle.
- If a custom-store consumer outside this repo can't absorb an API break on a reasonable timeline, keep
  `#834` (A0) and apply **only** the footgun mitigation by convention (keep overriding `recover` in
  wrappers) rather than breaking the contract.
- If `Stored` and `Buffered` are slated to diverge, switch the follow-up to **MG-A**.

## Status of the branches referenced

- `cas-unify` — the unify design; one clean commit; code + tests + docs; fully green incl. IT.
- `mg-probe` `956501d` — MG-A evidence probe; `core` compiles; isolated to `Snapshots.scala`; tests not
  migrated (do not merge).
