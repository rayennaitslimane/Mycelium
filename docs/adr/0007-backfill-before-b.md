# ADR 0007: A node enters `B` only through backfill

Status: Accepted · 2026-10-18

## Context
A node that was away (or is new to a slot) may lack writes. If it is read from or counted in
peering before catching up, old or missing data wins.

## Decision
- **A node always purges before it holds a slot.** On `404` or `out`, the agent deletes all of
  its local PG data before `join`. The purge waits for in-flight shard operations, so none can
  land after it ([agent.go](../../node/agent/agent.go)).
- **A newly assigned slot starts `backfilling`:** it is in `U` (it receives every new write,
  ADR 0004) but not in `B` (never read from or counted in peering).
- **Backfill (primary):** for each object, under its lock, the primary copies the latest
  committed version to the slot unless `lookup` shows it already there. Then it sends
  `backfill_done`.
- **The monitor accepts `backfill_done` only if its epoch is current** and the slot still
  belongs to that node. Otherwise the primary re-peers and resends.

## Protects
Invariant 2: a node joins `B` only after it has everything. A node that missed a write is never
read from or counted.

## If violated
- **Joining `B` early, or skipping the purge:** reads or peering could see stale data, and the
  stale data could be chosen as the winner.
- **Accepting a stale `backfill_done`:** a slot could be marked complete for a placement that no
  longer exists.

## Trade-offs
- **Full re-backfill:** any node out longer than `grace` is fully re-backfilled; there is no
  delta recovery (a spec non-goal).
- **Slow convergence:** each accepted `backfill_done` bumps the epoch and makes other PGs'
  concurrent reports stale, so they retry. Recovery converges, but slowly when many PGs
  backfill at once.
