# ADR 0002: Placement: fixed hash, explicit table, no rebalancing

Status: Accepted · 2026-10-18

## Context
Each object needs a stable home that every component can compute, and data should move as
little and as predictably as possible.

## Decision
- **Object to PG:** first 8 bytes of `sha256(id)`, mod `pg_num`. `pg_num` is fixed at
  bootstrap, so an object never changes PG.
- **PG to nodes:** an explicit table stored in the map (`pgs[pg][slot]`).
  - It is created once, when `bootstrap_nodes` nodes are up: slot `i` of PG `p` is the node at
    index `(p+i) mod N` of the sorted node ids.
  - Later joiners are spares.
- **The table changes only when a node fails.** A slot whose node is `out` (or vacant) goes to
  the up node not already in that PG that holds the fewest slots (ties: lowest id), marked
  `backfilling`. With no candidate, the slot stays vacant.
- There is no rebalancing: adding nodes never moves data.

## Protects
Predictability. Data moves only to replace a failed node, and only through backfill
(ADR 0007).

## If violated
Data would move without anyone deciding to move it, or a placement change without backfill
would leave holders that lack the data. Changing `pg_num` would make existing objects
unreachable.

## Trade-off
New nodes add no capacity until a failure hands them slots, and load can become uneven over
time. Growing the cluster would require a new monitor transition that moves slots and reuses
backfill.

## Rejected
- **Rendezvous hashing or CRUSH:** placement computed from the node set rebalances
  automatically. That is more data movement, and placement is no longer an explicit, auditable
  fact in the map.
