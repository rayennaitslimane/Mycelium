# ADR 0005: Fencing, not clocks, protects writes

Status: Accepted · 2026-10-18

## Context
A primary that is partitioned or paused can wake up still believing it is primary after a new
primary has taken over. Its in-flight writes must not complete.

## Decision
- **Each PG on each holder has a durable `fence` (an epoch)**, kept in the store
  ([shard_store.cpp](../../node/store/shard_store.cpp)). The store is the fence's only owner.
- **Every primary-to-holder request carries the sender's map epoch.** A holder rejects it with
  `409 {fence}` if `epoch < fence`.
- **The fence check runs atomically with the operation it guards**, under the per-PG mutex in
  the store.
- **Peering starts with `scan`, which raises the fence to the new epoch** on every slot in `U`
  before the new primary does anything else.
- **On `409`, the primary steps down:**
  - it stops serving the PG;
  - it refetches the map;
  - it does nothing more for that PG until a newer epoch arrives (`peers.ErrStepDown`).

## Protects
Invariant 3: a deposed primary cannot complete a write. Completing one needs an ack from every
slot in `U`, and the new primary has already fenced all of them. Write safety therefore needs
no clocks.

## If violated
- **Fence check separate from the operation:** a stale write could slip in between the check
  and the write.
- **Fence kept only in memory:** a holder restart would forget it and accept the old primary's
  writes.

Either way, two primaries could each complete conflicting writes.

## Rejected
- **Leases alone:** they depend on clocks, and a long pause (GC, VM migration) breaks them. The
  serve gate (ADR 0008) still uses a lease, but only to keep reads fresh, never for write safety.
