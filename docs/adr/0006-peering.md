# ADR 0006: Peering decides the truth after a failure

Status: Accepted · 2026-10-18

## Context
After a map change or a failed operation, holders may have staged versions that were never
committed everywhere. The primary must settle every such object before serving it.

## Decision
Peering runs on the primary whenever the epoch changes or an operation fails. The PG is
inactive and answers `503` meanwhile ([peering.go](../../node/primary/peering.go)).
1. **`scan` every slot in `U`:** this fences them (ADR 0005). All must answer. The dirty ids are
   the union of what they return.
2. **`lookup` the dirty ids on `B`.** The **winner** is the highest version held by **≥ `k`
   slots of `B`**.
3. **With a winner `w`:**
   - commit `w` where it is held;
   - re-encode it onto `B` slots that lack it;
   - discard every version above `w`.

   **Without a winner,** discard every version seen. Discards go to all of `U`.
4. **Mark the PG active.**

Rules that keep peering correct:
- **`min_size ≥ k`:** an acknowledged write is on every slot in `B` (ADR 0004), so any `k` of
  them find it.
- **Quorum split:** peering and backfill need only `|B| ≥ k`; client I/O needs `|B| ≥ min_size`.
- **No overlap with operations:** the per-PG read/write lock is held shared by each operation
  and exclusively by peering.

## Protects
Invariants 1 and 3. Acknowledged writes are kept, and unacknowledged ones are settled one way or
the other.

## If violated
- **Peering requiring `min_size`:** with `m=1` and the default `min_size = k+1`, one failure
  leaves `|B| = k`. The PG then can't peer or backfill, so it stays stuck forever. This is why
  the code departs from the spec's literal "serviceable only if `|B| ≥ min_size`" for peering.
- **A winner below `k` copies:** a version that can't be decoded becomes the truth.
- **Peering during a write:** the write's commit could undo a peering discard, or the reverse.

## Rejected
- **Choosing the winner by majority vote:** could ignore an acknowledged write that only `k`
  slots hold.
