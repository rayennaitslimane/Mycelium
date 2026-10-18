# ADR 0004: Writes: stage everywhere, then commit everywhere

Status: Accepted · 2026-10-18

## Context
An acknowledged write must survive any failure that leaves the PG serviceable, even though its
shards are spread over several nodes.

## Decision
The primary takes the object lock, assigns `v = (epoch, ++seq)`, encodes the object, then:
1. `PUT` each shard of `v` to **every slot in `U`**, backfilling slots included. All must ack;
   holders store it as staged.
2. `commit v` on **every slot in `U`**. All must ack. A commit promotes `v` and deletes older
   versions.
3. Reply with `v` only then.

On any failure, the primary replies `503` and re-peers (ADR 0006). The best-effort `discard v`
is sent **only if step 1 failed**, before anything was committed
([io.go](../../node/primary/io.go)).

## Protects
Invariant 1: every node in `B` holds every acknowledged write. Peering can find it among any
`k` of them. Sending writes to backfilling slots means a node being backfilled never misses a
write (ADR 0007).

## If violated
- **Acknowledging before all of `U` commits:** a later `B` could lack the write, and peering
  might not find it. The write is lost.
- **Discarding after a partial commit:** on a holder that already committed `v`, the commit
  removed the older version, so discarding `v` leaves nothing. Losing a few holders this way
  can drop the last acknowledged value below `k` copies. This is why the code departs from the
  spec's literal "send a best-effort discard on any failure".

## Rejected
- **Quorum writes (a majority of `U`):** needs read repair or read quorums, a much larger design.
- **One-phase writes (no staging):** a failed write could leave some holders on the new version
  and some on the old one, with nothing to tell peering which one to keep.
