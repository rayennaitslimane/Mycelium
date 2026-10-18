# ADR 0003: Versions and client semantics

Status: Accepted · 2026-10-18

## Context
Holders must agree on which write of an object is newest, without a global clock and without
request IDs.

## Decision
- **Version:** `(epoch, seq)`, compared numerically and written `epoch.seq`.
  - The primary assigns `seq` per PG and keeps it in memory.
  - `seq` restarts at 0 whenever the PG becomes active at a new epoch.
- **Any primary restart or change bumps the epoch:**
  - a new primary implies a map change;
  - a restarted node reports a new `boot_id`, and the monitor bumps the epoch for it (ADR 0008).

  This is what keeps versions unique, even though `seq` lives only in memory.
- **A delete is a write of a tombstone.** Committing a tombstone removes the object, and
  deleting an absent object succeeds.
- **Client contract:**
  - Whole-object `GET`, `PUT` and `DELETE`, up to `max_object`; the reply carries the version.
  - `421` (not primary) and `503` (PG inactive) mean "refetch the map and retry".
  - The gateway retries a bounded number of times with backoff.
  - No request IDs: a timed-out request has an unknown outcome, and retries are at-least-once,
    last writer wins.

## Protects
Invariant 4: versions are unique. Peering (ADR 0006) relies on "highest version wins".

## If violated
Two different writes sharing a version would make holders disagree about which one won, so
peering could pick either one.

## Rejected
- **Request IDs or exactly-once semantics:** this needs durable deduplication state per client.
  It is out of scope (a spec non-goal).
- **Wall-clock timestamps as versions:** clock skew would reorder writes.
