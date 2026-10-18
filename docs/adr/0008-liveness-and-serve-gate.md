# ADR 0008: Liveness, the serve gate and the one timing assumption

Status: Accepted · 2026-10-18

## Context
Fencing (ADR 0005) protects writes but not reads. A primary that has been cut off can still
answer reads from its own view after a replacement has accepted newer writes.

## Decision
- **Heartbeats:** nodes heartbeat every `lease/4`. The monitor marks a node `out` after `grace`
  without one, and requires `grace ≥ 2×lease`.
- **Serve gate:** a node serves, as primary or as holder, only while all three hold
  (`agent.CanServe`):
  - its last successful heartbeat was **sent** less than `lease` ago;
  - the reply said `up`;
  - its map epoch equals the reply epoch.
- **Monitor restart:** every known node counts as just heard from, so a restart doesn't mark
  nodes `out`.
- **Boot ids:**
  - a heartbeat with a new `boot_id` from an `up` node bumps the epoch, which re-peers its PGs
    and keeps versions unique (ADR 0003);
  - the monitor keeps boot ids only in memory, so an unknown `boot_id` (after a monitor restart
    or leader change) counts as new.

## Protects
Fresh reads. A deposed primary stops serving (after `lease`) before the monitor can reassign
its role (after `grace ≥ 2×lease`). While the monitor is unreachable, every node stops serving
after `lease`.

## Assumption
Clocks run at similar rates; they don't need to be synchronized. This is the system's **only**
timing assumption, and it affects read freshness, never write safety.

## If violated
- **`grace < 2×lease`, or timing from heartbeat receipt rather than send:** an old and a new
  primary could both serve reads at the same time.
- **Treating an unknown `boot_id` as unchanged:** a node restarted while the monitor was down
  could reuse versions.

## Rejected
- **Read quorums:** slower reads, and they still need versions to be compared across holders.
