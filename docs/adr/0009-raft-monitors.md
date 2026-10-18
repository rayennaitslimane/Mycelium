# ADR 0009: Monitor replication with Raft and leader leases

Status: Accepted · 2026-10-18

## Context
A single monitor is a single point of failure. Replicating it must not weaken ADRs 0001 and
0008: there must still be one ordered history of maps, and one authority answering heartbeats.

## Decision
- **Replicated log:** monitors replicate **events** (not maps) through Raft
  ([raft.go](../../monitor/raft/raft.go)). Each replica applies them with the same pure
  `transitions.Apply` and persists the map with the Raft index it reflects.
- **One authority:** only the leader answers the monitor routes. Followers answer `421`, and
  nodes and gateways move on to the next monitor.
- **Leader leases (check-quorum):**
  - a leader acts only while a majority has acknowledged it within the election timeout;
  - followers that heard from a live leader within that timeout refuse to vote;
  - a restarted monitor counts as having just heard from a leader.
- **Ready before serving:** a new leader commits a no-op and applies all earlier entries before
  serving. Then it treats every node as just heard from.
- **Rejected events:** an event that `Apply` rejects (for example, a duplicate join) leaves the
  map unchanged on every replica. `Propose` returns the rejection to the caller.
- **No snapshots, compaction or membership changes:** a storage failure is fatal.

## Protects
- **Invariant 5 under replication:** one ordered history, persisted before use.
- **ADR 0008 under replication:** an isolated old leader stops confirming heartbeats before a new
  leader can be elected. The serve gate's assumption still holds.

## If violated
- **No leader lease:** two monitors could both answer heartbeats. A node told `up` by the old
  leader keeps serving after the new leader reassigned its PGs.
- **Replicating maps instead of events, or applying non-deterministically:** replicas could
  diverge.

## Rejected
- **An external coordinator (etcd, ZooKeeper):** an extra system to run and secure. The needs
  here are small enough for an embedded Raft.
