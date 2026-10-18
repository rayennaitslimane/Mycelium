# ADR 0001: The map is the single source of truth

Status: Accepted · 2026-10-18

## Context
Every component has to agree on which nodes are alive and which nodes hold each PG. If any
component could change that information, two parts of the system could act on placements that
contradict each other.

## Decision
- Only the monitor changes the map. Every change is a pure, deterministic function
  `transitions.Apply(map, event) -> map'` that bumps `epoch`
  ([transitions.go](../../monitor/transitions/transitions.go)). It returns a new map and never
  modifies the old one.
- The new map is persisted (write, fsync, rename) before any reply that depends on it, so
  `epoch` survives a restart.
- Everything else (`pg(id)`, `U`, `B`, the primary) is computed from the map by
  [mapmodel](../../core/mapmodel/mapmodel.go) and is never stored.
- Each rule has exactly one owner:
  - map rules: `monitor/transitions`
  - serve gate: `node/agent`
  - fence: `node/store`

  Nothing else re-implements them.

## Protects
Invariant 5: `epoch` only increases, and the map changes only at the monitor and is persisted
before use. Every other ADR relies on "a newer epoch means a newer truth".

## If violated
A node could act on a placement the monitor later forgets, or two epochs could describe
different maps. Fences and versions (ADRs 0003 and 0005) would then compare meaningless numbers.

## Rejected
- **Nodes agreeing on placement among themselves (gossip):** no single ordering of changes,
  and much harder to reason about.
- **Mutating the map in place:** Raft replicas could drift apart (ADR 0009).
