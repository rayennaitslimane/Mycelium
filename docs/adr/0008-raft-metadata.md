# ADR-0008: Raft-Replicated LMDB Metadata

**Status:** Accepted  
**Date:** 2026-10-01

## Context

Object metadata (wrapped DEK, KEK ID, erasure parameters, shard locations, version) and the node registry lived in a single PostgreSQL instance (ADR-0003). That instance was both the availability single point of failure and the only coordination point between processes: losing it made every `put` and `get` fail, and multi-process writers were explicitly out of scope (ADR-0006).

The metadata access pattern is small and key-oriented: point reads and writes by object ID, a full scan for `list()`, a compare-and-swap for `repair()`, and a small ordered node list. None of it needs SQL. What it does need is replication with a single serialisation point so that version fencing stays correct across processes.

## Decision

### State machine: LMDB, one transaction per log entry

`MetadataStore` is rewritten on LMDB and exposed through `IMetadataStore`. All mutations are expressed as a msgpack `MetadataCommand` (`Put`, `ConditionalPut`, `Remove`, `RegisterNode`, `UnregisterNode`) and applied by `MetadataStore::apply(cmd, log_idx)`, which runs in a single LMDB write transaction that also persists `last_applied = log_idx`. Entries with `log_idx <= last_applied` are skipped, so a replay after a crash never applies a command twice. `apply` is deterministic (no clocks, no randomness).

The state environment (`<data_dir>/state/`) holds these DBIs:

| DBI | Key | Value |
|-----|-----|-------|
| `objects` | object ID | msgpack `ObjectMetadata` (incl. shard locations and version) |
| `nodes` | node address | u64 registration sequence (preserves registration order for placement) |
| `meta` | `last_applied`, `next_node_seq` | u64 |
| `dedup` | `request_id` | msgpack `(log_idx, ok)` |

Object IDs are validated as UUIDs before a command is built, replacing the PostgreSQL `::uuid` cast.

### Replication: NuRaft

[NuRaft](https://github.com/eBay/NuRaft) (`nuraft/3.0.0` via Conan) provides the consensus protocol. The adapters are:

- `MetadataStateMachine`: `commit()` calls `MetadataStore::apply`, and `last_commit_index()` returns `last_applied`.
- `LmdbLogStore` / `LmdbStateMgr`: the Raft log, cluster config and server state live in a **separate** LMDB environment (`<data_dir>/raft/`, DBIs `log` and `state`), so log compaction never contends with state-machine writes.
- `MetadataServer`: wraps `raft_launcher` with a C++ API: `propose(cmd)`, a lease-gated `read()`, and `add_server()`.

**Membership** is dynamic. Node 1 bootstraps a cluster containing only itself, and other nodes join through `add_srv`. A joining node starts with `skip_initial_election_timeout_` on first boot so it never elects itself leader of a one-node cluster. Each server's HTTP endpoint is stored in its `srv_config` aux field, which lets any node locate the leader's HTTP endpoint.

**Reads** are served by the leader under a lease. NuRaft's `leadership_expiry_` (400 ms) is set below `election_timeout_lower_bound_` (500 ms), so a leader that loses contact with its quorum steps down before a new leader can be elected. NuRaft appends a config entry on becoming leader, so a read is served only when the node is leader **and** the term of the last committed log equals the current term. Once that holds, every earlier entry is applied locally.

### HTTP surface, snapshots and client

- **HTTP surface:** `MetadataHttpServer` serves JSON (nlohmann; Botan base64 for the DEK) under `/meta/objects` and `/meta/nodes`, plus `/status`, `/admin/servers` and `/admin/fault`. A follower answers leader-only requests with `307`, with `Location` set to the leader. When no leader is known, it answers `503`.
- **`metadata_server` binary:** `metadata_server <id> <raft_host:port> <http_host:port> <data_dir>`. Setting `MYC_ENABLE_FAULT_INJECTION=1` enables `/admin/fault`, which arms a one-shot crash (exit code 86) either before or after the next apply. Crash-restart tests spawn real processes and send them `SIGKILL`.
- **Snapshots:** logical LMDB snapshots every `snapshot_distance` entries (default 1000). That many log entries are kept after compaction. `create_snapshot` opens an `MDB_NOTLS` read transaction, and a background thread then writes 1 MiB chunk files to `<data_dir>/snapshots/<idx>/`. `apply_snapshot` restores all DBIs in one write transaction.
- **`MetadataClient`:** an `IMetadataStore` over HTTP. It caches the leader, follows `307` redirects, and retries `503` and connection errors with backoff (50 ms doubling to 1 s, 10 s deadline). Every mutation call carries one `request_id` that is reused across retries. A `dedup` DBI maps each `request_id` to its original result and is pruned by a deterministic window of 100,000 log entries.

## Consequences

**Positive:**
- PostgreSQL and Docker are no longer required. Metadata survives the loss of a minority of metadata servers.
- Every mutation is serialised by the Raft log, so `conditional_put` version fencing (ADR-0006 §5) holds across processes, not just within one.
- Crash safety is local and simple. The log entry and the `last_applied` marker commit in the same LMDB transaction, so replay is idempotent.
- `list()` is a single cursor scan in ID order, and a delete removes shard locations with the record.

**Negative / Risks:**
- Lease reads assume bounded clock drift between servers, small relative to the 100 ms gap between leadership expiry and the election timeout lower bound.
- During bootstrap (1–2 members), NuRaft does not expire leadership, so the lease guarantee only holds once three members are present.
- Every write pays a Raft round-trip plus two fsyncs (log and state).
- Membership removal is out of scope. Raft and HTTP traffic are unauthenticated plaintext, like the rest of the system.
