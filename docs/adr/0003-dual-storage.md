# ADR-0003: Dual Storage

**Status:** Accepted (amended)  
**Date:** 2026-06-11

> **Amendment (2026-10-01):** Object metadata no longer lives in PostgreSQL. It
> is stored in LMDB and replicated with Raft ([ADR-0008](0008-raft-metadata.md)).
> See the [Amendment](#amendment-2026-10-01-metadata-moves-to-raft-replicated-lmdb)
> section at the end of this document. The shard-payload half of this decision is
> unchanged.

## Context

The system manages two fundamentally different categories of data:

1. **Shard payloads** - opaque binary blobs (encrypted shard ciphertext + nonce), written and read by key, with no relational structure, high write throughput requirements, and a need for crash-safe atomic multi-shard transactions.
2. **Object metadata** - structured records (object ID, size, checksum, erasure parameters, shard location strings, wrapped DEK, KEK ID) that must be queryable, must support cascading deletes, and form the authoritative index for reconstructing any object.

Using a single storage technology for both categories would either force a relational database to handle large binary blobs inefficiently or force a key-value store to implement relational integrity manually.

## Decision

### Shard payloads: LMDB (`lmdb::lmdb`)

Each `StorageNode` embeds one **Lightning Memory-Mapped Database (LMDB)** environment opened at a configurable filesystem path. LMDB provides:

- **ACID transactions** - all shards in a single `put()` call are committed atomically in one `mdb_txn_commit`. If the process dies mid-write no partial batch is left on disk.
- **Memory-mapped reads** - `mdb_get` on a read transaction returns a pointer directly into the mmap region; no copy is made until `bytes_from_mdb_value` copies the bytes into a `std::vector`. This is the fastest possible read path for a key-value store.
- **Single-writer, multiple-reader** - LMDB enforces one writer transaction at a time while allowing concurrent readers. This matches the server's concurrent request model without introducing partial-write visibility. The 1 GB map size is set at open time; no data is pre-allocated.

The environment is opened once in `StorageNode::StorageNode()` and closed in the destructor via RAII. Copy and move constructors are deleted; there is exactly one `StorageNode` per process.

Transactions in `get()` are opened with `MDB_RDONLY` and ended with `mdb_txn_abort` (releasing the read lock), never `mdb_txn_commit`, which is correct per LMDB semantics for read-only transactions.

### Object metadata: PostgreSQL via libpqxx

`MetadataStore` holds a single persistent `pqxx::connection` to a PostgreSQL database. The schema is:

```sql
object_metadata (
    id UUID PRIMARY KEY,
    size BIGINT NOT NULL CHECK (size >= 0),
    checksum TEXT NOT NULL,
    erasure_spec BYTEA NOT NULL,
    encrypted_dek BYTEA NOT NULL,
    kek_id TEXT NOT NULL
)

object_shard_locations (
    object_id UUID NOT NULL REFERENCES object_metadata(id) ON DELETE CASCADE,
    shard_index INTEGER NOT NULL CHECK (shard_index >= 0),
    location TEXT NOT NULL,
    PRIMARY KEY (object_id, shard_index)
)
```

Key design points:
- `ON DELETE CASCADE` ensures that deleting a row from `object_metadata` atomically removes all associated shard location rows - no orphaned location records.
- An index on `object_shard_locations(object_id)` accelerates per-object shard location lookups.
- All SQL operations are registered as **prepared statements** at construction time via `conn_.prepare(...)`, including `put_object_metadata`, `delete_object_shard_locations`, `insert_object_shard_location`, `get_object_metadata`, `get_object_shard_locations`, `remove_object_metadata`, and `list_object_metadata`. This both eliminates SQL injection risk and avoids repeated query planning overhead.
- `put()` uses `ON CONFLICT (id) DO UPDATE` (upsert) so re-uploading an object with the same ID is a safe idempotent operation.

### Shard location encoding

Shard locations are stored as strings of the form `"host:port/object_id/version/shard_index"`, where `version` is a per-`put` random token. The internal shard key format is `object_id/version/shard_index`.

Parsing invariant: the first `/` splits node address from shard key (`host:port/key`), and all remaining `/` separators belong to the shard key.

Using a versioned keyspace ensures concurrent writes to the same `object_id` do not overwrite each other's shard payloads in LMDB. Only after all versioned shard writes complete successfully is metadata upserted to reference that exact version.

Per [ADR-0006](0006-concurrency-contract.md), per-object ordering is intentionally not guaranteed. Concurrent `put(X)` / `put(X)` or `put(X)` / `remove(X)` may interleave, producing last-writer-wins metadata and leaving non-referenced versioned shard payloads in LMDB.

This approach avoids a third table but means the storage topology is embedded in every metadata row - changing node addresses requires updating all location strings.

### Known gap: N+1 query in `list()`

`MetadataStore::list()` issues one SQL query to fetch all object rows and then calls `get_shard_locations()` - a second query - for each object:

```cpp
for (const auto& row : rows) {
    ObjectMetadata metadata = row_to_object_metadata(row);
    metadata.shard_locations = get_shard_locations(tx, metadata.id); // N additional queries
}
```

For a collection of N objects this produces N+1 total round-trips to PostgreSQL. The correct fix is a single JOIN query that fetches both tables in one round-trip, assembling the shard location vectors client-side. This is deferred as a known POC limitation.

## Consequences

**Positive:**
- LMDB's memory-mapped design gives near-zero-copy shard reads; no intermediate buffer allocation in the storage layer.
- LMDB's single-file, single-process model is operationally simple - no daemon, no network, no separate configuration.
- PostgreSQL provides referential integrity, cascading deletes, and a rich query interface for metadata without any manual bookkeeping.
- Prepared statements prevent SQL injection and reduce per-query planning cost.
- The two stores are independently scalable: metadata can be moved to a managed PostgreSQL service without touching shard storage, and vice versa.

**Negative / Risks:**
- `MetadataStore` still holds a single `pqxx::connection`. [ADR-0006](0006-concurrency-contract.md) fixes thread safety by serialising access with a mutex, but this also serialises all metadata operations through one lock and can become a throughput bottleneck under high concurrency.
- The N+1 query in `list()` degrades linearly with the number of stored objects and will become unacceptably slow at scale.
- Shard location strings encode the storage topology. Node address changes require a migration of all location strings in PostgreSQL.
- Versioned shard keys can leave orphaned shard payloads for losing concurrent writers and for `put/remove` races on the same object ID. This is an intentional storage-leak trade-off that preserves read correctness under the ADR-0006 contract.
- Multi-process writers sharing the same PostgreSQL metadata are not coordinated by this design; the documented concurrency contract assumes single-process ownership of one `StorageClient` instance.
- The `erasure_spec` field is stored as `BYTEA` (msgpack blob) rather than as structured columns, making it opaque to SQL queries and migrations.

## Amendment (2026-10-01): Metadata moves to Raft-replicated LMDB

### What changed

- PostgreSQL and libpqxx are removed. `MetadataStore` is an LMDB environment with `objects`, `nodes` and `meta` DBIs. Each object record is one msgpack `ObjectMetadata` value that carries its shard locations, so deleting the record removes the locations with it (this replaces `ON DELETE CASCADE`).
- The node registry keeps registration order through a per-node sequence number (this replaces `ORDER BY registered_at`).
- Object IDs are validated as UUIDs in code (this replaces the `::uuid` cast).
- Replication, leader election and the multi-process story are defined in [ADR-0008](0008-raft-metadata.md).

### Consequences of the amendment

- Both halves of the dual-storage split now use LMDB, but they stay separate stores with different owners: storage nodes own shard payloads, and metadata servers own metadata.
- The pqxx connection-pool and N+1 `list()` notes above are obsolete. `list()` is a single cursor scan.
- Ad-hoc SQL queries over metadata are no longer possible. All access goes through `IMetadataStore`.
