
# Post Quantum Distributed Object Store (Mycelium)

Mycelium is a C++20 distributed object store designed for a zero-trust model, using ML-KEM-768 for post-quantum key wrapping, AES-256-GCM for data encryption, and ISA-L Reed-Solomon erasure coding for fault tolerance. It stores encrypted shards and metadata in separate backends and exposes a simple HTTP API for reliable object storage and retrieval.

Today’s storage encryption still depends on RSA and elliptic-curve cryptography which a capable quantum computer could eventually break. Mycelium explores an alternative aspiring to distributed RAIN (Redundant Array of Independant Nodes) principles and integrating a layer of post-quantum security.


## I. Architecture Overview

Mermaid diagram for runtime wiring and boundaries:

```
[API Consumer]
      |
      v
[StorageClientServer :8080]
      |
      v
[StorageClient]
      │
      ├── ErasureCodec (ISA-L RS)
      ├── Crypto (AES-256-GCM + ML-KEM-768)
      ├── ShardTransport
      │         ├── Node :9001 -> StorageNode -> LMDB1
      │         ├── Node :9002 -> StorageNode -> LMDB2
      │         └── Node :9003 -> StorageNode -> LMDB3
      └── MetadataClient -> metadata_server x3 :7001-7003
                              (NuRaft + LMDB, ADR-0008)
```

Design decisions are documented as ADRs in [docs/adr/](docs/adr/).

| ADR | Decision |
|-----|----------|
| [0001](docs/adr/0001-quantum-encryption.md) | ML-KEM-768 KEM-based KEK ring + AES-256-GCM per-object DEK with shard-index AAD |
| [0002](docs/adr/0002-erasure-codec.md) | ISA-L Reed-Solomon erasure coding with per-object `k`/`m`/`shard_size` |
| [0003](docs/adr/0003-dual-storage.md) | LMDB for shard payloads, separate store for object metadata (amended by 0008) |
| [0004](docs/adr/0004-shard-transport.md) | Parallel HTTP shard dispatch with dynamic placement and thread-local per-node client reuse |
| [0005](docs/adr/0005-http-surface.md) | cpp-httplib embedded HTTP server with Base64 object payloads and JSON erasure parameters |
| [0006](docs/adr/0006-concurrency-contract.md) | Concurrency contract: MetadataStore mutex, KEK ring shared_mutex, tolerated per-object races |
| [0007](docs/adr/0007-metrics-collector.md) | Inline metrics instrumentation with ScopedTimer, CSV export, benchmark harness |
| [0008](docs/adr/0008-raft-metadata.md) | LMDB metadata replicated with NuRaft, lease reads on the leader |


## II. Prerequisites

- Linux
- GCC >= 11 or Clang >= 14 (C++20)
- CMake >= 3.23
- Conan >= 2.x
- Git


## III. Building

```bash
# Build for the first time
./scripts/build.sh

# Clean build
./scripts/build.sh clean

# Remove all build files
./scripts/build.sh reset
```

## IV. Testing

### Unit Tests

```bash
./build/Release/myc_unit_tests
```

### Raft Tests

These tests cover in-process clusters plus crash-restart tests that spawn `metadata_server` processes. Requires setting up integration, *see below*.

```bash
./build/Release/myc_raft_tests
```

### Integration Tests

Integration tests need a 3-node metadata cluster, the storage nodes and the storage client running locally. Metadata data lives in `/tmp/myc-meta{1..3}`.

```bash
# Start all services
./scripts/setup-integration.sh

# Run tests
./build/Release/myc_integration_tests

# Stop all services
./scripts/teardown-integration.sh
```

Services exposed on the host:

| Service | Port |
|---------|------|
| Storage Client API | `localhost:8080` |
| Metadata Servers (HTTP) | `localhost:7001`-`7003` |
| Metadata Servers (Raft) | `localhost:7101`-`7103` |
| Storage Node 1 | `localhost:9001` |
| Storage Node 2 | `localhost:9002` |
| Storage Node 3 | `localhost:9003` |

## V. Client API

### Store an object

```bash
DATA_B64=$(printf 'hello world' | base64 -w0)

curl -X PUT http://localhost:8080/objects/00000000-0000-0000-0000-000000000001 \
  -H "Content-Type: application/json" \
  -d "{\"data\":\"${DATA_B64}\",\"k\":2,\"m\":1,\"shard_size\":20}"
# {"status":"ok"}
```

The `data` field is Base64-encoded object bytes. Erasure parameters are supplied per object via `k`, `m`, and `shard_size`.

### Retrieve an object

```bash
curl http://localhost:8080/objects/00000000-0000-0000-0000-000000000001
# {"data":"aGVsbG8gd29ybGQ="}

# Decode returned Base64 payload
echo 'aGVsbG8gd29ybGQ=' | base64 -d
# hello world
```

### Delete an object

```bash
curl -X DELETE http://localhost:8080/objects/00000000-0000-0000-0000-000000000001
# {"status":"ok"}
```

### List all objects

```bash
curl http://localhost:8080/objects
# [{"id":"00000000-0000-0000-0000-000000000001","size":11,"checksum":"b94d27b9..."}]
```

### Inspect object health

```bash
curl http://localhost:8080/objects/00000000-0000-0000-0000-000000000001/health
# {
#   "object_id": "00000000-0000-0000-0000-000000000001",
#   "total_shards": 3,
#   "available_shards": 3,
#   "required_shards": 2,
#   "missing_indices": [],
#   "healthy": true,
#   "fully_replicated": true
# }
```

Returns shard availability by probing each storage node. `healthy` indicates whether enough shards exist for reconstruction (`available >= required`). `fully_replicated` indicates all shards are present.

### Repair a degraded object

```bash
curl -X POST http://localhost:8080/objects/00000000-0000-0000-0000-000000000001/repair
# {"status":"ok","repaired":true}
```

Reconstructs missing shards from surviving ones using erasure coding, re-encrypts them with the original DEK (fresh nonces), and places them on eligible nodes. Uses optimistic version fencing and returns `"repaired": false` if a concurrent mutation occurred during repair.


## VI. Storage Node API

Used internally by the client. Each node exposes:

| Method | Route | Body | Description |
|--------|-------|------|-------------|
| GET | `/health` | - | Health check |
| PUT | `/shards?location=<shard_key>` | raw bytes | Store a shard |
| GET | `/shards?location=<shard_key>` | - | Retrieve a shard |
| DELETE | `/shards?location=<shard_key>` | - | Delete a shard |

## VII. Rough Backlog

* ~~Improve the placement strategy by selecting nodes from those available.~~ &nbsp;&nbsp; Implemented via dynamic node registry.
* ~~Add a repair strategy that operates on a single route.~~ &nbsp;&nbsp; Implemented via `/objects/:id/repair`.
* ~~Implement metrics collection to begin benchmarking.~~ &nbsp;&nbsp; Implemented via `MetricsCollector` + `benchmark` executable.
* Introduce a chunking strategy to manage data or workload more effectively.
* Implement a garbage collection strategy to clean up unused resources.

## VIII. Benchmarking

The `benchmark` executable runs PUT/GET/REPAIR operations across parameter combinations and exports timing results to CSV.

```bash
# Start the integration cluster first
./scripts/setup-integration.sh

# Run the benchmark
# Results are written to docs/metrics/benchmark_results.csv by default
./build/Release/benchmark
```

Configurable via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BENCHMARK_METADATA_ENDPOINTS` | `127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003` | Metadata server HTTP endpoints |
| `BENCHMARK_CSV_PATH` | `benchmark_results.csv` | Output CSV file path |

CSV columns: `operation`, `k`, `m`, `shard_size`, `object_size`, `failed_nodes`, `total_nodes`, `crypto_ms`, `transport_ms`, `total_ms`, `success`, `storage_overhead`.

The exported CSV is analyzed in the Jupyter notebooks under [docs/metrics/](docs/metrics/README.md) ([performance](docs/metrics/01_performance.ipynb), [reliability](docs/metrics/02_reliability.ipynb)).

### System evaluation

[SYSTEM-EVALUATION.md](SYSTEM-EVALUATION.md) is the written analysis of those benchmark runs. It walks through where latency actually goes (post-quantum crypto is effectively free; shard transport and an uninstrumented metadata layer dominate), how erasure durability tracks Reed-Solomon theory, the storage-overhead trade-offs, and the highest-leverage fixes, all cross-referenced to the [ADRs](docs/adr/) and the metrics notebooks.
