# ADR-0007: Metrics Collector

**Status:** Accepted  
**Date:** 2026-06-15

## Context

myc needs a controlled benchmarking framework to measure system performance under varying erasure parameters and failure scenarios. The goal is evaluation-grade metrics, not production observability. Measurements must capture real execution paths (crypto phase, transport phase, total latency) without duplicating orchestration logic or introducing external dependencies.

Key requirements:
- Measure PUT, GET, and REPAIR latency with phase-level breakdown (crypto vs transport)
- Measure storage overhead from actual serialized shard payload sizes
- Track recovery success rate under node failures
- Parameterize by `k`, `m`, `shard_size`, `object_size`, and `failed_nodes / total_nodes`
- Persist results as CSV for analysis

## Decision

### Inline instrumentation via nullable collector pointer

`StorageClient` gains a `MetricsCollector* collector_ = nullptr` member with a `set_collector()` setter. When null (default), no metrics are recorded - existing tests and production paths are unaffected.

Inline `ScopedTimer` RAII objects are placed around the real crypto, transport,
and metadata-store boundaries inside `put()`, `get()`, and `repair()`. At method
end, an `if (collector_)` guard builds and records an `OperationRecord`. The
`metadata_ms` phase isolates time spent in `MetadataStore` (the persistence
round-trips on the critical path) so it is measured rather than guessed.

```cpp
struct ScopedTimer {
    double* target;
    std::chrono::steady_clock::time_point start;
    explicit ScopedTimer(double* t)
        : target(t), start(std::chrono::steady_clock::now()) {}
    ~ScopedTimer() {
        *target = std::chrono::duration<double, std::milli>(
            std::chrono::steady_clock::now() - start).count();
    }
};
```

### Storage overhead from actual payloads

Computed inside `put()` as `sum(serialized_shards[i].size()) / bytes.size()`, capturing real expansion from nonce, GCM tag, and msgpack framing - not a theoretical `(k+m)*shard_size/object_size` estimate.

### CSV output only

Results are persisted as CSV with columns: `operation`, `k`, `m`, `shard_size`, `object_size`, `failed_nodes`, `total_nodes`, `crypto_ms`, `transport_ms`, `metadata_ms`, `total_ms`, `success`, `storage_overhead`. CSV is sufficient for spreadsheet and scripting analysis; no JSON export is provided.

### Benchmark harness as a separate executable

`cmd/BenchmarkMain.cpp` drives parameter sweeps by calling the real `StorageClient` API (`put`, `get`, `repair`). It does not duplicate any orchestration logic - phase timings are captured inside the instrumented methods. The harness only generates test data, iterates parameter combinations, and triggers the CSV export.

## Consequences

**Positive:**
- Zero overhead in production/test paths (null pointer check only)
- Measures real system behaviour, not synthetic reconstructions
- No new library dependencies (uses `<chrono>`, `<fstream>` from stdlib)
- Simple to extend: add new fields to `OperationRecord`, add timers to new methods
- Recovery success rate is derivable from REPAIR records (`success` column)

**Negative / Risks:**
- Inline timers add minor code to `StorageClient::put/get/repair` (guarded by collector null-check)
- Benchmark executable requires a running integration cluster (same as integration tests)
- CSV has no schema versioning; column changes require updating any downstream analysis scripts
