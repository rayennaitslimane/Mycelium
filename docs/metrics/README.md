# Benchmark Metrics Analysis

Analysis of the benchmarks produced by `cmd/BenchmarkMain.cpp`. The benchmark
harness exercises the real `StorageClient` API across an erasure-parameter sweep
and exports per-operation metrics as CSV (see
[ADR-0007](../adr/0007-metrics-collector.md)).

## Contents

| File | Purpose |
|------|---------|
| `metrics_utils.py` | Shared CSV loading, validation and plotting helpers. |
| `01_performance.ipynb` | Latency (phase breakdown) and storage-overhead analysis. |
| `02_reliability.ipynb` | Recovery success rate and repair behaviour. |
| `03_metadata_scaling.ipynb` | `list()` JOIN scaling and connection-pool throughput. |
| `requirements.txt` | Python dependencies for the notebooks. |
| `sample_results.csv` | Synthetic fallback so notebooks run without a cluster. |

## CSV schema

Produced by `MetricsCollector::export_csv`:

```
operation, k, m, shard_size, object_size, failed_nodes, total_nodes,
crypto_ms, transport_ms, metadata_ms, total_ms, success, storage_overhead
```

`operation` is one of `PUT`, `GET`, `REPAIR`. `storage_overhead` is populated on
`PUT`; `failed_nodes` is populated on `REPAIR`. `metadata_ms` is the time spent
in `MetadataStore` for the operation.

## Generating the data

The benchmark requires a running integration cluster (PostgreSQL + storage
nodes), the same as the integration tests:

```bash
cd myc-dev
./scripts/setup-integration.sh
./build/Release/benchmark        # writes docs/metrics/benchmark_results.csv
./scripts/teardown-integration.sh
```

Override the output path or DB connection via `BENCHMARK_CSV_PATH` and
`BENCHMARK_METADATA_CONN`.

If `benchmark_results.csv` is absent, the notebooks fall back to
`sample_results.csv` so they can still be executed.

### Metadata scaling benchmarks

The pool and `list()` JOIN are throughput/scaling changes that the latency sweep
above cannot capture (it is single-threaded and never calls `list()`). The
`metadata_benchmark` executable measures them directly against `MetadataStore`
and needs only PostgreSQL:

```bash
cd myc-dev
./scripts/run-metadata-benchmarks.sh   # writes metadata_list_sweep.csv + metadata_throughput.csv
```

It produces two CSVs consumed by `03_metadata_scaling.ipynb`:

- `metadata_list_sweep.csv` — `num_objects, shards_per_object, list_ms, per_object_ms`
  (one JOIN vs the N+1 per-object access pattern).
- `metadata_throughput.csv` — `pool_size, threads, total_ops, duration_ms,
  throughput_ops_per_sec, mean_latency_ms` (`pool_size=1` reproduces the old
  single-connection serialization).

Mode and ranges are configurable via `META_BENCH_MODE` (`list`, `throughput`,
`all`), `META_LIST_SIZES`, `META_POOL_SIZES`, `META_THREADS`,
`META_OPS_PER_THREAD` and `META_DATASET`.

## Running the notebooks

```bash
cd myc-dev/docs/metrics
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook        # open 01_performance.ipynb / 02_reliability.ipynb
```

## Shutdown and cleanup

After working with the notebooks, stop Jupyter with `Ctrl + C`, then run:

```bash
deactivate
rm -rf .venv
```

This removes the virtual environment and all packages installed from
`requirements.txt`. Nothing is installed system-wide. The environment can be
recreated at any time.