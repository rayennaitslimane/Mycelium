#!/usr/bin/env bash
set -euo pipefail

# Runs the MetadataStore micro-benchmarks (connection pool + list() JOIN). These
# only need PostgreSQL, not the storage-node cluster, so this script brings up
# just the postgres-test service.
#
#   ./scripts/run-metadata-benchmarks.sh            # list sweep + throughput
#   META_BENCH_MODE=list ./scripts/run-metadata-benchmarks.sh
#   META_BENCH_MODE=throughput ./scripts/run-metadata-benchmarks.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BENCHMARK_BIN="$PROJECT_DIR/build/Release/metadata_benchmark"

if [[ ! -x "$BENCHMARK_BIN" ]]; then
  echo "ERROR: metadata_benchmark not found at $BENCHMARK_BIN. Build first: ./scripts/build.sh" >&2
  exit 1
fi

echo "Starting postgres-test..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d postgres-test

echo "Waiting for postgres to be healthy..."
until docker compose -f "$PROJECT_DIR/docker-compose.yml" exec -T postgres-test \
    pg_isready -U test_user -d myc_test > /dev/null 2>&1; do
  sleep 1
done
echo "Postgres is ready."

cd "$PROJECT_DIR"
"$BENCHMARK_BIN"

echo "Done. CSVs written under docs/metrics/."
