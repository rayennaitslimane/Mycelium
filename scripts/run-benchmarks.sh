#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

BENCHMARK_BIN="$PROJECT_DIR/build/Release/benchmark"
OUT_CSV="${BENCHMARK_CSV_PATH:-$PROJECT_DIR/docs/metrics/benchmark_results.csv}"

if [[ ! -x "$BENCHMARK_BIN" ]]; then
  echo "ERROR: benchmark binary not found at $BENCHMARK_BIN. Build first: ./scripts/build.sh" >&2
  exit 1
fi

# Scenario matrix: "node_count:comma,separated,failed_node,counts".
# total_nodes is driven by node_count; failed_nodes is driven by each value in
# the list (clamped to m per combination inside the benchmark).
SCENARIOS=(
  "4:0,1"
  "8:0,1,2"
  "12:0,1,2,4"
)

# Start each scenario from a clean aggregate so results are reproducible.
rm -f "$OUT_CSV"

for scenario in "${SCENARIOS[@]}"; do
  node_count="${scenario%%:*}"
  failed_list="${scenario##*:}"

  echo "=== Scenario: ${node_count} nodes ==="
  NODE_COUNT="$node_count" "$SCRIPT_DIR/setup-integration.sh"

  IFS=',' read -ra failures <<< "$failed_list"
  for f in "${failures[@]}"; do
    echo "--- node_count=${node_count} failed_nodes=${f} ---"
    BENCHMARK_CSV_PATH="$OUT_CSV" \
    BENCHMARK_CSV_APPEND=1 \
    BENCHMARK_FAILED_NODES="$f" \
      "$BENCHMARK_BIN"
  done

  # teardown-integration.sh prompts three times (metadata / node data / kek file).
  # Auto-answer "yes" so each scenario starts from a clean cluster.
  printf 'y\ny\ny\n' | "$SCRIPT_DIR/teardown-integration.sh"
done

echo "Aggregated results written to $OUT_CSV"
