#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Number of storage nodes to register and start. Lets the benchmark vary
# total_nodes across scenarios. Ports are allocated sequentially from 9001.
NODE_COUNT="${NODE_COUNT:-12}"
START_PORT=9001
END_PORT=$((START_PORT + NODE_COUNT - 1))
METADATA_ROOT="${METADATA_ROOT:-/tmp/myc-meta}"
METADATA_NODES=3
METADATA_ENDPOINTS=""
for i in $(seq 1 "$METADATA_NODES"); do
  METADATA_ENDPOINTS="${METADATA_ENDPOINTS:+$METADATA_ENDPOINTS,}127.0.0.1:$((7000 + i))"
done

# Nodes are registered by storage_client on startup (order defines placement).
REGISTER_NODES=""
for port in $(seq "$START_PORT" "$END_PORT"); do
  REGISTER_NODES="${REGISTER_NODES:+$REGISTER_NODES,}localhost:$port"
done

echo "Starting storage nodes..."

for port in $(seq "$START_PORT" "$END_PORT"); do
  data_dir="/tmp/node$((port-9000))-data"

  mkdir -p "$data_dir"

  if ! pgrep -f "storage_node 0.0.0.0 $port" > /dev/null; then
    "$PROJECT_DIR/build/Release/storage_node" 0.0.0.0 "$port" "$data_dir" &
  fi
done

sleep 1

echo "Starting metadata servers..."

# HTTP on 7001-7003, raft on 7101-7103; node 1 bootstraps, the others join.
for i in $(seq 1 "$METADATA_NODES"); do
  if ! pgrep -f "metadata_server $i " > /dev/null; then
    "$PROJECT_DIR/build/Release/metadata_server" "$i" \
      "127.0.0.1:$((7100 + i))" \
      "127.0.0.1:$((7000 + i))" \
      "$METADATA_ROOT$i" &
  fi
done

echo "Waiting for a metadata leader..."
leader_ready=false
for i in $(seq 1 30); do
  if curl -sf http://127.0.0.1:7001/status | grep -qv '"leader_id":-1'; then
    leader_ready=true
    break
  fi
  sleep 1
done
if [[ "$leader_ready" != true ]]; then
  echo "ERROR: metadata cluster did not elect a leader in time."
  exit 1
fi

for i in $(seq 2 "$METADATA_NODES"); do
  curl -sfL -X POST http://127.0.0.1:7001/admin/servers \
    -d "{\"id\":$i,\"raft_endpoint\":\"127.0.0.1:$((7100 + i))\",\"http_endpoint\":\"127.0.0.1:$((7000 + i))\"}" \
    > /dev/null
done

echo "Starting storage client..."

if ! pgrep -f "storage_client 0.0.0.0 8080" > /dev/null; then
  MYC_REGISTER_NODES="$REGISTER_NODES" "$PROJECT_DIR/build/Release/storage_client" \
    0.0.0.0 \
    8080 \
    "$METADATA_ENDPOINTS" &
fi

echo "Waiting for storage client to be ready..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:8080/objects > /dev/null 2>&1; then
    echo "Storage client is ready on 0.0.0.0:8080"
    exit 0
  fi
  sleep 1
done

echo "ERROR: Storage client did not become ready in time."
exit 1
