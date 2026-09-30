#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Stopping storage client..."
pkill -f "$PROJECT_DIR/build/Release/storage_client" || true

echo "Stopping storage nodes..."
pkill -f "$PROJECT_DIR/build/Release/storage_node" || true


echo "Stopping postgres-test..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" down

# Single confirmation prompt
read -r -p "Reset storage node data? (y/N): " answer
if [[ "$answer" =~ ^[Yy]$ ]]; then
  echo "Resetting storage node data..."
  for port in {9001..9012}; do
    rm -rf "/tmp/node$((port-9000))-data"
  done
else
  echo "Keeping existing storage node data."
fi

# Single confirmation prompt
read -r -p "Reset /tmp/myc_kek.json? (y/N): " answer
if [[ "$answer" =~ ^[Yy]$ ]]; then
  echo "Resetting /tmp/myc_kek.json..."
  rm -f /tmp/myc_kek.json
else
  echo "Keeping existing /tmp/myc_kek.json."
fi

echo "Teardown complete."
