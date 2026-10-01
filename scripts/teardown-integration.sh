#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "Stopping storage client..."
pkill -f "$PROJECT_DIR/build/Release/storage_client" || true

echo "Stopping storage nodes..."
pkill -f "$PROJECT_DIR/build/Release/storage_node" || true

echo "Stopping metadata servers..."
pkill -f "$PROJECT_DIR/build/Release/metadata_server" || true

METADATA_ROOT="${METADATA_ROOT:-/tmp/myc-meta}"

# Single confirmation prompt
read -r -p "Reset metadata in ${METADATA_ROOT}{1..3}? (y/N): " answer
if [[ "$answer" =~ ^[Yy]$ ]]; then
  echo "Resetting metadata..."
  rm -rf "${METADATA_ROOT}1" "${METADATA_ROOT}2" "${METADATA_ROOT}3"
else
  echo "Keeping existing metadata."
fi

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
