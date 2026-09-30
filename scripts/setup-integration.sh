#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Number of storage nodes to register and start. Lets the benchmark vary
# total_nodes across scenarios. Ports are allocated sequentially from 9001.
NODE_COUNT="${NODE_COUNT:-12}"
START_PORT=9001
END_PORT=$((START_PORT + NODE_COUNT - 1))

echo "Starting postgres-test..."
docker compose -f "$PROJECT_DIR/docker-compose.yml" up -d postgres-test

echo "Waiting for postgres to be healthy..."
until docker compose -f "$PROJECT_DIR/docker-compose.yml" exec -T postgres-test pg_isready -U test_user -d myc_test > /dev/null 2>&1; do
  sleep 1
done
echo "Postgres is ready."

echo "Registering storage nodes in database..."

docker compose -f "$PROJECT_DIR/docker-compose.yml" exec -T postgres-test \
  psql -U test_user -d myc_test -c "
    CREATE TABLE IF NOT EXISTS nodes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      address TEXT NOT NULL UNIQUE,
      registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  "

for port in $(seq "$START_PORT" "$END_PORT"); do
  docker compose -f "$PROJECT_DIR/docker-compose.yml" exec -T postgres-test \
    psql -U test_user -d myc_test -c \
    "INSERT INTO nodes (address) VALUES ('localhost:$port') ON CONFLICT DO NOTHING;"
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

echo "Starting storage client..."

if ! pgrep -f "storage_client 0.0.0.0 8080" > /dev/null; then
  "$PROJECT_DIR/build/Release/storage_client" \
    0.0.0.0 \
    8080 \
    "host=localhost port=5433 dbname=myc_test user=test_user password=test_password" &
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
