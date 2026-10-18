#!/usr/bin/env bash
# Starts a local cluster: 1 monitor, $NODES nodes and 1 gateway. Ctrl-C stops it.
# State, logs and pid files go to ./run; delete it (make clean) to start over.
set -euo pipefail
cd "$(dirname "$0")/.."

NODES=${NODES:-4}
K=${K:-2}
M=${M:-1}
MON=http://127.0.0.1:7000
RUN=run

mkdir -p "$RUN"
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true; wait' EXIT

start() { # name command...
  local name=$1
  shift
  echo "$*" >>"$RUN/$name.log" # the command line, to restart it by hand
  "$@" >>"$RUN/$name.log" 2>&1 &
  pids+=($!)
  echo $! >"$RUN/$name.pid"
  echo "started $name (pid $!, log $RUN/$name.log)"
}

# Short lease/grace so failures are visible within seconds.
start monitor bin/monitor -listen 127.0.0.1:7000 -data "$RUN/monitor" \
  -k "$K" -m "$M" -bootstrap-nodes $((K + M)) -pg-num 16 -lease 2s -grace 5s
sleep 0.5
for i in $(seq 1 "$NODES"); do
  port=$((9000 + i))
  start "node$i" bin/node -id "node$i" -addr "http://127.0.0.1:$port" \
    -listen "127.0.0.1:$port" -data "$RUN/node$i" -monitors "$MON"
done
start gateway bin/gateway -listen 127.0.0.1:8080 -monitors "$MON"

echo
echo "objects: http://127.0.0.1:8080/objects/{id}    map: $MON/map    Ctrl-C to stop"
wait
