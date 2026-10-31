#!/usr/bin/env bash
# Stop the local broker started by kafka-start.sh (only that PID, never other Kafka processes).
#
# Usage: KAFKA_HOME=... ./kafka-stop.sh [--kill]
#   (default)  SIGTERM = controlled shutdown, waits up to 60s, then SIGKILL
#   --kill     SIGKILL immediately (simulates a crash / power loss)
set -euo pipefail
# shellcheck source=kafka-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/kafka-env.sh"

mode="${1:-}"
if ! pid="$(kafka_pid)"; then
  echo "kafka-stop: not running"
  rm -f "$KAFKA_PID_FILE"
  exit 0
fi

# If it was paused with kafka-pause.sh it cannot handle SIGTERM until resumed.
kill -CONT "$pid" 2>/dev/null || true

if [ "$mode" = "--kill" ]; then
  echo "kafka-stop: SIGKILL pid $pid"
  kill -KILL "$pid" 2>/dev/null || true
else
  echo "kafka-stop: SIGTERM pid $pid (controlled shutdown)"
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "kafka-stop: still alive after 60s, SIGKILL"
    kill -KILL "$pid" 2>/dev/null || true
  fi
fi

for _ in $(seq 1 20); do
  kill -0 "$pid" 2>/dev/null || break
  sleep 0.5
done
rm -f "$KAFKA_PID_FILE"
echo "kafka-stop: stopped"
