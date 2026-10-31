#!/usr/bin/env bash
# Block until the local broker answers metadata requests on BOTH listeners
# (PLAINTEXT, and SASL_PLAINTEXT authenticated as the admin user via SCRAM-SHA-256),
# or fail after $KAFKA_READY_TIMEOUT seconds (default 90).
#
# Usage: KAFKA_HOME=... ./kafka-wait-ready.sh [timeout-seconds]
set -euo pipefail
# shellcheck source=kafka-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/kafka-env.sh"

timeout_s="${1:-$KAFKA_READY_TIMEOUT}"
deadline=$(( $(date +%s) + timeout_s ))

stage="tcp"
while :; do
  if ! kafka_pid >/dev/null && [ -f "$KAFKA_PID_FILE" ]; then
    echo "kafka-wait-ready: broker process exited; last log lines:" >&2
    tail -n 30 "$KAFKA_LOG_DIR/server.out" >&2 || true
    exit 1
  fi
  case "$stage" in
    tcp)
      if kafka_port_open "$KAFKA_HOST" "$KAFKA_PLAINTEXT_PORT" && kafka_port_open "$KAFKA_HOST" "$KAFKA_SASL_PORT"; then
        stage="plaintext"; continue
      fi ;;
    plaintext)
      if kafka_cli kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_PLAINTEXT" --list >/dev/null 2>&1; then
        stage="sasl"; continue
      fi ;;
    sasl)
      if kafka_cli kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SASL" \
           --command-config "$KAFKA_CLIENT_SCRAM256_PROPS" --list >/dev/null 2>&1; then
        echo "kafka-wait-ready: ready (PLAINTEXT $KAFKA_BOOTSTRAP_PLAINTEXT, SASL_PLAINTEXT $KAFKA_BOOTSTRAP_SASL)"
        exit 0
      fi ;;
  esac
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "kafka-wait-ready: timed out after ${timeout_s}s (stuck at stage: $stage)" >&2
    tail -n 30 "$KAFKA_LOG_DIR/server.out" >&2 || true
    exit 1
  fi
  sleep 1
done
