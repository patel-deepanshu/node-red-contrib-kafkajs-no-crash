#!/usr/bin/env bash
# Create topics idempotently (--if-not-exists) through the PLAINTEXT listener.
#
# Usage: KAFKA_HOME=... ./kafka-create-topics.sh name[:partitions] [name[:partitions] ...]
#   e.g. ./kafka-create-topics.sh e2e-in e2e-out:3
set -euo pipefail
# shellcheck source=kafka-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/kafka-env.sh"

if [ $# -eq 0 ]; then
  echo "usage: $0 topic[:partitions] ..." >&2
  exit 2
fi

for spec in "$@"; do
  name="${spec%%:*}"
  parts="1"
  [ "$spec" != "$name" ] && parts="${spec#*:}"
  kafka_cli kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_PLAINTEXT" --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor 1 >/dev/null
  echo "kafka-create-topics: $name (partitions=$parts) ok"
done
