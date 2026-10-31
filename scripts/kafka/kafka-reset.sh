#!/usr/bin/env bash
# Stop the broker (if running) and delete ALL its state under $KAFKA_BASE_DIR
# (config, data, logs). The next kafka-start.sh formats fresh storage.
#
# Usage: KAFKA_HOME=... ./kafka-reset.sh
set -euo pipefail
# shellcheck source=kafka-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/kafka-env.sh"

"$(dirname "${BASH_SOURCE[0]}")/kafka-stop.sh" --kill
case "$KAFKA_BASE_DIR" in
  ""|"/"|"$HOME") echo "kafka-reset: refusing to delete '$KAFKA_BASE_DIR'" >&2; exit 1 ;;
esac
rm -rf "$KAFKA_BASE_DIR"
echo "kafka-reset: removed $KAFKA_BASE_DIR"
