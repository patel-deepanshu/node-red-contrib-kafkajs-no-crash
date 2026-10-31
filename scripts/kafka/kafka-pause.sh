#!/usr/bin/env bash
# Freeze / unfreeze the broker JVM with SIGSTOP / SIGCONT.
# A frozen broker keeps its TCP sockets open but never answers: clients see request
# timeouts instead of ECONNREFUSED - a closer simulation of a dead network path / NAT drop
# than kafka-stop.sh.
#
# Usage: KAFKA_HOME=... ./kafka-pause.sh pause|resume
set -euo pipefail
# shellcheck source=kafka-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/kafka-env.sh"

pid="$(kafka_pid)" || { echo "kafka-pause: not running" >&2; exit 1; }
case "${1:-}" in
  pause)  kill -STOP "$pid"; echo "kafka-pause: paused pid $pid" ;;
  resume) kill -CONT "$pid"; echo "kafka-pause: resumed pid $pid" ;;
  *) echo "usage: $0 pause|resume" >&2; exit 2 ;;
esac
