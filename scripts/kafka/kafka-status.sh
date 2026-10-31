#!/usr/bin/env bash
# Report whether the local broker is up. Exit 0 = process alive and both listeners answer
# metadata requests, 1 = otherwise. Prints a one-line summary per check.
#
# Usage: KAFKA_HOME=... ./kafka-status.sh
set -uo pipefail
# shellcheck source=kafka-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/kafka-env.sh"

rc=0
if pid="$(kafka_pid)"; then
  state="$(awk '/^State:/{print $2}' "/proc/$pid/status" 2>/dev/null || true)"
  if [ "$state" = "T" ]; then echo "process : PAUSED (pid $pid, SIGSTOP)"; rc=1; else echo "process : running (pid $pid)"; fi
else
  echo "process : not running"; rc=1
fi

if kafka_cli kafka-broker-api-versions.sh --bootstrap-server "$KAFKA_BOOTSTRAP_PLAINTEXT" >/dev/null 2>&1; then
  echo "PLAINTEXT      $KAFKA_BOOTSTRAP_PLAINTEXT : ok"
else
  echo "PLAINTEXT      $KAFKA_BOOTSTRAP_PLAINTEXT : DOWN"; rc=1
fi
for props in "$KAFKA_CLIENT_SASL_PLAIN_PROPS" "$KAFKA_CLIENT_SCRAM256_PROPS" "$KAFKA_CLIENT_SCRAM512_PROPS"; do
  mech="$(sed -n 's/^sasl.mechanism=//p' "$props" 2>/dev/null)"
  if [ -f "$props" ] && kafka_cli kafka-broker-api-versions.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SASL" \
       --command-config "$props" >/dev/null 2>&1; then
    echo "SASL_PLAINTEXT $KAFKA_BOOTSTRAP_SASL ($mech) : ok"
  else
    echo "SASL_PLAINTEXT $KAFKA_BOOTSTRAP_SASL (${mech:-?}) : DOWN"; rc=1
  fi
done
exit $rc
