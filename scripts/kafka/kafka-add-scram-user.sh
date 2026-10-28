#!/usr/bin/env bash
# Create/update a SCRAM-SHA-256 and SCRAM-SHA-512 credential on the running broker (via PLAINTEXT listener).
# SCRAM users created at format time (admin + $KAFKA_USER) already exist; use this for extra test users.
# PLAIN users are defined statically in server.properties (regenerated on every kafka-start.sh).
#
# Usage: KAFKA_HOME=... ./kafka-add-scram-user.sh <username> <password>
set -euo pipefail
# shellcheck source=kafka-env.sh
. "$(dirname "${BASH_SOURCE[0]}")/kafka-env.sh"
[ $# -eq 2 ] || { echo "usage: $0 <username> <password>" >&2; exit 2; }
# One mechanism per request: the broker rejects two alterations of one user in a single request
# (DuplicateResourceException: A user credential cannot be altered twice in the same request).
for mech in SCRAM-SHA-256 SCRAM-SHA-512; do
  kafka_cli kafka-configs.sh --bootstrap-server "$KAFKA_BOOTSTRAP_PLAINTEXT" --alter --entity-type users --entity-name "$1" \
    --add-config "${mech}=[iterations=4096,password=$2]" >/dev/null
done
echo "kafka-add-scram-user: $1 ok"
