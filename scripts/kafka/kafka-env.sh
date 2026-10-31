# shellcheck shell=bash
# Common settings for the local single-node KRaft Kafka used by the E2E tests.
# Sourced by the other kafka-*.sh scripts; every value can be overridden via env.
#
#   KAFKA_HOME            extracted Apache Kafka distribution (must contain bin/kafka-server-start.sh)
#   KAFKA_BASE_DIR        where config, data, logs and the pid file live (default: <repo>/.kafka-local, git-ignored)
#   KAFKA_HOST            bind / advertised host               (default 127.0.0.1)
#   KAFKA_PLAINTEXT_PORT  PLAINTEXT listener                    (default 19092)
#   KAFKA_SASL_PORT       SASL_PLAINTEXT listener               (default 19093)
#   KAFKA_CONTROLLER_PORT KRaft controller listener (internal)  (default 19094)
#   KAFKA_ADMIN_USER / KAFKA_ADMIN_PASSWORD   SASL superuser    (default admin / admin-secret)
#   KAFKA_USER / KAFKA_PASSWORD               SASL normal user  (default alice / alice-secret)
#   KAFKA_AUTO_CREATE_TOPICS  broker auto.create.topics.enable  (default true)
#   KAFKA_NUM_PARTITIONS      default partitions for new topics (default 1)
#   KAFKA_HEAP_OPTS           JVM heap                          (default -Xms256m -Xmx512m)
#   KAFKA_READY_TIMEOUT       seconds to wait for readiness     (default 90)

_kafka_env_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${KAFKA_HOME:?KAFKA_HOME must point at an extracted Apache Kafka distribution (e.g. .../kafka_2.13-3.9.1)}"
if [ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
  echo "kafka-env: $KAFKA_HOME/bin/kafka-server-start.sh not found or not executable" >&2
  exit 2
fi

: "${KAFKA_BASE_DIR:=$(cd "$_kafka_env_dir/../.." && pwd)/.kafka-local}"
: "${KAFKA_HOST:=127.0.0.1}"
: "${KAFKA_PLAINTEXT_PORT:=19092}"
: "${KAFKA_SASL_PORT:=19093}"
: "${KAFKA_CONTROLLER_PORT:=19094}"
: "${KAFKA_ADMIN_USER:=admin}"
: "${KAFKA_ADMIN_PASSWORD:=admin-secret}"
: "${KAFKA_USER:=alice}"
: "${KAFKA_PASSWORD:=alice-secret}"
: "${KAFKA_AUTO_CREATE_TOPICS:=true}"
: "${KAFKA_NUM_PARTITIONS:=1}"
: "${KAFKA_HEAP_OPTS:=-Xms256m -Xmx512m}"
: "${KAFKA_READY_TIMEOUT:=90}"

KAFKA_CONF_DIR="$KAFKA_BASE_DIR/config"
KAFKA_DATA_DIR="$KAFKA_BASE_DIR/data"
KAFKA_LOG_DIR="$KAFKA_BASE_DIR/logs"
KAFKA_PID_FILE="$KAFKA_BASE_DIR/kafka.pid"
KAFKA_SERVER_PROPS="$KAFKA_CONF_DIR/server.properties"
KAFKA_CLIENT_PLAINTEXT_PROPS="$KAFKA_CONF_DIR/client-plaintext.properties"
KAFKA_CLIENT_SASL_PLAIN_PROPS="$KAFKA_CONF_DIR/client-sasl-plain.properties"
KAFKA_CLIENT_SCRAM256_PROPS="$KAFKA_CONF_DIR/client-sasl-scram256.properties"
KAFKA_CLIENT_SCRAM512_PROPS="$KAFKA_CONF_DIR/client-sasl-scram512.properties"
KAFKA_BOOTSTRAP_PLAINTEXT="$KAFKA_HOST:$KAFKA_PLAINTEXT_PORT"
KAFKA_BOOTSTRAP_SASL="$KAFKA_HOST:$KAFKA_SASL_PORT"

# Kafka's launcher scripts read these.
export KAFKA_HEAP_OPTS
export LOG_DIR="$KAFKA_LOG_DIR"

kafka_pid() {
  # Prints the PID of our broker if it is alive, returns 1 otherwise.
  [ -f "$KAFKA_PID_FILE" ] || return 1
  local pid
  pid="$(cat "$KAFKA_PID_FILE" 2>/dev/null)"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
  # Guard against PID reuse: must be a java process running kafka.Kafka.
  if [ -r "/proc/$pid/cmdline" ] && ! tr '\0' ' ' <"/proc/$pid/cmdline" | grep -q 'kafka\.Kafka'; then
    return 1
  fi
  echo "$pid"
}

kafka_port_open() { # host port
  (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null
}

# Run a Kafka CLI tool without the server's log4j/heap settings leaking in.
kafka_cli() { # tool [args...]
  local tool="$1"; shift
  # kafka-run-class.sh falls back to config/tools-log4j.properties (WARN to stderr) for tools.
  KAFKA_HEAP_OPTS="-Xmx256m" KAFKA_LOG4J_OPTS="" LOG_DIR="$KAFKA_LOG_DIR" "$KAFKA_HOME/bin/$tool" "$@"
}
