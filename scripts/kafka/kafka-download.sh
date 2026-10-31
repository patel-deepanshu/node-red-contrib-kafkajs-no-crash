#!/usr/bin/env bash
# Download + verify (SHA-512) + extract an Apache Kafka binary distribution into a cache dir,
# then print the resulting KAFKA_HOME on stdout (last line). Idempotent: re-uses an existing extract.
#
#   KAFKA_VERSION     default 3.9.1
#   KAFKA_SCALA       default 2.13
#   KAFKA_CACHE_DIR   default ~/.cache/kafka-e2e
#   KAFKA_MIRROR      default https://archive.apache.org/dist/kafka  (any URL curl understands, incl. file://)
#
# Usage: export KAFKA_HOME="$(./kafka-download.sh | tail -n1)"
set -euo pipefail

: "${KAFKA_VERSION:=3.9.1}"
: "${KAFKA_SCALA:=2.13}"
: "${KAFKA_CACHE_DIR:=${XDG_CACHE_HOME:-$HOME/.cache}/kafka-e2e}"
: "${KAFKA_MIRROR:=https://archive.apache.org/dist/kafka}"

name="kafka_${KAFKA_SCALA}-${KAFKA_VERSION}"
home="$KAFKA_CACHE_DIR/$name"
if [ -x "$home/bin/kafka-server-start.sh" ]; then
  echo "$home"
  exit 0
fi

mkdir -p "$KAFKA_CACHE_DIR"
tmp="$(mktemp -d "$KAFKA_CACHE_DIR/.dl.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

url="$KAFKA_MIRROR/$KAFKA_VERSION/$name.tgz"
echo "kafka-download: $url" >&2
curl -fsSL --retry 3 -o "$tmp/$name.tgz" "$url"
curl -fsSL --retry 3 -o "$tmp/$name.tgz.sha512" "$url.sha512"

# Apache publishes the digest in `gpg --print-md` format (hex split over several lines, upper-case).
expected="$(sed 's/^[^:]*://' "$tmp/$name.tgz.sha512" | tr -d ' \t\r\n' | tr 'A-F' 'a-f')"
actual="$(sha512sum "$tmp/$name.tgz" | awk '{print $1}')"
if [ "$expected" != "$actual" ]; then
  echo "kafka-download: SHA-512 mismatch for $name.tgz" >&2
  echo "  expected $expected" >&2
  echo "  actual   $actual" >&2
  exit 1
fi
echo "kafka-download: checksum ok" >&2

tar -xzf "$tmp/$name.tgz" -C "$tmp"
mv "$tmp/$name" "$home"
echo "$home"
