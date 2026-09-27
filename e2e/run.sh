#!/usr/bin/env bash
# Runs the e2e suite against one Kafka image: starts the three-node cluster
# from e2e/compose.yaml, runs `go test -tags e2e`, and removes the cluster.
#
#   e2e/run.sh                                  # apache/kafka:4.3.1
#   KAFKA_IMAGE=confluentinc/cp-kafka:8.3.2 e2e/run.sh
#   KEEP=1 e2e/run.sh                           # leave the cluster up afterwards
#
# Extra arguments go to `go test`, e.g. `e2e/run.sh -run TestReadOnly`.
set -euo pipefail

cd "$(dirname "$0")/.."
export KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.1}"
export E2E_COMPOSE="$PWD/e2e/compose.yaml"

compose() { docker compose -f "$E2E_COMPOSE" "$@"; }

if [ -z "${KEEP:-}" ]; then
  trap 'compose down -v --remove-orphans >/dev/null 2>&1 || true' EXIT
fi

echo "e2e: starting a 3-node cluster on $KAFKA_IMAGE"
compose up -d --quiet-pull
status=0
go test -tags e2e -count=1 -timeout 10m -v "$@" ./e2e/ || status=$?
if [ "$status" -ne 0 ]; then
  compose logs --tail 200 >&2 || true
fi
exit "$status"
