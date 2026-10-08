#!/usr/bin/env bash
# Controlled failure injection against the reference compose stack (Phase 9).
# Requires: docker compose prod stack running with archive + postgres.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

COMPOSE="${COMPOSE:-docker compose -f docker-compose.prod.yml}"
METRICS="${METRICS_URL:-http://127.0.0.1:9090}"

ready() { curl -fsS "$METRICS/readyz" | tee /dev/stderr | grep -q '"mode"'; }
health() { curl -fsS "$METRICS/healthz"; }
checkpoint() {
  curl -fsS "$METRICS/readyz" | python3 -c 'import sys,json; print(json.load(sys.stdin).get("checkpoint_round",0))'
}

echo "==> baseline"
health
ready || echo "(not ready yet — catch-up may be ok)"
cp1="$(checkpoint || echo 0)"
echo "checkpoint=$cp1"

echo "==> 1) process crash (kill follower)"
$COMPOSE kill -s SIGKILL follower || $COMPOSE kill follower
sleep 2
$COMPOSE up -d follower
for i in $(seq 1 60); do
  if curl -fsS "$METRICS/healthz" >/dev/null 2>&1; then break; fi
  sleep 2
done
cp2="$(checkpoint || echo 0)"
echo "after_restart checkpoint=$cp2 (expect >= $cp1; resume cp+1)"
health

echo "==> 2) PostgreSQL unavailable"
$COMPOSE stop postgres
sleep 5
code="$(curl -s -o /tmp/readyz.json -w '%{http_code}' "$METRICS/readyz" || true)"
echo "readyz_http=$code body=$(cat /tmp/readyz.json 2>/dev/null || true)"
echo "expect: degraded/unready (503) and checkpoint frozen while DB down"
cp_down="$(checkpoint || echo 0)"
sleep 8
cp_down2="$(checkpoint || echo 0)"
echo "checkpoint_while_down $cp_down → $cp_down2 (should not advance)"
$COMPOSE start postgres
echo "waiting for recovery..."
for i in $(seq 1 60); do
  code="$(curl -s -o /tmp/readyz.json -w '%{http_code}' "$METRICS/readyz" || true)"
  if [[ "$code" == "200" ]]; then
    echo "recovered readyz=200"
    break
  fi
  sleep 2
done
health

echo "==> 3) host/container restart (compose recreate follower)"
$COMPOSE up -d --force-recreate follower
sleep 5
health
echo "done — verify archive: ARCHIVE_PATH=... go run ./cmd/verify -archive ..."
echo "manual: stop algod / break archive mount to exercise remaining cases (see docs/phase9-production.md)"
