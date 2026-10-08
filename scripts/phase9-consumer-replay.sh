#!/usr/bin/env bash
# Reference consumer: replay archive → Postgres while follower may still be live.
# Demonstrates that consumers are independent of acquisition.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ARCHIVE_PATH="${ARCHIVE_PATH:-$ROOT/archive}"
DSN="${DATABASE_URL:-postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable}"
START="${START:-}"
END="${END:-}"

info="$(go run ./cmd/archive-info -archive "$ARCHIVE_PATH")"
echo "$info"
if [[ -z "$START" ]]; then
  START="$(echo "$info" | awk '/^First round:/{print $3}')"
fi
if [[ -z "$END" ]]; then
  END="$(echo "$info" | awk '/^Checkpoint:/{print $2}')"
fi
if [[ "$START" == "-" || "$END" == "none" ]]; then
  echo "archive empty" >&2
  exit 1
fi

echo "==> consumer replay $START–$END (follower can keep writing live)"
go run ./cmd/verify -archive "$ARCHIVE_PATH" -start "$START" -end "$END"
go run ./cmd/replay -archive "$ARCHIVE_PATH" -start "$START" -end "$END" \
  -sink postgres -database "$DSN" -batch "${BATCH:-10}"
echo "consumer_ok — restart this script anytime; follower acquisition is unaffected"
