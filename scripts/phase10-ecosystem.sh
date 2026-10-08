#!/usr/bin/env bash
# Phase 10 — ecosystem validation (archive + reference consumer, no live Voi required).
set -euo pipefail
cd "$(dirname "$0")/.."

ARCHIVE="${ARCHIVE_PATH:-./archive}"
START="${START:-1}"
END="${END:-100}"
CP="${CHECKPOINT:-./tmp/phase10-consumer.cp}"

if [[ ! -d "$ARCHIVE" ]]; then
  echo "archive not found: $ARCHIVE (set ARCHIVE_PATH or run follower first)" >&2
  exit 1
fi

echo "== verify archive =="
go run ./cmd/verify -archive "$ARCHIVE"

echo "== archive info =="
go run ./cmd/archive-info -archive "$ARCHIVE"

mkdir -p "$(dirname "$CP")"
rm -f "$CP"

echo "== reference consumer historical =="
go run ./examples/block-consumer \
  --archive "$ARCHIVE" --start "$START" --end "$END" \
  --checkpoint "$CP"

echo "== consumer restart (should process 0 new blocks) =="
go run ./examples/block-consumer \
  --archive "$ARCHIVE" --start "$START" --end "$END" \
  --checkpoint "$CP"

echo "== second independent consumer =="
CP2="./tmp/phase10-consumer-b.cp"
rm -f "$CP2"
go run ./examples/block-consumer \
  --archive "$ARCHIVE" --start "$START" --end "$(( START + (END - START) / 2 ))" \
  --checkpoint "$CP2"

echo "ok consumer_a=$CP consumer_b=$CP2"
