#!/usr/bin/env bash
# Archive backup / restore / verify workflow (Phase 9).
# Does not require a Postgres dump for historical raw blocks.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ARCHIVE_PATH="${ARCHIVE_PATH:-$ROOT/archive}"
BACKUP_ROOT="${BACKUP_ROOT:-$ROOT/backups}"
COMPOSE="${COMPOSE:-docker compose -f docker-compose.prod.yml}"

usage() {
  cat <<EOF
usage:
  $0 backup              # export full durable range to backups/<timestamp>
  $0 restore DEST_DIR    # import latest (or BACKUP=) into empty DEST_DIR + verify
  $0 verify [ARCHIVE]    # integrity check
EOF
}

cmd="${1:-}"
case "$cmd" in
  backup)
    mkdir -p "$BACKUP_ROOT"
    info="$(go run ./cmd/archive-info -archive "$ARCHIVE_PATH")"
    echo "$info"
    first="$(echo "$info" | awk '/^First round:/{print $3}')"
    last="$(echo "$info" | awk '/^Checkpoint:/{print $2}')"
    if [[ "$first" == "-" || "$last" == "none" || -z "$first" || -z "$last" ]]; then
      echo "archive has no durable range" >&2
      exit 1
    fi
    stamp="$(date -u +%Y%m%dT%H%M%SZ)"
    out="$BACKUP_ROOT/archive-$stamp"
    echo "==> exporting $first–$last → $out"
    go run ./cmd/archive-export -archive "$ARCHIVE_PATH" -out "$out" -start "$first" -end "$last"
    go run ./cmd/verify -archive "$out"
    echo "backup_ok path=$out"
    ;;
  restore)
    dest="${2:-}"
    if [[ -z "$dest" ]]; then usage; exit 2; fi
    src="${BACKUP:-}"
    if [[ -z "$src" ]]; then
      src="$(ls -1dt "$BACKUP_ROOT"/archive-* 2>/dev/null | head -1 || true)"
    fi
    if [[ -z "$src" || ! -d "$src" ]]; then
      echo "no backup found (set BACKUP= or run backup first)" >&2
      exit 1
    fi
    echo "==> importing $src → $dest"
    mkdir -p "$(dirname "$dest")"
    go run ./cmd/archive-import -in "$src" -archive "$dest"
    go run ./cmd/verify -archive "$dest"
    echo "restore_ok path=$dest"
    echo "optional: go run ./cmd/replay -archive $dest -start N -end M -sink postgres ..."
    ;;
  verify)
    path="${2:-$ARCHIVE_PATH}"
    go run ./cmd/verify -archive "$path"
    ;;
  *)
    usage
    exit 2
    ;;
esac
