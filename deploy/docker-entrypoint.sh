#!/bin/sh
# Ensure archive directory is writable, then drop privileges when started as root.
set -eu
ARCHIVE_PATH="${ARCHIVE_PATH:-/var/lib/voi-fast-follower/archive}"
mkdir -p "$ARCHIVE_PATH"
if [ "$(id -u)" = "0" ]; then
  chown -R follower:follower /var/lib/voi-fast-follower 2>/dev/null || true
  exec su-exec follower /app/follower "$@"
fi
exec /app/follower "$@"
