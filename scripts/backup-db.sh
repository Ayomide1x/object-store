#!/usr/bin/env bash
# Daily metadata backup on the VM (DEPLOY.md): pg_dump of the objectstore
# database, custom format, through the running postgres container.
#   - Written to a temp file, checked with pg_restore --list, then renamed, so
#     a failed or partial dump never takes a backup's name.
#   - Backups older than KEEP_DAYS are deleted, but only after a good one.
#
# Metadata only: the chunk files on the node volumes are not backed up. A
# restore brings back the keys and replicas as they were at the dump: objects
# written since are lost, and a chunk GC deleted since fails its reads (each
# read is checksummed; nothing wrong is served).
#
# Usage: scripts/backup-db.sh [DIR] [KEEP_DAYS]
#   DIR        default ~/backups/objectstore
#   KEEP_DAYS  default 14
# Run from the repo directory's cron entry (DEPLOY.md); docker compose there
# reads .env, so it includes the production override.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
dir="${1:-$HOME/backups/objectstore}"
keep="${2:-14}"
[[ "$keep" =~ ^[1-9][0-9]*$ ]] || { echo "KEEP_DAYS must be a positive integer, got '$keep'" >&2; exit 1; }
mkdir -p "$dir"
cd "$root"

name="objectstore-$(date -u +%Y%m%dT%H%M%SZ).dump"
tmp="$dir/.$name.partial"
trap 'rm -f "$tmp"' EXIT

docker compose exec -T postgres pg_dump -U objectstore -d objectstore -Fc >"$tmp"
# The dump's table of contents must list the tables, or it is not a backup.
toc="$(docker compose exec -T postgres pg_restore --list <"$tmp")"
grep -q 'TABLE DATA public objects' <<<"$toc" || { echo "backup check failed: no objects table in $tmp" >&2; exit 1; }
mv "$tmp" "$dir/$name"
echo "$(date -u +%FT%TZ) wrote $dir/$name ($(wc -c <"$dir/$name" | tr -d ' ') bytes)"

find "$dir" -maxdepth 1 -name 'objectstore-*.dump' -mtime "+$((keep - 1))" -print -delete \
  | sed 's/^/deleted /'
