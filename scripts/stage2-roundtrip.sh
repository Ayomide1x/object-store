#!/usr/bin/env bash
# Stage 2 acceptance: round-trip files at the chunking edge cases and compare.
#
# Usage: scripts/stage2-roundtrip.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/liveness.sh"
# up --wait means the coordinator is serving; uploads need ALIVE nodes.
wait_alive 4
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mib=$((1 << 20))
# name:size-in-bytes
cases="zero:0 one:1 exact-4mib:$((4 * mib)) 4mib-plus-1:$((4 * mib + 1)) hundred-mib:$((100 * mib))"

status=0
for c in $cases; do
  name="${c%%:*}"
  size="${c##*:}"
  if ((size == 0)); then
    : > "$work/$name.in" # macOS head rejects -c 0
  else
    head -c "$size" /dev/urandom > "$work/$name.in"
  fi
  osctl_run put "$work/$name.in" "roundtrip/$name"
  osctl_run get "roundtrip/$name" "$work/$name.out"
  chunks=$(( (size + 4 * mib - 1) / (4 * mib) ))
  if cmp -s "$work/$name.in" "$work/$name.out"; then
    printf '%-12s %10d bytes  %3d chunks  IDENTICAL\n' "$name" "$size" "$chunks"
  else
    printf '%-12s %10d bytes  %3d chunks  DIFFERENT\n' "$name" "$size" "$chunks"
    status=1
  fi
done
exit "$status"
