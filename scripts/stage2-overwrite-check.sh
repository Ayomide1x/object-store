#!/usr/bin/env bash
# Stage 2 acceptance: a get issued during an in-flight overwrite returns either
# the old or the new object, never a mix.
#
# A writer alternately overwrites one key with file A and file B while two
# readers download it in a loop and classify each download by SHA-256. A and B
# are the same size, so a mixed download can't be caught by size alone; only
# the hash catches it.
#
# Usage: scripts/stage2-overwrite-check.sh [ITERATIONS] [SIZE_MIB]
#   ITERATIONS  overwrites to perform (default 10)
#   SIZE_MIB    size of A and B in MiB (default 100)
#
# Disk cost: every put writes SIZE_MIB of chunk files that nothing deletes until
# stage 8 GC, so a run adds about (ITERATIONS + 1) x SIZE_MIB to node1's volume.
# Reset with: docker compose down -v && docker compose up --build -d --wait
#
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

iterations="${1:-10}"
size_mib="${2:-100}"
root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
key="overwrite-check"
work="$(mktemp -d)"

cleanup() {
  touch "$work/stop"
  wait 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

sha() { shasum -a 256 "$1" | cut -d' ' -f1; }

echo "generating two ${size_mib} MiB files"
head -c $((size_mib << 20)) /dev/urandom > "$work/A"
head -c $((size_mib << 20)) /dev/urandom > "$work/B"
sha_a="$(sha "$work/A")"
sha_b="$(sha "$work/B")"
"$osctl" put "$work/A" "$key" 2>/dev/null

# Each reader appends one line per download to its log: A, B, MIX or ERROR.
reader() {
  local id="$1" out="$work/read-$1"
  while [[ ! -e "$work/stop" ]]; do
    if "$osctl" get "$key" "$out" 2>>"$work/reader-$id.err"; then
      case "$(sha "$out")" in
        "$sha_a") echo A ;;
        "$sha_b") echo B ;;
        *) echo MIX ;;
      esac
    else
      echo ERROR
    fi >> "$work/reader-$id.log"
  done
}
: > "$work/reader-1.log"
: > "$work/reader-2.log"
reader 1 &
reader 2 &

echo "overwriting $iterations times while 2 readers download"
writer_errors=0
for ((i = 1; i <= iterations; i++)); do
  if ((i % 2 == 1)); then src=B; else src=A; fi
  if ! "$osctl" put "$work/$src" "$key" 2>>"$work/writer.err"; then
    writer_errors=$((writer_errors + 1))
  fi
done
# Let the readers finish the download they are in, so the final state is read too.
touch "$work/stop"
wait

count() { cat "$work"/reader-*.log | grep -cx "$1" || true; }
a="$(count A)"; b="$(count B)"; mix="$(count MIX)"; errors="$(count ERROR)"

echo
echo "downloads: A=$a  B=$b  MIX=$mix  ERROR=$errors   (writer errors: $writer_errors)"
if ((errors > 0)); then
  echo "reader errors (first 5):"
  cat "$work"/reader-*.err | head -5
fi

echo
echo "node1 chunk volume usage:"
node="$(docker compose -f "$root/docker-compose.yml" ps -q node1)"
docker run --rm --volumes-from "$node" alpine:3 du -sh /data/chunks

if ((mix == 0 && errors == 0 && writer_errors == 0 && a > 0 && b > 0)); then
  echo "PASS: no mixed downloads; reads overlapped overwrites (both versions seen)"
else
  echo "FAIL: need MIX=0, ERROR=0, writer errors=0, and both A and B seen at least once"
  exit 1
fi
