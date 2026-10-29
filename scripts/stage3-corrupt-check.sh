#!/usr/bin/env bash
# Stage 3 acceptance: flipping one byte of a stored chunk file makes get fail
# loudly instead of returning bad data.
#
# Puts two 10 MiB objects (3 chunks each), flips one data byte in the middle
# chunk of the first, then expects:
#   - get of the first object fails with DataLoss and leaves no output file
#   - the node logs a verification failure naming that chunk
#   - get of the second object still returns identical bytes
# Finally it flips the byte back (XOR 0xFF is its own inverse), even on
# failure, and checks the first object reads back identical. That leaves every
# chunk on the volume valid for the next script.
#
# The node image is distroless (no shell), so the byte is flipped from a
# throwaway alpine container that mounts node1's volume with --volumes-from.
#
# Usage: scripts/stage3-corrupt-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
work="$(mktemp -d)"
node="$("${compose[@]}" ps -q node1)"
flipped="" # chunk ID whose byte is currently flipped

# Inverts one byte of a chunk file on node1's volume.
flip() {
  docker run --rm --volumes-from "$node" alpine:3 sh -c '
    f=/data/chunks/$1; off=$2
    before=$(od -An -tu1 -j "$off" -N1 "$f" | tr -d " ")
    printf "$(printf "\\%03o" $((before ^ 255)))" | dd of="$f" bs=1 seek="$off" count=1 conv=notrunc 2>/dev/null
    after=$(od -An -tu1 -j "$off" -N1 "$f" | tr -d " ")
    echo "byte at $off: $before -> $after"
  ' sh "$1" "$2"
}
cleanup() {
  if [[ -n "$flipped" ]]; then
    echo "restoring chunk $flipped"
    flip "$flipped" "$offset" | sed 's/^/  /'
  fi
  rm -rf "$work"
}
trap cleanup EXIT

key1="corrupt-check/one-$$"
key2="corrupt-check/two-$$"
head -c $((10 << 20)) /dev/urandom > "$work/one"
head -c $((10 << 20)) /dev/urandom > "$work/two"
"$osctl" put "$work/one" "$key1" 2>/dev/null
"$osctl" put "$work/two" "$key2" 2>/dev/null

chunk="$("${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tAc \
  "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$key1' AND c.chunk_index = 1")"
offset=$((56 + 1000000)) # past the 56-byte header, inside the data
echo "flipping byte $offset of chunk $chunk (index 1 of $key1)"
flip "$chunk" "$offset"
flipped="$chunk"

status=0
echo
echo "get $key1 (corrupted):"
if "$osctl" get "$key1" "$work/one.out" 2>"$work/get1.err"; then
  echo "  FAIL: get succeeded on a corrupted chunk"
  status=1
else
  tail -1 "$work/get1.err" | sed 's/^/  /'
  if grep -q "DataLoss" "$work/get1.err"; then
    echo "  ok: failed with DataLoss"
  else
    echo "  FAIL: error is not DataLoss"
    status=1
  fi
fi
if [[ -e "$work/one.out" ]]; then
  echo "  FAIL: an output file was left behind"
  status=1
else
  echo "  ok: no output file"
fi

if "${compose[@]}" logs node1 | grep "failed verification" | grep -q "$chunk"; then
  echo "  ok: node logged a verification failure for chunk $chunk"
else
  echo "  FAIL: no node log line for chunk $chunk"
  status=1
fi

echo
echo "get $key2 (untouched):"
if "$osctl" get "$key2" "$work/two.out" 2>/dev/null && cmp -s "$work/two" "$work/two.out"; then
  echo "  ok: IDENTICAL"
else
  echo "  FAIL: untouched object did not round-trip"
  status=1
fi

echo
echo "restoring the flipped byte:"
flip "$chunk" "$offset" | sed 's/^/  /'
flipped=""
if "$osctl" get "$key1" "$work/one.out" 2>/dev/null && cmp -s "$work/one" "$work/one.out"; then
  echo "  ok: $key1 reads back IDENTICAL after restore"
else
  echo "  FAIL: $key1 did not read back after restore"
  status=1
fi

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
