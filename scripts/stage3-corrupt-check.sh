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
# Since stage 4 each chunk has several replicas. The byte is flipped on the
# replica get reads first (the lowest node_id, as GetObject orders them): read
# failover is stage 5, so until then that replica decides the outcome.
#
# The node image is distroless (no shell), so the byte is flipped from a
# throwaway alpine container that mounts that node's volume with --volumes-from.
#
# Usage: scripts/stage3-corrupt-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
source "$root/scripts/lib/osctl.sh"
compose=(docker compose -f "$root/docker-compose.yml")
work="$(mktemp -d)"
node=""    # container of the node holding the replica get reads first
flipped="" # chunk ID whose byte is currently flipped

# Inverts one byte of a chunk file on $node's volume.
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
osctl_run put "$work/one" "$key1"
osctl_run put "$work/two" "$key2"

chunk="$("${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tAc \
  "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$key1' AND c.chunk_index = 1")"
reader="$("${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tAc \
  "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$chunk' ORDER BY node_id LIMIT 1")"
node="$("${compose[@]}" ps -q "$reader")"
offset=$((56 + 1000000)) # past the 56-byte header, inside the data
echo "flipping byte $offset of chunk $chunk (index 1 of $key1) on $reader, the replica get reads first"
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

if "${compose[@]}" logs "$reader" | grep "failed verification" | grep -q "$chunk"; then
  echo "  ok: $reader logged a verification failure for chunk $chunk"
else
  echo "  FAIL: no $reader log line for chunk $chunk"
  status=1
fi

echo
echo "get $key2 (untouched):"
if osctl_run get "$key2" "$work/two.out" && cmp -s "$work/two" "$work/two.out"; then
  echo "  ok: IDENTICAL"
else
  echo "  FAIL: untouched object did not round-trip"
  status=1
fi

echo
echo "restoring the flipped byte:"
flip "$chunk" "$offset" | sed 's/^/  /'
flipped=""
if osctl_run get "$key1" "$work/one.out" && cmp -s "$work/one" "$work/one.out"; then
  echo "  ok: $key1 reads back IDENTICAL after restore"
else
  echo "  FAIL: $key1 did not read back after restore"
  status=1
fi

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
