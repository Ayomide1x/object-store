#!/usr/bin/env bash
# Stage 3 acceptance, adapted to replication and failover: flipping a byte in
# stored chunk files makes get fail loudly instead of returning bad data.
#
# Since stage 5 a get fails over between replicas, so one corrupt replica no
# longer fails it (stage5-failover-check.sh covers that). To keep stage 3's
# criterion, this flips one data byte in EVERY replica of the middle chunk of
# a 10 MiB object, then expects:
#   - get of that object fails, naming each replica's DataLoss, and leaves no
#     output file
#   - each replica's node logs a verification failure for that chunk
#   - get of a second, untouched object still returns identical bytes
#   - the get's reports, confirmed by the nodes, mark all three replicas BAD
# Finally it flips the bytes back, so every file on the volumes is valid for the
# next script. The BAD marks stay (BAD is terminal), so the object then fails at
# GetObject with DATA_LOSS; the script checks that too.
#
# Usage: scripts/stage3-corrupt-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/volume.sh"
source "$root/scripts/lib/liveness.sh"
# up --wait means the coordinator is serving; uploads need ALIVE nodes.
wait_alive 4
work="$(mktemp -d)"
offset=$((56 + 1000000)) # past the 56-byte header, inside the data

flipped="$work/flipped" # "node chunk" per byte currently flipped
: > "$flipped"
cleanup() {
  while read -r node chunk; do
    [[ -n "$node" ]] && flip_byte "$node" "$chunk" "$offset" >/dev/null
  done <"$flipped"
  rm -rf "$work"
}
trap cleanup EXIT

sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -c "$1"; }
status=0
ok() { echo "  ok: $*"; }
fail() { echo "  FAIL: $*"; status=1; }

key1="corrupt-check/one-$$"
key2="corrupt-check/two-$$"
head -c $((10 << 20)) /dev/urandom > "$work/one"
head -c $((10 << 20)) /dev/urandom > "$work/two"
osctl_run put "$work/one" "$key1"
osctl_run put "$work/two" "$key2"

chunk="$(sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$key1' AND c.chunk_index = 1")"
replicas="$(sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$chunk' ORDER BY node_id" | tr '\n' ' ')"
echo "flipping byte $offset of chunk $chunk (index 1 of $key1) on every replica: $replicas"
for n in $replicas; do
  printf '  %s: ' "$n"
  flip_byte "$n" "$chunk" "$offset"
  echo "$n $chunk" >> "$flipped"
done

echo
echo "get $key1 (every replica of chunk 1 corrupted):"
if "$osctl" get "$key1" "$work/one.out" 2>"$work/get1.err"; then
  fail "get succeeded with every replica corrupt"
else
  err="$(grep -v ' level=INFO ' "$work/get1.err" | grep 'osctl:' | tail -1)"
  echo "  ${err:0:200}..."
  missing=""
  for n in $replicas; do grep -q "$n: rpc error: code = DataLoss" <<<"$err" || missing="$missing $n"; done
  [[ -z "$missing" ]] && ok "failed loudly, naming each replica's DataLoss" || fail "no DataLoss for:$missing"
fi
[[ -e "$work/one.out" ]] && fail "an output file was left behind" || ok "no output file"
for n in $replicas; do
  if "${compose[@]}" logs "$n" | grep "failed verification" | grep -q "$chunk"; then
    ok "$n logged a verification failure for chunk $chunk"
  else
    fail "no $n log line for chunk $chunk"
  fi
done

echo
echo "get $key2 (untouched):"
if osctl_run get "$key2" "$work/two.out" && cmp -s "$work/two" "$work/two.out"; then
  ok "IDENTICAL"
else
  fail "untouched object did not round-trip"
fi

echo
echo "replica states after the get's reports:"
for n in $replicas; do
  st="$(sql "SELECT state FROM chunk_replicas WHERE chunk_id = '$chunk' AND node_id = '$n'")"
  echo "  $n: $st"
  [[ "$st" == BAD ]] || fail "$n was not marked BAD"
done

echo
echo "restoring the flipped bytes:"
while read -r node c; do
  printf '  %s: ' "$node"
  flip_byte "$node" "$c" "$offset"
done <"$flipped"
: > "$flipped"
if "$osctl" get "$key1" "$work/one.out" 2>"$work/get1b.err"; then
  fail "get succeeded although every replica is marked BAD"
elif grep -q "code = DataLoss.*no healthy replicas" "$work/get1b.err"; then
  ok "files restored, but BAD is terminal: get now fails at GetObject with DataLoss"
else
  fail "get failed differently: $(grep -v ' level=INFO ' "$work/get1b.err" | tail -1)"
fi

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
