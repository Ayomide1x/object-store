#!/usr/bin/env bash
# Stage 5 acceptance: reads fail over between replicas, and a replica is marked
# BAD only when its own node confirms the client's report.
#
# Runs with OSCTL_READ_ORDER=listed, so the replica read first is known: the
# lowest node_id among a chunk's healthy replicas. Each step uses its own object.
#   1. Corrupt the first replica of chunk 1: get IDENTICAL; that replica is
#      marked BAD; the next get reads chunk 1 from another node, no failover.
#   2. Delete the first replica's file: get IDENTICAL; marked BAD (missing).
#   3. Stop the first replica's node: get IDENTICAL; nothing marked BAD.
#   4. Pause it: get finishes within the read deadline; nothing marked BAD.
#      Since stage 6, steps 3 and 4 check a precondition, as stage 4 step 3
#      does: right before the get, the node's persisted state must still be
#      ALIVE (detection takes 6 s of silence; the get starts at once). If it is
#      not, the step fails as "inconclusive, rerun", because a node already
#      listed DEAD is read last and the get would not exercise failover. A pass
#      therefore always means the get really failed over from the stopped or
#      paused node. Liveness-ordered reads are covered by
#      stage6-liveness-check.sh and TestDeadReplicasAreReadLast.
#   5. Corrupt 2 of 3 replicas of a chunk: get IDENTICAL from the third;
#      the two are BAD, the third HEALTHY.
#   6. Corrupt all 3: get fails loudly naming each replica, no output file; all
#      three are then BAD, and the next get fails at GetObject with DATA_LOSS.
# Every flipped byte is flipped back at the end (BAD marks stay: BAD is terminal),
# unless stage 8 GC has deleted that BAD file by then (unflip checks the byte).
#
# Usage: scripts/stage5-failover-check.sh
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
run="$$"
deadline=10
offset=$((56 + 1000000)) # past the 56-byte header, inside chunk data
export OSCTL_READ_ORDER=listed OSCTL_CHUNK_READ_TIMEOUT="${deadline}s"

flipped="$work/flipped" # "node chunk offset flipped_value" per flipped byte
: > "$flipped"
restore() {
  "${compose[@]}" unpause node1 node2 node3 node4 >/dev/null 2>&1 || true
  "${compose[@]}" up -d --wait >/dev/null 2>&1 || true
  restore_flipped "$flipped" >/dev/null
  rm -rf "$work"
}
trap restore EXIT

status=0
fail() { echo "  FAIL: $*"; status=1; }
ok() { echo "  ok: $*"; }
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -c "$1"; }
now() { python3 -c 'import time; print(time.time())'; }
mkfile_mib() { head -c $(($2 << 20)) /dev/urandom > "$work/$1"; }
corrupt() { corrupt_byte "$1" "$2" "$offset" "$flipped" | sed 's/^/     /'; }

chunk_of() { sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$1' AND c.chunk_index = $2"; }
replicas_of() { sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$1' ORDER BY node_id"; }
state_of() { sql "SELECT state || coalesce(' (' || bad_reason || ')', '') FROM chunk_replicas WHERE chunk_id = '$1' AND node_id = '$2'"; }
# bad_count KEY: BAD replicas of KEY's object. Scoped to the step's own object:
# since stage 8, GC deletes earlier steps' BAD rows after the read grace.
bad_count() { sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
                   WHERE k.key = '$1' AND r.state = 'BAD'"; }
node_state() { sql "SELECT state FROM nodes WHERE node_id = '$1'"; }
# still_alive NODE: the precondition for steps 3 and 4.
still_alive() {
  local st
  st="$(node_state "$1")"
  if [[ "$st" == ALIVE ]]; then
    ok "precondition: the coordinator still sees $1 ALIVE"
  else
    fail "precondition: $1 is already $st, so the get would not fail over from it; inconclusive, rerun"
  fi
}
# The node a get's log says served CHUNK.
read_from() { sed -n "s/.*msg=\"chunk read\".*chunk_id=$2 node_id=\\([^ ]*\\).*/\\1/p" "$1" | tail -1; }

# get_identical KEY SRC LOG: get must succeed and match SRC; the full log is kept.
get_identical() {
  if "$osctl" get "$1" "$work/out" 2>"$3" && cmp -s "$2" "$work/out"; then
    ok "get IDENTICAL"
  else
    fail "get did not return identical bytes"
    grep -v ' level=INFO ' "$3" | tail -5 | sed 's/^/    /'
  fi
  rm -f "$work/out"
}

echo "1. corrupt the first replica of chunk 1"
mkfile_mib s1 10
osctl_run put "$work/s1" "failover/one-$run"
c1="$(chunk_of "failover/one-$run" 1)"
first="$(replicas_of "$c1" | head -1)"
echo "   chunk 1 = $c1, read first from $first"
corrupt "$first" "$c1"
get_identical "failover/one-$run" "$work/s1" "$work/get1.log"
grep -q "failing over.*chunk_id=$c1 node_id=$first\|chunk_id=$c1 node_id=$first.*failing over" "$work/get1.log" \
  && ok "the get failed over from $first" || fail "no failover from $first in the log"
grep "bad replica reported" "$work/get1.log" | grep -q "node_id=$first.*verdict=VERDICT_MARKED_BAD" \
  && ok "report verdict: VERDICT_MARKED_BAD" || fail "no MARKED_BAD verdict in the log"
st="$(state_of "$c1" "$first")"
echo "     $first: $st"
[[ "$st" == "BAD (corrupt: "* ]] && ok "$first is BAD with a corrupt reason" || fail "$first is not BAD (corrupt)"
get_identical "failover/one-$run" "$work/s1" "$work/get1b.log"
second="$(read_from "$work/get1b.log" "$c1")"
[[ -n "$second" && "$second" != "$first" ]] && ok "the next get read chunk 1 from $second, not $first" \
  || fail "the next get read chunk 1 from '${second}'"
grep -q "failing over" "$work/get1b.log" && fail "the next get still failed over" || ok "the next get did not fail over"

echo
echo "2. delete the first replica's file"
mkfile_mib s2 10
osctl_run put "$work/s2" "failover/two-$run"
c0="$(chunk_of "failover/two-$run" 0)"
first="$(replicas_of "$c0" | head -1)"
echo "   chunk 0 = $c0, read first from $first; deleting its file"
delete_chunk_file "$first" "$c0"
get_identical "failover/two-$run" "$work/s2" "$work/get2.log"
st="$(state_of "$c0" "$first")"
echo "     $first: $st"
[[ "$st" == "BAD (missing)" ]] && ok "$first is BAD (missing)" || fail "$first is not BAD (missing)"

echo
echo "3. stop the first replica's node"
mkfile_mib s3 10
osctl_run put "$work/s3" "failover/three-$run"
c0="$(chunk_of "failover/three-$run" 0)"
first="$(replicas_of "$c0" | head -1)"
before="$(bad_count "failover/three-$run")"
echo "   chunk 0 read first from $first; stopping it"
"${compose[@]}" stop "$first" >/dev/null 2>&1
still_alive "$first"
get_identical "failover/three-$run" "$work/s3" "$work/get3.log"
grep -q "failing over.*node_id=$first\|node_id=$first.*failing over" "$work/get3.log" \
  && ok "the get failed over from the stopped node $first" || fail "no failover from $first in the log"
[[ "$(bad_count "failover/three-$run")" == "$before" ]] && ok "nothing marked BAD (a stopped node is not a bad replica)" \
  || fail "a replica was marked BAD"
grep -q "bad replica reported" "$work/get3.log" && fail "a report was sent for a stopped node" || ok "no report sent"
"${compose[@]}" up -d --wait "$first" >/dev/null 2>&1
wait_alive 1 "$first" || fail "$first not ALIVE after restart"

echo
echo "4. pause the first replica's node"
mkfile_mib s4 10
osctl_run put "$work/s4" "failover/four-$run"
c0="$(chunk_of "failover/four-$run" 0)"
first="$(replicas_of "$c0" | head -1)"
t0="$(now)"; osctl_run get "failover/four-$run" "$work/out4"; baseline="$(python3 -c "print(round($(now) - $t0, 2))")"
before="$(bad_count "failover/four-$run")"
echo "   chunk 0 read first from $first; baseline get ${baseline}s; pausing it"
"${compose[@]}" pause "$first" >/dev/null
still_alive "$first"
t0="$(now)"
get_identical "failover/four-$run" "$work/s4" "$work/get4.log"
elapsed="$(python3 -c "print(round($(now) - $t0, 2))")"
limit="$(python3 -c "print(round($deadline + $baseline + 5, 2))")"
echo "   get took ${elapsed}s (limit ${limit}s = ${deadline}s deadline + ${baseline}s baseline + 5s)"
python3 -c "import sys; sys.exit(0 if $elapsed < $limit else 1)" && ok "finished within the read deadline" \
  || fail "took longer than the limit"
[[ "$(bad_count "failover/four-$run")" == "$before" ]] && ok "nothing marked BAD" || fail "a replica was marked BAD"
grep -q "failing over.*node_id=$first\|node_id=$first.*failing over" "$work/get4.log" \
  && ok "the get failed over from the paused node $first after its read deadline" \
  || fail "no failover from $first in the log"
"${compose[@]}" unpause "$first" >/dev/null
wait_alive 1 "$first" || fail "$first not ALIVE after unpause"

echo
echo "5. corrupt 2 of 3 replicas of chunk 1"
mkfile_mib s5 10
osctl_run put "$work/s5" "failover/five-$run"
c1="$(chunk_of "failover/five-$run" 1)"
read -r r1 r2 r3 <<<"$(replicas_of "$c1" | tr '\n' ' ')"
echo "   chunk 1 replicas: $r1 $r2 $r3; corrupting $r1 and $r2"
corrupt "$r1" "$c1"
corrupt "$r2" "$c1"
get_identical "failover/five-$run" "$work/s5" "$work/get5.log"
[[ "$(read_from "$work/get5.log" "$c1")" == "$r3" ]] && ok "chunk 1 was served by $r3" || fail "chunk 1 not served by $r3"
for n in "$r1" "$r2" "$r3"; do echo "     $n: $(state_of "$c1" "$n")"; done
[[ "$(state_of "$c1" "$r1")" == BAD* && "$(state_of "$c1" "$r2")" == BAD* && "$(state_of "$c1" "$r3")" == HEALTHY ]] \
  && ok "$r1 and $r2 BAD, $r3 HEALTHY" || fail "replica states wrong"

echo
echo "6. corrupt all 3 replicas of chunk 1"
mkfile_mib s6 10
osctl_run put "$work/s6" "failover/six-$run"
c1="$(chunk_of "failover/six-$run" 1)"
all="$(replicas_of "$c1" | tr '\n' ' ')"
echo "   chunk 1 replicas: $all; corrupting all of them"
for n in $all; do corrupt "$n" "$c1"; done
if "$osctl" get "failover/six-$run" "$work/out6" 2>"$work/get6.log"; then
  fail "get succeeded with every replica corrupt"
else
  err="$({ grep -v ' level=INFO ' "$work/get6.log" | grep 'osctl:' || true; } | tail -1)"
  echo "     ${err:0:300}"
  missing=""
  for n in $all; do grep -q "$n: rpc error: code = DataLoss" <<<"$err" || missing="$missing $n"; done
  [[ -z "$missing" && "$err" == *"all 3 replicas failed"* ]] && ok "failed loudly, naming each replica's DataLoss" \
    || fail "error does not name every replica:$missing"
fi
[[ -e "$work/out6" ]] && fail "an output file was left behind" || ok "no output file"
bad=0; for n in $all; do [[ "$(state_of "$c1" "$n")" == BAD* ]] && bad=$((bad + 1)); done
[[ "$bad" == 3 ]] && ok "all 3 replicas marked BAD" || fail "$bad of 3 replicas marked BAD"
if "$osctl" get "failover/six-$run" "$work/out6" 2>"$work/get6b.log"; then
  fail "the next get succeeded"
else
  grep -q "code = DataLoss.*no healthy replicas" "$work/get6b.log" \
    && ok "the next get fails at GetObject: DataLoss, no healthy replicas" || fail "the next get failed differently"
fi

echo
echo "restoring the flipped bytes, then checking every touched file:"
restore_flipped "$flipped"
check_touched "$flipped" || status=1

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
