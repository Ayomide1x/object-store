#!/usr/bin/env bash
# Stage 6 acceptance: heartbeats and liveness.
#
#   1. All 4 ALIVE. Put object X (5 chunks, so some chunk's lowest-ID replica
#      is node2). Time a listed-order get of X as the baseline.
#   2. Stop node2: SUSPECT, then DEAD within the window.
#   3. A new upload places nothing on node2; every chunk gets 3 replicas from
#      the 3 live nodes.
#   4. A get of X (node2 first in listed order for some chunk) takes about the
#      baseline, not a read deadline: node2 is listed DEAD, so it is read last.
#   5. Start node2: ALIVE within one interval of its first beats.
#   6. Pause node3: SUSPECT, then DEAD. Unpause: ALIVE.
#   7. Restart the coordinator: no node is ever DEAD over a full DEAD window,
#      all 4 are ALIVE within about one interval, and a put works right away.
#   8. node3 and node4 DEAD: a put succeeds with every chunk on node1 and
#      node2. node2 DEAD too: initiate fails with UNAVAILABLE.
#
# Thresholds are the Compose defaults: heartbeat every 2 s, SUSPECT after 6 s,
# DEAD after 15 s, 2 recovery beats, sweep every 1 s.
#
# Usage: scripts/stage6-liveness-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/liveness.sh"
interval=2 suspect=6 dead=15
margin=3 # sweep (1 s) + persistence + polling
export OSCTL_READ_ORDER=listed

work="$(mktemp -d)"
run="$$"
restore() {
  "${compose[@]}" unpause node1 node2 node3 node4 >/dev/null 2>&1 || true
  "${compose[@]}" up -d --wait >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap restore EXIT

status=0
fail() { echo "  FAIL: $*"; status=1; }
ok() { echo "  ok: $*"; }
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -F'|' -c "$1"; }
now() { python3 -c 'import time; print(time.time())'; }
since() { python3 -c "print(round($(now) - $1, 2))"; }
lt() { python3 -c "import sys; sys.exit(0 if $1 < $2 else 1)"; }
mkfile_mib() { head -c $(($2 << 20)) /dev/urandom > "$work/$1"; }
latest_object() { sql "SELECT object_id FROM objects WHERE key = '$1' ORDER BY created_at DESC LIMIT 1"; }

echo "1. all 4 ALIVE; object X and a baseline get"
wait_alive 4
mkfile_mib x 20
osctl_run put "$work/x" "liveness/x-$run"
objx="$(latest_object "liveness/x-$run")"
cx="$(sql "SELECT c.chunk_id FROM chunks c JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.origin = 'upload' WHERE c.object_id = '$objx'
           GROUP BY c.chunk_id HAVING min(r.node_id) = 'node2' LIMIT 1")"
[[ -n "$cx" ]] && echo "   chunk $cx has node2 as its first listed replica" || fail "no chunk of X lists node2 first"
t0="$(now)"; osctl_run get "liveness/x-$run" "$work/x.out"; baseline="$(since "$t0")"
echo "   baseline get ${baseline}s"

echo
echo "2. stop node2"
"${compose[@]}" stop node2 >/dev/null 2>&1
stopped="$(now)"
wait_state node2 SUSPECT $((suspect + interval + margin)) && ok "node2 SUSPECT" || fail "node2 never SUSPECT"
wait_state node2 DEAD $((dead + interval + margin)) && ok "node2 DEAD $(since "$stopped")s after the stop (window ${dead}s + one interval)" \
  || fail "node2 not DEAD within the window"

echo
echo "3. a new upload with node2 DEAD"
mkfile_mib y 10
osctl_run put "$work/y" "liveness/y-$run"
objy="$(latest_object "liveness/y-$run")"
[[ "$(sql "SELECT count(*) FROM chunk_placements p JOIN chunks c USING (chunk_id) WHERE c.object_id = '$objy' AND p.node_id = 'node2' AND p.origin = 'initiate'")" == 0 ]] \
  && ok "nothing placed on node2" || fail "a chunk was placed on node2"
bad="$(sql "SELECT count(*) FROM (SELECT c.chunk_id FROM chunks c JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.origin = 'upload'
            WHERE c.object_id = '$objy' GROUP BY c.chunk_id
            HAVING string_agg(r.node_id, ',' ORDER BY r.node_id) <> 'node1,node3,node4') t")"
[[ "$bad" == 0 ]] && ok "every chunk has 3 replicas: node1, node3, node4" || fail "$bad chunks without exactly node1,node3,node4"

echo
echo "4. get X, whose chunk $cx lists node2 first"
t0="$(now)"
if "$osctl" get "liveness/x-$run" "$work/x.out" 2>"$work/getx.log" && cmp -s "$work/x" "$work/x.out"; then
  elapsed="$(since "$t0")"
  echo "   get took ${elapsed}s (baseline ${baseline}s; a read deadline would be 10 s)"
  lt "$elapsed" "$(python3 -c "print($baseline + 2)")" && ok "about baseline time, not a failover deadline" \
    || fail "took too long for a DEAD replica listed last"
  grep -q "failing over.*node_id=node2\|node_id=node2.*failing over" "$work/getx.log" \
    && fail "the get tried node2 and failed over" || ok "node2 was not tried: listed DEAD, read last"
else
  fail "get of X failed:"; grep -v ' level=INFO ' "$work/getx.log" | tail -3 | sed 's/^/    /'
fi

echo
echo "5. start node2"
"${compose[@]}" up -d --wait node2 >/dev/null 2>&1
wait_state node2 ALIVE $((2 * interval + margin)) && ok "node2 ALIVE (two recovery beats, one interval apart)" \
  || fail "node2 not ALIVE within one interval of starting"

echo
echo "6. pause node3"
"${compose[@]}" pause node3 >/dev/null
wait_state node3 SUSPECT $((suspect + interval + margin)) && ok "node3 SUSPECT" || fail "node3 never SUSPECT"
wait_state node3 DEAD $((dead + interval + margin)) && ok "node3 DEAD" || fail "node3 never DEAD"
"${compose[@]}" unpause node3 >/dev/null
wait_state node3 ALIVE $((2 * interval + margin)) && ok "node3 ALIVE after unpause" || fail "node3 not ALIVE after unpause"

echo
echo "7. restart the coordinator"
wait_alive 4
"${compose[@]}" restart coordinator >/dev/null 2>&1
"${compose[@]}" up -d --wait coordinator >/dev/null 2>&1
healthy="$(now)"
all_alive=""
saw_dead=""
put_ok=""
# Sample every 0.5 s for a full DEAD window after the coordinator is serving.
while lt "$(since "$healthy")" "$((dead + margin))"; do
  states="$(liveness_states)"
  [[ "$states" == *"=DEAD"* ]] && saw_dead="$saw_dead [$(since "$healthy")s: $states]"
  if [[ -z "$all_alive" && "$(grep -o '=ALIVE' <<<"$states" | wc -l | tr -d ' ')" == 4 ]]; then
    all_alive="$(since "$healthy")"
    mkfile_mib z 10
    "$osctl" put "$work/z" "liveness/after-coordinator-restart-$run" 2>"$work/z.err" && put_ok=yes || put_ok=no
  fi
  sleep 0.5
done
[[ -z "$saw_dead" ]] && ok "no node was DEAD over the ${dead}s window after the restart" || fail "false DEAD:$saw_dead"
if [[ -n "$all_alive" ]]; then
  echo "   all 4 ALIVE ${all_alive}s after the coordinator was serving"
  lt "$all_alive" "$((interval + margin))" && ok "within one interval" || fail "slower than one interval"
else
  fail "not all nodes ALIVE after the coordinator restart"
fi
[[ "$put_ok" == yes ]] && ok "a put right then succeeded" \
  || fail "the put after the restart failed: $(grep -v ' level=INFO ' "$work/z.err" 2>/dev/null | tail -1)"

echo
echo "8. node3 and node4 DEAD, then node2 too"
"${compose[@]}" stop node3 node4 >/dev/null 2>&1
wait_state node3 DEAD $((dead + interval + margin)) || fail "node3 never DEAD"
wait_state node4 DEAD $((dead + interval + margin)) || fail "node4 never DEAD"
mkfile_mib w 10
if osctl_run put "$work/w" "liveness/two-alive-$run"; then
  objw="$(latest_object "liveness/two-alive-$run")"
  bad="$(sql "SELECT count(*) FROM (SELECT c.chunk_id FROM chunks c JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.origin = 'upload'
              WHERE c.object_id = '$objw' GROUP BY c.chunk_id
              HAVING string_agg(r.node_id, ',' ORDER BY r.node_id) <> 'node1,node2') t")"
  [[ "$bad" == 0 ]] && ok "with 2 ALIVE: every chunk on exactly node1 and node2" || fail "$bad chunks not on exactly node1,node2"
  osctl_run get "liveness/two-alive-$run" "$work/w.out" && cmp -s "$work/w" "$work/w.out" && ok "get IDENTICAL" \
    || fail "get did not round-trip"
else
  fail "the put with 2 ALIVE nodes failed"
fi
"${compose[@]}" stop node2 >/dev/null 2>&1
wait_state node2 DEAD $((dead + interval + margin)) || fail "node2 never DEAD"
if "$osctl" put "$work/w" "liveness/one-alive-$run" 2>"$work/w.err"; then
  fail "a put succeeded with 1 ALIVE node"
elif grep -q "code = Unavailable.*1 of 4 storage nodes alive, need 2" "$work/w.err"; then
  ok "with 1 ALIVE: UNAVAILABLE, 1 of 4 storage nodes alive, need 2"
else
  fail "failed, but not with UNAVAILABLE: $(grep -v ' level=INFO ' "$work/w.err" | tail -1)"
fi
"${compose[@]}" up -d --wait >/dev/null 2>&1
wait_alive 4 || fail "not all nodes ALIVE at the end"

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
