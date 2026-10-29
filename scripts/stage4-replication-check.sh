#!/usr/bin/env bash
# Stage 4 acceptance: replication with 4 nodes, 3 replicas per chunk, commit
# at 2 verified replicas.
#
#   1. All 4 up: every chunk has 3 replica rows equal to its placed nodes, and
#      each replica file passes the header, chunk-ID and SHA-256 check on its
#      own node's volume.
#   2. One stopped: once the coordinator sees it DEAD (stage 6), it is never
#      placed; every chunk gets 3 replicas from the 3 live nodes, which is still
#      "placed nodes minus the stopped one".
#   3. One paused (hung), put at once, before the coordinator can notice
#      (SUSPECT takes 6 s of silence): the paused node is placed, and the upload
#      finishes within the write deadline plus normal time with nothing
#      recorded on it. Precondition checked: the paused node was placed.
#   4. Two stopped, put at once, before detection. Each outcome must match the
#      per-chunk rule computed from chunk_placements (success exactly when every
#      chunk has >= 2 live placed nodes), and the recorded replicas must match.
#      Placement width tells which side of detection an attempt landed on: 3
#      nodes before, exactly the 2 ALIVE ones after. 1-chunk objects are put
#      until a predicted success and a predicted failure are both seen before
#      detection; a 3-chunk object placed before detection must fail (rotation
#      always puts one of its chunks on both stopped nodes). If detection comes
#      first, both nodes are restarted, brought back ALIVE, and stopped again,
#      up to 3 cycles. After detection (both DEAD): a 3-chunk object succeeds
#      with exactly 2 replicas per chunk on the 2 ALIVE nodes.
#   5. Three stopped and DEAD: initiate fails with UNAVAILABLE. Nothing new
#      is created; the overwritten key still resolves to its previous version,
#      and a new key to nothing.
#   6. Regression smoke test: stop three nodes, bring everything back with
#      `up -d --wait`, wait for 4 ALIVE, then put at once. It must succeed.
#      Restarting can reassign the nodes' IPs; before the stage 4 fix, the
#      coordinator kept calling the old addresses and refused commits. The swap
#      doesn't happen every run, so this is a smoke test; the deterministic
#      proof is the coordinator tests for wrong-node responses and UNAVAILABLE
#      retry.
#
# Node volumes are read from a throwaway alpine container with --volumes-from,
# because the node image is distroless.
#
# Usage: scripts/stage4-replication-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
source "$root/scripts/lib/osctl.sh"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/liveness.sh"
# up --wait means the coordinator is serving; uploads need ALIVE nodes.
wait_alive 4
work="$(mktemp -d)"
run="$$"
deadline=10
export OSCTL_CHUNK_WRITE_TIMEOUT="${deadline}s"

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
latest_object() { sql "SELECT object_id FROM objects WHERE key = '$1' ORDER BY created_at DESC LIMIT 1"; }

# check_replicas OBJECT_ID DEAD: each chunk's recorded replicas must equal its
# placed nodes minus DEAD (a Postgres array literal such as '{node2}').
check_replicas() {
  local rows bad=0
  rows="$(sql "
    SELECT c.chunk_index,
           coalesce((SELECT string_agg(node_id, ',' ORDER BY node_id) FROM chunk_placements p
                     WHERE p.chunk_id = c.chunk_id AND p.node_id <> ALL('$2'::text[])), '-'),
           coalesce((SELECT string_agg(node_id, ',' ORDER BY node_id) FROM chunk_replicas r
                     WHERE r.chunk_id = c.chunk_id), '-')
    FROM chunks c WHERE c.object_id = '$1' ORDER BY 1")"
  printf '     %-6s %-20s %s\n' chunk expected recorded
  while IFS='|' read -r idx want got; do
    printf '     %-6s %-20s %s\n' "$idx" "$want" "$got"
    [[ "$want" == "$got" ]] || bad=1
  done <<<"$rows"
  ((bad == 0))
}

# verify_files OBJECT_ID: every recorded replica's file passes the header,
# chunk-ID and hash check on its own node's volume.
verify_files() {
  local node ids out bad=0
  for node in node1 node2 node3 node4; do
    ids="$(sql "SELECT r.chunk_id FROM chunk_replicas r JOIN chunks c USING (chunk_id)
                WHERE c.object_id = '$1' AND r.node_id = '$node'" | tr '\n' ' ')"
    [[ -n "${ids// /}" ]] || continue
    out="$(docker run --rm --volumes-from "$("${compose[@]}" ps -aq "$node")" alpine:3 sh -c '
      for id in "$@"; do
        f=/data/chunks/$id
        if [ ! -f "$f" ]; then echo "MISSING $id"; continue; fi
        magic=$(head -c 8 "$f")
        hid=$(dd if="$f" bs=1 skip=8 count=16 2>/dev/null | od -An -tx1 -v | tr -d " \n")
        hsum=$(dd if="$f" bs=1 skip=24 count=32 2>/dev/null | od -An -tx1 -v | tr -d " \n")
        dsum=$(tail -c +57 "$f" | sha256sum | cut -d" " -f1)
        if [ "$magic" = OSCHUNK1 ] && [ "$hid" = "$(echo "$id" | tr -d -)" ] && [ "$hsum" = "$dsum" ]; then
          echo "OK $id"
        else
          echo "BAD $id"
        fi
      done' sh $ids)"
    echo "     $node: $(grep -c '^OK' <<<"$out" || true) ok, $(grep -vc '^OK' <<<"$out" || true) bad"
    grep -v '^OK' <<<"$out" | sed 's/^/       /' || true
    grep -qv '^OK' <<<"$out" && bad=1
  done
  ((bad == 0))
}

mkfile_mib() { head -c $(($2 << 20)) /dev/urandom > "$work/$1"; }

echo "1. all 4 nodes up"
mkfile_mib v1 10
t0="$(now)"
osctl_run put "$work/v1" "replication/all-up-$run"
baseline="$(python3 -c "print(round($(now) - $t0, 2))")"
obj1="$(latest_object "replication/all-up-$run")"
echo "   put took ${baseline}s"
check_replicas "$obj1" '{}' && ok "every chunk has 3 replicas, equal to its placement" || fail "replicas differ from placement"
[[ "$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = '$obj1'")" == 9 ]] \
  && ok "9 replica rows for 3 chunks" || fail "expected 9 replica rows"
verify_files "$obj1" && ok "every replica file verifies on its own node's volume" || fail "a replica file did not verify"
osctl_run get "replication/all-up-$run" "$work/v1.out" && cmp -s "$work/v1" "$work/v1.out" \
  && ok "get IDENTICAL" || fail "get did not round-trip"

# placement_width OBJECT_ID: how many nodes chunk 0 was placed on (3 before
# the coordinator detects stopped nodes, the number of ALIVE nodes after).
placement_width() { sql "SELECT count(*) FROM chunk_placements p JOIN chunks c USING (chunk_id) WHERE c.object_id = '$1' AND c.chunk_index = 0"; }
placed_on() { sql "SELECT count(*) FROM chunk_placements p JOIN chunks c USING (chunk_id) WHERE c.object_id = '$1' AND p.node_id = '$2'"; }

echo
echo "2. node2 stopped and DEAD"
"${compose[@]}" stop node2 >/dev/null 2>&1
wait_state node2 DEAD 30 || fail "node2 never reached DEAD"
mkfile_mib v2 10
if "$osctl" put "$work/v2" "replication/one-stopped-$run" 2>"$work/put2.err"; then
  ok "upload succeeded"
  obj2="$(latest_object "replication/one-stopped-$run")"
  [[ "$(placed_on "$obj2" node2)" == 0 ]] && ok "nothing placed on the DEAD node" || fail "a chunk was placed on node2"
  check_replicas "$obj2" '{node2}' && ok "each chunk records its placed nodes minus node2" || fail "replicas wrong"
  [[ "$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = '$obj2'")" == 9 ]] \
    && ok "3 replicas per chunk from the 3 live nodes" || fail "expected 9 replica rows"
  osctl_run get "replication/one-stopped-$run" "$work/v2.out" && cmp -s "$work/v2" "$work/v2.out" \
    && ok "get IDENTICAL with node2 down" || fail "get failed with node2 down"
else
  fail "upload failed:"; grep -v ' level=INFO ' "$work/put2.err" | tail -5 | sed 's/^/    /'
fi
"${compose[@]}" up -d --wait node2 >/dev/null 2>&1
wait_alive 1 node2 || fail "node2 not ALIVE after restart"

echo
echo "3. node3 paused (hung), put before the coordinator notices"
"${compose[@]}" pause node3 >/dev/null
mkfile_mib v3 10
t0="$(now)"
if "$osctl" put "$work/v3" "replication/one-paused-$run" 2>"$work/put3.err"; then
  elapsed="$(python3 -c "print(round($(now) - $t0, 2))")"
  obj3="$(latest_object "replication/one-paused-$run")"
  if [[ "$(placed_on "$obj3" node3)" == 0 ]]; then
    fail "precondition: node3 was not placed (detected before initiate?); inconclusive, rerun"
  else
    ok "precondition: node3 was placed on $(placed_on "$obj3" node3) chunks"
  fi
  limit="$(python3 -c "print(round($deadline + $baseline + 5, 2))")"
  echo "   put took ${elapsed}s (limit ${limit}s = ${deadline}s deadline + ${baseline}s baseline + 5s)"
  python3 -c "import sys; sys.exit(0 if $elapsed < $limit else 1)" \
    && ok "finished within the deadline" || fail "took longer than the limit"
  check_replicas "$obj3" '{node3}' && ok "nothing recorded on node3" || fail "replicas wrong"
else
  fail "upload failed:"; grep -v ' level=INFO ' "$work/put3.err" | tail -5 | sed 's/^/    /'
  obj3="$(latest_object "replication/one-paused-$run")"
fi
"${compose[@]}" unpause node3 >/dev/null
# After unpause, node3 sees the cancelled streams and removes its temps.
ids3="$(sql "SELECT p.chunk_id FROM chunk_placements p JOIN chunks c USING (chunk_id)
             WHERE c.object_id = '$obj3' AND p.node_id = 'node3'" | tr '\n' ' ')"
leftover=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  leftover="$(docker run --rm --volumes-from "$("${compose[@]}" ps -q node3)" alpine:3 sh -c '
    ls /data/tmp
    for id in "$@"; do [ -e "/data/chunks/$id" ] && echo "final file $id"; done; true' sh $ids3)"
  [[ -z "$leftover" ]] && break
  sleep 1
done
if [[ -z "$leftover" ]]; then
  ok "after unpause, node3 has no final file or temp for its $(wc -w <<<"$ids3" | tr -d ' ') placed chunks"
else
  fail "node3 still has: $leftover"
fi
wait_alive 1 node3 || fail "node3 not ALIVE after unpause"

echo
echo "4. node3 and node4 stopped"

# two_stopped_put NAME MIB: puts a new object with node3 and node4 stopped.
# Sets $width (chunk 0's placement width: 3 before detection, 2 after). Before
# detection, predicts the outcome from the placements and checks the actual
# outcome and recorded replicas against it, setting $expected and $matched.
two_stopped_put() {
  local name="$1" mib="$2" key="replication/two-stopped-$1-$run" obj chunks min_live actual st
  mkfile_mib "$name" "$mib"
  if "$osctl" put "$work/$name" "$key" 2>"$work/$name.err"; then actual=success; else actual=failure; fi
  obj="$(latest_object "$key")"
  width="$(placement_width "$obj")"
  matched=no
  if [[ "$width" != 3 ]]; then
    echo "   $name: placed on $width nodes: the coordinator already excluded the stopped nodes"
    return
  fi
  chunks="$(sql "SELECT count(*) FROM chunks WHERE object_id = '$obj'")"
  # Fewest live placed nodes on any chunk of this object.
  min_live="$(sql "SELECT min(n) FROM (SELECT count(*) FILTER (WHERE p.node_id NOT IN ('node3', 'node4')) AS n
                   FROM chunks c JOIN chunk_placements p USING (chunk_id)
                   WHERE c.object_id = '$obj' GROUP BY c.chunk_id) t")"
  if ((min_live >= 2)); then expected=success; else expected=failure; fi
  echo "   $name: $chunks chunks, fewest live placed nodes on a chunk = $min_live -> expected $expected, actual $actual"
  if [[ "$actual" != "$expected" ]]; then
    fail "outcome does not follow the per-chunk rule"
    return
  fi
  if [[ "$actual" == success ]]; then
    if check_replicas "$obj" '{node3,node4}'; then
      ok "recorded exactly the live placed nodes"
      matched=yes
    else
      fail "recorded replicas differ from the live placed nodes"
    fi
  else
    echo "     $(grep -v ' level=INFO ' "$work/$name.err" | tail -1 | cut -c1-150)"
    st="$(sql "SELECT status FROM objects WHERE object_id = '$obj'")"
    if [[ "$st" == PENDING && "$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id)
                                      WHERE c.object_id = '$obj'")" == 0 && \
          "$(sql "SELECT count(*) FROM keys WHERE key = '$key'")" == 0 ]]; then
      ok "PENDING, no replicas recorded, key does not resolve"
      matched=yes
    else
      fail "a failed upload left state behind (status $st)"
    fi
  fi
}

seen_success=no
seen_failure=no
three_failed=no
attempt=0
for cycle in 1 2 3; do
  echo "   cycle $cycle: stopping node3 and node4"
  "${compose[@]}" stop node3 node4 >/dev/null 2>&1
  width=3 # a fresh cycle starts before detection; each put measures it again
  if [[ "$three_failed" == no ]]; then
    two_stopped_put "three-chunk-c$cycle" 10
    if [[ "$width" == 3 && "$matched" == yes ]]; then
      if [[ "$expected" == failure ]]; then
        three_failed=yes
        ok "the 3-chunk object was predicted to fail, as rotation placement requires"
      else
        fail "rotation placement should make a 3-chunk object fail with 2 nodes stopped"
      fi
    fi
  fi
  while [[ "$width" == 3 ]] && ! [[ "$seen_success" == yes && "$seen_failure" == yes ]] && ((attempt < 12)); do
    attempt=$((attempt + 1))
    two_stopped_put "one-chunk-$attempt" 1
    if [[ "$matched" == yes ]]; then
      [[ "$expected" == success ]] && seen_success=yes
      [[ "$expected" == failure ]] && seen_failure=yes
    fi
  done
  [[ "$seen_success" == yes && "$seen_failure" == yes && "$three_failed" == yes ]] && break
  ((cycle < 3)) || break
  echo "   detection came first: restarting node3 and node4 for another cycle"
  "${compose[@]}" up -d --wait node3 node4 >/dev/null 2>&1
  wait_alive 1 node3 node4 || fail "node3/node4 not ALIVE"
  attempt=0
done
echo "   before detection: success seen and matched: $seen_success; failure seen and matched: $seen_failure; 3-chunk failed: $three_failed"
if [[ "$seen_success" == yes && "$seen_failure" == yes && "$three_failed" == yes ]]; then
  ok "both branches of the per-chunk rule exercised before detection"
else
  fail "a branch of the per-chunk rule was not exercised before detection"
fi

wait_state node3 DEAD 30 || fail "node3 never reached DEAD"
wait_state node4 DEAD 30 || fail "node4 never reached DEAD"
mkfile_mib after-detection 10
if osctl_run put "$work/after-detection" "replication/two-dead-$run"; then
  obj4="$(latest_object "replication/two-dead-$run")"
  bad="$(sql "SELECT count(*) FROM (SELECT c.chunk_id FROM chunks c JOIN chunk_placements p USING (chunk_id)
              WHERE c.object_id = '$obj4' GROUP BY c.chunk_id
              HAVING string_agg(p.node_id, ',' ORDER BY p.node_id) <> 'node1,node2') t")"
  [[ "$bad" == 0 ]] && check_replicas "$obj4" '{}' \
    && ok "after detection: succeeded, every chunk on exactly node1 and node2" \
    || fail "after detection: placement or replicas not exactly node1,node2"
else
  fail "after detection: the upload failed"
fi
"${compose[@]}" up -d --wait node3 node4 >/dev/null 2>&1
wait_alive 1 node3 node4 || fail "node3/node4 not ALIVE"

echo
echo "5. node1, node2 and node3 stopped and DEAD"
"${compose[@]}" stop node1 node2 node3 >/dev/null 2>&1
for n in node1 node2 node3; do wait_state "$n" DEAD 30 || fail "$n never reached DEAD"; done
mkfile_mib v5 10
if "$osctl" put "$work/v5" "replication/all-up-$run" 2>"$work/put5.err"; then
  fail "overwrite succeeded with only one node up"
elif grep -q "code = Unavailable.*1 of 4 storage nodes alive, need 2" "$work/put5.err"; then
  ok "overwrite refused at initiate: UNAVAILABLE, 1 of 4 storage nodes alive"
else
  fail "overwrite failed, but not with UNAVAILABLE: $(grep -v ' level=INFO ' "$work/put5.err" | tail -1)"
fi
[[ "$(sql "SELECT object_id FROM keys WHERE key = 'replication/all-up-$run'")" == "$obj1" ]] \
  && ok "key still resolves to its previous version (step 1's object)" || fail "key pointer moved"
[[ "$(sql "SELECT count(*) FROM objects WHERE key = 'replication/all-up-$run'")" == 1 ]] \
  && ok "no new object was created" || fail "a new object row exists"
if "$osctl" put "$work/v5" "replication/fresh-$run" 2>"$work/fresh-put.err"; then
  fail "a new key committed with only one node up"
fi
if "$osctl" get "replication/fresh-$run" "$work/fresh.out" 2>"$work/fresh.err"; then
  fail "a new key resolved"
elif grep -q NotFound "$work/fresh.err"; then
  ok "a new key resolves to nothing (NotFound)"
else
  fail "get of the new key failed, but not with NotFound"
fi

echo
echo "6. stop three nodes, up --wait, wait for ALIVE, put at once (IP-change regression)"
ips() {
  local n
  for n in node1 node2 node3 node4; do
    printf '%s=%s ' "$n" "$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
      "$("${compose[@]}" ps -aq "$n")")"
  done
}
"${compose[@]}" up -d --wait >/dev/null 2>&1
wait_alive 4 || fail "not all nodes ALIVE before the restart"
before="$(ips)"
"${compose[@]}" stop node1 node2 node3 >/dev/null 2>&1
"${compose[@]}" up -d --wait >/dev/null 2>&1
wait_alive 4 || fail "not all nodes ALIVE after the restart"
after="$(ips)"
echo "   IPs before: $before"
echo "   IPs after:  $after"
[[ "$before" == "$after" ]] && echo "   (no IP change this run: the smoke test cannot hit the bug)" \
  || echo "   (IPs changed: the coordinator must re-resolve)"
mkfile_mib v6 10
if osctl_run put "$work/v6" "replication/after-restart-$run"; then
  ok "put right after wait_alive succeeded"
else
  fail "put right after wait_alive failed"
fi

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
