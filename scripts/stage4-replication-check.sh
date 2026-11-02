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
#      After unpause, node3 must hold no temp file, and no final file for its
#      placed chunks, with one exception since stage 7: a chunk that committed
#      with 2 replicas is topped up by the reconciler, which may pick node3
#      as soon as it is back. Such a file is allowed only if Postgres has a
#      HEALTHY row for it on node3 with origin repair or repair-found, and
#      the file passes the header, chunk-ID and SHA-256 check.
#   4. Two stopped, put at once, before detection. Each outcome must match the
#      per-chunk rule computed from chunk_placements (success exactly when every
#      chunk has >= 2 live placed nodes), and the recorded replicas must match.
#      Placement width tells which side of detection an attempt landed on: 3
#      nodes before, exactly the 2 ALIVE ones after. 1-chunk objects are put
#      until a predicted success and a predicted failure are both seen before
#      detection; a 3-chunk object placed before detection must fail (rotation
#      always puts one of its chunks on both stopped nodes). Each 1-chunk put
#      is a fair coin (see MAX_DRAWS below). If detection comes first, both
#      nodes are restarted, brought back ALIVE, and stopped again, until both
#      outcomes are seen or 12 draws are made (at most 10 cycles); running out
#      is INCONCLUSIVE, not FAIL (a 1 in 2,048 chance). After detection (both
#      DEAD): a 3-chunk object succeeds with exactly 2 replicas per chunk on
#      the 2 ALIVE nodes.
#   5. Three stopped and DEAD: initiate fails with UNAVAILABLE. Nothing new
#      is created; the overwritten key still resolves to its previous version,
#      and a new key to nothing.
#   6. Node addresses change.
#      a. Smoke test: stop three nodes, bring everything back with
#         `up -d --wait`, wait for 4 ALIVE, then put at once. It must succeed.
#         Restarting can reassign the nodes' IPs (not every run).
#      b. A forced swap (scripts/lib/ipswap.sh): node1 and node3 are stopped,
#         given each other's IP addresses, and started. Then every node must
#         be reachable from the coordinator for every kind of call:
#         - an upload records replicas on every placed node (StatChunks);
#         - for each node, a chunk that node lacks is made under-replicated
#           (one replica corrupted and read, so it is marked BAD) and repaired
#           onto it (VerifyChunk, PushChunk, StatChunks);
#         - those objects are removed, and garbage collection deletes their
#           files on every node (DeleteChunks; a row goes only when the node
#           confirms).
#      The coordinator once kept a channel stuck after an address change: not
#      ready, never failing, every call timing out. The deterministic proof of
#      the fix is NodeAddressChangeTest; this step checks the whole path.
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
source "$root/scripts/lib/volume.sh" # check_chunk_files, corrupt_byte
source "$root/scripts/lib/ipswap.sh"
# up --wait means the coordinator is serving; uploads need ALIVE nodes.
wait_alive 4
work="$(mktemp -d)"
run="$$"
flipped="$work/flipped"
: > "$flipped"
# Compose demo values: a repair pass and a GC pass every 10 s, read grace 60 s.
interval=10 read_grace=60 margin=20
deadline=10
export OSCTL_CHUNK_WRITE_TIMEOUT="${deadline}s"

restore() {
  "${compose[@]}" unpause node1 node2 node3 node4 >/dev/null 2>&1 || true
  if [[ "${swapped:-}" == yes ]]; then unpin_ips node1 node3 >/dev/null 2>&1 || true; fi
  "${compose[@]}" up -d --wait >/dev/null 2>&1 || true
  restore_flipped "$flipped" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap restore EXIT

status=0
fail() { echo "  FAIL: $*"; status=1; }
source "$root/scripts/lib/outcome.sh"
ok() { echo "  ok: $*"; }
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -F'|' -c "$1"; }
now() { python3 -c 'import time; print(time.time())'; }
# wait_until TIMEOUT WANT QUERY: polls QUERY until it returns WANT.
wait_until() {
  local timeout="$1" want="$2" q="$3" start=$SECONDS
  until [[ "$(sql "$q")" == "$want" ]]; do
    ((SECONDS - start >= timeout)) && return 1
    sleep 1
  done
}
latest_object() { sql "SELECT object_id FROM objects WHERE key = '$1' ORDER BY created_at DESC LIMIT 1"; }

# check_replicas OBJECT_ID DEAD: each chunk's replicas recorded at upload must
# equal its placed nodes minus DEAD (a Postgres array literal such as '{node2}').
# Since stage 7 the reconciler can add rows in the background (origin 'repair');
# they are additions, never removals, so only origin = 'upload' rows are compared.
check_replicas() {
  local rows bad=0
  rows="$(sql "
    SELECT c.chunk_index,
           coalesce((SELECT string_agg(node_id, ',' ORDER BY node_id) FROM chunk_placements p
                     WHERE p.chunk_id = c.chunk_id AND p.origin = 'initiate' AND p.node_id <> ALL('$2'::text[])), '-'),
           coalesce((SELECT string_agg(node_id, ',' ORDER BY node_id) FROM chunk_replicas r
                     WHERE r.chunk_id = c.chunk_id AND r.origin = 'upload'), '-')
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
[[ "$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = '$obj1' AND r.origin = 'upload'")" == 9 ]] \
  && ok "9 replica rows for 3 chunks" || fail "expected 9 replica rows"
verify_files "$obj1" && ok "every replica file verifies on its own node's volume" || fail "a replica file did not verify"
osctl_run get "replication/all-up-$run" "$work/v1.out" && cmp -s "$work/v1" "$work/v1.out" \
  && ok "get IDENTICAL" || fail "get did not round-trip"

# placement_width OBJECT_ID: how many nodes chunk 0 was placed on (3 before
# the coordinator detects stopped nodes, the number of ALIVE nodes after).
# Since stage 8, repair records its copy targets as placements too (origin 'repair').
placement_width() { sql "SELECT count(*) FROM chunk_placements p JOIN chunks c USING (chunk_id) WHERE c.object_id = '$1' AND c.chunk_index = 0 AND p.origin = 'initiate'"; }
placed_on() { sql "SELECT count(*) FROM chunk_placements p JOIN chunks c USING (chunk_id) WHERE c.object_id = '$1' AND p.node_id = '$2' AND p.origin = 'initiate'"; }

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
  [[ "$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = '$obj2' AND r.origin = 'upload'")" == 9 ]] \
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
    inconclusive "precondition: node3 was not placed (detected before initiate?)"
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
# After unpause, node3 sees the cancelled streams and removes its temps. A
# final file for a placed chunk is allowed only as a recorded, verified repair
# copy (see the header); anything else left on node3 is a FAIL.
ids3="$(sql "SELECT p.chunk_id FROM chunk_placements p JOIN chunks c USING (chunk_id)
             WHERE c.object_id = '$obj3' AND p.node_id = 'node3' AND p.origin = 'initiate'" | tr '\n' ' ')"
# leftover_on_node3: prints each temp file and each final file that isn't an
# allowed repair copy, one per line; prints the allowed copies to $work/repaired3.
leftover_on_node3() {
  local temps finals id
  : > "$work/repaired3"
  temps="$(docker run --rm --volumes-from "$("${compose[@]}" ps -q node3)" alpine:3 ls /data/tmp)"
  [[ -n "$temps" ]] && sed 's/^/temp file /' <<<"$temps"
  finals="$(docker run --rm --volumes-from "$("${compose[@]}" ps -q node3)" alpine:3 sh -c '
    for id in "$@"; do [ -e "/data/chunks/$id" ] && echo "$id"; done; true' sh $ids3)"
  for id in $finals; do
    if [[ "$(sql "SELECT count(*) FROM chunk_replicas WHERE chunk_id = '$id' AND node_id = 'node3'
                  AND state = 'HEALTHY' AND origin IN ('repair', 'repair-found')")" == 1 && \
          "$(check_chunk_files node3 "$id")" == "OK $id" ]]; then
      echo "$id" >> "$work/repaired3"
    else
      echo "final file $id (no HEALTHY repair row on node3, or it fails verification)"
    fi
  done
  return 0
}
leftover=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  leftover="$(leftover_on_node3)"
  [[ -z "$leftover" ]] && break
  sleep 1
done
if [[ -z "$leftover" ]]; then
  repaired="$(grep -c . "$work/repaired3" || true)"
  ok "after unpause, node3 has no temp file and no unrecorded final file for its $(wc -w <<<"$ids3" | tr -d ' ') placed chunks$( ((repaired > 0)) && echo " ($repaired verified repair copies, recorded as such)")"
else
  fail "node3 still has: $(tr '\n' ';' <<<"$leftover")"
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
                   WHERE c.object_id = '$obj' AND p.origin = 'initiate' GROUP BY c.chunk_id) t")"
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

# Each 1-chunk put before detection is an independent fair coin. Placement
# starts at a random one of the 4 nodes (sorted) and takes 3 in a row, so the
# start decides which node is left out: starting at node1 or node4 leaves out
# node4 or node3, placing both live nodes (success); starting at node2 or
# node3 places only one (failure). P = 1/2 each, independent per put.
#
# The step misses a branch only if every draw lands on the same side:
# P(miss) = 2 x (1/2)^N = 2^(1-N) after N draws. A cycle's window (stop to
# detection, ~4-6 s) fits about 2 draws, so the old limit of 3 cycles gave
# N of about 6: 2^-5 = 1 in 32, which is what failed. Now draws accumulate
# across cycles until both outcomes are seen or MAX_DRAWS are made:
# 2^(1-12) = 2^-11, about 1 in 2,048. MAX_CYCLES only bounds a window that
# fits fewer draws than usual; most runs finish within 1 or 2 cycles
# (expected draws until both are seen: 3).
MAX_DRAWS=12
MAX_CYCLES=10
seen_success=no
seen_failure=no
three_failed=no
draws=0
puts=0
both_seen() { [[ "$seen_success" == yes && "$seen_failure" == yes ]]; }
for ((cycle = 1; cycle <= MAX_CYCLES; cycle++)); do
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
  while [[ "$width" == 3 ]] && ! both_seen && ((draws < MAX_DRAWS)); do
    puts=$((puts + 1))
    # Each put needs its own key, or a failed put's key would still resolve
    # to an earlier success.
    two_stopped_put "one-chunk-$puts" 1
    if [[ "$width" == 3 && "$matched" == yes ]]; then
      draws=$((draws + 1))
      [[ "$expected" == success ]] && seen_success=yes
      [[ "$expected" == failure ]] && seen_failure=yes
    fi
  done
  if both_seen && [[ "$three_failed" == yes ]]; then break; fi
  if ((draws >= MAX_DRAWS)) && [[ "$three_failed" == yes ]]; then break; fi
  ((cycle < MAX_CYCLES)) || break
  echo "   detection came first ($draws of at most $MAX_DRAWS draws so far): restarting node3 and node4"
  "${compose[@]}" up -d --wait node3 node4 >/dev/null 2>&1
  wait_alive 1 node3 node4 || fail "node3/node4 not ALIVE"
done
echo "   before detection: $draws draws; success seen and matched: $seen_success; failure seen and matched: $seen_failure; 3-chunk failed: $three_failed"
if both_seen && [[ "$three_failed" == yes ]]; then
  ok "both branches of the per-chunk rule exercised before detection"
else
  # Every draw that was made followed the rule (a mismatch is a FAIL above);
  # the coin just never produced one of the cases.
  missing=""
  [[ "$seen_success" == yes ]] || missing+=" a predicted success,"
  [[ "$seen_failure" == yes ]] || missing+=" a predicted failure,"
  [[ "$three_failed" == yes ]] || missing+=" a 3-chunk object placed before detection,"
  inconclusive "not exercised before detection:${missing%,} ($draws draws in $cycle cycles; with 12 draws this happens 1 time in 2,048)"
fi

wait_state node3 DEAD 30 || fail "node3 never reached DEAD"
wait_state node4 DEAD 30 || fail "node4 never reached DEAD"
mkfile_mib after-detection 10
if osctl_run put "$work/after-detection" "replication/two-dead-$run"; then
  obj4="$(latest_object "replication/two-dead-$run")"
  bad="$(sql "SELECT count(*) FROM (SELECT c.chunk_id FROM chunks c JOIN chunk_placements p USING (chunk_id)
              WHERE c.object_id = '$obj4' AND p.origin = 'initiate' GROUP BY c.chunk_id
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
echo "6a. stop three nodes, up --wait, wait for ALIVE, put at once (restart smoke test)"
"${compose[@]}" up -d --wait >/dev/null 2>&1
wait_alive 4 || fail "not all nodes ALIVE before the restart"
"${compose[@]}" stop node1 node2 node3 >/dev/null 2>&1
"${compose[@]}" up -d --wait >/dev/null 2>&1
wait_alive 4 || fail "not all nodes ALIVE after the restart"
mkfile_mib v6 10
if osctl_run put "$work/v6" "replication/after-restart-$run"; then
  ok "put right after wait_alive succeeded"
else
  fail "put right after wait_alive failed"
fi

echo
echo "6b. node1 and node3 swap IP addresses; every node stays reachable for every call"
swapped=yes
echo "   swap: $(swap_ips node1 node3)"
wait_alive 4 || fail "not all nodes ALIVE after the swap"
[[ "$(node_ip node1)" != "$(node_ip node3)" ]] || fail "the swap left two nodes on one address"

# Uploads: StatChunks to every placed node.
mkfile_mib v6b 10
if osctl_run put "$work/v6b" "replication/after-swap-$run"; then
  obj6="$(latest_object "replication/after-swap-$run")"
  check_replicas "$obj6" '{}' && ok "after the swap, every chunk recorded on every placed node" \
    || fail "after the swap, a placed node's replica was not recorded"
else
  fail "the upload after the swap failed"
fi

# Repair onto each node in turn: a 1-chunk object leaves one node out (a
# random one of 4); put until one leaves out the node, then corrupt its
# first-listed replica and read it, so it is marked BAD and the reconciler
# copies the chunk to the only node without a row: that node.
mkfile_mib one6 1
repaired_keys=()
for n in node1 node2 node3 node4; do
  key="" cid=""
  for try in $(seq 1 40); do
    k="replication/swap-repair-$n-$try-$run"
    osctl_run put "$work/one6" "$k" >/dev/null || continue
    c="$(sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$k'")"
    if [[ "$(sql "SELECT count(*) FROM chunk_replicas WHERE chunk_id = '$c' AND node_id = '$n'")" == 0 && \
          "$(sql "SELECT count(*) FROM chunk_replicas WHERE chunk_id = '$c' AND state = 'HEALTHY'")" == 3 ]]; then
      key="$k" cid="$c"
      break
    fi
    repaired_keys+=("$k") # not this node's object: removed with the others below
  done
  if [[ -z "$key" ]]; then
    # A random start leaves out each node 1 time in 4: 40 tries miss with
    # probability (3/4)^40, under 1 in 99,000.
    inconclusive "no 1-chunk object without a replica on $n in 40 puts"
    continue
  fi
  repaired_keys+=("$key")
  victim="$(sql "SELECT min(node_id) FROM chunk_replicas WHERE chunk_id = '$cid'")"
  corrupt_byte "$victim" "$cid" $((56 + 1000)) "$flipped" >/dev/null
  OSCTL_READ_ORDER=listed "$osctl" get "$key" "$work/one6.out" 2>/dev/null || true
  if wait_until $((2 * interval + margin)) "HEALTHY repair" \
       "SELECT state || ' ' || origin FROM chunk_replicas WHERE chunk_id = '$cid' AND node_id = '$n'"; then
    [[ "$(check_chunk_files "$n" "$cid")" == "OK $cid" ]] && ok "$n: a repair copy landed and verifies" \
      || fail "$n: the repair copy fails verification"
  else
    fail "$n: no repair copy landed ($victim's replica: $(sql "SELECT state FROM chunk_replicas WHERE chunk_id = '$cid' AND node_id = '$victim'"))"
  fi
done

# GC on every node: remove the objects; after the read grace their files are
# deleted, and a chunk's rows go only when every node holding it confirms.
keys_sql="$(printf "'%s'," "replication/after-swap-$run" "${repaired_keys[@]}")"
keys_sql="${keys_sql%,}"
sql "SELECT r.node_id || ' ' || r.chunk_id FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
     WHERE k.key IN ($keys_sql)" | tr '|' ' ' > "$work/gc6"
chunks_sql="$(awk '{printf "%s'\''%s'\''", (NR > 1 ? "," : ""), $2}' "$work/gc6")"
holders="$(awk '{print $1}' "$work/gc6" | sort -u | paste -sd, -)"
echo "   removing $(wc -l < "$work/gc6" | tr -d ' ') replica files on: $holders"
[[ "$holders" == "node1,node2,node3,node4" ]] || fail "the removed objects' files are not on all four nodes: $holders"
for k in "replication/after-swap-$run" "${repaired_keys[@]}"; do
  "$osctl" rm "$k" >/dev/null 2>&1 || true
done
echo "   waiting for the read grace (${read_grace}s) and garbage collection"
if wait_until $((read_grace + 3 * interval + margin)) 0 "SELECT count(*) FROM chunks WHERE chunk_id IN ($chunks_sql)"; then
  left=""
  for n in node1 node2 node3 node4; do
    ids="$(awk -v n="$n" '$1 == n {print $2}' "$work/gc6")"
    [[ -z "$ids" ]] && continue
    # grep finds nothing when every file is gone: not an error.
    left+="$(check_chunk_files "$n" $ids | { grep -v '^MISSING ' || true; } | sed "s/^/$n: /")"
  done
  [[ -z "$left" ]] && ok "garbage collection deleted the files on all four nodes, each confirmed by its node" \
    || fail "files left after GC: $left"
else
  fail "chunk rows still present: deletions on $(sql "SELECT coalesce(string_agg(DISTINCT node_id, ','), 'no node') FROM pending_deletions WHERE chunk_id IN ($chunks_sql)") not confirmed"
fi

# Give node1 and node3 dynamic addresses again, or a later restart can find
# its fixed address taken.
unpin_ips node1 node3
swapped=no
wait_alive 4 || fail "not all nodes ALIVE after unpinning their addresses"

finish
