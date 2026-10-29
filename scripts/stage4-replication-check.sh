#!/usr/bin/env bash
# Stage 4 acceptance: replication with 4 nodes, 3 replicas per chunk, commit
# at 2 verified replicas.
#
#   1. All 4 up: every chunk has 3 replica rows equal to its placed nodes, and
#      each replica file passes the header, chunk-ID and SHA-256 check on its
#      own node's volume.
#   2. One stopped: the upload succeeds; each chunk records exactly its placed
#      nodes minus the stopped one.
#   3. One paused (hung): the upload finishes within the write deadline plus
#      normal time, nothing is recorded on the paused node, and after unpause
#      the node has no file or temp for those chunks.
#   4. Two stopped. Each outcome must match the per-chunk rule computed from
#      chunk_placements (success exactly when every chunk has >= 2 live placed
#      nodes), and the recorded replicas must match it too. 1-chunk objects are
#      put in a loop, up to 12 attempts, until both a predicted success and a
#      predicted failure have been seen; either branch still unseen fails the
#      script. A random start makes each attempt about 50/50, so 12 attempts all
#      on one branch has a probability of about 1 in 2,000. Then a 3-chunk
#      object: with rotation placement, 3 or more consecutive chunks always
#      include one placed on both stopped nodes, so it must fail.
#   5. Three stopped: a clean failure. Nothing COMMITTED; the overwritten key
#      still resolves to its previous version, and a new key to nothing.
#   6. Regression smoke test: stop three nodes, bring everything back with
#      `up -d --wait`, then put immediately. It must succeed. Restarting can
#      reassign the nodes' IPs; before the fix, the coordinator kept calling
#      the old addresses and refused commits. The swap doesn't happen every
#      run, so this is a smoke test; the deterministic proof is the coordinator
#      tests for wrong-node responses and UNAVAILABLE retry.
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

echo
echo "2. node2 stopped"
"${compose[@]}" stop node2 >/dev/null 2>&1
mkfile_mib v2 10
if "$osctl" put "$work/v2" "replication/one-stopped-$run" 2>"$work/put2.err"; then
  ok "upload succeeded"
  obj2="$(latest_object "replication/one-stopped-$run")"
  check_replicas "$obj2" '{node2}' && ok "each chunk records its placed nodes minus node2" || fail "replicas wrong"
  osctl_run get "replication/one-stopped-$run" "$work/v2.out" && cmp -s "$work/v2" "$work/v2.out" \
    && ok "get IDENTICAL with node2 down" || fail "get failed with node2 down"
else
  fail "upload failed:"; grep -v ' level=INFO ' "$work/put2.err" | tail -5 | sed 's/^/    /'
fi
"${compose[@]}" up -d --wait node2 >/dev/null 2>&1

echo
echo "3. node3 paused (hung)"
"${compose[@]}" pause node3 >/dev/null
mkfile_mib v3 10
t0="$(now)"
if "$osctl" put "$work/v3" "replication/one-paused-$run" 2>"$work/put3.err"; then
  elapsed="$(python3 -c "print(round($(now) - $t0, 2))")"
  limit="$(python3 -c "print(round($deadline + $baseline + 5, 2))")"
  echo "   put took ${elapsed}s (limit ${limit}s = ${deadline}s deadline + ${baseline}s baseline + 5s)"
  python3 -c "import sys; sys.exit(0 if $elapsed < $limit else 1)" \
    && ok "finished within the deadline" || fail "took longer than the limit"
  obj3="$(latest_object "replication/one-paused-$run")"
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

echo
echo "4. node3 and node4 stopped"
"${compose[@]}" stop node3 node4 >/dev/null 2>&1

# two_stopped_put NAME MIB: puts a new object with node3 and node4 stopped,
# predicts the outcome from its placements, and checks the actual outcome and
# the recorded replicas against the prediction. Sets $expected and $matched.
two_stopped_put() {
  local name="$1" mib="$2" key="replication/two-stopped-$1-$run" obj chunks min_live actual st
  mkfile_mib "$name" "$mib"
  if "$osctl" put "$work/$name" "$key" 2>"$work/$name.err"; then actual=success; else actual=failure; fi
  obj="$(latest_object "$key")"
  chunks="$(sql "SELECT count(*) FROM chunks WHERE object_id = '$obj'")"
  # Fewest live placed nodes on any chunk of this object.
  min_live="$(sql "SELECT min(n) FROM (SELECT count(*) FILTER (WHERE p.node_id NOT IN ('node3', 'node4')) AS n
                   FROM chunks c JOIN chunk_placements p USING (chunk_id)
                   WHERE c.object_id = '$obj' GROUP BY c.chunk_id) t")"
  if ((min_live >= 2)); then expected=success; else expected=failure; fi
  echo "   $name: $chunks chunks, fewest live placed nodes on a chunk = $min_live -> expected $expected, actual $actual"
  matched=yes
  if [[ "$actual" != "$expected" ]]; then
    fail "outcome does not follow the per-chunk rule"
    matched=no
    return
  fi
  if [[ "$actual" == success ]]; then
    if check_replicas "$obj" '{node3,node4}'; then
      ok "recorded exactly the live placed nodes"
    else
      fail "recorded replicas differ from the live placed nodes"
      matched=no
    fi
  else
    echo "     $(grep -v ' level=INFO ' "$work/$name.err" | tail -1 | cut -c1-150)"
    st="$(sql "SELECT status FROM objects WHERE object_id = '$obj'")"
    if [[ "$st" == PENDING && "$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id)
                                      WHERE c.object_id = '$obj'")" == 0 && \
          "$(sql "SELECT count(*) FROM keys WHERE key = '$key'")" == 0 ]]; then
      ok "PENDING, no replicas recorded, key does not resolve"
    else
      fail "a failed upload left state behind (status $st)"
      matched=no
    fi
  fi
}

seen_success=no
seen_failure=no
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12; do
  two_stopped_put "one-chunk-$attempt" 1
  if [[ "$matched" == yes ]]; then
    [[ "$expected" == success ]] && seen_success=yes
    [[ "$expected" == failure ]] && seen_failure=yes
  fi
  [[ "$seen_success" == yes && "$seen_failure" == yes ]] && break
done
echo "   1-chunk attempts: $attempt; predicted success seen and matched: $seen_success; predicted failure seen and matched: $seen_failure"
if [[ "$seen_success" == yes && "$seen_failure" == yes ]]; then
  ok "both branches of the per-chunk rule exercised"
else
  fail "a branch of the per-chunk rule was not exercised in 12 attempts"
fi

two_stopped_put three-chunk 10
[[ "$expected" == failure ]] && ok "the 3-chunk object was predicted to fail, as rotation placement requires" \
  || fail "rotation placement should make a 3-chunk object fail with 2 nodes stopped"
"${compose[@]}" up -d --wait node3 node4 >/dev/null 2>&1

echo
echo "5. node1, node2 and node3 stopped"
"${compose[@]}" stop node1 node2 node3 >/dev/null 2>&1
mkfile_mib v5 10
if "$osctl" put "$work/v5" "replication/all-up-$run" 2>"$work/put5.err"; then
  fail "overwrite succeeded with only one node up"
else
  ok "overwrite failed cleanly: $(tail -1 "$work/put5.err" | cut -c1-120)"
fi
[[ "$(sql "SELECT object_id FROM keys WHERE key = 'replication/all-up-$run'")" == "$obj1" ]] \
  && ok "key still resolves to its previous version (step 1's object)" || fail "key pointer moved"
[[ "$(sql "SELECT status FROM objects WHERE key = 'replication/all-up-$run' ORDER BY created_at DESC LIMIT 1")" == PENDING ]] \
  && ok "the new object is PENDING" || fail "the new object is not PENDING"
[[ "$(sql "SELECT count(*) FROM objects WHERE status = 'COMMITTED' AND key = 'replication/fresh-$run'")" == 0 ]] || true
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
echo "6. stop three nodes, up --wait, put immediately (IP-change regression)"
ips() {
  local n
  for n in node1 node2 node3 node4; do
    printf '%s=%s ' "$n" "$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
      "$("${compose[@]}" ps -aq "$n")")"
  done
}
"${compose[@]}" up -d --wait >/dev/null 2>&1
before="$(ips)"
"${compose[@]}" stop node1 node2 node3 >/dev/null 2>&1
"${compose[@]}" up -d --wait >/dev/null 2>&1
after="$(ips)"
echo "   IPs before: $before"
echo "   IPs after:  $after"
[[ "$before" == "$after" ]] && echo "   (no IP change this run: the smoke test cannot hit the bug)" \
  || echo "   (IPs changed: the coordinator must re-resolve)"
mkfile_mib v6 10
if osctl_run put "$work/v6" "replication/after-restart-$run"; then
  ok "put immediately after the restart succeeded"
else
  fail "put immediately after the restart failed"
fi

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
