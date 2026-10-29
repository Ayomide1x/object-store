#!/usr/bin/env bash
# Stage 7 acceptance: re-replication.
#
#   1. One node lost: stop node2 and wait past DEAD plus the 30 s grace. Every
#      chunk of A and B gets back to 3 HEALTHY replicas on the other nodes;
#      each repaired file passes the header, chunk-ID and SHA-256 check on its
#      own node's volume; both objects read back identical with node2 down.
#   2. Repeat pass: a later pass starts no copies; A and B's rows are unchanged.
#   3. Node returns: start node2. Its old rows count again: chunks it held now
#      have 4 HEALTHY replicas (over-replication is left in place), none BAD,
#      and everything reads back identical.
#   4. Bad replica: corrupt the first replica of a chunk of C and read it, so
#      it is reported and marked BAD; repair copies the chunk to another node.
#   5. Under-replicated from birth: with node3 and node4 DEAD, D is stored with
#      2 replicas per chunk; once they are back, D is topped up to 3.
#   6. Target killed mid-copy: stop node1; when a repair copy lands on node4
#      (a temp file appears) node4 is SIGKILLed. No partial file exists under
#      any final name on node4; after restart its temps are gone; a later pass
#      completes the copies. A node-to-node copy of a 4 MiB chunk takes
#      milliseconds, faster than `docker kill`, so the watcher uses inotify on
#      node4's /data/tmp and sends SIGKILL to node4's process itself, from a
#      container in the Docker VM's PID namespace. If the kill still misses (no
#      temp file at the kill), the step fails as inconclusive.
#
# Uses the Compose defaults: DEAD after 15 s, grace 30 s, a pass every 10 s.
#
# Usage: scripts/stage7-repair-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/volume.sh"
source "$root/scripts/lib/liveness.sh"
dead=15 grace=30 interval=10 margin=15 copy_timeout=30
offset=$((56 + 1000000))

work="$(mktemp -d)"
run="$$"
watcher="stage7-watcher-$$"
flipped="$work/flipped"
: > "$flipped"
restore() {
  docker rm -f "$watcher" >/dev/null 2>&1 || true
  "${compose[@]}" up -d --wait >/dev/null 2>&1 || true
  while read -r node chunk; do
    [[ -n "$node" ]] && flip_byte "$node" "$chunk" "$offset" >/dev/null
  done <"$flipped"
  rm -rf "$work"
}
trap restore EXIT

status=0
fail() { echo "  FAIL: $*"; status=1; }
ok() { echo "  ok: $*"; }
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -F'|' -c "$1"; }
db_now() { sql "SELECT clock_timestamp()"; }
mkfile_mib() { head -c $(($2 << 20)) /dev/urandom > "$work/$1"; }
object_of() { sql "SELECT object_id FROM keys WHERE key = '$1'"; }

# short KEY... [--not NODE]: how many chunks of these objects have fewer than 3
# HEALTHY replicas, not counting rows on NODE.
short() {
  local not="none" keys=()
  while (($#)); do
    if [[ "$1" == --not ]]; then not="$2"; shift 2; else keys+=("'$1'"); shift; fi
  done
  sql "SELECT count(*) FROM (SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id)
       LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.state = 'HEALTHY' AND r.node_id <> '$not'
       WHERE k.key IN ($(IFS=,; echo "${keys[*]}")) GROUP BY c.chunk_id HAVING count(r.node_id) < 3) t"
}

# wait_repaired TIMEOUT KEY... [--not NODE]: until short() is 0.
wait_repaired() {
  local timeout="$1" start=$SECONDS n; shift
  while :; do
    n="$(short "$@")"
    if [[ "$n" == 0 ]]; then echo "   repaired after $((SECONDS - start))s"; return 0; fi
    if ((SECONDS - start >= timeout)); then echo "  FAIL: $n chunks still short after ${timeout}s" >&2; return 1; fi
    sleep 1
  done
}

# wait_passes SINCE N TIMEOUT: until N passes that started after SINCE have finished.
wait_passes() {
  local since="$1" want="$2" timeout="$3" start=$SECONDS
  while (( $(sql "SELECT count(*) FROM repair_passes WHERE started_at > '$since' AND finished_at IS NOT NULL") < want )); do
    if ((SECONDS - start >= timeout)); then echo "  FAIL: fewer than $want passes after ${timeout}s" >&2; return 1; fi
    sleep 1
  done
}

# verify_repaired KEY...: every repair-origin replica of these objects passes
# the file checks on its own node's volume.
verify_repaired() {
  local node ids out bad=0 keys
  keys="$(printf "'%s'," "$@")"; keys="${keys%,}"
  for node in node1 node2 node3 node4; do
    ids="$(sql "SELECT r.chunk_id FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r ON r.chunk_id = c.chunk_id
                WHERE k.key IN ($keys) AND r.node_id = '$node' AND r.origin <> 'upload' AND r.state = 'HEALTHY'" | tr '\n' ' ')"
    [[ -n "${ids// /}" ]] || continue
    out="$(check_chunk_files "$node" $ids)"
    echo "     $node: $(grep -c '^OK' <<<"$out" || true) repaired files ok, $(grep -vc '^OK' <<<"$out" || true) bad"
    grep -qv '^OK' <<<"$out" && bad=1
  done
  ((bad == 0))
}

get_identical() {
  if osctl_run get "$1" "$work/out" && cmp -s "$2" "$work/out"; then ok "$1 reads back IDENTICAL"; else fail "$1 did not read back"; fi
  rm -f "$work/out"
}

echo "1. one node lost: node2"
wait_alive 4
mkfile_mib a 10; mkfile_mib b 10
osctl_run put "$work/a" "repair/a-$run"
osctl_run put "$work/b" "repair/b-$run"
before_rows="$(sql "SELECT count(*) FROM chunk_replicas")"
t1="$(db_now)"
"${compose[@]}" stop node2 >/dev/null 2>&1
wait_state node2 DEAD $((dead + 5)) || fail "node2 never DEAD"
echo "   waiting out the ${grace}s grace, then for repair"
if wait_repaired $((grace + 2 * interval + margin)) "repair/a-$run" "repair/b-$run" --not node2; then
  ok "every chunk of A and B has 3 HEALTHY replicas without node2"
else
  fail "A or B not repaired"
fi
copies="$(sql "SELECT coalesce(sum(copies_started), 0) FROM repair_passes WHERE started_at > '$t1'")"
echo "   copies started since node2 stopped: $copies"
((copies > 0)) && ok "repair passes made copies" || fail "no copies recorded in repair_passes"
verify_repaired "repair/a-$run" "repair/b-$run" && ok "every repaired file verifies on its own volume" \
  || fail "a repaired file did not verify"
get_identical "repair/a-$run" "$work/a"
get_identical "repair/b-$run" "$work/b"

echo
echo "2. repeat pass"
rows="$(sql "SELECT count(*) FROM chunk_replicas")"
t2="$(db_now)"
wait_passes "$t2" 2 $((3 * interval + margin)) || fail "no further passes"
last="$(sql "SELECT copies_started FROM repair_passes WHERE started_at > '$t2' AND finished_at IS NOT NULL ORDER BY pass_id DESC LIMIT 1")"
echo "   latest pass: copies_started = $last; replica rows $rows -> $(sql "SELECT count(*) FROM chunk_replicas")"
[[ "$last" == 0 ]] && ok "a second pass makes no new copies" || fail "a later pass still copied"
[[ "$(sql "SELECT count(*) FROM chunk_replicas")" == "$rows" ]] && ok "no new replica rows" || fail "replica rows changed"

echo
echo "3. node2 returns"
"${compose[@]}" up -d --wait node2 >/dev/null 2>&1
wait_alive 1 node2 || fail "node2 not ALIVE"
t3="$(db_now)"
wait_passes "$t3" 1 $((2 * interval + margin)) || true
four="$(sql "SELECT count(*) FROM (SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
             WHERE k.key IN ('repair/a-$run', 'repair/b-$run') AND r.state = 'HEALTHY'
             GROUP BY c.chunk_id HAVING count(*) = 4) t")"
held="$(sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
             WHERE k.key IN ('repair/a-$run', 'repair/b-$run') AND r.node_id = 'node2' AND r.origin = 'upload'")"
echo "   chunks of A and B with node2's old replica: $held; with 4 HEALTHY replicas: $four"
[[ "$four" == "$held" ]] && ok "over-replication left in place: each chunk node2 held has 4 HEALTHY" \
  || fail "expected $held chunks with 4 HEALTHY replicas"
[[ "$(sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
           WHERE k.key IN ('repair/a-$run', 'repair/b-$run') AND r.state = 'BAD'")" == 0 ]] \
  && ok "no replica of A or B is BAD" || fail "a replica is BAD"
get_identical "repair/a-$run" "$work/a"
get_identical "repair/b-$run" "$work/b"

echo
echo "4. a BAD replica is repaired onto another node"
mkfile_mib c 10
osctl_run put "$work/c" "repair/c-$run"
c1="$(sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = 'repair/c-$run' AND c.chunk_index = 1")"
first="$(sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$c1' ORDER BY node_id LIMIT 1")"
echo "   corrupting chunk 1's replica on $first, then reading it first"
flip_byte "$first" "$c1" "$offset" | sed 's/^/     /'; echo "$first $c1" >> "$flipped"
OSCTL_READ_ORDER=listed osctl_run get "repair/c-$run" "$work/c.out" && cmp -s "$work/c" "$work/c.out" \
  && ok "get IDENTICAL (failed over)" || fail "get failed"
[[ "$(sql "SELECT state FROM chunk_replicas WHERE chunk_id = '$c1' AND node_id = '$first'")" == BAD ]] \
  && ok "$first's replica is BAD" || fail "$first's replica was not marked BAD"
if wait_repaired $((2 * interval + margin)) "repair/c-$run"; then
  new="$(sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$c1' AND origin = 'repair' AND state = 'HEALTHY'")"
  ok "chunk 1 has 3 HEALTHY replicas again, the new one on $new"
  [[ -n "$new" && "$new" != "$first" ]] || fail "the repair went to the BAD node"
  [[ "$(check_chunk_files "$new" "$c1")" == "OK $c1" ]] && ok "the new file on $new verifies" || fail "the new file does not verify"
else
  fail "chunk 1 was not repaired"
fi

echo
echo "5. under-replicated from birth"
"${compose[@]}" stop node3 node4 >/dev/null 2>&1
wait_state node3 DEAD $((dead + 5)) || fail "node3 never DEAD"
wait_state node4 DEAD $((dead + 5)) || fail "node4 never DEAD"
mkfile_mib d 10
osctl_run put "$work/d" "repair/d-$run"
born="$(sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id) WHERE k.key = 'repair/d-$run'")"
chunks_d="$(sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) WHERE k.key = 'repair/d-$run'")"
echo "   D stored with $born replica rows for $chunks_d chunks"
[[ "$born" == $((2 * chunks_d)) ]] && ok "born with 2 replicas per chunk" || fail "expected 2 per chunk"
"${compose[@]}" up -d --wait node3 node4 >/dev/null 2>&1
wait_alive 1 node3 node4 || fail "node3/node4 not ALIVE"
wait_repaired $((2 * interval + margin)) "repair/d-$run" && ok "D topped up to 3 HEALTHY per chunk" || fail "D not topped up"
verify_repaired "repair/d-$run" || fail "a topped-up file did not verify"
get_identical "repair/d-$run" "$work/d"

echo
echo "6. target killed mid-copy"
mkfile_mib e 100
osctl_run put "$work/e" "repair/e-$run"
node4="$(node_container node4)"
node4_pid="$(docker inspect -f '{{.State.Pid}}' "$node4")"
# inotifyd reports each file created in /data/tmp the moment it appears; the
# kill goes straight to node4's process (PID $node4_pid in the VM).
docker run -d --name "$watcher" --pid host --volumes-from "$node4" alpine:3 sh -c "
  echo watching
  inotifyd - /data/tmp:n | while read -r ev dir file; do kill -9 $node4_pid; echo killed \$file; break; done" >/dev/null
until docker logs "$watcher" 2>&1 | grep -q watching; do sleep 0.2; done
"${compose[@]}" stop node1 >/dev/null 2>&1
echo "   node1 stopped; waiting for repair copies onto node4"
start=$SECONDS
until docker logs "$watcher" 2>&1 | grep -q killed; do
  if ((SECONDS - start > dead + grace + 2 * interval + margin)); then break; fi
  sleep 0.5
done
if docker logs "$watcher" 2>&1 | grep -q killed; then
  echo "   node4: $(docker inspect -f '{{.State.Status}} (exit {{.State.ExitCode}})' "$node4")"
  temps="$(docker run --rm --volumes-from "$node4" alpine:3 ls /data/tmp)"
  if [[ -n "$temps" ]]; then
    ok "precondition: node4 was killed with a copy in flight ($(wc -l <<<"$temps" | tr -d ' ') temp file)"
  else
    fail "precondition: no temp file on node4: the kill missed the copy; inconclusive, rerun"
  fi
  all="$(docker run --rm --volumes-from "$node4" alpine:3 ls /data/chunks | tr '\n' ' ')"
  out="$(check_chunk_files node4 $all)"
  [[ -z "$(grep -v '^OK' <<<"$out")" ]] && ok "every final file on node4 verifies ($(grep -c '^OK' <<<"$out") files): no partial copy" \
    || fail "a final file on node4 is partial or corrupt"
  "${compose[@]}" up -d --wait node4 >/dev/null 2>&1
  wait_alive 1 node4 || fail "node4 not ALIVE after restart"
  [[ -z "$(docker run --rm --volumes-from "$node4" alpine:3 ls /data/tmp)" ]] && ok "node4's temps are gone after restart" \
    || fail "node4 still has temps"
  # A copy whose target vanished mid-stream can hang until the copy timeout.
  wait_repaired $((copy_timeout + 2 * interval + margin)) "repair/e-$run" --not node1 && ok "a later pass completed E's copies" \
    || fail "E not fully repaired"
else
  fail "no repair copy landed on node4 in time; inconclusive, rerun"
fi
"${compose[@]}" up -d --wait node1 >/dev/null 2>&1
wait_alive 4 || fail "not all nodes ALIVE at the end"
get_identical "repair/e-$run" "$work/e"

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
