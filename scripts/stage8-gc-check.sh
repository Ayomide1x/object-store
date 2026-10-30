#!/usr/bin/env bash
# Stage 8 acceptance: deletes and garbage collection.
#
#   1. Abandoned upload: osctl is SIGKILLed partway through a 100 MiB put.
#      After the pending timeout, none of its chunk files exist on any node,
#      its metadata is gone, and every placed node holds a tombstone for each
#      of its chunks.
#   2. rm: the key is NotFound at once while its files are still on disk; after
#      the read grace its files are gone from every node and its metadata is
#      gone. A second rm fails with "key not found".
#   3. Overwrite: a get of the old version is held mid-read (the node it reads
#      chunk 0 from is paused) while an overwrite commits; once the node is
#      unpaused the get reads the old chunks after the swap and returns the old
#      bytes. After the read grace the old version's files are gone, and the
#      new version reads back identical.
#   4. BAD file: chunk 0 of a new object is on 3 nodes. The fourth is stopped
#      (so it can't be a repair target), and chunk 0's first replica is
#      corrupted and read, so it is marked BAD. After the read grace, GC
#      deletes that file without a tombstone, and the BAD node becomes the
#      repair target: it gets a HEALTHY repair row and a file that verifies.
#   5. Node down during GC: rm a key, stop a node holding its chunk 0. After
#      the grace, the other nodes' files are gone, and the stopped node's
#      tasks wait (its files are still on its volume). Once it is ALIVE again
#      its files are removed, then the object's metadata.
#   6. Disk usage: once no retired object is left, every chunk file on every
#      node belongs to a COMMITTED object through a replica row on that node
#      (HEALTHY or BAD) or a placement there (the unrecorded copies stage 8
#      leaves), every HEALTHY row has its file, and the bytes per node equal
#      the sum of 56 + size over those files.
#
# Uses the Compose demo values: read grace 60 s, pending timeout 120 s, a GC
# pass every 10 s; DEAD after 15 s, repair grace 30 s, a repair pass every 10 s.
#
# Usage: scripts/stage8-gc-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/volume.sh"
source "$root/scripts/lib/liveness.sh"
read_grace=60 pending_timeout=120 interval=10 dead=15 margin=20
offset=$((56 + 1000000))

work="$(mktemp -d)"
run="$$"
flipped="$work/flipped"
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
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -F' ' -c "$1"; }
mkfile_mib() { head -c $(($2 << 20)) /dev/urandom > "$work/$1"; }
latest_object() { sql "SELECT object_id FROM objects WHERE key = '$1' ORDER BY created_at DESC LIMIT 1"; }
# placements_of OBJECT: "node chunk" for every placement of the object's chunks.
placements_of() { sql "SELECT p.node_id, p.chunk_id FROM chunk_placements p JOIN chunks c USING (chunk_id)
                       WHERE c.object_id = '$1' ORDER BY p.node_id, c.chunk_index"; }

# wait_sql TIMEOUT WANT QUERY: polls until QUERY prints WANT. Prints how long
# it took, or the last value seen.
wait_sql() {
  local timeout="$1" want="$2" q="$3" start=$SECONDS got
  while :; do
    got="$(sql "$q")"
    if [[ "$got" == "$want" ]]; then
      echo "   (after $((SECONDS - start))s)"
      return 0
    fi
    if ((SECONDS - start >= timeout)); then
      echo "   timed out after ${timeout}s; last: ${got:-<empty>}"
      return 1
    fi
    sleep 2
  done
}

# files_states LIST: for a file of "node chunk" lines, prints "node STATE chunk"
# for each, from each node's volume (works while a node is stopped).
files_states() {
  local n
  for n in $(cut -d' ' -f1 "$1" | sort -u); do
    # shellcheck disable=SC2046
    check_chunk_files "$n" $(awk -v n="$n" '$1 == n {print $2}' "$1") | sed "s/^/$n /"
  done
}
count_state() { grep -c " $2 " "$1" || true; }

# tombstones_of NODE CHUNK...: how many of the chunks have a tombstone on NODE.
tombstones_of() {
  local node="$1"; shift
  docker run --rm --volumes-from "$(node_container "$node")" alpine:3 sh -c '
    n=0; for id in "$@"; do [ -f "/data/tombstones/$id" ] && n=$((n + 1)); done; echo $n' sh "$@"
}

wait_alive 4

echo "1. abandoned upload: osctl killed mid-put"
mkfile_mib big 100
key1="gc/abandoned-$run"
"$osctl" put "$work/big" "$key1" 2>"$work/put1.log" &
put_pid=$!
obj1=""
for _ in $(seq 1 100); do
  obj1="$(sql "SELECT object_id FROM objects WHERE key = '$key1'")"
  [[ -n "$obj1" ]] && break
  sleep 0.1
done
c0="$(sql "SELECT chunk_id FROM chunks WHERE object_id = '$obj1' AND chunk_index = 0")"
n0="$(sql "SELECT node_id FROM chunk_placements WHERE chunk_id = '$c0' ORDER BY node_id LIMIT 1")"
for _ in $(seq 1 60); do
  [[ "$(check_chunk_files "$n0" "$c0")" == "OK $c0" ]] && break
  sleep 0.2
done
kill -9 "$put_pid" 2>/dev/null || true
wait "$put_pid" 2>/dev/null || true
placements_of "$obj1" > "$work/p1"
files_states "$work/p1" > "$work/f1"
st1="$(sql "SELECT status FROM objects WHERE object_id = '$obj1'")"
echo "   object $obj1: $st1; $(count_state "$work/f1" OK) of $(wc -l < "$work/p1" | tr -d ' ') placed chunk files written before the kill"
if [[ "$st1" == PENDING && "$(count_state "$work/f1" OK)" -gt 0 && "$(count_state "$work/f1" MISSING)" -gt 0 ]]; then
  ok "precondition: killed mid-upload (PENDING, some chunk files written, others not)"
else
  fail "precondition: not killed mid-upload (status $st1); inconclusive, rerun"
fi
echo "   waiting for the pending timeout (${pending_timeout}s) and a GC pass"
wait_sql $((pending_timeout + interval + margin)) 0 "SELECT count(*) FROM objects WHERE object_id = '$obj1'" \
  && ok "the abandoned upload's object row is gone" || fail "the abandoned upload was not collected"
files_states "$work/p1" > "$work/f1"
[[ "$(count_state "$work/f1" MISSING)" == "$(wc -l < "$work/p1" | tr -d ' ')" ]] \
  && ok "none of its $(wc -l < "$work/p1" | tr -d ' ') placed chunk files exists on any node" \
  || fail "chunk files left: $(grep -v MISSING "$work/f1" | tr '\n' ';')"
ids1="{$(cut -d' ' -f2 "$work/p1" | sort -u | paste -sd, -)}"
left="$(sql "SELECT (SELECT count(*) FROM chunks WHERE object_id = '$obj1')
               + (SELECT count(*) FROM chunk_placements WHERE chunk_id = ANY('$ids1'::uuid[]))
               + (SELECT count(*) FROM pending_deletions WHERE chunk_id = ANY('$ids1'::uuid[]))")"
[[ "$left" == 0 ]] && ok "no chunks, placements or deletion tasks left for it" || fail "$left metadata rows left"
tomb_missing=""
for n in $(cut -d' ' -f1 "$work/p1" | sort -u); do
  ids="$(awk -v n="$n" '$1 == n {print $2}' "$work/p1")"
  # shellcheck disable=SC2086
  [[ "$(tombstones_of "$n" $ids)" == "$(wc -w <<<"$ids" | tr -d ' ')" ]] || tomb_missing="$tomb_missing $n"
done
[[ -z "$tomb_missing" ]] && ok "every placed node has a tombstone for each of its chunks" \
  || fail "tombstones missing on:$tomb_missing"

echo
echo "2. rm"
mkfile_mib s2 10
key2="gc/rm-$run"
osctl_run put "$work/s2" "$key2"
obj2="$(latest_object "$key2")"
placements_of "$obj2" > "$work/p2"
osctl_run rm "$key2" && ok "rm succeeded" || fail "rm failed"
if "$osctl" get "$key2" "$work/s2.out" 2>"$work/get2.log"; then
  fail "get succeeded after rm"
else
  grep -q "code = NotFound" "$work/get2.log" && ok "get: NotFound at once" || fail "get failed differently"
fi
files_states "$work/p2" > "$work/f2"
[[ "$(count_state "$work/f2" OK)" == "$(wc -l < "$work/p2" | tr -d ' ')" ]] \
  && ok "its files are still on disk during the grace" || fail "files went before the grace"
if "$osctl" rm "$key2" 2>"$work/rm2.log"; then
  fail "a second rm succeeded"
else
  grep -q 'key not found' "$work/rm2.log" && ok "second rm: exit 1, key not found" || fail "second rm failed differently"
fi
echo "   waiting for the read grace (${read_grace}s) and a GC pass"
wait_sql $((read_grace + interval + margin)) 0 "SELECT count(*) FROM objects WHERE object_id = '$obj2'" \
  && ok "its metadata is gone" || fail "the removed object was not collected"
files_states "$work/p2" > "$work/f2"
[[ "$(count_state "$work/f2" MISSING)" == "$(wc -l < "$work/p2" | tr -d ' ')" ]] \
  && ok "its files are gone from every node" || fail "files left: $(grep -v MISSING "$work/f2" | tr '\n' ';')"

echo
echo "3. overwrite during a get"
mkfile_mib v1 100
mkfile_mib v2 100
key3="gc/overwrite-$run"
osctl_run put "$work/v1" "$key3"
old3="$(latest_object "$key3")"
placements_of "$old3" > "$work/p3"
held3="$(sql "SELECT min(r.node_id) FROM chunks c JOIN chunk_replicas r USING (chunk_id) WHERE c.object_id = '$old3' AND c.chunk_index = 0")"
echo "   pausing $held3, which the get reads chunk 0 from first, so the get is held mid-read"
"${compose[@]}" pause "$held3" >/dev/null
OSCTL_READ_ORDER=listed OSCTL_CHUNK_READ_TIMEOUT=120s "$osctl" get "$key3" "$work/v1.out" 2>"$work/get3.log" &
get_pid=$!
sleep 2 # GetObject has returned the old chunk list; the read of chunk 0 is blocked
osctl_run put "$work/v2" "$key3" || fail "the overwrite failed"
new3="$(latest_object "$key3")"
if kill -0 "$get_pid" 2>/dev/null && [[ "$(sql "SELECT object_id FROM keys WHERE key = '$key3'")" == "$new3" ]]; then
  ok "precondition: the overwrite committed while the get of the old version was still reading"
else
  fail "precondition: the get finished before the overwrite committed; inconclusive, rerun"
fi
"${compose[@]}" unpause "$held3" >/dev/null
if wait "$get_pid" && cmp -s "$work/v1" "$work/v1.out"; then
  grep -q "object_id=$old3" "$work/get3.log" && ok "the get started on the old version returned its bytes after the swap, IDENTICAL" \
    || fail "the get did not read the old object"
else
  fail "the get of the old version failed or differs"
  grep -v ' level=INFO ' "$work/get3.log" | tail -3 | sed 's/^/    /'
fi
echo "   waiting for the read grace (${read_grace}s) and a GC pass"
wait_sql $((read_grace + 2 * interval + margin)) 0 "SELECT count(*) FROM objects WHERE object_id = '$old3'" \
  && ok "the old version's metadata is gone" || fail "the old version was not collected"
files_states "$work/p3" > "$work/f3"
[[ "$(count_state "$work/f3" MISSING)" == "$(wc -l < "$work/p3" | tr -d ' ')" ]] \
  && ok "the old version's $(wc -l < "$work/p3" | tr -d ' ') chunk files are gone" \
  || fail "old files left: $(grep -v MISSING "$work/f3" | tr '\n' ';')"
osctl_run get "$key3" "$work/v2.out" && cmp -s "$work/v2" "$work/v2.out" \
  && ok "the new version reads back IDENTICAL" || fail "the new version does not read back"
[[ "$(sql "SELECT object_id FROM keys WHERE key = '$key3'")" == "$new3" ]] || fail "the key does not point at the new version"

echo
echo "4. a BAD replica's file is deleted, then repaired onto the same node"
mkfile_mib s4 10
key4="gc/bad-$run"
osctl_run put "$work/s4" "$key4"
obj4="$(latest_object "$key4")"
# Chunk 0 is on 3 of the 4 nodes. Stopping the fourth (spare) leaves the BAD
# replica's own node as the only repair target. (Picking node4 up front failed
# whenever every chunk had a replica on node4.)
c4="$(sql "SELECT chunk_id FROM chunks WHERE object_id = '$obj4' AND chunk_index = 0")"
holders4="$(sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$c4' ORDER BY node_id" | tr '\n' ' ')"
spare=""
for n in node1 node2 node3 node4; do [[ " $holders4" == *" $n "* ]] || spare="$n"; done
first="${holders4%% *}"
if [[ -z "$c4" || "$(wc -w <<<"$holders4" | tr -d ' ')" != 3 || -z "$spare" ]]; then
  fail "precondition: chunk 0 of $key4 should have 3 replicas, has: ${holders4:-none}"
  echo; echo "FAIL"; exit 1
fi
echo "   chunk $c4 is on $holders4; stopping $spare, so only $first can take the repair"
"${compose[@]}" stop "$spare" >/dev/null 2>&1
wait_state "$spare" DEAD $((dead + 10)) || fail "$spare never DEAD"
corrupt_byte "$first" "$c4" "$offset" "$flipped" | sed 's/^/     /'
OSCTL_READ_ORDER=listed osctl_run get "$key4" "$work/s4.out" && cmp -s "$work/s4" "$work/s4.out" \
  && ok "get IDENTICAL (failed over)" || fail "get failed"
[[ "$(sql "SELECT state FROM chunk_replicas WHERE chunk_id = '$c4' AND node_id = '$first'")" == BAD ]] \
  && ok "$first's replica is BAD" || fail "$first's replica was not marked BAD"
sleep $((interval + 2))
[[ "$(sql "SELECT count(*) FROM chunk_replicas WHERE chunk_id = '$c4'")" == 3 ]] \
  && ok "no repair yet: $spare is down and $first has a BAD row" || fail "the chunk was repaired somewhere unexpected"
echo "   waiting for the read grace (${read_grace}s), the GC delete, then a repair pass"
if wait_sql $((read_grace + 3 * interval + margin)) "HEALTHY repair" \
     "SELECT state, origin FROM chunk_replicas WHERE chunk_id = '$c4' AND node_id = '$first'"; then
  ok "$first holds a HEALTHY repair row for the chunk again"
else
  fail "$first did not become the repair target"
fi
"${compose[@]}" logs --no-color "$first" 2>/dev/null | grep '"chunk deleted"' | grep "\"chunk_id\":\"$c4\"" \
  | grep -q '"tombstone":false' && ok "$first's node log: the BAD file was deleted without a tombstone" \
  || fail "no tombstone-free delete of the BAD file in $first's log"
[[ "$(check_chunk_files "$first" "$c4")" == "OK $c4" ]] && ok "the new file on $first verifies" || fail "the file on $first does not verify"
[[ "$(tombstones_of "$first" "$c4")" == 0 ]] && ok "no tombstone for the live chunk" || fail "the live chunk was tombstoned"
"${compose[@]}" up -d --wait "$spare" >/dev/null 2>&1
wait_alive 1 "$spare" || fail "$spare not ALIVE after restart"

echo
echo "5. GC while a node is DEAD"
mkfile_mib s5 10
key5="gc/down-$run"
osctl_run put "$work/s5" "$key5"
obj5="$(latest_object "$key5")"
placements_of "$obj5" > "$work/p5"
# The node to stop: one that holds chunk 0, so it has files of this object.
down5="$(sql "SELECT min(p.node_id) FROM chunk_placements p JOIN chunks c USING (chunk_id)
              WHERE c.object_id = '$obj5' AND c.chunk_index = 0")"
awk -v n="$down5" '$1 == n' "$work/p5" > "$work/p5-down"
awk -v n="$down5" '$1 != n' "$work/p5" > "$work/p5-others"
echo "   $down5 holds $(wc -l < "$work/p5-down" | tr -d ' ') of its chunks"
osctl_run rm "$key5"
"${compose[@]}" stop "$down5" >/dev/null 2>&1
wait_state "$down5" DEAD $((dead + 10)) || fail "$down5 never DEAD"
echo "   waiting for the read grace (${read_grace}s) and a GC pass"
wait_sql $((read_grace + interval + margin)) "$down5" \
  "SELECT string_agg(DISTINCT d.node_id, ',') FROM pending_deletions d JOIN chunks c USING (chunk_id) WHERE c.object_id = '$obj5'" \
  && ok "only $down5's deletion tasks are left" || fail "unexpected deletion tasks"
files_states "$work/p5-others" > "$work/f5"
[[ "$(count_state "$work/f5" MISSING)" == "$(wc -l < "$work/p5-others" | tr -d ' ')" ]] \
  && ok "the other nodes' files are gone" || fail "other nodes' files left"
files_states "$work/p5-down" > "$work/f5b"
[[ "$(count_state "$work/f5b" OK)" == "$(wc -l < "$work/p5-down" | tr -d ' ')" ]] \
  && ok "$down5's files are still on its volume" || fail "$down5's files changed while it was down"
[[ "$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id) WHERE c.object_id = '$obj5' AND r.node_id = '$down5'")" \
   == "$(wc -l < "$work/p5-down" | tr -d ' ')" ]] && ok "$down5's replica rows stay until it confirms" || fail "$down5's rows changed while it was down"
echo "   $(sql "SELECT count(*) || ' tasks for $down5, attempts ' || max(attempts) FROM pending_deletions d JOIN chunks c USING (chunk_id) WHERE c.object_id = '$obj5'") (not attempted while DEAD)"
[[ "$(sql "SELECT status FROM objects WHERE object_id = '$obj5'")" == DELETED ]] && ok "the object is DELETED, not yet removed" \
  || fail "the object row changed while $down5 had files"
"${compose[@]}" up -d --wait "$down5" >/dev/null 2>&1
wait_alive 1 "$down5" || fail "$down5 not ALIVE after restart"
wait_sql $((2 * interval + margin)) 0 "SELECT count(*) FROM objects WHERE object_id = '$obj5'" \
  && ok "after $down5 returned, the object's metadata is gone" || fail "not collected after $down5 returned"
files_states "$work/p5-down" > "$work/f5b"
[[ "$(count_state "$work/f5b" MISSING)" == "$(wc -l < "$work/p5-down" | tr -d ' ')" ]] \
  && ok "$down5's files for the removed object are gone" || fail "$down5 kept files no row points to"

echo
echo "6. per-node chunk usage matches the live objects"
wait_sql $((read_grace + 3 * interval + margin)) 0 \
  "SELECT count(*) FROM objects WHERE status IN ('SUPERSEDED', 'DELETED', 'ABANDONED') OR status = 'PENDING'" \
  && ok "no retired or pending object left" || fail "retired objects still waiting"
for n in node1 node2 node3 node4; do
  docker run --rm --volumes-from "$(node_container "$n")" alpine:3 sh -c \
    'cd /data/chunks && for f in *; do [ -f "$f" ] && echo "$f $(stat -c %s "$f")"; done; true' > "$work/disk-$n"
  sql "SELECT c.chunk_id, c.size_bytes, coalesce(r.state, 'PLACED')
       FROM chunks c JOIN objects o USING (object_id)
       LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.node_id = '$n'
       WHERE o.status = 'COMMITTED'
         AND (r.node_id IS NOT NULL
              OR EXISTS (SELECT 1 FROM chunk_placements p WHERE p.chunk_id = c.chunk_id AND p.node_id = '$n'))" > "$work/meta-$n"
  if line="$(python3 -I - "$work/disk-$n" "$work/meta-$n" <<'PYEOF'
import sys
disk = {}
for l in open(sys.argv[1]):
    if l.strip():
        name, size = l.split()
        disk[name] = int(size)
meta = {}
for l in open(sys.argv[2]):
    if l.strip():
        cid, size, state = l.split()
        meta[cid] = (int(size), state)
problems = []
orphans = [f for f in disk if f not in meta]
if orphans:
    problems.append(f"{len(orphans)} files of no live object, e.g. {orphans[0]}")
for cid, (size, state) in meta.items():
    if state == "HEALTHY" and cid not in disk:
        problems.append(f"HEALTHY row without a file: {cid}")
    if cid in disk and disk[cid] != 56 + size:
        problems.append(f"{cid}: {disk[cid]} bytes, expected {56 + size}")
expected = sum(56 + meta[f][0] for f in disk if f in meta)
actual = sum(disk.values())
rows = sum(1 for f in disk if f in meta and meta[f][1] != "PLACED")
placed = sum(1 for f in disk if f in meta and meta[f][1] == "PLACED")
print(f"{len(disk)} files, {actual} bytes; expected {expected} bytes from live objects "
      f"({rows} with a replica row, {placed} placement-only)")
if problems or actual != expected:
    print("; ".join(problems[:5]))
    sys.exit(1)
PYEOF
)"; then
    ok "$n: $line"
  else
    fail "$n: $line"
  fi
done

wait_alive 4 || fail "not all nodes ALIVE at the end"

echo
echo "restoring the flipped bytes, then checking every touched file:"
restore_flipped "$flipped"
check_touched "$flipped" || status=1

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
