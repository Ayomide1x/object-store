#!/usr/bin/env bash
# The demo, and stage 9's acceptance test. It tells one story on the shipped
# docker-compose.yml, checking that each thing it shows really happened:
#
#   1. Upload a 40 MiB file: 10 chunks, 3 replicas each.
#   2. Stop a node and watch it go SUSPECT, then DEAD.
#   3. Wait out the repair grace, then watch re-replication copy every chunk
#      the stopped node held onto the other nodes.
#   4. Start the node again: its old copies count again (over-replicated,
#      kept on purpose).
#   5. Corrupt one replica on disk, download anyway (the client fails over),
#      and watch the replica get marked BAD and the chunk repaired.
#   6. Show the final replica state with osctl stat.
#
# Timings are read from `docker compose config`, so what the demo says (and
# how long it waits) is what the running stack does. Any check that fails
# stops the demo with FAIL and the reason; a precondition that isn't met
# stops it as inconclusive. It never prints a step it didn't verify.
#
# Usage: scripts/demo.sh
# Needs a fresh stack (docker compose down -v && docker compose up --build
# --wait) and bin/osctl (make build).
# DEMO_PAUSE=SECONDS holds after each step (default 0), so a viewer (or the
# stage 11 UI) can catch up. It pauses only the script, never the cluster.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/volume.sh"
source "$root/scripts/lib/liveness.sh"

pause="${DEMO_PAUSE:-0}"
offset=$((56 + 1000000)) # past the 56-byte header, inside chunk 1's data
work="$(mktemp -d)"
flipped="$work/flipped"
: > "$flipped"
stopped=""

# ---- output ---------------------------------------------------------------

if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
  tty=yes
  b=$'\e[1m' dim=$'\e[2m' grn=$'\e[32m' red=$'\e[31m' yel=$'\e[33m' cyn=$'\e[36m' off=$'\e[0m'
  okmark="${grn}✓${off}" badmark="${red}✗${off}"
else
  tty=no
  b="" dim="" grn="" red="" yel="" cyn="" off=""
  okmark="ok:" badmark="FAIL:"
fi

step_no=0
step() {
  step_no=$((step_no + 1))
  echo
  echo "${b}${cyn}── $step_no/6  $1 ${off}"
}
say() { echo "   $*"; }
note() { echo "   ${dim}$*${off}"; }
ok() { echo "   $okmark $*"; }
# fail REASON: the demo stops here, with the reason.
fail() {
  echo "   $badmark $*"
  echo
  echo "${b}${red}FAIL${off}"
  exit 1
}
inconclusive() { fail "inconclusive: $*"; }
hold() { if ((pause > 0)); then sleep "$pause"; fi; }

# progress TEXT: a line that updates in place on a terminal, and is printed
# only when it changes otherwise (logs, CI).
last_progress=""
progress() {
  if [[ "$tty" == yes ]]; then
    printf '\r\e[K   %s' "$1"
  elif [[ "$1" != "$last_progress" ]]; then
    echo "   $1"
  fi
  last_progress="$1"
}
progress_done() { if [[ "$tty" == yes && -n "$last_progress" ]]; then echo; fi; last_progress=""; }

now() { python3 -c 'import time; print(time.time())'; }
since() { python3 -c "import time; print(f'{time.time() - $1:.1f}')"; }

# ---- cleanup --------------------------------------------------------------

cleanup() {
  progress_done
  if [[ -n "$stopped" ]]; then
    "${compose[@]}" start "$stopped" >/dev/null 2>&1 || true
  fi
  restore_flipped "$flipped" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

# ---- the stack ------------------------------------------------------------

sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -F' ' -c "$1" </dev/null; }

# Durations from docker compose config, in seconds (the Spring and Go
# formats used there: 500ms, 15s, 1m, 2h).
"${compose[@]}" config --format json > "$work/config.json"
setting() {
  python3 -I - "$work/config.json" "$1" "$2" <<'EOF'
import json, re, sys
cfg, service, name = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
v = str(cfg["services"][service]["environment"][name])
m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h)?", v)
if not m:
    sys.exit(f"{service} {name}={v!r}: not a duration")
n = float(m.group(1)) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600, None: 1}[m.group(2)]
print(f"{n:g}")
EOF
}
suspect_after="$(setting coordinator COORDINATOR_SUSPECT_AFTER)"
dead_after="$(setting coordinator COORDINATOR_DEAD_AFTER)"
grace="$(setting coordinator COORDINATOR_REPAIR_GRACE)"
interval="$(setting coordinator COORDINATOR_REPAIR_INTERVAL)"
beats="$(setting coordinator COORDINATOR_RECOVERY_BEATS)"
beat="$(setting node1 NODE_HEARTBEAT_INTERVAL)"
read_grace="$(setting coordinator COORDINATOR_GC_READ_GRACE)"
margin=15 # slack on every wait: a sweep, a pass, a slow machine

# replica_map KEY: osctl stat KEY reduced to one line per chunk and one
# column per node: H (HEALTHY, from the upload), R (HEALTHY, a repair copy),
# BAD, or "." (no replica).
replica_map() {
  "$osctl" stat "$1" 2>"$work/stat.err" > "$work/stat.out" || {
    sed 's/^/   /' "$work/stat.err"
    fail "osctl stat $1 failed"
  }
  python3 -I - "$work/stat.out" "$(liveness_states)" <<'EOF'
import sys, collections
rows = [l.split() for l in open(sys.argv[1]) if l[:1].isdigit()]
live = dict(kv.split("=") for kv in sys.argv[2].split())
nodes = sorted(live)
cells = collections.defaultdict(dict)
for r in rows:
    index, node, state, origin = int(r[0]), r[4], r[5], r[6]
    if node == "-":
        continue
    cells[index][node] = "BAD" if state == "BAD" else ("R" if origin.startswith("repair") else "H")
print("   chunk  " + "".join(f"{n:>8}" for n in nodes))
print("          " + "".join(f"{live[n]:>8}" for n in nodes))
for i in sorted({int(r[0]) for r in rows}):
    print(f"   {i:>5}  " + "".join(f"{cells[i].get(n, '.'):>8}" for n in nodes))
print("   H = replica from the upload, R = copied by repair, BAD = marked bad, . = none")
EOF
}

# healthy_on_live KEY: HEALTHY replicas of KEY's chunks on ALIVE nodes.
healthy_on_live() {
  sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
       JOIN nodes n ON n.node_id = r.node_id
       WHERE k.key = '$1' AND r.state = 'HEALTHY' AND n.state = 'ALIVE'"
}

# watch_state NODE TARGET TIMEOUT: prints NODE's state changes, with the time
# since $t0, until it reaches TARGET. The states seen go to $work/seen.
watch_state() {
  local node="$1" target="$2" timeout="$3" start=$SECONDS prev="" cur
  : > "$work/seen"
  while :; do
    cur="$(liveness_states | tr ' ' '\n' | sed -n "s/^$node=//p")"
    if [[ -n "$cur" && "$cur" != "$prev" ]]; then
      if [[ -n "$prev" ]]; then say "$(printf '%6ss  %s → %s' "$(since "$t0")" "$prev" "$cur")"; fi
      echo "$cur" >> "$work/seen"
      prev="$cur"
    fi
    [[ "$cur" == "$target" ]] && return 0
    ((SECONDS - start >= timeout)) && return 1
    sleep 0.3
  done
}

# ---- preconditions --------------------------------------------------------

[[ -x "$osctl" ]] || inconclusive "bin/osctl is missing: run make build"
LIVENESS_WAIT_TIMEOUT=30 wait_alive 4 >/dev/null || inconclusive "not all 4 nodes are ALIVE: $(liveness_states)"
if [[ "$(sql "SELECT count(*) FROM objects")" != 0 ]]; then
  inconclusive "the cluster is not fresh (it holds objects). Run: docker compose down -v && docker compose up --build --wait"
fi

echo "${b}object store demo${off}: 1 coordinator (metadata in Postgres), 4 storage nodes."
note "Shipped settings: SUSPECT after ${suspect_after}s of silence, DEAD after ${dead_after}s,"
note "repair grace ${grace}s, a repair pass every ${interval}s."

# ---- 1. upload --------------------------------------------------------------

step "Upload a file"
key="demo/photo-archive.bin"
head -c $((40 << 20)) /dev/urandom > "$work/original"
say "osctl put: 40 MiB → split into 4 MiB chunks; each chunk is sent to 3 of the 4"
say "nodes in parallel, verified and fsynced there, and committed once at least 2 confirm."
t=$(now)
osctl_run put "$work/original" "$key" || fail "the upload failed"
ok "uploaded in $(since "$t")s"
replica_map "$key"
nchunks="$(sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$key'")"
[[ "$nchunks" == 10 ]] || fail "expected 10 chunks, the coordinator has $nchunks"
[[ "$(sql "SELECT count(*) FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
           WHERE k.key = '$key' AND r.state = 'HEALTHY' AND r.origin = 'upload'")" == 30 ]] \
  || fail "not every chunk has 3 HEALTHY replicas"
sql "SELECT r.node_id, r.chunk_id FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
     WHERE k.key = '$key' ORDER BY r.node_id" > "$work/replicas"
for n in node1 node2 node3 node4; do
  ids="$(awk -v n="$n" '$1 == n {print $2}' "$work/replicas")"
  [[ -z "$ids" ]] && continue
  # shellcheck disable=SC2086 # one argument per chunk ID
  if check_chunk_files "$n" $ids | grep -qv '^OK '; then fail "a replica file on $n fails verification"; fi
done
ok "30 replicas recorded, and every replica file verifies on its node's disk"
hold

# ---- 2. stop a node ---------------------------------------------------------

step "Stop a node"
# The node to stop holds some chunks and misses at least one: step 5 needs a
# chunk with exactly 3 replicas and a free node to repair it onto. Of those,
# the one holding the most chunks, for the most repair work to watch.
read -r stopped held < <(python3 -I - "$work/replicas" <<'EOF'
import sys, collections
held = collections.Counter(line.split()[0] for line in open(sys.argv[1]) if line.strip())
fit = [(n, held[n]) for n in ("node1", "node2", "node3", "node4") if 0 < held[n] < 10]
print(*max(fit, key=lambda x: x[1]) if fit else ("", 0))
EOF
)
[[ -n "$stopped" ]] || inconclusive "no node holds some but not all chunks (odds about 4 in a million)"
say "$stopped holds $held of the 10 chunks. Stopping it: it stops sending heartbeats"
say "(every ${beat}s), and the coordinator judges it by the silence alone."
t0=$(now)
"${compose[@]}" stop "$stopped" >/dev/null 2>&1
say "$(printf '%6ss  %s stopped' "$(since "$t0")" "$stopped")"
watch_state "$stopped" DEAD "$(python3 -c "print(int($dead_after + $margin))")" \
  || fail "$stopped did not become DEAD within ${dead_after}s + ${margin}s (seen: $(tr '\n' ' ' < "$work/seen"))"
[[ "$(tr '\n' ' ' < "$work/seen")" == "ALIVE SUSPECT DEAD " ]] \
  || fail "expected ALIVE → SUSPECT → DEAD, saw: $(tr '\n' ' ' < "$work/seen")"
t_dead=$(now)
ok "$stopped went SUSPECT, then DEAD; uploads now skip it"
hold

# ---- 3. re-replication ------------------------------------------------------

step "Re-replication"
say "$stopped's copies keep counting for ${grace}s after it is DEAD: a node that is"
say "only rebooting shouldn't cause $held chunks to be copied. Then they stop counting."
while :; do
  left="$(python3 -c "print(max(0, int($grace - (__import__('time').time() - $t_dead) + 0.999)))")"
  ((left == 0)) && break
  # Off a terminal, a line every 10 s is enough.
  if [[ "$tty" == yes ]] || ((left % 10 == 0)); then progress "grace: ${left}s left"; fi
  sleep 1
done
progress_done
say "Grace over. Every ${interval}s the coordinator looks for chunks with fewer than 3"
say "counted replicas, and orders a node-to-node copy from a verified replica"
say "(the coordinator never touches the bytes):"
deadline=$((SECONDS + ${interval%.*} * 2 + margin))
: > "$work/copies"
while :; do
  # The count first, then the copies: every copy the count includes is listed.
  live="$(healthy_on_live "$key")"
  sql "SELECT c.chunk_index, r.node_id FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
       WHERE k.key = '$key' AND r.origin IN ('repair', 'repair-found') AND r.state = 'HEALTHY' ORDER BY r.created_at" > "$work/copies.now"
  if ! cmp -s "$work/copies" "$work/copies.now"; then
    progress_done
    comm -13 <(sort "$work/copies") <(sort "$work/copies.now") | sort -n | while read -r idx node; do
      say "$(printf '%6ss  chunk %s copied to %s' "$(since "$t0")" "$idx" "$node")"
    done
    cp "$work/copies.now" "$work/copies"
  fi
  progress "repair copies landed: $(wc -l < "$work/copies" | tr -d ' ') of $held"
  [[ "$live" == 30 ]] && break
  ((SECONDS >= deadline)) && { progress_done; fail "re-replication incomplete after the grace + 2 passes + ${margin}s: $live/30"; }
  sleep 0.5
done
progress_done
[[ "$(wc -l < "$work/copies" | tr -d ' ')" == "$held" ]] \
  || fail "expected $held repair copies (one per chunk $stopped held), got $(wc -l < "$work/copies" | tr -d ' ')"
while read -r idx node; do
  id="$(sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$key' AND c.chunk_index = $idx")"
  [[ "$node" != "$stopped" ]] || fail "chunk $idx was copied to the stopped node"
  [[ "$(check_chunk_files "$node" "$id")" == "OK $id" ]] || fail "the copy of chunk $idx on $node fails verification"
done < "$work/copies"
ok "every chunk is back to 3 healthy replicas, and each new copy verifies on disk"
replica_map "$key"
hold

# ---- 4. the node returns ----------------------------------------------------

step "The node comes back"
say "Starting $stopped. It must send $beats heartbeats in a row before it counts as ALIVE."
t0=$(now)
"${compose[@]}" start "$stopped" >/dev/null 2>&1
watch_state "$stopped" ALIVE "$(python3 -c "print(int(${beats%.*} * $beat + 30 + $margin))")" \
  || fail "$stopped did not return to ALIVE"
returned="$stopped"
stopped=""
over="$(sql "SELECT count(*) FROM (SELECT r.chunk_id FROM keys k JOIN chunks c USING (object_id)
             JOIN chunk_replicas r USING (chunk_id) WHERE k.key = '$key' AND r.state = 'HEALTHY'
             GROUP BY r.chunk_id HAVING count(*) = 4) x")"
[[ "$over" == "$held" ]] || fail "expected $held chunks with 4 replicas (those $returned held), got $over"
[[ "$(healthy_on_live "$key")" == $((30 + held)) ]] || fail "replicas were lost while $returned was down"
ok "$returned is ALIVE; its $held old copies verify and count again"
say "Those chunks now have 4 replicas. The extra copy is kept on purpose: removing"
say "it would be rebalancing, which this system leaves out (see DECISIONS.md)."
hold

# ---- 5. corrupt a replica ---------------------------------------------------

step "Corrupt a replica, download anyway"
read -r cidx cid < <(sql "SELECT c.chunk_index, c.chunk_id FROM keys k JOIN chunks c USING (object_id)
  WHERE k.key = '$key' AND NOT EXISTS (SELECT 1 FROM chunk_replicas r WHERE r.chunk_id = c.chunk_id AND r.node_id = '$returned')
  ORDER BY c.chunk_index LIMIT 1")
[[ -n "$cid" ]] || inconclusive "no chunk without a replica on $returned"
victim="$(sql "SELECT min(node_id) FROM chunk_replicas WHERE chunk_id = '$cid'")"
say "Flipping one byte of chunk $cidx's file on $victim's disk (a silent disk error)."
corrupt_byte "$victim" "$cid" "$offset" "$flipped" > /dev/null || fail "could not flip the byte"
say "osctl get, reading $victim's replica first:"
OSCTL_READ_ORDER=listed "$osctl" get "$key" "$work/download" 2>"$work/get.log" \
  || { grep -v ' level=INFO ' "$work/get.log" | tail -3 | sed 's/^/   /'; fail "the download failed"; }
grep -q "failing over.*chunk_id=$cid node_id=$victim\|chunk_id=$cid node_id=$victim.*failing over" "$work/get.log" \
  || fail "osctl did not fail over from $victim's replica of chunk $cidx"
say "   chunk $cidx: $victim's copy failed its SHA-256 check → read from the next replica"
grep "bad replica reported" "$work/get.log" | grep -q "node_id=$victim.*verdict=VERDICT_MARKED_BAD" \
  || fail "the bad replica was not reported and marked"
say "   reported it; the coordinator asked $victim to verify its own copy, and marked it BAD"
cmp -s "$work/original" "$work/download" || fail "the download differs from the original"
ok "the download is byte-identical to the original"
[[ "$(sql "SELECT state FROM chunk_replicas WHERE chunk_id = '$cid' AND node_id = '$victim'")" == BAD ]] \
  || fail "$victim's replica is not BAD in Postgres"
ok "$victim's replica of chunk $cidx is BAD in Postgres; reads skip it from now on"
say "Chunk $cidx is down to 2 healthy replicas; the only node without one is $returned:"
t0=$(now)
deadline=$((SECONDS + ${interval%.*} + margin))
until [[ "$(sql "SELECT state || ' ' || origin FROM chunk_replicas WHERE chunk_id = '$cid' AND node_id = '$returned'")" == "HEALTHY repair"* ]]; do
  ((SECONDS >= deadline)) && fail "chunk $cidx was not repaired onto $returned within a pass + ${margin}s"
  sleep 0.5
done
say "$(printf '%6ss  chunk %s copied to %s' "$(since "$t0")" "$cidx" "$returned")"
[[ "$(check_chunk_files "$returned" "$cid")" == "OK $cid" ]] || fail "the repair copy on $returned fails verification"
ok "chunk $cidx is back to 3 healthy replicas"
note "$victim's BAD file stays until garbage collection deletes it, ${read_grace}s later (stage 8)."
hold

# ---- 6. final state ---------------------------------------------------------

step "Final replica state (osctl stat)"
"$osctl" stat "$key" 2>/dev/null | sed -n '1,5p' | sed 's/^/   /'
replica_map "$key"
under="$(sql "SELECT count(*) FROM (SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id)
  LEFT JOIN chunk_replicas r ON r.chunk_id = c.chunk_id AND r.state = 'HEALTHY'
  WHERE k.key = '$key' GROUP BY c.chunk_id HAVING count(r.node_id) < 3) x")"
[[ "$under" == 0 ]] || fail "$under chunks have fewer than 3 healthy replicas"
sql "SELECT r.node_id, r.chunk_id FROM keys k JOIN chunks c USING (object_id) JOIN chunk_replicas r USING (chunk_id)
     WHERE k.key = '$key' AND r.state = 'HEALTHY' ORDER BY r.node_id" > "$work/healthy"
for n in node1 node2 node3 node4; do
  ids="$(awk -v n="$n" '$1 == n {print $2}' "$work/healthy")"
  [[ -z "$ids" ]] && continue
  # shellcheck disable=SC2086 # one argument per chunk ID
  if check_chunk_files "$n" $ids | grep -qv '^OK '; then fail "a HEALTHY replica file on $n fails verification"; fi
done
ok "every chunk has at least 3 healthy replicas, and every one of them verifies on disk"
"$osctl" get "$key" "$work/final" 2>/dev/null || fail "the final download failed"
cmp -s "$work/original" "$work/final" || fail "the final download differs from the original"
ok "a final download (replicas in random order) is byte-identical"
restore_flipped "$flipped" > /dev/null
check_touched "$flipped" > "$work/touched" || { sed 's/^/ /' "$work/touched"; fail "a touched file is left corrupt"; }
ok "the flipped byte is restored; the file stays BAD in Postgres (BAD is final)"

echo
echo "${b}${grn}PASS${off}"
