#!/usr/bin/env bash
# Stage 3 acceptance, adapted to replication (stage 4): SIGKILL one node in
# the middle of a 100 MiB upload, then show that no partial file exists under
# any final chunk name on it, and that its stray temp files are gone after a
# restart. Since stage 4 the upload itself survives: the other replicas carry
# it, and the killed node is not recorded for what it never finished.
#
# Steps:
#   a. Start a watcher container that busy-waits until a temp file appears in
#      node1's /data/tmp (a chunk write in progress), then SIGKILLs node1
#      through the Docker socket. Polling from inside a container keeps the
#      delay between "temp seen" and "kill" to milliseconds.
#   b. Start a 100 MiB put. With 2 of 3 replicas still writing, it must commit.
#   c. With node1 down: the temp files left behind are the evidence the kill
#      landed mid-write. None means the kill missed, including a kill that
#      landed after the upload finished: inconclusive, and the script fails.
#   d. Check every file under node1's /data/chunks: magic, chunk ID and
#      SHA-256 all valid. No partial file under any final name.
#   e. Restart node1 and wait for its healthcheck. The node reports healthy
#      only after it has cleaned its temps and opened its listener.
#   f. /data/tmp must now be empty.
#   g. The node must have logged removing each temp file from step c.
#   h. The object is COMMITTED. Node1 is recorded for no chunk from the
#      interrupted one onwards (it never finished them), every chunk has at
#      least 2 replicas, and get returns identical bytes.
#
# Usage: scripts/stage3-crash-check.sh
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
watcher="stage3-crash-watcher-$$"
key="crash-check/big-$$"

cleanup() {
  docker rm -f "$watcher" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

status=0
fail() { echo "  FAIL: $*"; status=1; }

# Runs a shell snippet in alpine with node1's /data mounted (works while node1 is stopped).
on_volume() { docker run --rm --volumes-from "$node" alpine:3 sh -c "$1"; }

node="$("${compose[@]}" ps -q node1)"
head -c $((100 << 20)) /dev/urandom > "$work/big"

echo "a. starting watcher"
docker run -d --name "$watcher" \
  -v /var/run/docker.sock:/var/run/docker.sock --volumes-from "$node" \
  docker:cli sh -c "
    echo watching
    until ls /data/tmp/chunk-* >/dev/null 2>&1; do :; done
    docker kill -s KILL $node >/dev/null
    echo killed" >/dev/null
until docker logs "$watcher" 2>&1 | grep -q watching; do sleep 0.2; done

echo "b. uploading 100 MiB to $key"
if "$osctl" put "$work/big" "$key" 2>"$work/put.err"; then
  echo "  put committed with node1 killed mid-upload"
else
  fail "the upload failed:"
  grep -v ' level=INFO ' "$work/put.err" | tail -5 | sed 's/^/    /'
fi
docker wait "$watcher" >/dev/null
docker logs "$watcher" 2>&1 | grep -q killed || fail "watcher did not kill node1"
echo "  node1 state: $(docker inspect -f '{{.State.Status}} (exit {{.State.ExitCode}})' "$node")"

echo "c. temp files left by the kill:"
temps="$(on_volume 'ls /data/tmp')"
if [[ -z "$temps" ]]; then
  fail "no temp file: the kill landed between chunk writes or after the upload; inconclusive, rerun"
else
  sed 's/^/     /' <<<"$temps"
fi

echo "d. verifying every final chunk file"
check="$(on_volume '
  n=0; bad=0
  for f in /data/chunks/*; do
    [ -e "$f" ] || continue
    n=$((n + 1)); id=$(basename "$f")
    magic=$(head -c 8 "$f")
    hid=$(dd if="$f" bs=1 skip=8 count=16 2>/dev/null | od -An -tx1 -v | tr -d " \n")
    hsum=$(dd if="$f" bs=1 skip=24 count=32 2>/dev/null | od -An -tx1 -v | tr -d " \n")
    dsum=$(tail -c +57 "$f" | sha256sum | cut -d" " -f1)
    if [ "$magic" != OSCHUNK1 ] || [ "$hid" != "$(echo "$id" | tr -d -)" ] || [ "$hsum" != "$dsum" ]; then
      echo "BAD $id"; bad=$((bad + 1))
    fi
  done
  echo "checked=$n bad=$bad"')"
sed 's/^/     /' <<<"$check"
grep -q 'bad=0$' <<<"$check" || fail "a final chunk file is partial or corrupt"

echo "e. restarting node1"
if "${compose[@]}" up -d --wait node1 >/dev/null 2>&1; then
  echo "  node1 healthy (temps cleaned, listener open)"
else
  fail "node1 did not become healthy after restart"
fi
# The get in step h reads node1's replicas: wait until the coordinator sees it.
wait_alive 1 node1 || fail "node1 not ALIVE after restart"

echo "f. temp directory after restart:"
left="$(on_volume 'ls /data/tmp')"
if [[ -z "$left" ]]; then
  echo "     (empty)"
else
  sed 's/^/     /' <<<"$left"
  fail "stray temp files survived the restart"
fi

echo "g. node1 log, for each temp file from step c:"
logs="$("${compose[@]}" logs node1)"
while IFS= read -r temp; do
  [[ -n "$temp" ]] || continue
  # Match this run's temp file by name, not any earlier removal.
  if line="$(grep "removed stray temp" <<<"$logs" | grep -F "\"file\":\"$temp\"")"; then
    sed 's/^/     /' <<<"$line"
  else
    fail "node1 did not log removing $temp"
  fi
done <<<"$temps"

echo "h. metadata for the upload:"
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -F'|' -c "$1"; }
obj="$(sql "SELECT object_id FROM objects WHERE key = '$key'")"
state="$(sql "SELECT status FROM objects WHERE object_id = '$obj'")"
echo "     objects.status = ${state:-<no row>}"
[[ "$state" == COMMITTED ]] || fail "expected COMMITTED"
# The chunk node1 was writing when it died: from the first temp file's name.
inflight="$(sed -n 's/^chunk-\([0-9a-f-]\{36\}\)-.*/\1/p' <<<"$temps" | head -1)"
if [[ -n "$inflight" ]]; then
  idx="$(sql "SELECT chunk_index FROM chunks WHERE chunk_id = '$inflight'")"
  # origin = 'upload': a later repair may legitimately copy onto node1 (stage 7).
  late="$(sql "SELECT count(*) FROM chunk_replicas r JOIN chunks c USING (chunk_id)
               WHERE c.object_id = '$obj' AND r.node_id = 'node1' AND c.chunk_index >= $idx AND r.origin = 'upload'")"
  echo "     node1 was writing chunk $idx; node1 replicas recorded from chunk $idx on: $late"
  [[ "$late" == 0 ]] || fail "node1 is recorded for a chunk it never finished"
fi
short="$(sql "SELECT count(*) FROM (SELECT c.chunk_id FROM chunks c LEFT JOIN chunk_replicas r USING (chunk_id)
              WHERE c.object_id = '$obj' GROUP BY c.chunk_id HAVING count(r.node_id) < 2) t")"
[[ "$short" == 0 ]] && echo "     every chunk has >= 2 replicas" || fail "$short chunks have fewer than 2 replicas"
if osctl_run get "$key" "$work/big.out" && cmp -s "$work/big" "$work/big.out"; then
  echo "     get: IDENTICAL"
else
  fail "get did not return identical bytes"
fi

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
