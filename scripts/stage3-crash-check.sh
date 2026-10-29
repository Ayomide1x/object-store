#!/usr/bin/env bash
# Stage 3 acceptance: SIGKILL the node in the middle of a 100 MiB upload, then
# show that no partial file exists under any final chunk name, and that the
# stray temp files are gone after a restart.
#
# Steps:
#   a. Start a watcher container that busy-waits until a temp file appears in
#      node1's /data/tmp (a chunk write in progress), then SIGKILLs node1
#      through the Docker socket. Polling from inside a container keeps the
#      delay between "temp seen" and "kill" to milliseconds.
#   b. Start a 100 MiB put. It must fail.
#   c. With the node down: the temp files left behind are the evidence the
#      kill landed mid-write. None at all means the kill missed: inconclusive.
#   d. Check every file under /data/chunks: magic, chunk ID and SHA-256 all
#      valid. No partial file under any final name.
#   e. Restart node1. A successful small put proves it is listening, and the
#      node cleans temps before it starts listening.
#   f. /data/tmp must now be empty.
#   g. The node must have logged removing each temp file from step c.
#   h. The interrupted object must still be PENDING and its key must not
#      resolve. If the kill landed after the upload finished, the object is
#      COMMITTED and this step fails the script.
#
# Usage: scripts/stage3-crash-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
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
  fail "the upload succeeded: the node was never killed mid-upload"
else
  echo "  put failed as expected: $(tail -1 "$work/put.err")"
fi
docker wait "$watcher" >/dev/null
docker logs "$watcher" 2>&1 | grep -q killed || fail "watcher did not kill node1"
echo "  node1 state: $(docker inspect -f '{{.State.Status}} (exit {{.State.ExitCode}})' "$node")"

echo "c. temp files left by the kill:"
temps="$(on_volume 'ls /data/tmp')"
if [[ -z "$temps" ]]; then
  fail "no temp file: the kill landed between chunk writes; inconclusive, rerun"
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
"${compose[@]}" up -d --wait node1 >/dev/null 2>&1
head -c 1000 /dev/urandom > "$work/probe"
if "$osctl" put "$work/probe" "crash-check/probe-$$" 2>/dev/null; then
  echo "  probe put succeeded: node1 is serving"
else
  fail "probe put after restart failed"
fi

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

echo "h. metadata for the interrupted upload:"
state="$("${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tAc \
  "SELECT status FROM objects WHERE key = '$key'")"
echo "     objects.status = ${state:-<no row>}"
if [[ "$state" != "PENDING" ]]; then
  fail "expected PENDING. COMMITTED means the kill landed after the upload finished, so this run proves nothing"
fi
if "$osctl" get "$key" "$work/big.out" 2>"$work/get.err"; then
  fail "the interrupted key resolved"
elif grep -q NotFound "$work/get.err"; then
  echo "     get: NotFound (key does not resolve)"
else
  fail "get failed, but not with NotFound: $(tail -1 "$work/get.err")"
fi

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
