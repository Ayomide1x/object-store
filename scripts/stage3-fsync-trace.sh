#!/usr/bin/env bash
# Stage 3 check on the real node binary: every chunk write makes its system
# calls in the order that makes it crash-safe:
#
#   fsync(temp file) -> linkat(temp, chunks/<id>) -> fsync(chunks dir) -> unlinkat(temp)
#
# The Go test TestCommitDurabilityOrder checks the same order through a test
# seam. This checks the binary actually running in the container, by attaching
# strace from a side container that shares node1's PID namespace. Since stage 4
# a chunk goes to 3 of 4 nodes, so the number of links expected on node1 is
# read from chunk_placements after the upload.
#
# What it cannot show: that fsync is sufficient on this disk. That needs
# power-loss testing, which is out of scope.
#
# Usage: scripts/stage3-fsync-trace.sh
# Needs the Compose stack up and bin/osctl built (make build). Pulls alpine and
# installs strace into it, so needs network access the first time.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
source "$root/scripts/lib/osctl.sh"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/liveness.sh"
# up --wait means the coordinator is serving; uploads need ALIVE nodes.
wait_alive 4
work="$(mktemp -d)"
tracer="stage3-fsync-trace-$$"

cleanup() {
  docker rm -f "$tracer" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

node="$("${compose[@]}" ps -q node1)"
docker run -d --name "$tracer" --pid "container:$node" --cap-add SYS_PTRACE -v "$work:/out" alpine:3 \
  sh -c 'apk add -q strace >/dev/null && exec strace -f -p 1 -e trace=openat,close,fsync,linkat,unlinkat -o /out/trace' >/dev/null
until docker logs "$tracer" 2>&1 | grep -q "attached"; do
  if [[ "$(docker inspect -f '{{.State.Running}}' "$tracer")" != "true" ]]; then
    echo "strace container exited:"; docker logs "$tracer"; exit 1
  fi
  sleep 0.2
done

# 12 MiB: three chunks, of which node1 is placed on two or three.
head -c $((12 << 20)) /dev/urandom > "$work/payload"
osctl_run put "$work/payload" "fsync-trace/$$"
docker stop "$tracer" >/dev/null # SIGTERM: strace detaches and flushes
expected="$("${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tAc \
  "SELECT count(*) FROM chunk_placements p JOIN chunks c USING (chunk_id) JOIN objects o USING (object_id)
   WHERE o.key = 'fsync-trace/$$' AND p.node_id = 'node1'")"
echo "node1 is placed on $expected chunks of this upload"

python3 -I - "$work/trace" "$expected" <<'EOF'
import re, sys

full = re.compile(r'^(\d+)\s+(\w+)\((.*)\)\s+=\s+(-?\d+)')
unfinished = re.compile(r'^(\d+)\s+(\w+)\((.*) <unfinished \.\.\.>$')
resumed = re.compile(r'^(\d+)\s+<\.\.\. (\w+) resumed>(.*)\)\s+=\s+(-?\d+)')

# Events in the order the calls started: (name, args, return value).
events, pending = [], {}
for line in open(sys.argv[1]):
    line = line.rstrip("\n")
    if m := full.match(line):
        events.append([m[2], m[3], int(m[4])])
    elif m := unfinished.match(line):
        pending[m[1]] = len(events)
        events.append([m[2], m[3], None])
    elif m := resumed.match(line):
        i = pending.pop(m[1], None)
        if i is not None:
            events[i][1] += m[3]
            events[i][2] = int(m[4])

def path_of(args):
    m = re.search(r'"([^"]*)"', args)
    return m[1] if m else None

# Resolve fsync(fd) to a path by tracking openat/close.
fds, timeline = {}, []
for name, args, ret in events:
    if name == "openat" and ret is not None and ret >= 0:
        fds[ret] = path_of(args)
    elif name == "close":
        fds.pop(int(args.split(",")[0]), None)
    elif name == "fsync":
        timeline.append(("fsync", fds.get(int(args.split(",")[0])), ret))
    elif name == "linkat":
        paths = re.findall(r'"([^"]*)"', args)
        timeline.append(("link", tuple(paths), ret))
    elif name == "unlinkat":
        timeline.append(("unlink", path_of(args), ret))

links = [(i, e) for i, e in enumerate(timeline) if e[0] == "link" and e[1][1].startswith("/data/chunks/")]
if not links:
    print("FAIL: no linkat into /data/chunks was traced")
    sys.exit(1)

ok = True
for i, (_, (temp, final), ret) in links:
    chunk = final.rsplit("/", 1)[1]
    synced = any(e == ("fsync", temp, 0) for e in timeline[:i])
    dir_sync = next((j for j in range(i + 1, len(timeline)) if timeline[j] == ("fsync", "/data/chunks", 0)), None)
    unlinked = dir_sync is not None and any(e == ("unlink", temp, 0) for e in timeline[dir_sync + 1:])
    steps = [("fsync(temp) before link", synced), ("link succeeded", ret == 0),
             ("fsync(/data/chunks) after link", dir_sync is not None), ("unlink(temp) after dir fsync", unlinked)]
    print(f"chunk {chunk}:")
    for label, good in steps:
        print(f"  {'ok  ' if good else 'FAIL'} {label}")
        ok &= good
expected = int(sys.argv[2])
print("PASS" if ok and len(links) == expected else
      f"FAIL ({len(links)} chunk links traced, expected {expected})" if ok else "FAIL")
sys.exit(0 if ok and len(links) == expected else 1)
EOF
