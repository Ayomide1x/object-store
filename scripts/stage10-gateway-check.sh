#!/usr/bin/env bash
# Stage 10 acceptance: the HTTP gateway, through curl, against the real stack.
#   1. curl round trips of 0 B, 1 B, exactly 4 MiB and 100 MiB are
#      byte-identical (through gateway1).
#   2. A PUT without Content-Length (none, or chunked) gets 411, before the
#      coordinator is asked anything; one over 5 GiB gets 413 without its body
#      being sent.
#   3. Every replica of a later chunk corrupt: curl reports a broken transfer
#      (exit 18) and has exactly the verified chunks before it; the replicas
#      are then BAD in Postgres, and the next GET is a 500 (DATA_LOSS).
#   4. Two gateways: what one stores the other serves, both ways; restarting
#      gateway1 loses nothing; both list the same keys as osctl, page by page.
#   5. The gateway matches osctl: objects move between them in both
#      directions; overwrites through either are seen by both (and a GET
#      during gateway overwrites returns the old or the new object, never a
#      mix); rm through either is seen by both, and a missing key is 404 /
#      "not found" alike; stat shows the same replicas.
#   6. GET /cluster: 4 nodes ALIVE and nothing under-replicated.
# Every flipped byte is flipped back at the end, and every touched file must be
# gone or verify.
#
# Usage: scripts/stage10-gateway-check.sh
# Needs the Compose stack up and bin/osctl built (make build).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/volume.sh"
source "$root/scripts/lib/liveness.sh"
source "$root/scripts/lib/gateway.sh"
[[ -x "$osctl" ]] || { echo "FAIL: $osctl missing: run make build"; exit 1; }
work="$(mktemp -d)"
run="$$"
offset=$((56 + 1000)) # past the 56-byte header, inside chunk data

flipped="$work/flipped"
: > "$flipped"
restore() {
  "${compose[@]}" up -d --wait >/dev/null 2>&1 || true
  restore_flipped "$flipped" >/dev/null
  rm -rf "$work"
}
trap restore EXIT

status=0
fail() { echo "  FAIL: $*"; status=1; }
ok() { echo "  ok: $*"; }
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -c "$1" </dev/null; }
# mkfile NAME BYTES (macOS head refuses a count of 0)
mkfile() { if (($2 == 0)); then : >"$work/$1"; else head -c "$2" /dev/urandom >"$work/$1"; fi; }
key() { echo "stage10/$run/$1"; }

# put_gw GATEWAY KEY FILE: PUT through a gateway; prints the HTTP status.
put_gw() {
  curl -sS -o "$work/put.json" -w '%{http_code}' -T "$3" "$(key_url "$1" "$2")" || echo "curl-exit-$?"
}
# get_gw GATEWAY KEY OUT: GET through a gateway into OUT; prints the HTTP
# status and curl's exit code, as "200 0".
get_gw() {
  local code rc=0
  code="$(curl -sS -o "$3" -D "$work/headers" -w '%{http_code}' "$(key_url "$1" "$2")" 2>"$work/curl.err")" || rc=$?
  echo "$code $rc"
}
object_id_header() { tr -d '\r' <"$work/headers" | sed -n 's/^[Xx]-[Oo]bject-[Ii]d: //p'; }
same() { cmp -s "$1" "$2"; }
# keys_gw GATEWAY PREFIX LIMIT: every key under PREFIX, following the cursor.
keys_gw() {
  python3 -I - "$1" "$2" "$3" <<'EOF'
import json, sys, urllib.parse, urllib.request
gw, prefix, limit = sys.argv[1:]
after = ""
while True:
    q = urllib.parse.urlencode({"prefix": prefix, "start_after": after, "limit": limit})
    page = json.load(urllib.request.urlopen(f"{gw}/objects?{q}", timeout=10))
    for o in page["objects"]:
        print(o["key"])
    after = page["next_start_after"]
    if not after:
        break
EOF
}
keys_osctl() { "$osctl" ls "$1" 2>/dev/null | tail -n +2 | awk '{print $1}'; }

echo "== preconditions"
wait_alive 4
wait_gateway "$GATEWAY1"
wait_gateway "$GATEWAY2"

echo "== 1. curl round trips through gateway1"
for size in 0 1 $((4 << 20)) $((100 << 20)); do
  mkfile "rt-$size" "$size"
  code="$(put_gw "$GATEWAY1" "$(key "size-$size")" "$work/rt-$size")"
  read -r gcode grc <<<"$(get_gw "$GATEWAY1" "$(key "size-$size")" "$work/rt-$size.out")"
  if [[ "$code" == 200 && "$gcode $grc" == "200 0" ]] && same "$work/rt-$size" "$work/rt-$size.out"; then
    ok "$size bytes: IDENTICAL"
  else
    fail "$size bytes: PUT $code, GET $gcode (curl exit $grc), $(same "$work/rt-$size" "$work/rt-$size.out" && echo identical || echo DIFFERENT)"
  fi
done

echo "== 2. Content-Length required, size capped"
mkfile small 100
initiated_before="$(sql "SELECT count(*) FROM objects")"
code="$(curl -sS -o /dev/null -w '%{http_code}' -X PUT -H 'Transfer-Encoding: chunked' \
  --data-binary @"$work/small" "$(key_url "$GATEWAY1" "$(key chunked)")")"
[[ "$code" == 411 ]] && ok "chunked PUT: 411" || fail "chunked PUT: got $code, want 411"
# curl always declares a length for a body, so the header-less PUT is sent raw.
# With the Host a real client sends: since stage 11 the gateway refuses any
# Host not in GATEWAY_ALLOWED_HOSTS with 403.
raw_status() {
  python3 -I - "$1" <<'EOF'
import socket, sys
s = socket.create_connection(("127.0.0.1", 9081), timeout=10)
s.sendall(sys.argv[1].encode())
print(s.recv(200).split(b"\r\n")[0].decode())
EOF
}
line="$(raw_status "PUT /objects/stage10%2F$run%2Fno-length HTTP/1.1"$'\r\n'"Host: 127.0.0.1:9081"$'\r\n\r\n')"
[[ "$line" == *" 411 "* ]] && ok "PUT without Content-Length: $line" || fail "PUT without Content-Length: got '$line', want 411"
line="$(raw_status "PUT /objects/stage10%2F$run%2Ftoo-big HTTP/1.1"$'\r\n'"Host: 127.0.0.1:9081"$'\r\n'"Content-Length: $(((5 << 30) + 1))"$'\r\n'"Expect: 100-continue"$'\r\n\r\n')"
[[ "$line" == *" 413 "* ]] && ok "PUT of 5 GiB + 1 byte: $line, before any body was sent" || fail "oversized PUT: got '$line', want 413"
initiated_after="$(sql "SELECT count(*) FROM objects")"
[[ "$initiated_after" == "$initiated_before" ]] && ok "the coordinator was asked nothing (objects rows: $initiated_after)" \
  || fail "objects rows went from $initiated_before to $initiated_after: a refused PUT reached the coordinator"

echo "== 3. every replica of a later chunk corrupt: a broken transfer"
mkfile broken $((12 << 20)) # chunks 0, 1, 2 of 4 MiB
k="$(key broken)"
code="$(put_gw "$GATEWAY1" "$k" "$work/broken")"
[[ "$code" == 200 ]] || fail "PUT of the 12 MiB object: $code"
chunk1="$(sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$k' AND c.chunk_index = 1")"
holders="$(sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$chunk1' AND state = 'HEALTHY' ORDER BY node_id" | tr '\n' ' ')"
echo "   chunk 1 is $chunk1, on: $holders"
for n in $holders; do corrupt_byte "$n" "$chunk1" "$offset" "$flipped" | sed 's/^/     /'; done
read -r gcode grc <<<"$(get_gw "$GATEWAY2" "$k" "$work/broken.out")"
got="$(wc -c <"$work/broken.out" | tr -d ' ')"
if [[ "$gcode" == 200 && "$grc" == 18 ]]; then
  ok "curl reports a broken transfer: exit 18 ($(tr '\n' ' ' <"$work/curl.err"))"
else
  fail "GET: status $gcode, curl exit $grc; want 200 then exit 18 (partial transfer)"
fi
# (macOS cmp has no byte limit: compare against the original's first 4 MiB.)
if [[ "$got" == $((4 << 20)) ]] && head -c $((4 << 20)) "$work/broken" | cmp -s - "$work/broken.out"; then
  ok "short body: exactly the verified chunk 0 ($got of $((12 << 20)) bytes), identical to the original's first 4 MiB"
else
  fail "body: $got bytes; want exactly chunk 0 ($((4 << 20)) bytes) matching the original"
fi
bad="$(sql "SELECT count(*) FROM chunk_replicas WHERE chunk_id = '$chunk1' AND state = 'BAD'")"
[[ "$bad" == 3 ]] && ok "all 3 replicas of chunk 1 are marked BAD (the gateway's reports, confirmed by the nodes)" \
  || fail "$bad replicas of chunk 1 BAD, want 3"
code="$(curl -sS -o "$work/err.json" -w '%{http_code}' "$(key_url "$GATEWAY1" "$k")")"
reason="$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["code"])' "$work/err.json" 2>/dev/null || true)"
[[ "$code" == 500 && "$reason" == DATA_LOSS ]] && ok "the next GET: 500 DATA_LOSS, before any byte" \
  || fail "next GET: $code $reason, want 500 DATA_LOSS"
"$osctl" rm "$k" 2>/dev/null || fail "rm of the broken object"

echo "== 4. two gateways, and a restart"
mkfile a $((9 << 20))
mkfile b $((5 << 20))
code="$(put_gw "$GATEWAY1" "$(key a)" "$work/a")"; [[ "$code" == 200 ]] || fail "PUT a via gateway1: $code"
code="$(put_gw "$GATEWAY2" "$(key b)" "$work/b")"; [[ "$code" == 200 ]] || fail "PUT b via gateway2: $code"
read -r gcode grc <<<"$(get_gw "$GATEWAY2" "$(key a)" "$work/a.2")"; a_id="$(object_id_header)"
same "$work/a" "$work/a.2" && ok "stored through gateway1, served by gateway2: IDENTICAL" || fail "a via gateway2: $gcode $grc, differs"
read -r gcode grc <<<"$(get_gw "$GATEWAY1" "$(key b)" "$work/b.1")"
same "$work/b" "$work/b.1" && ok "stored through gateway2, served by gateway1: IDENTICAL" || fail "b via gateway1: $gcode $grc, differs"
echo "   docker compose restart gateway1"
"${compose[@]}" restart gateway1 >/dev/null 2>&1
wait_gateway "$GATEWAY1" || fail "gateway1 did not come back"
lost=0
for gw in "$GATEWAY1" "$GATEWAY2"; do
  for f in a b; do
    read -r gcode grc <<<"$(get_gw "$gw" "$(key $f)" "$work/$f.after")"
    same "$work/$f" "$work/$f.after" || { fail "$f via $gw after the restart: $gcode $grc, differs"; lost=1; }
  done
done
get_gw "$GATEWAY1" "$(key a)" "$work/a.after" >/dev/null
[[ "$(object_id_header)" == "$a_id" ]] || { fail "a's object ID changed across the restart"; lost=1; }
((lost == 0)) && ok "after the restart both gateways serve both objects IDENTICAL, same object IDs"
k1="$(keys_gw "$GATEWAY1" "stage10/$run/" 2)"
k2="$(keys_gw "$GATEWAY2" "stage10/$run/" 3)"
ko="$(keys_osctl "stage10/$run/")"
if [[ "$k1" == "$ko" && "$k2" == "$ko" && -n "$ko" ]]; then
  ok "gateway1 (pages of 2), gateway2 (pages of 3) and osctl ls list the same $(wc -l <<<"$ko" | tr -d ' ') keys"
else
  fail "listings differ: gateway1 [$(tr '\n' ' ' <<<"$k1")] gateway2 [$(tr '\n' ' ' <<<"$k2")] osctl [$(tr '\n' ' ' <<<"$ko")]"
fi

echo "== 5. the gateway matches osctl"
mkfile c $((6 << 20))
mkfile d 12345
osctl_run put "$work/c" "$(key c)" || fail "osctl put c"
read -r gcode grc <<<"$(get_gw "$GATEWAY1" "$(key c)" "$work/c.gw")"
same "$work/c" "$work/c.gw" && ok "osctl put -> gateway GET: IDENTICAL" || fail "osctl put -> gateway GET: $gcode $grc, differs"
code="$(put_gw "$GATEWAY2" "$(key d)" "$work/d")"
osctl_run get "$(key d)" "$work/d.osctl" || true
same "$work/d" "$work/d.osctl" && ok "gateway PUT -> osctl get: IDENTICAL" || fail "gateway PUT ($code) -> osctl get differs"

kv="$(key overwritten)"
for v in 1 2 3; do mkfile "v$v" $((5 << 20)); done
osctl_run put "$work/v1" "$kv" || fail "osctl put v1"
code="$(put_gw "$GATEWAY1" "$kv" "$work/v2")"
osctl_run get "$kv" "$work/v.osctl" || true
get_gw "$GATEWAY2" "$kv" "$work/v.gw" >/dev/null
same "$work/v2" "$work/v.osctl" && same "$work/v2" "$work/v.gw" \
  && ok "overwrite through the gateway: osctl and the gateway both get the new version" \
  || fail "after a gateway overwrite (PUT $code), osctl or the gateway got another version"
osctl_run put "$work/v3" "$kv" || fail "osctl put v3"
get_gw "$GATEWAY2" "$kv" "$work/v.gw" >/dev/null
stat_id="$("$osctl" stat "$kv" 2>/dev/null | sed -n 's/^object: *//p')"
if same "$work/v3" "$work/v.gw" && [[ "$(object_id_header)" == "$stat_id" ]]; then
  ok "overwrite through osctl: the gateway gets the new version, X-Object-Id = osctl stat's object ($stat_id)"
else
  fail "after an osctl overwrite the gateway got another version, or X-Object-Id $(object_id_header) != $stat_id"
fi

# Gets during overwrites: each must be the old or the new object, never a mix.
kr="$(key racing)"
mkfile e $((9 << 20))
mkfile f $((9 << 20))
put_gw "$GATEWAY1" "$kr" "$work/e" >/dev/null
( for _ in 1 2 3 4; do put_gw "$GATEWAY1" "$kr" "$work/f" >/dev/null; put_gw "$GATEWAY1" "$kr" "$work/e" >/dev/null; done; touch "$work/writer.done" ) &
writer=$!
gets=0 mixed=0
while [[ ! -f "$work/writer.done" ]]; do
  read -r gcode grc <<<"$(get_gw "$GATEWAY2" "$kr" "$work/r.out")"
  gets=$((gets + 1))
  if [[ "$gcode $grc" != "200 0" ]] || { ! same "$work/r.out" "$work/e" && ! same "$work/r.out" "$work/f"; }; then
    mixed=$((mixed + 1))
    echo "     get $gets: status $gcode, curl exit $grc, $(wc -c <"$work/r.out" | tr -d ' ') bytes, neither version"
  fi
done
wait "$writer"
if ((mixed == 0 && gets >= 3)); then
  ok "$gets gets during 8 gateway overwrites: each was the old or the new object"
elif ((mixed == 0)); then
  fail "only $gets gets overlapped the overwrites; inconclusive, rerun"
else
  fail "$mixed of $gets gets during overwrites returned neither version"
fi

code="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$(key_url "$GATEWAY1" "$(key c)")")"
err="$( { "$osctl" get "$(key c)" "$work/gone" 2>&1 || true; } | { grep 'osctl:' || true; } )"
gcode="$(curl -sS -o /dev/null -w '%{http_code}' "$(key_url "$GATEWAY2" "$(key c)")")"
if [[ "$code" == 200 && "$err" == *"not found"* && "$gcode" == 404 ]] && ! keys_osctl "stage10/$run/" | grep -qx "$(key c)" \
   && ! keys_gw "$GATEWAY2" "stage10/$run/" 100 | grep -qx "$(key c)"; then
  ok "rm through the gateway (200): osctl get says not found, GET is 404, gone from both listings"
else
  fail "rm through the gateway: DELETE $code; osctl: '$err'; GET $gcode"
fi
osctl_run rm "$(key d)" || fail "osctl rm d"
c1="$(curl -sS -o /dev/null -w '%{http_code}' "$(key_url "$GATEWAY1" "$(key d)")")"
c2="$(curl -sS -o /dev/null -w '%{http_code}' "$(key_url "$GATEWAY2" "$(key d)")")"
[[ "$c1 $c2" == "404 404" ]] && ok "rm through osctl: GET is 404 on both gateways" || fail "after osctl rm: GETs $c1 $c2, want 404 404"
code="$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$(key_url "$GATEWAY1" "$(key never-existed)")")"
err="$( { "$osctl" rm "$(key never-existed)" 2>&1 || true; } | { grep 'osctl:' || true; } )"
[[ "$code" == 404 && "$err" == *"key not found"* ]] && ok "rm of a missing key: 404 and osctl's 'key not found' alike" \
  || fail "rm of a missing key: DELETE $code, osctl '$err'"

curl -sS -o "$work/stat.json" "$(key_url "$GATEWAY1" "$kv")/stat"
stat_gw="$(python3 -I - "$work/stat.json" <<'EOF'
import json, sys
s = json.load(open(sys.argv[1]))
print(s["object_id"], *sorted("%d:%s:%s" % (c["index"], r["node_id"], r["state"]) for c in s["chunks"] for r in c["replicas"]))
EOF
)"
stat_os="$("$osctl" stat "$kv" 2>/dev/null | awk '/^object:/ {id=$2} /^[0-9]/ {print $1":"$5":"$6}' | sort | tr '\n' ' ')"
stat_os="$stat_id ${stat_os% }"
[[ "$stat_gw" == "$stat_os" ]] && ok "stat through the gateway shows the same object and replicas as osctl stat" \
  || fail "stat differs: gateway [$stat_gw] osctl [$stat_os]"

echo "== 6. GET /cluster"
start=$SECONDS
while :; do
  seen="$(cluster_summary "$GATEWAY2")"
  [[ "$seen" == *"under=0" && "$({ grep -o '=ALIVE' <<<"$seen" || true; } | wc -l | tr -d ' ')" == 4 ]] && break
  ((SECONDS - start >= 30)) && break
  sleep 1
done
[[ "$seen" == *"under=0" && "$seen" == *node4=ALIVE* ]] && ok "/cluster: $seen" || fail "/cluster: $seen, want 4 ALIVE and under=0"

echo "== restoring flipped bytes"
restore_flipped "$flipped"
check_touched "$flipped" || status=1

if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
