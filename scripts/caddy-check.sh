#!/usr/bin/env bash
# Local check of the Caddy behaviour the deployment relies on (DEPLOY.md),
# with deploy/objectstore.caddy exactly as deployed, against the stack running
# with the production override. As on the VM: Caddy (the VM's version, host
# networking) proxies to the gateways' published loopback ports; only the
# certificate differs (Caddy's local CA instead of ACME). The clients also
# use host networking and resolve objectstore.ayomideo.dev to 127.0.0.1.
#   1. Basic auth: no password, or a wrong one, gets 401 on every route,
#      /healthz included; the right one gets through.
#   2. Host and Origin pass through unchanged: a same-origin PUT (Sec-Fetch-Site,
#      or Origin alone) succeeds, a foreign Origin gets 403 CROSS_ORIGIN.
#   3. %2F in a key reaches the gateway still encoded: the key keeps its "/",
#      " " and "%", and a raw "/" is still refused as two segments.
#   4. A PUT over 5 GiB gets 413, and the coordinator is asked nothing.
#   5. A download broken mid-stream (every replica of chunk 1 corrupt) fails
#      at the client over HTTP/1.1, HTTP/2 and HTTP/3: status 200, then an
#      error, never a clean end, and the body is a prefix of chunk 0.
#   6. Basic auth's cost, with the hash made as DEPLOY.md makes it (bcrypt,
#      cost 10): the first request with the password (a full hash check),
#      repeated requests with it (the UI polls about once a second per open
#      tab; FAIL above 50 ms), and a new wrong password each time (each a full
#      check: what a password-guessing client costs Caddy's CPU; FAIL above
#      100 ms).
#   7. A 512 MiB upload at 2 MB/s over HTTP/2 (about 4.5 minutes, longer than
#      any default timeout) succeeds; its first chunks are written on the nodes
#      long before it ends (streamed, not buffered), Caddy's memory stays flat,
#      and it downloads identical.
# Flipped bytes are flipped back at the end and every touched file must be
# gone or verify. Objects are removed.
#
# Usage: OBJECTSTORE_DB_PASSWORD=... scripts/caddy-check.sh
# Needs the stack up with the production override (project "objectstore"),
# built from this tree:
#   docker compose -p objectstore -f docker-compose.yml -f docker-compose.prod.yml up -d --build --wait
# and ports 80 and 443 (TCP, and UDP 443) free on the Docker host. Pulls
# caddy:2.11.7, httpd:2.4.69 (htpasswd), alpine:3 and golang:1.27 (for an HTTP/3 client built with
# quic-go v0.54.0), so it needs the network.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
: "${OBJECTSTORE_DB_PASSWORD:?the production override needs OBJECTSTORE_DB_PASSWORD}"
compose=(docker compose -p objectstore -f "$root/docker-compose.yml" -f "$root/docker-compose.prod.yml")
source "$root/scripts/lib/volume.sh"
CADDY_IMAGE=caddy:2.11.7
HTPASSWD_IMAGE=httpd:2.4.69
NAME=objectstore.ayomideo.dev
work="$(mktemp -d)"
run="$$"
caddy="caddycheck-caddy-$run"
client="caddycheck-client-$run"
offset=$((56 + 1000)) # past the 56-byte header, inside chunk data
flipped="$work/flipped"
: > "$flipped"
keys=()

status=0
fail() { echo "  FAIL: $*"; status=1; }
source "$root/scripts/lib/outcome.sh"
ok() { echo "  ok: $*"; }
sql() { "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -c "$1" </dev/null; }
mkfile() { head -c "$2" /dev/urandom >"$work/$1"; }
# enc KEY: the key as one path segment ("/" " " "%" encoded).
enc() { python3 -I -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }
url() { echo "https://$NAME/objects/$(enc "$1")"; }
# c ARGS...: curl in the client container, with Caddy's root as the only CA.
c() { docker exec "$client" curl -sS --cacert /w/root.crt "$@"; }
auth=()

cleanup() {
  local k
  for k in "${keys[@]+"${keys[@]}"}"; do
    c "${auth[@]}" -o /dev/null -X DELETE "$(url "$k")" 2>/dev/null || true
  done
  restore_flipped "$flipped" >/dev/null 2>&1 || true
  docker rm -f "$caddy" "$client" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT

echo "== setup"
"${compose[@]}" ps --format '{{.Service}}' | grep -qx gateway1 || { echo "FAIL: the production stack is not up"; exit 1; }
pw="$(head -c 24 /dev/urandom | base64 | tr -d '/+=')"
# As DEPLOY.md makes it: bcrypt at cost 10, the password on stdin.
hash="$(printf '%s' "$pw" | docker run --rm -i "$HTPASSWD_IMAGE" htpasswd -niBC 10 check | head -1 | cut -d: -f2-)"
echo "   $CADDY_IMAGE; password hash: ${hash:0:7}... (${#hash} characters)"
{
  printf '{\n\tlocal_certs\n\tskip_install_trust\n}\n\n'
  cat "$root/deploy/objectstore.caddy"
} >"$work/Caddyfile"
docker run -d --name "$caddy" --network host -e OBJECTSTORE_USER=check -e "OBJECTSTORE_PASSWORD_HASH=$hash" \
  -v "$work/Caddyfile:/etc/caddy/Caddyfile:ro" "$CADDY_IMAGE" >/dev/null
docker run -d --name "$client" --network host --add-host "$NAME:127.0.0.1" -v "$work:/w" alpine:3 sleep 3600 >/dev/null
docker exec "$client" apk add -q curl >/dev/null
for _ in $(seq 30); do
  docker cp "$caddy:/data/caddy/pki/authorities/local/root.crt" "$work/root.crt" >/dev/null 2>&1 && break
  sleep 1
done
[[ -s "$work/root.crt" ]] || { docker logs "$caddy" | tail -5; echo "FAIL: Caddy did not start"; exit 1; }
auth=(-u "check:$pw")
for _ in $(seq 30); do
  [[ "$(c -o /dev/null -w '%{http_code}' "https://$NAME/healthz" 2>/dev/null || true)" == 401 ]] && break
  sleep 1
done
mkdir -p "$work/h3"
cat >"$work/h3/main.go" <<'GO'
// h3get downloads a URL over HTTP/3 only and reports how it ended:
// "status=N bytes=N err=..." and exit 0 only on a clean end of body.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/quic-go/quic-go/http3"
)

func main() {
	url, user, pass, caFile, out := os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5]
	pem, err := os.ReadFile(caFile)
	if err != nil {
		panic(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	tr := &http3.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	defer tr.Close()
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.SetBasicAuth(user, pass)
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		fmt.Printf("status=0 bytes=0 err=%v\n", err)
		os.Exit(1)
	}
	f, _ := os.Create(out)
	n, err := io.Copy(f, resp.Body)
	f.Close()
	fmt.Printf("status=%d proto=%s bytes=%d err=%v\n", resp.StatusCode, resp.Proto, n, err)
	if err != nil {
		os.Exit(1)
	}
}
GO
docker run --rm -v "$work/h3:/src" -w /src -e CGO_ENABLED=0 golang:1.27 sh -c \
  'go mod init h3get && go get github.com/quic-go/quic-go@v0.54.0 && go mod tidy && go build -o h3get .' >/dev/null 2>&1 \
  || { echo "FAIL: could not build the HTTP/3 client"; exit 1; }
echo "   Caddy up; HTTP/3 client built"
# The first request with the password, before any other: its hash check can't
# have been cached.
cold="$(c "${auth[@]}" -o /dev/null -w '%{time_total}' "https://$NAME/healthz" | awk '{ printf "%.1f", $1 * 1000 }')"

echo "== 1. basic auth"
for path in /healthz /cluster /objects /; do
  code="$(c -o /dev/null -w '%{http_code}' "https://$NAME$path")"
  wrong="$(c -o /dev/null -w '%{http_code}' -u "check:wrong" "https://$NAME$path")"
  right="$(c "${auth[@]}" -o /dev/null -w '%{http_code}' "https://$NAME$path")"
  [[ "$code" == 401 && "$wrong" == 401 && "$right" == 200 ]] \
    && ok "$path: no password $code, wrong password $wrong, right password $right" \
    || fail "$path: no password $code, wrong $wrong, right $right; want 401, 401, 200"
done

echo "== 2. Host and Origin reach the gateway unchanged"
echo data >"$work/small"
k="caddycheck/$run/origin"; keys+=("$k")
code="$(c "${auth[@]}" -o /dev/null -w '%{http_code}' -T /w/small -H 'Sec-Fetch-Site: same-origin' -H "Origin: https://$NAME" "$(url "$k")")"
[[ "$code" == 200 ]] && ok "PUT with Sec-Fetch-Site same-origin: $code" || fail "same-origin PUT: $code, want 200"
code="$(c "${auth[@]}" -o /dev/null -w '%{http_code}' -T /w/small -H "Origin: https://$NAME" "$(url "$k")")"
[[ "$code" == 200 ]] && ok "PUT with only a same-host Origin: $code" || fail "Origin-only PUT: $code, want 200"
code="$(c "${auth[@]}" -o "/w/x.json" -w '%{http_code}' -T /w/small -H 'Origin: https://evil.example' "$(url "$k")")"
[[ "$code" == 403 ]] && grep -q CROSS_ORIGIN "$work/x.json" && ok "PUT with a foreign Origin: $code CROSS_ORIGIN" \
  || fail "foreign-Origin PUT: $code $(cat "$work/x.json"), want 403 CROSS_ORIGIN"

echo "== 3. %2F stays encoded"
k="caddycheck/$run/dir/a b%c.bin"; keys+=("$k")
code="$(c "${auth[@]}" -o /dev/null -w '%{http_code}' -T /w/small "$(url "$k")")"
[[ "$code" == 200 ]] || fail "PUT $(url "$k"): $code"
got="$(c "${auth[@]}" "$(url "$k")/stat" | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["key"])')"
[[ "$got" == "$k" ]] && ok "PUT $(url "$k") stored the key \"$got\"" || fail "stored key \"$got\", want \"$k\""
listed="$(c "${auth[@]}" "https://$NAME/objects?prefix=$(enc "caddycheck/$run/dir/")" \
  | python3 -I -c 'import json,sys; print(",".join(o["key"] for o in json.load(sys.stdin)["objects"]))')"
[[ "$listed" == "$k" ]] && ok "prefix listing finds it" || fail "prefix listing: \"$listed\""
code="$(c "${auth[@]}" -o "/w/x.json" -w '%{http_code}' "https://$NAME/objects/caddycheck/$run/dir/a%20b%25c.bin")"
[[ "$code" == 400 ]] && grep -q BAD_KEY "$work/x.json" && ok "a raw \"/\" is still two segments: $code BAD_KEY" \
  || fail "raw-slash GET: $code, want 400 BAD_KEY"

echo "== 4. a PUT over 5 GiB"
before="$(sql "SELECT count(*) FROM objects")"
head -c $((1 << 20)) /dev/zero >"$work/onemeg"
read -r code sent <<<"$(c "${auth[@]}" --http1.1 -o /w/x.json -w '%{http_code} %{size_upload}' --max-time 20 -X PUT \
  -H "Content-Length: $(((5 << 30) + 1))" -H 'Expect: 100-continue' --data-binary @/w/onemeg \
  "$(url "caddycheck/$run/too-big")" 2>/dev/null || true)"
after="$(sql "SELECT count(*) FROM objects")"
[[ "$code" == 413 ]] && ok "413 ($sent body bytes sent by curl)" || fail "oversized PUT: $code, want 413"
[[ "$before" == "$after" ]] && ok "the coordinator was asked nothing" || fail "objects rows $before -> $after"

echo "== 5. a download broken mid-stream"
mkfile broken $((12 << 20)) # chunks 0, 1, 2 of 4 MiB
head -c $((4 << 20)) "$work/broken" >"$work/chunk0"
# broken_object PROTO: a fresh object with every replica of chunk 1 corrupt; prints its key.
broken_object() {
  local k="caddycheck/$run/broken-$1" chunk holders n
  keys+=("$k")
  [[ "$(c "${auth[@]}" -o /dev/null -w '%{http_code}' -T /w/broken "$(url "$k")")" == 200 ]] || { echo "PUT failed"; return 1; }
  chunk="$(sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$k' AND c.chunk_index = 1")"
  holders="$(sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$chunk' AND state = 'HEALTHY'")"
  for n in $holders; do corrupt_byte "$n" "$chunk" "$offset" "$flipped" >/dev/null; done
  echo "$k"
}
# prefix_of_chunk0 FILE: FILE is no longer than chunk 0 and matches its start.
prefix_of_chunk0() {
  local n
  n="$(wc -c <"$1" | tr -d ' ')"
  ((n <= 4 << 20)) && head -c "$n" "$work/chunk0" | cmp -s - "$1"
}
for proto in http1.1 http2; do
  k="$(broken_object "$proto")"
  rc=0
  code="$(c "${auth[@]}" "--$proto" -o "/w/out-$proto" -w '%{http_code} %{http_version}' "$(url "$k")" 2>"$work/err-$proto")" || rc=$?
  got="$(wc -c <"$work/out-$proto" | tr -d ' ')"
  if [[ "$code" == "200 "* && $rc != 0 ]] && prefix_of_chunk0 "$work/out-$proto"; then
    ok "$proto: status ${code% *} over HTTP/${code#* }, then curl exit $rc ($(tr '\n' ' ' <"$work/err-$proto")), $got of $((12 << 20)) bytes, a prefix of chunk 0"
  else
    fail "$proto: status $code, curl exit $rc, $got bytes; want 200, a non-zero exit and a prefix of chunk 0"
  fi
done
k="$(broken_object http3)"
cp "$work/h3/h3get" "$work/h3get"
rc=0
line="$(docker exec "$client" /w/h3get "$(url "$k")" check "$pw" /w/root.crt /w/out-h3)" || rc=$?
got="$(wc -c <"$work/out-h3" | tr -d ' ')"
if [[ "$line" == "status=200 proto=HTTP/3"* && $rc != 0 ]] && prefix_of_chunk0 "$work/out-h3"; then
  ok "http3: $line, a prefix of chunk 0"
else
  fail "http3: $line (exit $rc); want status 200 over HTTP/3, an error and a prefix of chunk 0"
fi

echo "== 6. basic auth's cost per request"
# avg_ms N URL ARGS...: the mean time of N requests on one connection, in ms.
avg_ms() {
  local n="$1" urls=() i
  shift
  for i in $(seq "$n"); do urls+=("$1"); done
  shift
  docker exec "$client" curl -sS --cacert /w/root.crt "$@" -o /dev/null -w '%{time_total}\n' "${urls[@]}" \
    | awk '{ s += $1 } END { printf "%.1f", s / NR * 1000 }'
}
direct="$(avg_ms 20 http://127.0.0.1:9081/healthz -H "Host: $NAME")"
noauth="$(avg_ms 20 "https://$NAME/healthz")"
authed="$(avg_ms 20 "https://$NAME/healthz" "${auth[@]}")"
# Caddy caches each password's result, so each guess is a new password.
wrong="$(for i in 1 2 3 4 5; do
  c -o /dev/null -w '%{time_total}\n' -u "check:guess-$run-$i" "https://$NAME/healthz"
done | awk '{ s += $1 } END { printf "%.1f", s / NR * 1000 }')"
echo "   per request: gateway direct $direct ms; through Caddy: no password (401) $noauth ms,"
echo "   first with the password $cold ms, repeated with it $authed ms, a new wrong password each time (401) $wrong ms"
if awk -v a="$authed" 'BEGIN { exit !(a <= 50) }'; then
  ok "repeated requests with the password take $authed ms: fine for the UI's polling"
else
  fail "repeated requests with the password take $authed ms (over 50 ms): too slow for the UI's polling"
fi
if awk -v w="$wrong" 'BEGIN { exit !(w <= 100) }'; then
  ok "a wrong password costs $wrong ms: bcrypt at cost 10"
else
  fail "a wrong password costs $wrong ms (over 100 ms): each guess holds a core that long"
fi

echo "== 7. a 512 MiB upload at 2 MB/s over HTTP/2"
mkfile big $((512 << 20))
k="caddycheck/$run/big"; keys+=("$k")
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
t0=$SECONDS
docker exec "$client" sh -c "curl -sS --cacert /w/root.crt -u 'check:$pw' --http2 --limit-rate 2M -o /w/big.json \
  -w '%{http_code} %{http_version}' -T /w/big '$(url "$k")' >/w/big.code 2>/w/big.err; echo \$? >/w/big.rc" &
upload=$!
first="" maxmem=0
written() { "${compose[@]}" logs --since "$since" node1 node2 node3 node4 2>/dev/null | grep -c '"chunk written"' || true; }
while kill -0 "$upload" 2>/dev/null; do
  sleep 10
  n="$(written)"
  mem="$(docker stats --no-stream --format '{{.MemUsage}}' "$caddy" | python3 -I -c '
import re, sys
v, u = re.match(r"([\d.]+)\s*([KMG]i?B)", sys.stdin.read()).groups()
print(int(float(v) * {"KiB": 1 / 1024, "kB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}[u]))')"
  ((mem > maxmem)) && maxmem=$mem
  [[ -z "$first" && "$n" -gt 0 ]] && first=$((SECONDS - t0))
  echo "   $((SECONDS - t0)) s: $n chunk replicas written, Caddy at $mem MiB"
done
wait "$upload" || true
took=$((SECONDS - t0))
code="$(cat "$work/big.code")"; rc="$(cat "$work/big.rc")"
[[ "$code" == "200 2" && "$rc" == 0 ]] && ok "upload succeeded after $took s over HTTP/2 (Content-Length reached the gateway: it answers 411 without one)" \
  || fail "upload: status $code, curl exit $rc ($(cat "$work/big.err")) after $took s"
[[ -n "$first" ]] && ((first * 4 <= took)) && ok "first chunk replicas written by $first s of $took s: streamed, not buffered" \
  || fail "first chunk replicas written at ${first:-never} s of $took s"
((maxmem <= 128)) && ok "Caddy's memory peaked at $maxmem MiB for a 512 MiB body" || fail "Caddy's memory peaked at $maxmem MiB"
rc=0
c "${auth[@]}" --http2 -o /w/big.out "$(url "$k")" || rc=$?
if [[ $rc == 0 ]] && cmp -s "$work/big" "$work/big.out"; then ok "downloads IDENTICAL"; else fail "download: curl exit $rc, differs"; fi

echo "== cleanup"
for k in "${keys[@]}"; do c "${auth[@]}" -o /dev/null -X DELETE "$(url "$k")" || true; done
keys=()
restore_flipped "$flipped"
check_touched "$flipped" || status=1

finish
