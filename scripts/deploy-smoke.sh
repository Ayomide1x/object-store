#!/usr/bin/env bash
# Smoke check of the deployment (DEPLOY.md), run from your own machine against
# the public URL, through Caddy:
#   1. No password, or a wrong one, gets 401.
#   2. GET /cluster: 4 nodes ALIVE.
#   3. A 9 MiB upload (3 chunks) over HTTP/2 under a key with "/" in it:
#      stat shows the key, a prefix listing finds it, and it downloads
#      identical.
#   4. A download broken mid-stream: every replica of chunk 1 of a 12 MiB
#      object is corrupted on the VM (over ssh), and the download fails at the
#      client over HTTP/2 (status 200, then an error, a short body), never a
#      clean end.
#   5. The same in your browser: a second broken object; open its URL, see
#      the download fail, and answer.
# Every object it made is removed (GC deletes the files after the read grace,
# 15 minutes), and every flipped byte is flipped back.
#
# Usage: scripts/deploy-smoke.sh URL SSH_TARGET [REPO_DIR_ON_VM]
#   scripts/deploy-smoke.sh https://objectstore.ayomideo.dev ubuntu@129.146.48.152
# REPO_DIR_ON_VM defaults to ~/object-store. Asks for the basic-auth user and
# password; the password is never written to disk or put on a command line
# (curl reads it from a config on stdin).
set -euo pipefail

[[ $# -ge 2 ]] || { sed -n '2,24p' "$0"; exit 2; }
base="${1%/}"
ssh_target="$2"
repo="${3:-~/object-store}"
root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
run="$(date -u +%Y%m%dT%H%M%SZ)-$$"
flipped="/tmp/objectstore-smoke-$run.flipped" # on the VM
keys=()

status=0
fail() { echo "  FAIL: $*"; status=1; }
source "$root/scripts/lib/outcome.sh"
ok() { echo "  ok: $*"; }

read -r -p "basic-auth user: " user
read -r -s -p "password: " pw
echo
# The credentials as a curl config line, quotes and backslashes escaped.
esc() { local s="${1//\\/\\\\}"; printf '%s' "${s//\"/\\\"}"; }
creds="user = \"$(esc "$user"):$(esc "$pw")\""
# cu ARGS...: curl with the credentials; cn: without.
cu() { printf '%s\n' "$creds" | curl -sS -K - "$@"; }
cn() { curl -sS "$@"; }
enc() { python3 -I -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }
url() { echo "$base/objects/$(enc "$1")"; }
# remote ARGS...: runs bash on the VM in the repo directory, script on stdin.
remote() { ssh "$ssh_target" "cd $repo && bash -s -- $(printf '%q ' "$@")"; }

cleanup() {
  local k
  for k in "${keys[@]+"${keys[@]}"}"; do cu -o /dev/null -X DELETE "$(url "$k")" 2>/dev/null || true; done
  remote "$flipped" <<'EOF' >/dev/null 2>&1 || true
compose=(docker compose)
source scripts/lib/volume.sh
[ -f "$1" ] && restore_flipped "$1"; rm -f "$1"
EOF
  rm -rf "$work"
}
trap cleanup EXIT

echo "== 1. the password is required"
code="$(cn -o /dev/null -w '%{http_code}' "$base/cluster")"
[[ "$code" == 401 ]] && ok "no password: $code" || fail "no password: $code, want 401"
code="$(printf 'user = "%s:not-the-password"\n' "$(esc "$user")" | curl -sS -K - -o /dev/null -w '%{http_code}' "$base/cluster")"
[[ "$code" == 401 ]] && ok "wrong password: $code" || fail "wrong password: $code, want 401"

echo "== 2. GET /cluster"
code="$(cu -o "$work/cluster.json" -w '%{http_code}' "$base/cluster")"
alive="$(python3 -I -c 'import json,sys; print(sum(n["liveness"] == "ALIVE" for n in json.load(open(sys.argv[1]))["nodes"]))' "$work/cluster.json" 2>/dev/null || echo "?")"
[[ "$code" == 200 && "$alive" == 4 ]] && ok "/cluster: 4 nodes ALIVE" || fail "/cluster: $code, $alive ALIVE"

echo "== 3. a round trip under a key with \"/\""
head -c $((9 << 20)) /dev/urandom >"$work/up"
k="smoke/$run/nested/file.bin"; keys+=("$k")
read -r code ver <<<"$(cu --http2 -o /dev/null -w '%{http_code} %{http_version}' -T "$work/up" "$(url "$k")")"
[[ "$code" == 200 ]] && ok "PUT $(url "$k"): $code over HTTP/$ver" || fail "PUT: $code"
got="$(cu "$(url "$k")/stat" | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["key"])' 2>/dev/null || true)"
[[ "$got" == "$k" ]] && ok "stat: key \"$got\"" || fail "stat key \"$got\", want \"$k\""
listed="$(cu "$base/objects?prefix=$(enc "smoke/$run/nested/")" \
  | python3 -I -c 'import json,sys; print(",".join(o["key"] for o in json.load(sys.stdin)["objects"]))' 2>/dev/null || true)"
[[ "$listed" == "$k" ]] && ok "prefix listing finds it" || fail "prefix listing: \"$listed\""
rc=0; cu --http2 -o "$work/down" "$(url "$k")" || rc=$?
[[ $rc == 0 ]] && cmp -s "$work/up" "$work/down" && ok "download IDENTICAL" || fail "download: curl exit $rc, differs"

# broken_object NAME: a 12 MiB object with every replica of chunk 1 corrupt
# (on the VM); prints its key.
head -c $((12 << 20)) /dev/urandom >"$work/broken"
head -c $((4 << 20)) "$work/broken" >"$work/chunk0"
broken_object() {
  local k="smoke/$run/$1"
  [[ "$(cu -o /dev/null -w '%{http_code}' -T "$work/broken" "$(url "$k")")" == 200 ]] || { echo "PUT of $k failed" >&2; return 1; }
  remote "$k" "$flipped" <<'EOF' >&2
set -euo pipefail
compose=(docker compose)
source scripts/lib/volume.sh
sql() { docker compose exec -T postgres psql -U objectstore -d objectstore -tA -c "$1" </dev/null; }
chunk="$(sql "SELECT c.chunk_id FROM keys k JOIN chunks c USING (object_id) WHERE k.key = '$1' AND c.chunk_index = 1")"
for n in $(sql "SELECT node_id FROM chunk_replicas WHERE chunk_id = '$chunk' AND state = 'HEALTHY'"); do
  echo "     corrupting $n's replica of chunk 1 ($chunk): $(corrupt_byte "$n" "$chunk" 1056 "$2")"
done
EOF
  echo "$k"
}

echo "== 4. a download broken mid-stream, over HTTP/2"
k="$(broken_object broken-curl)"; keys+=("$k")
rc=0
out="$(cu --http2 -o "$work/broken.out" -w '%{http_code} %{http_version}' "$(url "$k")" 2>"$work/err")" || rc=$?
read -r code ver <<<"$out"
n="$(wc -c <"$work/broken.out" | tr -d ' ')"
if [[ "$code" == 200 && $rc != 0 ]] && ((n <= 4 << 20)) && head -c "$n" "$work/chunk0" | cmp -s - "$work/broken.out"; then
  ok "status 200 over HTTP/$ver, then curl exit $rc ($(tr '\n' ' ' <"$work/err")); $n of $((12 << 20)) bytes, a prefix of chunk 0"
else
  fail "status $code, curl exit $rc, $n bytes; want 200, a non-zero exit and a prefix of chunk 0"
fi

echo "== 5. the same in your browser"
k="$(broken_object broken-browser)"; keys+=("$k")
echo "   Open this in your browser (log in if asked), and watch the download:"
echo "     $(url "$k")"
echo "   It must end as failed (Chrome: \"Failed - Network error\"; Firefox: \"Failed\"),"
echo "   never as a complete 12 MiB file. Open it once: the failed read marks the"
echo "   replicas bad, and a second try gets an error page before any byte."
read -r -p "   Did the download fail? [y/n] " answer
[[ "$answer" == y* ]] && ok "the browser showed the broken download as failed" || fail "the browser did not show the download as failed"

echo "== cleanup"
for k in "${keys[@]}"; do
  code="$(cu -o /dev/null -w '%{http_code}' -X DELETE "$(url "$k")")"
  echo "   DELETE $k: $code"
done
keys=()
finish
