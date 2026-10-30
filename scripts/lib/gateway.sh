# Shared by the scripts that use the HTTP gateways (stage 10).
#
# `docker compose up --wait` means each gateway answers /healthz: its process
# is serving HTTP. It does not mean requests will work: that needs the
# coordinator reachable through it, and uploads need ALIVE nodes. Anything
# that uses a gateway after starting or restarting one must call wait_gateway.

GATEWAY1=http://127.0.0.1:9081
GATEWAY2=http://127.0.0.1:9082

# cluster_summary URL: "node1=ALIVE node2=ALIVE ... under=N" from GET
# /cluster, or "unreachable: <reason>".
cluster_summary() {
  local body rc=0
  body="$(curl -sS --max-time 5 --fail-with-body "$1/cluster" 2>&1)" || rc=$?
  if ((rc != 0)); then
    echo "unreachable: $(tr '\n' ' ' <<<"$body" | cut -c1-200)"
    return 0
  fi
  python3 -I - "$body" 2>/dev/null <<'EOF' || echo "unreadable: $(cut -c1-200 <<<"$body")"
import json, sys
c = json.loads(sys.argv[1])
nodes = " ".join("%s=%s" % (n["node_id"], n["liveness"]) for n in c["nodes"])
print(nodes, "under=%d" % c["under_replicated_chunks"])
EOF
}

# wait_gateway URL [N]: waits until GET URL/cluster answers and shows at
# least N (default 4) nodes ALIVE. Times out after
# ${GATEWAY_WAIT_TIMEOUT:-30} s with the last answer seen.
wait_gateway() {
  local url="$1" want="${2:-4}" timeout="${GATEWAY_WAIT_TIMEOUT:-30}" start=$SECONDS seen alive
  while :; do
    seen="$(cluster_summary "$url")"
    alive="$({ grep -o '=ALIVE' <<<"$seen" || true; } | wc -l | tr -d ' ')"
    if ((alive >= want)); then
      echo "   gateway $url ready after $((SECONDS - start))s: $seen"
      return 0
    fi
    if ((SECONDS - start >= timeout)); then
      echo "  FAIL: gateway $url not ready after ${timeout}s; last seen: $seen" >&2
      return 1
    fi
    sleep 0.5
  done
}

# key_url GATEWAY KEY: the object URL for KEY, encoded as one path segment
# ("/" as %2F, and "." and ".." as %2E).
key_url() {
  python3 -I -c '
import sys, urllib.parse
k = sys.argv[2]
seg = k.replace(".", "%2E") if k in (".", "..") else urllib.parse.quote(k, safe="")
print(sys.argv[1] + "/objects/" + seg)
' "$1" "$2"
}
