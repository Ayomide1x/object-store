# Shared by the acceptance scripts. Expects the array $compose.
#
# Makes one TCP connection half-open on the real stack: its packets are
# dropped silently in both directions, in one service's network namespace,
# and nothing is closed. Only that connection, matched by both ends' address
# and port: a new connection between the same two services gets through, as a
# reconnect over a healthy path would. (Dropping all of a service's traffic
# proves nothing about keepalive: reconnects fail too until the drop ends, and
# then TCP retransmission revives the old connection by itself.)
#
# The rules are added from a helper container (alpine:3 with iptables,
# installed on first use, so it needs the network) that shares the service's
# network namespace and has NET_ADMIN. The rules live in that namespace, not in
# the helper: halfopen_cleanup deletes them, then the helpers.
#
# halfopen_ip SERVICE: the service's address on the Compose network.
# halfopen_conns SERVICE PEER_IP PEER_PORT|- LOCAL_PORT|-: the service's
#   ESTABLISHED connections with PEER_IP, as "LOCAL FOREIGN" lines
#   (ip:port each, IPv4), filtered by the peer's port or the service's own
#   ("-": any).
# halfopen_drop SERVICE LOCAL FOREIGN: drops that connection's packets.
# halfopen_cleanup: removes every rule and helper this script added.

halfopen_ip() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' \
    "$("${compose[@]}" ps -q "$1")" | awk '{print $1}'
}

# halfopen_helper SERVICE: the helper's name, started (and iptables installed)
# on first use. Often called in a subshell, so it keeps no shell state:
# halfopen_cleanup finds the helpers by name.
halfopen_helper() {
  local svc="$1" name="halfopen-$1-$$"
  if ! docker inspect "$name" >/dev/null 2>&1; then
    docker run -d --rm --name "$name" --net "container:$("${compose[@]}" ps -q "$svc")" \
      --cap-add NET_ADMIN alpine:3 sleep 3600 >/dev/null
    docker exec "$name" apk add -q iptables >/dev/null
  fi
  echo "$name"
}

halfopen_conns() {
  local svc="$1" peer="$2" peer_port="$3" local_port="$4"
  docker exec "$(halfopen_helper "$svc")" netstat -tn |
    awk -v peer="$peer" -v pp="$peer_port" -v lp="$local_port" '
      $6 == "ESTABLISHED" {
        # A dual-stack listener (Go on ":9102") shows IPv4 peers as ::ffff:a.b.c.d.
        sub(/^::ffff:/, "", $4); sub(/^::ffff:/, "", $5)
        split($4, l, ":"); split($5, f, ":")
        if (f[1] == peer && (pp == "-" || f[2] == pp) && (lp == "-" || l[2] == lp)) print $4, $5
      }'
}

halfopen_drop() {
  local svc="$1" lip="${2%:*}" lport="${2##*:}" fip="${3%:*}" fport="${3##*:}" h
  h="$(halfopen_helper "$svc")"
  docker exec "$h" iptables -I INPUT -p tcp -s "$fip" --sport "$fport" -d "$lip" --dport "$lport" \
    -m comment --comment halfopen-check -j DROP
  docker exec "$h" iptables -I OUTPUT -p tcp -s "$lip" --sport "$lport" -d "$fip" --dport "$fport" \
    -m comment --comment halfopen-check -j DROP
}

halfopen_cleanup() {
  local h rule
  for h in $(docker ps --filter "name=^halfopen-.*-$$\$" --format '{{.Names}}'); do
    # Each rule as printed, with -A turned into -D (no quoting: the comment has no spaces).
    docker exec "$h" iptables -S | { grep -- '--comment halfopen-check' || true; } | sed 's/^-A /-D /' |
      while read -r rule; do
        # shellcheck disable=SC2086
        docker exec "$h" iptables $rule
      done
    docker rm -f "$h" >/dev/null 2>&1 || true
  done
}
