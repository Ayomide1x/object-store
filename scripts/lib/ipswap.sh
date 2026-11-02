# Shared by the acceptance scripts. Expects the array $compose, and
# scripts/lib/volume.sh (node_container).
#
# swap_ips NODE_A NODE_B [DOWN_HOOK]: swaps the two nodes' IP addresses on the Compose
# network, on purpose rather than hoping a restart reshuffles them. Both are
# stopped (their connections close, as on a real restart), disconnected,
# reconnected while stopped, each with the other's address and its own
# service name as DNS alias, and started again. Each name then resolves to
# the address the other node had: exactly what a restart that reshuffles IPs
# does, made deterministic. Prints "NODE_A A->B NODE_B B->A".
#
# DOWN_HOOK, if given, is a command run while both nodes are stopped (for
# example uploads, so the coordinator calls the nodes while they are gone:
# failed lookups and connection attempts before the new addresses appear).
#
# Disconnecting a running container instead would drop its packets silently
# and leave half-open connections on both sides, a different failure (a lost
# host), which is not what this reproduces.
swap_ips() {
  local a="$1" b="$2" hook="${3:-}" ca cb net ipa ipb
  ca="$(node_container "$a")"; cb="$(node_container "$b")"
  net="$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$ca" | awk '{print $1}')"
  ipa="$(docker inspect -f "{{(index .NetworkSettings.Networks \"$net\").IPAddress}}" "$ca")"
  ipb="$(docker inspect -f "{{(index .NetworkSettings.Networks \"$net\").IPAddress}}" "$cb")"
  docker stop "$ca" "$cb" >/dev/null
  docker network disconnect "$net" "$ca"
  docker network disconnect "$net" "$cb"
  docker network connect --ip "$ipb" --alias "$a" "$net" "$ca"
  docker network connect --ip "$ipa" --alias "$b" "$net" "$cb"
  if [[ -n "$hook" ]]; then "$hook" >&2; fi
  docker start "$ca" "$cb" >/dev/null
  echo "$a $ipa->$ipb $b $ipb->$ipa"
}

# unpin_ips NODE...: gives nodes back dynamic addresses after swap_ips. A
# swapped node keeps its fixed address across restarts; once Docker hands that
# address to another container, the node can't start ("Address already in
# use"). Each node is stopped, reconnected without a fixed address, and
# started.
unpin_ips() {
  local n c net
  for n in "$@"; do
    c="$(node_container "$n")"
    net="$(docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}' "$c" | awk '{print $1}')"
    docker stop "$c" >/dev/null
    docker network disconnect "$net" "$c"
    docker network connect --alias "$n" "$net" "$c"
    docker start "$c" >/dev/null
  done
}

# node_ip NODE: the node's address on the Compose network.
node_ip() {
  docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' "$(node_container "$1")" | awk '{print $1}'
}
