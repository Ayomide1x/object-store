# Shared by the acceptance scripts. Expects the array $compose, for example
# compose=(docker compose -f "$root/docker-compose.yml").
#
# `docker compose up --wait` means the coordinator is serving. It does not mean
# uploads will succeed: placement uses only ALIVE nodes, and right after the
# stack (or a node, or the coordinator) starts, nodes are UNKNOWN for up to one
# heartbeat interval, or recovering from DEAD for two beats. Anything that puts
# after starting things must call wait_alive first.
#
# Both helpers poll the persisted states in the nodes table, which the
# coordinator writes on every transition (at most about 1 s behind it).

# liveness_states: "node1=ALIVE node2=DEAD ..." as persisted now.
liveness_states() {
  if [[ -z "${compose[*]+set}" ]]; then
    echo "liveness.sh: \$compose must be set before calling its helpers" >&2
    exit 2
  fi
  "${compose[@]}" exec -T postgres psql -U objectstore -d objectstore -tA -F= \
    -c "SELECT node_id, state FROM nodes ORDER BY node_id" 2>/dev/null | tr '\n' ' '
}

# wait_alive N [NODE...]: with no names, waits until at least N nodes are
# ALIVE; with names, until every named node is ALIVE (N is then ignored).
# Times out after ${LIVENESS_WAIT_TIMEOUT:-30} s with the last states seen.
wait_alive() {
  local want="$1"; shift
  local timeout="${LIVENESS_WAIT_TIMEOUT:-30}" start=$SECONDS states alive n ready
  while :; do
    states="$(liveness_states)"
    if (($# > 0)); then
      ready=yes
      for n in "$@"; do [[ " $states" == *" $n=ALIVE "* ]] || ready=no; done
    else
      alive="$(grep -o '=ALIVE' <<<"$states" | wc -l | tr -d ' ')"
      if ((alive >= want)); then ready=yes; else ready=no; fi
    fi
    if [[ "$ready" == yes ]]; then
      echo "   ready for uploads after $((SECONDS - start))s: $states"
      return 0
    fi
    if ((SECONDS - start >= timeout)); then
      if (($# > 0)); then
        echo "  FAIL: nodes $* not all ALIVE after ${timeout}s; last seen: $states" >&2
      else
        echo "  FAIL: fewer than $want nodes ALIVE after ${timeout}s; last seen: $states" >&2
      fi
      return 1
    fi
    sleep 0.5
  done
}

# wait_state NODE STATE TIMEOUT: waits until NODE's persisted state is STATE;
# prints how long it took. Fails with the last state seen.
wait_state() {
  local node="$1" want="$2" timeout="$3" start=$SECONDS t0 got
  t0="$(python3 -c 'import time; print(time.time())')"
  while :; do
    got="$(liveness_states | tr ' ' '\n' | sed -n "s/^$node=//p")"
    if [[ "$got" == "$want" ]]; then
      echo "   $node $want after $(python3 -c "import time; print(round(time.time() - $t0, 1))")s"
      return 0
    fi
    if ((SECONDS - start >= timeout)); then
      echo "  FAIL: $node not $want after ${timeout}s (last seen: ${got:-none})" >&2
      return 1
    fi
    sleep 0.5
  done
}
