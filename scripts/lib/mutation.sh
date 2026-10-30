# Shared by the mutation scripts. The patch under test is reverted however the
# script ends: normal exit, error, Ctrl-C (SIGINT) or kill (SIGTERM).
#
# Long commands run through run_child: in the background, then waited for. A
# trapped signal interrupts `wait` at once, whereas bash would otherwise run
# the trap only after a foreground command (a whole test run) finished.

mutation_applied=""
mutation_child=""

mutation_revert() {
  if [[ -n "$mutation_applied" ]]; then
    git apply -R "$mutation_applied"
    echo "reverted $mutation_applied" >&2
    mutation_applied=""
  fi
}

# Kills a process and all its descendants, children first.
kill_tree() {
  local c
  for c in $(pgrep -P "$1" 2>/dev/null); do
    kill_tree "$c"
  done
  kill "$1" 2>/dev/null || true
}

mutation_on_signal() {
  trap - EXIT INT TERM
  if [[ -n "$mutation_child" ]]; then
    kill_tree "$mutation_child"
    wait "$mutation_child" 2>/dev/null || true
  fi
  mutation_revert
  exit "$1"
}

trap mutation_revert EXIT
trap 'mutation_on_signal 130' INT
trap 'mutation_on_signal 143' TERM

# apply_mutation PATCH: applies it and records it for the revert.
apply_mutation() {
  git apply "$1"
  mutation_applied="$1"
}

# run_child CMD [ARGS...]: runs CMD (a command or function) and returns its status.
run_child() {
  "$@" &
  mutation_child=$!
  local rc=0
  wait "$mutation_child" || rc=$?
  mutation_child=""
  return "$rc"
}

# require_unapplied PATCH...: exits unless every patch applies forward and not
# in reverse, i.e. none is already applied (say, left by a killed run).
require_unapplied() {
  local p bad=0
  for p in "$@"; do
    if ! git apply --check "$p" >/dev/null 2>&1; then
      echo "$p: does not apply (already applied, or stale)" >&2
      bad=1
    fi
    if git apply -R --check "$p" >/dev/null 2>&1; then
      echo "$p: is applied to the tree" >&2
      bad=1
    fi
  done
  if [[ "$bad" -ne 0 ]]; then
    echo "refusing to start: fix the tree first (git diff shows what is applied)" >&2
    exit 1
  fi
}
