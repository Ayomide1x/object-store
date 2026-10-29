# Shared by the acceptance scripts. Expects $osctl to name the osctl binary.

# osctl_run ARGS...: runs osctl. Its progress logs (level=INFO on stderr) are
# kept out of the output, but if it fails, the command and its error and
# warning lines are printed. Returns osctl's exit status.
osctl_run() {
  local err rc=0
  err="$(mktemp)"
  "$osctl" "$@" 2>"$err" || rc=$?
  if ((rc != 0)); then
    echo "  osctl $1 failed (exit $rc): osctl $*"
    grep -v ' level=INFO ' "$err" | tail -8 | sed 's/^/    /'
  fi
  rm -f "$err"
  return "$rc"
}
