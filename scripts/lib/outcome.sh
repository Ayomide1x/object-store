# The verdict of an acceptance script, shared so every script and CI read it
# the same way. Expects the script's own `status` (0, or 1 after a fail).
#
#   PASS          exit 0   every check ran and held
#   FAIL          exit 1   a check ran and the product got it wrong
#   INCONCLUSIVE  exit 75  nothing failed, but a check could not run: a
#                          precondition didn't happen, or a random draw never
#                          produced the case to test. Nothing is proven either
#                          way: rerun. (75 is EX_TEMPFAIL in sysexits.h,
#                          "temporary failure, try again"; no tool these
#                          scripts run exits with it, so a crash under set -e
#                          can't pass for inconclusive.)
#
# FAIL wins: one failed check makes the run FAIL whatever else happened.

EXIT_INCONCLUSIVE=75
inconclusive_count=0

# inconclusive REASON: records a check that could not run, and goes on.
inconclusive() {
  echo "  INCONCLUSIVE: $*"
  inconclusive_count=$((inconclusive_count + 1))
}

# finish [PASS_DETAIL]: prints the verdict and exits with its code.
finish() {
  echo
  if ((status != 0)); then
    echo "FAIL"
    exit 1
  elif ((inconclusive_count > 0)); then
    echo "INCONCLUSIVE: nothing failed, but $inconclusive_count check(s) could not run; rerun (exit $EXIT_INCONCLUSIVE)"
    exit "$EXIT_INCONCLUSIVE"
  else
    echo "PASS${1:+ $1}"
    exit 0
  fi
}
