#!/usr/bin/env bash
# Mutation check for the node's Go tests.
#
# For each patch in scripts/mutations/node/: apply it, run the node tests,
# report which tests failed, and revert. Each mutation names the test that must
# catch it. A mutation that does not compile is reported as BUILD FAILED, never
# as detected. Finally, run the unmutated tests.
#
# Usage: scripts/node-mutation-check.sh
# The mutation is always reverted, even on Ctrl-C.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# The test that must fail under each mutation (a case statement keeps this
# working on macOS's bash 3.2).
expected_test() {
  case "$1" in
    n1-skip-read-verification) echo "TestReadChunkCorruptSendsNothing" ;;
    n2-plain-rename) echo "TestWriteChunkNeverReplacesAtFinalStep" ;;
    n3-no-fsync) echo "TestCommitDurabilityOrder" ;;
    n4-skip-write-verification) echo "TestWriteChunkFraming" ;;
    n5-no-temp-cleanup) echo "TestCleanTemps" ;;
    n6-link-before-compare) echo "TestWriteChunkFraming" ;;
    n7-skip-chunk-id-check) echo "TestReadChunkCorruptSendsNothing" ;;
    n8-no-read-limit) echo "TestReadChunkConcurrencyLimit" ;;
    *) echo "unknown mutation $1" >&2; return 1 ;;
  esac
}

applied=""
revert() {
  if [[ -n "$applied" ]]; then
    git apply -R "$applied"
    applied=""
  fi
}
trap revert EXIT

# Runs the node tests and prints "BUILD FAILED" or the names of failed tests,
# one per line (subtests as Parent/sub).
run_tests() {
  if ! go vet ./node/... >/dev/null 2>&1; then
    echo "BUILD FAILED"
    return
  fi
  go test -count=1 -json ./node/... 2>/dev/null | python3 -I -c '
import json, sys
for line in sys.stdin:
    try:
        ev = json.loads(line)
    except ValueError:
        continue
    if ev.get("Action") == "fail" and ev.get("Test"):
        print(ev["Test"])
' || true
}

status=0
for patch in scripts/mutations/node/*.patch; do
  name="$(basename "$patch" .patch)"
  target="$(expected_test "$name")"
  git apply --check "$patch"
  git apply "$patch"
  applied="$patch"
  failed="$(run_tests)"
  revert

  echo "=== mutation $name"
  if [[ "$failed" == "BUILD FAILED" ]]; then
    echo "    BUILD FAILED: the mutation does not compile, so it proves nothing"
    status=1
    continue
  fi
  echo "    failed tests: $(grep -c . <<<"$failed" || true)"
  while IFS= read -r f; do
    if [[ -n "$f" ]]; then echo "    FAILED $f"; fi
  done <<<"$failed"
  if grep -qx "$target\(/.*\)\{0,1\}" <<<"$failed"; then
    echo "    DETECTED by $target"
  else
    echo "    NOT DETECTED: expected $target to fail"
    status=1
  fi
done

echo "=== unmutated"
failed="$(run_tests)"
echo "    failed tests: $(grep -c . <<<"$failed" || true)"
while IFS= read -r f; do
  if [[ -n "$f" ]]; then echo "    FAILED $f"; status=1; fi
done <<<"$failed"
exit "$status"
