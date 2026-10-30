#!/usr/bin/env bash
# Mutation check for the Go tests (node, client and shared packages).
#
# For each patch in scripts/mutations/go/: apply it, run the Go tests,
# report which tests failed, and revert. Each mutation names the test that must
# catch it. A mutation that does not compile is reported as BUILD FAILED, never
# as detected. Finally, run the unmutated tests.
#
# Usage: scripts/go-mutation-check.sh
# Refuses to start if any patch in scripts/mutations/ is already applied. The
# mutation is reverted on exit, Ctrl-C and kill.
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
    n9-stat-skips-chunk-id-check) echo "TestStatChunks" ;;
    c1-client-quorum-of-one) echo "TestWriteChunkQuorum" ;;
    c2-no-write-deadline) echo "TestWriteChunkQuorum" ;;
    c3-no-skip-after-failure) echo "TestFailedNodeSkippedForRestOfUpload" ;;
    d1-no-reset-on-failover) echo "TestFailoverClassification/stream_breaks_mid-chunk" ;;
    d2-report-on-unavailable) echo "TestFailoverClassification/unavailable" ;;
    d3-no-read-deadline) echo "TestFailoverClassification/hung_past_the_deadline" ;;
    d4-no-demotion) echo "TestHungReplicaDemotedForRestOfGet" ;;
    d5-fail-over-on-invalid-argument) echo "TestInvalidArgumentDoesNotFailOver" ;;
    d6-unlisted-code-reported) echo "TestFailoverClassification/internal_(unlisted_code)" ;;
    n10-verify-ignores-expected-hash) echo "TestVerifyChunk/stored_hash_differs_from_expected" ;;
    h1-no-per-beat-timeout) echo "TestAHungCoordinatorCostsOneBeatAtATime" ;;
    h2-loop-exits-on-error) echo "TestSurvivesTheCoordinatorBeingDown" ;;
    h3-ignores-cancellation) echo "TestStopsPromptlyOnCancellation" ;;
    h4-default-reconnect-backoff) echo "TestSurvivesTheCoordinatorBeingDown" ;;
    r1-read-order-ignores-liveness) echo "TestDeadReplicasAreReadLast" ;;
    q1-push-ignores-expected-hash) echo "TestPushChunk/source_is_not_the_recorded_chunk" ;;
    q2-push-response-without-node-id) echo "TestPushChunk/copies_a_verified_chunk" ;;
    t1-delete-fails-on-missing) echo "TestDeleteChunks/repeated_delete" ;;
    t2-delete-response-without-node-id) echo "TestDeleteChunks/first_delete" ;;
    t3-no-tombstone-check-after-link) echo "TestWriteRacingDelete" ;;
    t4-no-tombstone-sweep-at-startup) echo "TestRecoverStore" ;;
    t5-unlink-before-tombstone-durable) echo "TestDeleteOrder" ;;
    t6-delete-ignores-addressed-node) echo "TestDeleteChunks/addressed_to_another_node:_nothing_deleted" ;;
    *) echo "unknown mutation $1" >&2; return 1 ;;
  esac
}

. scripts/lib/mutation.sh
out="$(mktemp)"
trap 'rm -f "$out"; mutation_revert' EXIT

# go_tests FILE: runs the Go tests and writes "BUILD FAILED" or the names of
# failed tests, one per line (subtests as Parent/sub), to FILE.
go_tests() {
  if ! go vet ./... >/dev/null 2>&1; then
    echo "BUILD FAILED" >"$1"
    return
  fi
  go test -count=1 -json ./... 2>/dev/null | python3 -I -c '
import json, sys
for line in sys.stdin:
    try:
        ev = json.loads(line)
    except ValueError:
        continue
    if ev.get("Action") == "fail" and ev.get("Test"):
        print(ev["Test"])
' >"$1" || true
}

require_unapplied scripts/mutations/*/*.patch

status=0
for patch in scripts/mutations/go/*.patch; do
  name="$(basename "$patch" .patch)"
  target="$(expected_test "$name")"
  apply_mutation "$patch"
  run_child go_tests "$out"
  mutation_revert
  failed="$(cat "$out")"

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
  # The target test, or any of its subtests, must have failed.
  hit=no
  while IFS= read -r f; do
    if [[ "$f" == "$target" || "$f" == "$target/"* ]]; then hit=yes; fi
  done <<<"$failed"
  if [[ "$hit" == yes ]]; then
    echo "    DETECTED by $target"
  else
    echo "    NOT DETECTED: expected $target to fail"
    status=1
  fi
done

echo "=== unmutated"
run_child go_tests "$out"
failed="$(cat "$out")"
echo "    failed tests: $(grep -c . <<<"$failed" || true)"
while IFS= read -r f; do
  if [[ -n "$f" ]]; then echo "    FAILED $f"; status=1; fi
done <<<"$failed"
exit "$status"
