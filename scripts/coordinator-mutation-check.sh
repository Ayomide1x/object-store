#!/usr/bin/env bash
# Mutation check for the coordinator integration tests.
#
# For each patch in scripts/mutations/coordinator/: apply it, run the integration tests,
# report which tests failed, and revert. Each mutation names the test that must
# catch it. Finally, run the unmutated suite.
#
# Usage: scripts/coordinator-mutation-check.sh
# Needs Docker running (Testcontainers). The mutation is always reverted, even
# on Ctrl-C.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
reports=coordinator/build/test-results/test

# The test that must fail under each mutation. (A case statement rather than an
# associative array keeps this working on macOS's bash 3.2.)
expected_test() {
  case "$1" in
    1-getobject-ignores-pointer) echo "overwriteIsInvisibleUntilCommit()" ;;
    2-no-transaction) echo "failedOverwriteLeavesOldObjectIntact()" ;;
    3-read-then-write-swap) echo "concurrentOverwritesAllSucceedAndLeaveOnePointer()" ;;
    4-inner-join-chunks) echo "zeroByteObjectResolvesWithEmptyChunkList()" ;;
    5-hash-not-recorded) echo "checksumsAreRecordedAndReturnedByGetObject()" ;;
    6-quorum-of-one) echo "commitRefusedWhenAnyChunkHasFewerThanTwoVerifiedReplicas()" ;;
    7-record-unverified-replicas) echo '"one missing"' ;;
    8-no-node-rpc-deadline) echo "hungNodeCostsOneDeadlineWhateverTheObjectSize()" ;;
    9-skip-hash-comparison) echo '"one with another hash"' ;;
    10-accept-wrong-node) echo "responseFromTheWrongNodeRecordsNothingForIt()" ;;
    11-no-retry-on-unavailable) echo "unavailableOnceIsRetriedAndTheReplicaCounts()" ;;
    12-mark-bad-without-confirmation) echo "reportContradictedByTheNodeMarksNothing()" ;;
    13-getobject-ignores-bad-state) echo "getObjectLeavesOutBadReplicas()" ;;
    14-verify-accepts-wrong-node) echo "confirmationFromTheWrongNodeMarksNothing()" ;;
    15-no-data-loss-without-healthy-replica) echo "getObjectFailsWithDataLossWhenAChunkHasNoHealthyReplica()" ;;
    l1-no-suspect-dead-transition) echo "silenceMakesANodeSuspectThenDeadAtTheThresholds()" ;;
    l2-placement-includes-non-alive) echo "placementUsesOnlyAliveNodes()" ;;
    l3-unknown-node-accepted) echo "aBeatFromAnUnregisteredNodeIsRejectedAndRecordsNothing()" ;;
    l4-dead-alive-on-one-beat) echo "aDeadNodeNeedsTwoConsecutiveBeatsToComeBack()" ;;
    l5-unknown-dead-too-early) echo "afterARestartANodeIsNeverDeadBeforeAFullWindow()" ;;
    l6-complete-asks-dead-nodes) echo "completeUploadDoesNotAskADeadNode()" ;;
    l7-conflicting-instance-accepted) echo "twoProcessesClaimingOneNodeAreRejectedUntilOneGoesQuiet()" ;;
    p1-dead-replicas-count-as-healthy) echo "aDeadNodesReplicaIsCopiedFromAHealthySourceToAFreeNode()" ;;
    p2-record-without-target-confirmation) echo "aFailedCopyRecordsNothing()" ;;
    p3-target-node-with-bad-row) echo "aNodeWithABadRowIsNeverATarget()" ;;
    p4-act-on-unknown) echo "aPassDoesNothingWhileAnyNodeIsUnknown()" ;;
    p5-copy-from-bad-replica) echo "aBadReplicaIsNeverASource()" ;;
    p6-no-concurrency-bound) echo "copiesAreBoundedInFlightAndOnePassRepairsEverything()" ;;
    p7-no-repair-grace) echo "nothingIsCopiedWithinTheGracePeriod()" ;;
    p8-push-again-on-target-has-chunk) echo "alreadyExistsWithAValidCopyIsRecordedNotRetried()" ;;
    p9-no-pass-pruning) echo "oldRepairPassRowsArePrunedAndRecentOnesKept()" ;;
    p10-caps-as-per-pass-budget) echo "copiesAreBoundedInFlightAndOnePassRepairsEverything()" ;;
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

run_tests() {
  (cd coordinator && ./gradlew --no-daemon -q test --rerun-tasks >/dev/null 2>&1) || true
}

# Prints the test count (over every test class) on the first line, then one
# failed test name per line.
summarize() {
  python3 -I - "$reports" <<'EOF'
import glob, sys, xml.etree.ElementTree as ET
total, failed = 0, []
for path in sorted(glob.glob(sys.argv[1] + "/TEST-*.xml")):
    suite = ET.parse(path).getroot()
    total += int(suite.get("tests"))
    failed += [tc.get("name") for tc in suite.iter("testcase")
               if tc.find("failure") is not None or tc.find("error") is not None]
print(total, *failed, sep="\n")
EOF
}

status=0
for patch in scripts/mutations/coordinator/*.patch; do
  name="$(basename "$patch" .patch)"
  target="$(expected_test "$name")"
  git apply --check "$patch"
  git apply "$patch"
  applied="$patch"
  # A mutation that doesn't compile would fail every test and look detected.
  if ! (cd coordinator && ./gradlew --no-daemon -q compileJava compileTestJava >/dev/null 2>&1); then
    revert
    echo "=== mutation $name"
    echo "    BUILD FAILED: the mutation does not compile, so it proves nothing"
    status=1
    continue
  fi
  run_tests
  revert

  summary="$(summarize)"
  total="$(sed -n 1p <<<"$summary")"
  failed="$(sed 1d <<<"$summary")"
  echo "=== mutation $name"
  echo "    tests: $total, failed: $(grep -c . <<<"$failed" || true)"
  while IFS= read -r f; do
    if [[ -n "$f" ]]; then echo "    FAILED $f"; fi
  done <<<"$failed"
  if grep -qxF "$target" <<<"$failed"; then
    echo "    DETECTED by $target"
  else
    echo "    NOT DETECTED: expected $target to fail"
    status=1
  fi
done

echo "=== unmutated"
run_tests
summary="$(summarize)"
failed="$(sed 1d <<<"$summary")"
echo "    tests: $(sed -n 1p <<<"$summary"), failed: $(grep -c . <<<"$failed" || true)"
while IFS= read -r f; do
  if [[ -n "$f" ]]; then echo "    FAILED $f"; status=1; fi
done <<<"$failed"
exit "$status"
