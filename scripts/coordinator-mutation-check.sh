#!/usr/bin/env bash
# Mutation check for the coordinator integration tests.
#
# For each patch in scripts/mutations/coordinator/: apply it, run the test
# class that holds the test that must catch it, report which tests failed,
# and revert. A mutation is DETECTED when that test fails. Only the target
# class runs per mutation; the full unmutated suite runs once at the end and
# guards everything else. Tests run on the Gradle daemon (stop it afterwards
# with `cd coordinator && ./gradlew --stop`).
#
# Usage: scripts/coordinator-mutation-check.sh
# Needs Docker running (Testcontainers). Refuses to start if any patch in
# scripts/mutations/ is already applied. The mutation is reverted on exit,
# Ctrl-C and kill.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
reports=coordinator/build/test-results/test

# The test class to run and the test in it that must fail, as Class|test, under
# each mutation. (A case statement rather than an
# associative array keeps this working on macOS's bash 3.2.)
expected_test() {
  case "$1" in
    1-getobject-ignores-pointer) echo "MetadataServiceCompleteUploadIT|overwriteIsInvisibleUntilCommit()" ;;
    2-no-transaction) echo "MetadataServiceCompleteUploadIT|failedOverwriteLeavesOldObjectIntact()" ;;
    3-read-then-write-swap) echo "MetadataServiceCompleteUploadIT|concurrentOverwritesAllSucceedAndLeaveOnePointer()" ;;
    4-inner-join-chunks) echo "MetadataServiceCompleteUploadIT|zeroByteObjectResolvesWithEmptyChunkList()" ;;
    5-hash-not-recorded) echo "MetadataServiceCompleteUploadIT|checksumsAreRecordedAndReturnedByGetObject()" ;;
    6-quorum-of-one) echo "MetadataServiceCompleteUploadIT|commitRefusedWhenAnyChunkHasFewerThanTwoVerifiedReplicas()" ;;
    7-record-unverified-replicas) echo 'MetadataServiceCompleteUploadIT|"one missing"' ;;
    8-no-node-rpc-deadline) echo "MetadataServiceCompleteUploadIT|hungNodeCostsOneDeadlineWhateverTheObjectSize()" ;;
    9-skip-hash-comparison) echo 'MetadataServiceCompleteUploadIT|"one with another hash"' ;;
    10-accept-wrong-node) echo "MetadataServiceCompleteUploadIT|responseFromTheWrongNodeRecordsNothingForIt()" ;;
    11-no-retry-on-unavailable) echo "MetadataServiceCompleteUploadIT|unavailableOnceIsRetriedAndTheReplicaCounts()" ;;
    12-mark-bad-without-confirmation) echo "BadReplicaReportIT|reportContradictedByTheNodeMarksNothing()" ;;
    13-getobject-ignores-bad-state) echo "BadReplicaReportIT|getObjectLeavesOutBadReplicas()" ;;
    14-verify-accepts-wrong-node) echo "BadReplicaReportIT|confirmationFromTheWrongNodeMarksNothing()" ;;
    15-no-data-loss-without-healthy-replica) echo "BadReplicaReportIT|getObjectFailsWithDataLossWhenAChunkHasNoHealthyReplica()" ;;
    l1-no-suspect-dead-transition) echo "LivenessTrackerTest|silenceMakesANodeSuspectThenDeadAtTheThresholds()" ;;
    l2-placement-includes-non-alive) echo "LivenessIT|placementUsesOnlyAliveNodes()" ;;
    l3-unknown-node-accepted) echo "LivenessTrackerTest|aBeatFromAnUnregisteredNodeIsRejectedAndRecordsNothing()" ;;
    l4-dead-alive-on-one-beat) echo "LivenessTrackerTest|aDeadNodeNeedsTwoConsecutiveBeatsToComeBack()" ;;
    l5-unknown-dead-too-early) echo "LivenessTrackerTest|afterARestartANodeIsNeverDeadBeforeAFullWindow()" ;;
    l6-complete-asks-dead-nodes) echo "LivenessIT|completeUploadDoesNotAskADeadNode()" ;;
    l7-conflicting-instance-accepted) echo "LivenessTrackerTest|twoProcessesClaimingOneNodeAreRejectedUntilOneGoesQuiet()" ;;
    p1-dead-replicas-count-as-healthy) echo "RepairIT|aDeadNodesReplicaIsCopiedFromAHealthySourceToAFreeNode()" ;;
    p2-record-without-target-confirmation) echo "RepairIT|aFailedCopyRecordsNothing()" ;;
    p3-target-node-with-bad-row) echo "RepairIT|aNodeWithABadRowIsNeverATarget()" ;;
    p4-act-on-unknown) echo "RepairIT|aPassDoesNothingWhileAnyNodeIsUnknown()" ;;
    p5-copy-from-bad-replica) echo "RepairIT|aBadReplicaIsNeverASource()" ;;
    p6-no-concurrency-bound) echo "RepairIT|copiesAreBoundedInFlightAndOnePassRepairsEverything()" ;;
    p7-no-repair-grace) echo "RepairIT|nothingIsCopiedWithinTheGracePeriod()" ;;
    p8-push-again-on-target-has-chunk) echo "RepairIT|alreadyExistsWithAValidCopyIsRecordedNotRetried()" ;;
    p9-no-pass-pruning) echo "RepairIT|oldRepairPassRowsArePrunedAndRecentOnesKept()" ;;
    p10-caps-as-per-pass-budget) echo "RepairIT|copiesAreBoundedInFlightAndOnePassRepairsEverything()" ;;
    g1-delete-before-retire) echo "GcIT|anUploadInProgressIsNeverTouched()" ;;
    g2-remove-row-before-confirm) echo "GcIT|rowsAreRemovedOnlyWhenTheNodeItselfConfirms()" ;;
    g3-abandon-without-transition) echo "GcIT|aLateCompleteOfAnAbandonedUploadFailsAndNothingCommits()" ;;
    g4-no-grace-for-superseded) echo "GcIT|aSupersededObjectKeepsItsFilesForTheReadGraceThenEverythingGoes()" ;;
    g5-delete-chunks-of-the-current-version) echo "GcIT|aSupersededObjectKeepsItsFilesForTheReadGraceThenEverythingGoes()" ;;
    g6-repair-records-replica-for-retired) echo "GcIT|repairNeverRecordsACopyForARetiredObjectAndGcFindsTheCopy()" ;;
    g7-drop-dead-node-tasks) echo "GcIT|aDeadNodesDeletionsWaitAndAreRetriedWhenItReturns()" ;;
    g8-bad-delete-tombstones) echo "GcIT|aBadFileIsDeletedWithoutATombstoneAndTheNodeIsATargetAgain()" ;;
    g9-repair-copies-without-placement) echo "GcIT|repairNeverRecordsACopyForARetiredObjectAndGcFindsTheCopy()" ;;
    g10-complete-on-superseded-fails) echo "GcIT|aRetriedCompleteSucceedsForSupersededAndDeletedAndFailsForAbandoned()" ;;
    g11-bad-delete-counts-raw-healthy-rows) echo "GcIT|aBadFileIsKeptUnlessTwoCountedHealthyReplicasRemain()" ;;
    g12-no-grace-for-bad-files) echo "GcIT|aBadFileIsDeletedWithoutATombstoneAndTheNodeIsATargetAgain()" ;;
    g13-no-overall-delete-bound) echo "GcIT|deletesAreBatchedAndBounded()" ;;
    g14-task-kept-after-confirmation) echo "GcIT|aSecondPassIsANoOp()" ;;
    g15-supersede-ignores-keys) echo "GcIT|theCurrentVersionIsNeverDeleted()" ;;
    g16-no-progress-check) echo "GcIT|anUnclearOutcomeIsAFailureAndThePassEnds()" ;;
    s1-list-cursor-is-first-key) echo "GatewayRpcIT|moreKeysThanTheLimitPageWithNoGapsOrDuplicates()" ;;
    s2-list-no-lookahead) echo "GatewayRpcIT|anExactMultipleOfTheLimitEndsWithoutAnEmptyPage()" ;;
    s3-address-set-ignored) echo "GatewayRpcIT|theCallerChoosesClientOrInternalAddresses()" ;;
    s4-unspecified-is-internal) echo "GatewayRpcIT|theCallerChoosesClientOrInternalAddresses()" ;;
    s5-status-counts-raw-healthy-rows) echo "GatewayRpcIT|underReplicatedIsCountedExactlyAsTheReconcilerCounts()" ;;
    s6-status-prunes-passes) echo "GatewayRpcIT|theStatusIsReadOnly()" ;;
    *) echo "unknown mutation $1" >&2; return 1 ;;
  esac
}

. scripts/lib/mutation.sh
pkg=io.github.ayomide1x.objectstore.coordinator

# gradle_test [PATTERN]: runs the tests matching PATTERN, or all of them.
# --rerun runs the test task even when Gradle thinks it is up to date.
gradle_test() {
  rm -rf "$reports"
  cd coordinator
  if [[ $# -gt 0 ]]; then
    ./gradlew -q test --rerun --tests "$1" >/dev/null 2>&1 || true
  else
    ./gradlew -q test --rerun >/dev/null 2>&1 || true
  fi
}

gradle_compile() {
  cd coordinator && ./gradlew -q compileJava compileTestJava >/dev/null 2>&1
}

# Prints the test count (over the classes that ran) on the first line, then
# one failed test name per line.
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

require_unapplied scripts/mutations/*/*.patch

start=$SECONDS
status=0
for patch in scripts/mutations/coordinator/*.patch; do
  name="$(basename "$patch" .patch)"
  expected="$(expected_test "$name")"
  class="${expected%%|*}"
  target="${expected#*|}"
  t0=$SECONDS
  apply_mutation "$patch"
  # A mutation that doesn't compile would fail every test and look detected.
  if ! run_child gradle_compile; then
    mutation_revert
    echo "=== mutation $name"
    echo "    BUILD FAILED: the mutation does not compile, so it proves nothing"
    status=1
    continue
  fi
  run_child gradle_test "$pkg.$class"
  mutation_revert

  summary="$(summarize)"
  total="$(sed -n 1p <<<"$summary")"
  failed="$(sed 1d <<<"$summary")"
  echo "=== mutation $name ($class, $((SECONDS - t0))s)"
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

echo "=== unmutated (full suite)"
run_child gradle_test
summary="$(summarize)"
failed="$(sed 1d <<<"$summary")"
echo "    tests: $(sed -n 1p <<<"$summary"), failed: $(grep -c . <<<"$failed" || true)"
while IFS= read -r f; do
  if [[ -n "$f" ]]; then echo "    FAILED $f"; status=1; fi
done <<<"$failed"
echo "=== total time: $((SECONDS - start))s"
exit "$status"
