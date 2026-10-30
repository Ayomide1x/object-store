#!/usr/bin/env bash
# Mutation check for the web UI's rules (web/): key encoding, the grid
# derivation, upload messages, polling, and the lint rules that keep keys out
# of markup and object bytes out of JavaScript.
#
# For each patch in scripts/mutations/web/: apply it, run the unit tests
# (Vitest) and lint, report what failed, and revert. Each mutation names the
# test (or lint rule) that must catch it. A mutation that does not typecheck
# is reported as BUILD FAILED, never as detected. Finally, run the unmutated
# checks.
#
# One more mutation needs the real stack, so it is not run here: the
# gateway's mid-stream abort replaced by a normal return (the Go patch
# w1-complete-after-midstream-failure) must make the browser test
# 04-broken-download fail. To run it:
#   git apply scripts/mutations/go/w1-complete-after-midstream-failure.patch
#   docker compose up -d --build --wait gateway1
#   (cd web && npx playwright test e2e/04-broken-download.spec.ts)   # must fail
#   git apply -R scripts/mutations/go/w1-complete-after-midstream-failure.patch
#   docker compose up -d --build --wait gateway1
#
# Usage: scripts/web-mutation-check.sh
# Needs web/node_modules (cd web && npm ci). Refuses to start if any patch in
# scripts/mutations/ is already applied. The mutation is reverted on exit,
# Ctrl-C and kill.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# What must fail under each mutation: a Vitest test ("file > describe > it")
# or "lint: RULE".
expected_test() {
  case "$1" in
    u1-encode-uri) echo "src/keys.test.ts > encodeKey > encodes reserved characters so a key stays one path segment" ;;
    u2-no-dot-segment-refusal) echo "src/keys.test.ts > encodeKey > refuses dot segments, which the URL parser would remove" ;;
    u3-repair-copy-shown-as-upload) echo "src/grid.test.ts > deriveGrid > marks repair copies, and only HEALTHY ones" ;;
    u4-bad-shown-as-healthy) echo "src/grid.test.ts > deriveGrid > shows BAD as bad, with its reason, never as healthy" ;;
    u5-absent-shown-as-bad) echo "src/grid.test.ts > deriveGrid > tells a node with no copy apart from a bad copy" ;;
    u6-retry-after-ignored) echo "src/uploadMessages.test.ts > describeUpload > uses Retry-After for a busy gateway" ;;
    u7-drop-after-send-reported-as-not-stored) echo "src/uploadMessages.test.ts > describeUpload > says a drop after the whole file was sent is outcome-unknown, never 'nothing stored'" ;;
    u8-size-limit-exclusive) echo "src/keys.test.ts > sizeProblem > allows exactly 5 GiB and refuses one byte more" ;;
    u9-key-rendered-as-markup) echo "src/components/KeyText.test.tsx > KeyText > renders markup in a key as text" ;;
    u10-download-through-a-blob) echo "lint: no-restricted-syntax" ;;
    u11-poller-overlaps-requests) echo "src/poller.test.ts > startPoller > never starts a request while one is in flight" ;;
    u12-polls-while-hidden) echo "src/poller.test.ts > startPoller > stops while the tab is hidden and polls at once when it is shown" ;;
    *) echo "unknown mutation $1" >&2; return 1 ;;
  esac
}

. scripts/lib/mutation.sh
out="$(mktemp)"
json="$(mktemp)"
trap 'rm -f "$out" "$json"; mutation_revert' EXIT

# web_checks FILE: typechecks, then writes the failed Vitest tests and the
# violated lint rules ("lint: RULE"), one per line, to FILE; or "BUILD FAILED".
web_checks() {
  if ! (cd web && npx tsc --noEmit) >/dev/null 2>&1; then
    echo "BUILD FAILED" >"$1"
    return
  fi
  : >"$1"
  (cd web && npx vitest run --reporter=json --outputFile="$json") >/dev/null 2>&1 || true
  python3 -I - "$json" "$root/web/" >>"$1" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
for f in report.get("testResults", []):
    name = f["name"].replace(sys.argv[2], "")
    for a in f.get("assertionResults", []):
        if a["status"] == "failed":
            print(" > ".join([name] + a["ancestorTitles"] + [a["title"]]))
PY
  (cd web && npx eslint --format json .) 2>/dev/null | python3 -I -c '
import json, sys
rules = {m["ruleId"] for f in json.load(sys.stdin) for m in f["messages"] if m.get("ruleId")}
for r in sorted(rules):
    print("lint: " + r)
' >>"$1" || true
}

require_unapplied scripts/mutations/*/*.patch

status=0
for patch in scripts/mutations/web/*.patch; do
  name="$(basename "$patch" .patch)"
  target="$(expected_test "$name")"
  apply_mutation "$patch"
  run_child web_checks "$out"
  mutation_revert
  failed="$(cat "$out")"

  echo "=== mutation $name"
  if [[ "$failed" == "BUILD FAILED" ]]; then
    echo "    BUILD FAILED: the mutation does not typecheck, so it proves nothing"
    status=1
    continue
  fi
  echo "    failed: $(grep -c . <<<"$failed" || true)"
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
run_child web_checks "$out"
failed="$(cat "$out")"
echo "    failed: $(grep -c . <<<"$failed" || true)"
while IFS= read -r f; do
  if [[ -n "$f" ]]; then echo "    FAILED $f"; status=1; fi
done <<<"$failed"
exit "$status"
