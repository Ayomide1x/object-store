#!/usr/bin/env bash
# Stage 11 acceptance: the web UI, in a real browser, against a fresh stack.
#   0. docker compose down -v && up --build; waits for 4 nodes ALIVE and both
#      gateways ready, and both gateways' Docker healthchecks healthy (the
#      probe sends an allowed Host, so it passes the Host check).
#   1. The Playwright suite (web/e2e), in order:
#        01-demo: while scripts/demo.sh runs (DEMO_PAUSE=5), the UI shows the
#          stopped node go Suspect then Dead, the repair pass that finds its
#          chunks under-replicated, the repair copies landing (every chunk
#          back to 3 of 3), and the demo's corrupted replica as Bad, then its
#          chunk repaired. demo.sh must pass too.
#        02-objects: browser uploads (0 B, 1 B, 4 MiB, 10 MiB) download
#          identically with osctl, and an osctl upload downloads identically
#          in the browser; paging, delete with confirmation, the . and ..
#          refusal.
#        03-corrupt: a corrupted replica shows as Bad, then its chunk is
#          repaired.
#        04-broken-download: a download whose later chunk has no good
#          replica is a failed download in the browser.
#   2. The real gateways refuse a foreign Host (403 on reads, writes and
#      /healthz alike) and a cross-origin DELETE (403, and the object stays),
#      and serve the UI with its Content-Security-Policy.
# Every wait in the browser tests is an assertion on a state; none sleeps.
#
# Usage: scripts/stage11-ui-check.sh
#   UPDATE_SCREENSHOTS=1 also copies the demo screenshots into docs/.
#   PW_CHANNEL=chrome runs the installed Google Chrome instead of Chromium.
# Needs bin/osctl (make build), and web/ set up: cd web && npm ci &&
# npx playwright install chromium.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
osctl="$root/bin/osctl"
compose=(docker compose -f "$root/docker-compose.yml")
source "$root/scripts/lib/osctl.sh"
source "$root/scripts/lib/liveness.sh"
source "$root/scripts/lib/gateway.sh"
[[ -x "$osctl" ]] || { echo "FAIL: $osctl missing: run make build"; exit 1; }
[[ -d "$root/web/node_modules" ]] || { echo "FAIL: web/node_modules missing: cd web && npm ci && npx playwright install chromium"; exit 1; }
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

status=0
fail() { echo "  FAIL: $*"; status=1; }
ok() { echo "  ok: $*"; }
t_start=$SECONDS

echo "0. a fresh stack"
"${compose[@]}" down -v >/dev/null 2>&1
"${compose[@]}" up --build --wait -d >"$work/up.log" 2>&1 || { tail -20 "$work/up.log"; echo "FAIL: the stack did not start"; exit 1; }
LIVENESS_WAIT_TIMEOUT=60 wait_alive 4 >/dev/null || { echo "FAIL: not all 4 nodes ALIVE: $(liveness_states)"; exit 1; }
wait_gateway "$GATEWAY1" || exit 1
wait_gateway "$GATEWAY2" || exit 1
for g in gateway1 gateway2; do
  health="$(docker inspect --format '{{.State.Health.Status}}' "$("${compose[@]}" ps -q "$g")")"
  [[ "$health" == healthy ]] && ok "$g healthcheck: healthy (with GATEWAY_ALLOWED_HOSTS set)" || fail "$g healthcheck: $health"
done

echo "1. the browser tests (Playwright)"
if (cd "$root/web" && npx playwright test) >"$work/playwright.log" 2>&1; then
  grep -E '^\s+✓' "$work/playwright.log" | sed 's/^ */  ok: /'
  ok "$(grep -E '[0-9]+ passed' "$work/playwright.log" | tr -s ' ')"
else
  cat "$work/playwright.log"
  fail "the Playwright suite failed (traces under web/test-results)"
fi
if [[ "${UPDATE_SCREENSHOTS:-}" == 1 ]]; then
  mkdir -p "$root/docs"
  for s in ui-demo-repair ui-demo-repair-dark ui-demo-repaired; do
    if [[ -f "$root/web/test-results/$s.png" ]]; then
      cp "$root/web/test-results/$s.png" "$root/docs/$s.png" && ok "docs/$s.png updated"
    fi
  done
fi

echo "2. the gateways' Host and origin checks"
key="stage11/$$/guarded.bin"
head -c 1000 /dev/urandom > "$work/guarded"
osctl_run put "$work/guarded" "$key" >/dev/null
url="$(key_url "$GATEWAY1" "$key")"
path="${url#"$GATEWAY1"}"
code() { curl -sS -o /dev/null -w '%{http_code}' "$@"; }
for target in "GET $path" "GET $path/stat" "GET /objects" "GET /cluster" "GET /healthz" "GET /" "DELETE $path" "PUT $path"; do
  read -r method p <<<"$target"
  got="$(code -X "$method" -H 'Host: evil.example:9081' --data-binary @/dev/null "$GATEWAY1$p")"
  [[ "$got" == 403 ]] && ok "$method $p with Host evil.example:9081: 403" || fail "$method $p with Host evil.example:9081: $got, want 403"
done
got="$(code -X DELETE -H 'Origin: http://evil.example' "$url")"
[[ "$got" == 403 ]] && ok "cross-origin DELETE: 403" || fail "cross-origin DELETE: $got, want 403"
got="$(code -X DELETE -H 'Sec-Fetch-Site: cross-site' "$url")"
[[ "$got" == 403 ]] && ok "DELETE with Sec-Fetch-Site cross-site: 403" || fail "DELETE with Sec-Fetch-Site cross-site: $got"
"$osctl" stat "$key" >/dev/null 2>&1 && ok "the object is still there" || fail "the object was deleted"
for g in "$GATEWAY1" "$GATEWAY2"; do
  csp="$(curl -sS -D - -o /dev/null "$g/" | tr -d '\r' | sed -n 's/^[Cc]ontent-[Ss]ecurity-[Pp]olicy: //p')"
  [[ "$csp" == *"script-src 'self'"* && "$csp" != *unsafe-inline* ]] && ok "$g/ serves the UI with its CSP" || fail "$g/ CSP: '$csp'"
done
"$osctl" rm "$key" >/dev/null 2>&1 || true

echo
if ((status == 0)); then
  echo "PASS (stage 11 UI acceptance, $((SECONDS - t_start))s)"
else
  echo "FAIL"
fi
exit $status
