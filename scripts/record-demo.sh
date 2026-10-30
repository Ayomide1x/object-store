#!/usr/bin/env bash
# Records scripts/demo.sh for the README: docs/demo.cast (asciinema, text,
# replay with `asciinema play docs/demo.cast`) and docs/demo.gif (agg, shown
# inline on GitHub). Idle stretches longer than 2 s play back as 2 s, so the
# waits are short to watch while the demo itself runs on the shipped timings.
#
# It resets the stack first (the demo needs a fresh one), and keeps the
# recording only if the demo ends in PASS.
#
# Usage: scripts/record-demo.sh
# Needs asciinema and agg (brew install asciinema agg), and bin/osctl.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
for tool in asciinema agg; do
  command -v "$tool" >/dev/null || { echo "record-demo: $tool not found (brew install asciinema agg)" >&2; exit 1; }
done
# The flags below are asciinema 3's (the Rust CLI); 2.x differs.
[[ "$(asciinema --version)" == *" 3."* ]] || { echo "record-demo: needs asciinema 3.x, found: $(asciinema --version)" >&2; exit 1; }
[[ -x bin/osctl ]] || { echo "record-demo: bin/osctl missing: run make build" >&2; exit 1; }

echo "resetting the stack: docker compose down -v && docker compose up --build --wait"
docker compose down -v >/dev/null 2>&1
docker compose up --build --wait -d >/dev/null 2>&1

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
# --return: asciinema exits with the demo's status, so a FAIL saves nothing.
# asciicast v2: the format every agg and player version reads.
if ! asciinema rec --return --overwrite --output-format asciicast-v2 --idle-time-limit 2 \
  --window-size 100x34 --title "object store demo" --command "scripts/demo.sh" "$tmp/demo.cast"; then
  echo "record-demo: the demo did not PASS; nothing saved" >&2
  exit 1
fi
mkdir -p docs
cp "$tmp/demo.cast" docs/demo.cast
agg --idle-time-limit 2 --last-frame-duration 5 --font-size 14 docs/demo.cast docs/demo.gif
echo "saved docs/demo.cast ($(du -h docs/demo.cast | cut -f1)) and docs/demo.gif ($(du -h docs/demo.gif | cut -f1))"
