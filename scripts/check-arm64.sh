#!/usr/bin/env bash
# Invariant 10 (arm64-clean), checked:
#
#   1. Every image the Compose file runs, built or pulled, is linux/arm64
#      locally (docker image inspect). Build first: docker compose build, or
#      up --build.
#   2. Every third-party image we use, in Compose, the Dockerfiles, the
#      scripts and the coordinator tests, publishes a linux/arm64 build
#      (docker buildx imagetools inspect, which reads the registry).
#
# Run it on an arm64 machine: there, step 1 proves the images run natively.
# On amd64, step 1 fails by design and step 2 still applies.
#
# Usage: scripts/check-arm64.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
compose=(docker compose -f "$root/docker-compose.yml")
status=0
ok() { echo "  ok: $*"; }
fail() { echo "  FAIL: $*"; status=1; }

echo "1. images Compose runs (local)"
for image in $("${compose[@]}" config --images | sort -u); do
  platform="$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image" 2>/dev/null || true)"
  if [[ -z "$platform" ]]; then
    fail "$image: not present locally (run docker compose build)"
  elif [[ "$platform" == linux/arm64 ]]; then
    ok "$image: $platform"
  else
    fail "$image: $platform"
  fi
done

# Every FROM line in our Dockerfiles, every image the scripts run, and the
# images the coordinator tests start. A new image anywhere must appear here,
# so the list is checked against the sources below.
third_party=(
  postgres:18                                # Compose, coordinator tests (Testcontainers)
  eclipse-temurin:25-jdk                     # coordinator build stage
  eclipse-temurin:25-jre                     # coordinator runtime
  golang:1.27                                # node and gateway build stage
  gcr.io/distroless/static-debian12:nonroot  # node and gateway runtime
  alpine:3                                   # scripts: volume helpers, fsync trace (apk strace)
  docker:cli                                 # stage 3 crash watcher
  testcontainers/ryuk:0.14.0                 # Testcontainers' cleanup container
)

echo "2. third-party images (registry)"
found="$(
  {
    grep -h '^FROM' "$root"/*/Dockerfile | awk '{print $2}'
    grep -h 'image:' "$root/docker-compose.yml" | awk '{print $2}'
    grep -rhoE '\b(alpine|docker):[A-Za-z0-9._-]+' "$root/scripts" --include='*.sh'
    grep -rhoE 'new PostgreSQLContainer\("[^"]+"\)' "$root/coordinator/src/test" | sed -E 's/.*\("([^"]+)"\)/\1/'
  } | sort -u
)"
for image in $found; do
  printf '%s\n' "${third_party[@]}" | grep -qxF "$image" || fail "$image is used but not in this script's list"
done
for image in "${third_party[@]}"; do
  if platforms="$(docker buildx imagetools inspect "$image" 2>/dev/null | awk '/Platform:/ {print $2}')"; then
    if grep -qE '^linux/arm64(/v8)?$' <<<"$platforms"; then
      ok "$image: $(grep -E '^linux/arm64' <<<"$platforms" | head -1)"
    else
      fail "$image: no linux/arm64 build (has: $(tr '\n' ' ' <<<"$platforms"))"
    fi
  else
    fail "$image: could not read its manifest"
  fi
done

echo
if ((status == 0)); then echo "PASS"; else echo "FAIL"; fi
exit "$status"
