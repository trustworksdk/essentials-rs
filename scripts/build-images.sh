#!/usr/bin/env bash
# Builds the exchange-simulator and payment-gateway images from this checkout, for Compose projects on this machine
# that refer to them by tag (see exchange-simulator/docs/running-with-your-app.md).
#
#   scripts/build-images.sh              # exchange-simulator:dev and payment-gateway:dev
#   TAG=demo scripts/build-images.sh     # exchange-simulator:demo and payment-gateway:demo
#   scripts/build-images.sh exchange     # one image only: exchange or gateway
#
# Both images build from the workspace root: they share Cargo.lock and the simulator-kit crate. Run it again after
# pulling changes; the cargo cache mounts in the Dockerfiles keep rebuilds incremental.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tag="${TAG:-dev}"
which="${1:-all}"

build() {
  local name="$1"
  echo "==> building ${name}:${tag}"
  docker build --file "${root}/${name}/Dockerfile" --tag "${name}:${tag}" "${root}"
}

case "${which}" in
  all) build exchange-simulator; build payment-gateway ;;
  exchange) build exchange-simulator ;;
  gateway) build payment-gateway ;;
  *) echo "usage: $0 [all|exchange|gateway]" >&2; exit 2 ;;
esac

echo "==> done:"
docker image ls --filter "reference=exchange-simulator:${tag}" --filter "reference=payment-gateway:${tag}" \
  --format '{{.Repository}}:{{.Tag}}  {{.CreatedSince}}  {{.Size}}'
