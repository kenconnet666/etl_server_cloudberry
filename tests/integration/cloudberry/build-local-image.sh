#!/usr/bin/env bash
# Build a runnable single-host Apache Cloudberry 2.1 Docker image locally.
#
# Apache Cloudberry 2.1 publishes no ready-to-run server image (only build/test
# env images). This script closes that gap: it installs the official 2.1.0
# convenience RPM into a Rocky Linux 9 base, commits that as `cbdb-local:2.1.0`,
# then runs a container and initializes a single-segment demo cluster via
# init-singlenode.sh. The result serves the target integration tests on
# host port 55433 (db=target, user=gpadmin).
#
# Prereqs: docker, gh (authenticated) or a pre-downloaded el9 RPM in ./rpm.
# Usage:   bash tests/integration/cloudberry/build-local-image.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${CBDB_WORK:-$HOME/cbdb}"
RPM_GLOB="apache-cloudberry-db-incubating-2.1.0-1.el9.x86_64.rpm"
IMAGE="cbdb-local:2.1.0"
CONTAINER="${CBDB_CONTAINER:-cbdb}"
HOST_PORT="${CBDB_PORT:-55433}"
SEGMENT_COUNT="${CBDB_SEGMENTS:-1}"

mkdir -p "${WORK}"

if ! ls "${WORK}/${RPM_GLOB}" >/dev/null 2>&1; then
  echo "== downloading Cloudberry 2.1.0 el9 RPM =="
  gh release download 2.1.0-incubating --repo apache/cloudberry \
    --pattern "*el9*.rpm" --dir "${WORK}" --skip-existing
fi

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
  echo "== installing RPM into rockylinux9 and committing ${IMAGE} =="
  docker rm -f cbdb-build >/dev/null 2>&1 || true
  docker run -d --name cbdb-build -v "${WORK}:/rpm" rockylinux:9 sleep 3600
  docker exec cbdb-build bash -c \
    "dnf install -y -q /rpm/${RPM_GLOB}"
  docker commit cbdb-build "${IMAGE}"
  docker rm -f cbdb-build >/dev/null 2>&1 || true
fi

echo "== starting ${CONTAINER} on host port ${HOST_PORT} =="
cp "${HERE}/init-singlenode.sh" "${WORK}/init-singlenode.sh"
cp "${HERE}/container-entrypoint.sh" "${WORK}/container-entrypoint.sh"
sed -i 's/\r$//' "${WORK}/init-singlenode.sh"
sed -i 's/\r$//' "${WORK}/container-entrypoint.sh"
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" --shm-size=1g \
  -p "${HOST_PORT}:5432" -v "${WORK}:/rpm" "${IMAGE}" \
  bash /rpm/container-entrypoint.sh

echo "== initializing single-host cluster with ${SEGMENT_COUNT} primary segment(s) (slow) =="
docker exec -e CBDB_SEGMENTS="${SEGMENT_COUNT}" "${CONTAINER}" bash /rpm/init-singlenode.sh

echo
echo "Cloudberry ready. Point target tests at it with:"
echo "  export PG2CB_TEST_TARGET_DSN=postgres://gpadmin@127.0.0.1:${HOST_PORT}/target"
