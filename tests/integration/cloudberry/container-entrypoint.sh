#!/usr/bin/env bash
# Restart-aware PID 1 for the disposable Cloudberry integration container.
set -euo pipefail

GPHOME=/usr/local/cloudberry-db
DATADIR=/data
COORDINATOR_DATA_DIRECTORY="${DATADIR}/coordinator/gpseg-1"

ssh-keygen -A >/dev/null 2>&1 || true
/usr/sbin/sshd 2>/dev/null || true

if [[ -s "${COORDINATOR_DATA_DIRECTORY}/postgresql.conf" ]]; then
  echo "== starting existing Cloudberry integration cluster =="
  su - gpadmin -c "
    set -e
    source ${GPHOME}/cloudberry-env.sh
    export MASTER_DATA_DIRECTORY=${COORDINATOR_DATA_DIRECTORY}
    export COORDINATOR_DATA_DIRECTORY=${COORDINATOR_DATA_DIRECTORY}
    gpstart -a
  "
fi

exec sleep infinity

