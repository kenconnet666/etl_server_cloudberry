#!/usr/bin/env bash
# Bring up a single-host Apache Cloudberry 2.1 inside a rockylinux9 container that
# already has the Cloudberry RPM installed at /usr/local/cloudberry-db.
#
# This is the local/CI workaround for the missing official runnable Cloudberry
# image (see tests/integration/README.md). It creates the gpadmin user, sets up
# passwordless SSH to localhost, initializes a single-segment demo cluster with
# gpinitsystem, and exposes it on 0.0.0.0:5432 for the target integration tests.
#
# Run as root inside the container:
#   docker exec cbtry bash /path/to/init-singlenode.sh
set -euo pipefail

GPHOME=/usr/local/cloudberry-db
DATADIR=/data
SEGMENT_COUNT=${CBDB_SEGMENTS:-1}
export MASTER_DATA_DIRECTORY="${DATADIR}/coordinator/gpseg-1"

if [[ ! ${SEGMENT_COUNT} =~ ^[1-9][0-9]*$ ]]; then
  echo "CBDB_SEGMENTS must be a positive integer" >&2
  exit 1
fi

echo "== create gpadmin user =="
id gpadmin >/dev/null 2>&1 || useradd -m -s /bin/bash gpadmin
mkdir -p "${DATADIR}"
chown -R gpadmin:gpadmin "${DATADIR}" "${GPHOME}"

echo "== start sshd and set up passwordless ssh to localhost =="
ssh-keygen -A >/dev/null 2>&1 || true
/usr/sbin/sshd 2>/dev/null || true
su - gpadmin -c '
  set -e
  mkdir -p ~/.ssh && chmod 700 ~/.ssh
  [ -f ~/.ssh/id_rsa ] || ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa >/dev/null
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
  ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null
  ssh-keyscan -H "$(hostname)" >> ~/.ssh/known_hosts 2>/dev/null
'

echo "== write gpinitsystem config =="
HOST=$(hostname)
su - gpadmin -c "cat > ~/hostfile <<EOF
${HOST}
EOF"
SEGMENT_DIRECTORIES=""
for index in $(seq 1 "${SEGMENT_COUNT}"); do
  directory="${DATADIR}/primary${index}"
  mkdir -p "${directory}"
  chown gpadmin:gpadmin "${directory}"
  SEGMENT_DIRECTORIES+="${directory} "
done
su - gpadmin -c "mkdir -p ${DATADIR}/coordinator"
su - gpadmin -c "cat > ~/gpinitsystem_config <<EOF
ARRAY_NAME=\"cbdb singlenode\"
SEG_PREFIX=gpseg
PORT_BASE=6000
declare -a DATA_DIRECTORY=(${SEGMENT_DIRECTORIES})
COORDINATOR_HOSTNAME=${HOST}
COORDINATOR_DIRECTORY=${DATADIR}/coordinator
COORDINATOR_PORT=5432
TRUSTED_SHELL=ssh
ENCODING=UNICODE
EOF"

echo "== run gpinitsystem =="
su - gpadmin -c "
  source ${GPHOME}/cloudberry-env.sh
  export MASTER_DATA_DIRECTORY=${MASTER_DATA_DIRECTORY}
  gpinitsystem -a -c ~/gpinitsystem_config -h ~/hostfile 2>&1 | tail -30
"

echo "== open external access on 5432 =="
su - gpadmin -c "
  source ${GPHOME}/cloudberry-env.sh
  export COORDINATOR_DATA_DIRECTORY=${MASTER_DATA_DIRECTORY}
  echo \"host all all 0.0.0.0/0 trust\" >> ${MASTER_DATA_DIRECTORY}/pg_hba.conf
  echo \"listen_addresses='*'\" >> ${MASTER_DATA_DIRECTORY}/postgresql.conf
  gpstop -u -a 2>&1 | tail -5
  createdb target 2>/dev/null || true
  psql -d target -c 'SELECT version();'
"
echo "== single-host Cloudberry with ${SEGMENT_COUNT} primary segment(s) ready on :5432 (db=target, user=gpadmin) =="
