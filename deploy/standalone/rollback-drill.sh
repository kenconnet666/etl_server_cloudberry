#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
COMPOSE=(docker compose --project-directory "$SCRIPT_DIR" -f "$SCRIPT_DIR/compose.yaml")

usage() {
  cat >&2 <<'EOF'
Usage:
  PG2CB_PREVIOUS_IMAGE=<repository@sha256:digest> \
  PG2CB_CONTROL_BACKUP=/absolute/path/control.dump \
  ./rollback-drill.sh

The script stops caddy/app, restores the paired control backup, runs the previous
image's migration command, and starts that exact image. It writes machine-readable
evidence when PG2CB_ROLLBACK_RESULT_FILE is set. Mutable image tags are rejected
unless PG2CB_ALLOW_MUTABLE_IMAGE=1 is set for a disposable local drill.
EOF
}

previous_image=${PG2CB_PREVIOUS_IMAGE:-}
backup=${PG2CB_CONTROL_BACKUP:-}
[[ -n "$previous_image" && -n "$backup" ]] || { usage; exit 2; }
[[ -r "$backup" ]] || { echo "control backup is not readable: $backup" >&2; exit 2; }
if [[ "$previous_image" != *@sha256:* && ${PG2CB_ALLOW_MUTABLE_IMAGE:-} != 1 ]]; then
  echo "PG2CB_PREVIOUS_IMAGE must be an immutable repository@sha256 reference" >&2
  exit 2
fi
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }

if ! docker image inspect "$previous_image" >/dev/null 2>&1; then
  docker pull "$previous_image" >/dev/null
fi
previous_image_id=$(docker image inspect --format '{{.Id}}' "$previous_image")
backup_sha256=$(sha256sum "$backup" | awk '{print $1}')
started_unix_ms=$(python3 -c 'import time; print(time.time_ns() // 1_000_000)')
backup_mtime_unix_ms=$(python3 - "$backup" <<'PY'
import os
import sys

print(os.stat(sys.argv[1]).st_mtime_ns // 1_000_000)
PY
)
backup_age_milliseconds=$((started_unix_ms - backup_mtime_unix_ms))
(( backup_age_milliseconds >= 0 )) || {
  echo "control backup modification time is in the future: $backup" >&2
  exit 2
}

"${COMPOSE[@]}" stop --timeout 45 caddy app >/dev/null
"${COMPOSE[@]}" rm --force --stop migrate >/dev/null 2>&1 || true
PG2CB_CONFIRM_LIVE_RESTORE=pg2cb_control \
  "$SCRIPT_DIR/control-db-backup.sh" restore-live "$backup"

PG2CB_IMAGE=$previous_image "${COMPOSE[@]}" run --rm migrate
# The one-shot migration above is the only migration allowed during this drill. Starting
# app/caddy without dependencies prevents Compose from silently creating another migrate job.
PG2CB_IMAGE=$previous_image "${COMPOSE[@]}" up -d --no-build --no-deps app
PG2CB_IMAGE=$previous_image "${COMPOSE[@]}" up -d --no-build --no-deps caddy

deadline=$((SECONDS + 180))
while :; do
  app_container=$(PG2CB_IMAGE=$previous_image "${COMPOSE[@]}" ps --quiet app)
  if [[ -n "$app_container" ]]; then
    app_state=$(docker inspect --format '{{.State.Status}}' "$app_container")
    app_health=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}' "$app_container")
    [[ "$app_state" == running && "$app_health" == healthy ]] && break
  fi
  if (( SECONDS >= deadline )); then
    echo "previous application image did not become healthy within 180 seconds" >&2
    PG2CB_IMAGE=$previous_image "${COMPOSE[@]}" ps >&2
    exit 1
  fi
  sleep 2
done

actual_image_id=$(docker inspect --format '{{.Image}}' "$app_container")
[[ "$actual_image_id" == "$previous_image_id" ]] || {
  echo "rollback app image mismatch: expected $previous_image_id, found $actual_image_id" >&2
  exit 1
}
PG2CB_IMAGE=$previous_image "${COMPOSE[@]}" exec -T app \
  curl --fail --silent http://127.0.0.1:8080/health/ready >/dev/null

completed_unix_ms=$(python3 -c 'import time; print(time.time_ns() // 1_000_000)')
rto_milliseconds=$((completed_unix_ms - started_unix_ms))
result_file=${PG2CB_ROLLBACK_RESULT_FILE:-}
python3 - "$previous_image" "$previous_image_id" "$backup" "$backup_sha256" \
  "$backup_mtime_unix_ms" "$backup_age_milliseconds" "$started_unix_ms" \
  "$completed_unix_ms" "$rto_milliseconds" "$result_file" <<'PY'
import json
import sys

result = {
    "previous_image": sys.argv[1],
    "previous_image_id": sys.argv[2],
    "control_backup": sys.argv[3],
    "control_backup_sha256": sys.argv[4],
    "control_backup_mtime_unix_ms": int(sys.argv[5]),
    "control_backup_age_milliseconds_at_start": int(sys.argv[6]),
    "started_unix_ms": int(sys.argv[7]),
    "completed_unix_ms": int(sys.argv[8]),
    "rto_milliseconds": int(sys.argv[9]),
    "app_healthy": True,
    "ready": True,
    "image_identity_verified": True,
}
encoded = json.dumps(result, separators=(",", ":"))
if sys.argv[10]:
    with open(sys.argv[10], "w", encoding="utf-8") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
print(encoded)
PY
