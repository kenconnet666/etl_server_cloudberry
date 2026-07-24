#!/usr/bin/env bash
set -Eeuo pipefail

# Control-plane backups are deliberately independent from the Docker volume. Keep the
# destination on a separate disk or backup service and retain the image digest beside it.

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
COMPOSE=(docker compose --project-directory "$SCRIPT_DIR" -f "$SCRIPT_DIR/compose.yaml")

usage() {
  cat <<'EOF'
Usage:
  control-db-backup.sh backup [output.dump]
  control-db-backup.sh verify <archive.dump>
  control-db-backup.sh restore-drill <archive.dump> [temporary_database]
  PG2CB_CONFIRM_LIVE_RESTORE=pg2cb_control control-db-backup.sh restore-live <archive.dump>

PG2CB_BACKUP_DIR controls the default backup destination (default: ./backups).
The restore drill creates and drops a temporary database in control-db; it never
overwrites pg2cb_control.
restore-live is destructive and refuses to run while app, migrate, or caddy is running.
EOF
}

compose_exec() {
  "${COMPOSE[@]}" exec -T control-db "$@"
}

postgres_admin_exec() {
  # POSTGRES_USER is intentionally the application-owned superuser in compose.yaml;
  # the official image does not create a separate role named postgres in this setup.
  "${COMPOSE[@]}" exec -T control-db "$@" -U pg2cb
}

require_archive() {
  local archive=$1
  [[ -f "$archive" ]] || { echo "backup archive does not exist: $archive" >&2; exit 2; }
  [[ -r "$archive" ]] || { echo "backup archive is not readable: $archive" >&2; exit 2; }
}

verify_archive() {
  local archive=$1
  require_archive "$archive"
  local listing
  listing=$(compose_exec pg_restore --list < "$archive")
  grep -q 'cloudberry_etl_control' <<< "$listing" || {
    echo "backup archive does not contain the control schema" >&2
    exit 1
  }
  grep -q 'schema_migrations' <<< "$listing" || {
    echo "backup archive does not contain schema_migrations" >&2
    exit 1
  }
}

backup() {
  local backup_dir=${PG2CB_BACKUP_DIR:-"$SCRIPT_DIR/backups"}
  local destination=${1:-"$backup_dir/pg2cb-control-$(date -u +%Y%m%dT%H%M%SZ).dump"}
  mkdir -p -- "$backup_dir"
  chmod 700 -- "$backup_dir"
  [[ "$destination" = /* ]] || destination=$(cd -- "$(dirname -- "$destination")" && pwd)/$(basename -- "$destination")
  local parent
  parent=$(dirname -- "$destination")
  mkdir -p -- "$parent"
  chmod 700 -- "$parent"
  local temporary
  temporary=$(mktemp "$parent/.pg2cb-control.XXXXXX")
  trap 'rm -f -- "$temporary"' EXIT

  compose_exec pg_dump -U pg2cb -d pg2cb_control -Fc --no-owner --no-privileges > "$temporary"
  verify_archive "$temporary"
  chmod 600 -- "$temporary"
  mv -f -- "$temporary" "$destination"
  trap - EXIT
  echo "$destination"
}

restore_drill() {
  local archive=$1
  local database=${2:-pg2cb_control_restore_drill}
  require_archive "$archive"
  [[ "$database" =~ ^[a-z_][a-z0-9_]{0,62}$ ]] || {
    echo "temporary database name must match [a-z_][a-z0-9_]{0,62}" >&2
    exit 2
  }
  verify_archive "$archive"

  postgres_admin_exec psql -d postgres -v ON_ERROR_STOP=1 \
    -c "DROP DATABASE IF EXISTS \"$database\""
  postgres_admin_exec psql -d postgres -v ON_ERROR_STOP=1 \
    -c "CREATE DATABASE \"$database\" OWNER pg2cb"
  cleanup_restore_drill() {
    local restore_database=$1
    postgres_admin_exec psql -d postgres -v ON_ERROR_STOP=1 \
      -c "DROP DATABASE IF EXISTS \"$restore_database\"" >/dev/null
  }
  restore_database_name=$database
  trap 'cleanup_restore_drill "$restore_database_name"' EXIT

  compose_exec pg_restore -U pg2cb -d "$database" --no-owner --no-privileges --exit-on-error < "$archive"
  local table_count
  table_count=$(postgres_admin_exec psql -d "$database" -At -v ON_ERROR_STOP=1 \
    -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'cloudberry_etl_control'")
  [[ "$table_count" =~ ^[1-9][0-9]*$ ]] || {
    echo "restore drill produced no control tables" >&2
    exit 1
  }
  cleanup_restore_drill "$database"
  trap - EXIT
    echo "restore drill passed: $table_count control tables restored to $database"
}

require_service_stopped() {
  local service=$1
  local container
  while IFS= read -r container; do
    [[ -n "$container" ]] || continue
    if [[ $(docker inspect --format '{{.State.Running}}' "$container") == true ]]; then
      echo "refusing live restore while Compose service $service is running ($container)" >&2
      exit 1
    fi
  done < <("${COMPOSE[@]}" ps --all --quiet "$service")
}

restore_live() {
  local archive=$1
  require_archive "$archive"
  verify_archive "$archive"
  [[ ${PG2CB_CONFIRM_LIVE_RESTORE:-} == pg2cb_control ]] || {
    echo "set PG2CB_CONFIRM_LIVE_RESTORE=pg2cb_control to authorize destructive restore" >&2
    exit 2
  }
  require_service_stopped app
  require_service_stopped migrate
  require_service_stopped caddy

  postgres_admin_exec psql -d postgres -v ON_ERROR_STOP=1 -c \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='pg2cb_control' AND pid <> pg_backend_pid()"
  postgres_admin_exec psql -d postgres -v ON_ERROR_STOP=1 \
    -c 'DROP DATABASE IF EXISTS "pg2cb_control"'
  postgres_admin_exec psql -d postgres -v ON_ERROR_STOP=1 \
    -c 'CREATE DATABASE "pg2cb_control" OWNER pg2cb'
  compose_exec pg_restore -U pg2cb -d pg2cb_control \
    --no-owner --no-privileges --exit-on-error < "$archive"

  local table_count
  table_count=$(postgres_admin_exec psql -d pg2cb_control -At -v ON_ERROR_STOP=1 \
    -c "SELECT count(*) FROM pg_tables WHERE schemaname = 'cloudberry_etl_control'")
  [[ "$table_count" =~ ^[1-9][0-9]*$ ]] || {
    echo "live restore produced no control tables" >&2
    exit 1
  }
  echo "live restore passed: $table_count control tables restored to pg2cb_control"
}

main() {
  local command=${1:-}
  case "$command" in
    backup)
      shift
      [[ $# -le 1 ]] || { usage >&2; exit 2; }
      backup "${1:-}"
      ;;
    verify)
      [[ $# -eq 2 ]] || { usage >&2; exit 2; }
      verify_archive "$2"
      echo "backup archive is valid: $2"
      ;;
    restore-drill)
      [[ $# -ge 2 && $# -le 3 ]] || { usage >&2; exit 2; }
      restore_drill "$2" "${3:-pg2cb_control_restore_drill}"
      ;;
    restore-live)
      [[ $# -eq 2 ]] || { usage >&2; exit 2; }
      restore_live "$2"
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
