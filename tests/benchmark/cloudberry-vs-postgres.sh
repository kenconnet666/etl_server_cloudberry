#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
POSTGRES_CONTAINER=${POSTGRES_CONTAINER:-pg2cb-it-pg18}
CLOUDBERRY_CONTAINER=${CLOUDBERRY_CONTAINER:-cbdb}
POSTGRES_DATABASE=${POSTGRES_DATABASE:-analytics_bench}
CLOUDBERRY_DATABASE=${CLOUDBERRY_DATABASE:-analytics_bench}
BENCH_ROWS=${PG2CB_ANALYTICS_BENCH_ROWS:-5000000}

usage() {
  echo "usage: $0 setup [rows] | run | clean" >&2
}

require_container() {
  local container=$1
  if [[ $(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true) != true ]]; then
    echo "required container is not running: $container" >&2
    exit 1
  fi
}

postgres_psql() {
  docker exec -i "$POSTGRES_CONTAINER" \
    psql -X -v ON_ERROR_STOP=1 -U postgres -d "$POSTGRES_DATABASE" "$@"
}

cloudberry_psql() {
  local command argument
  printf -v command \
    'source /usr/local/cloudberry-db/cloudberry-env.sh; exec psql -X -v ON_ERROR_STOP=1 -d %q' \
    "$CLOUDBERRY_DATABASE"
  for argument in "$@"; do
    printf -v command '%s %q' "$command" "$argument"
  done
  docker exec -i "$CLOUDBERRY_CONTAINER" su - gpadmin -c "$command"
}

ensure_databases() {
  if [[ $(docker exec "$POSTGRES_CONTAINER" psql -XAtq -U postgres -d postgres \
      -c "SELECT count(*) FROM pg_database WHERE datname='$POSTGRES_DATABASE'") == 0 ]]; then
    docker exec "$POSTGRES_CONTAINER" createdb -U postgres "$POSTGRES_DATABASE"
  fi
  local cloudberry_command
  cloudberry_command="source /usr/local/cloudberry-db/cloudberry-env.sh; psql -XAtq -d postgres -c \"SELECT count(*) FROM pg_database WHERE datname='$CLOUDBERRY_DATABASE'\""
  if [[ $(docker exec "$CLOUDBERRY_CONTAINER" su - gpadmin -c "$cloudberry_command") == 0 ]]; then
    docker exec "$CLOUDBERRY_CONTAINER" su - gpadmin -c \
      "source /usr/local/cloudberry-db/cloudberry-env.sh; createdb '$CLOUDBERRY_DATABASE'"
  fi
}

setup_engine() {
  local engine=$1
  local rows=$2
  echo "setting up $engine with $rows fact rows" >&2
  if [[ $engine == postgres ]]; then
    {
      sed 's/\r$//' "$SCRIPT_DIR/setup-postgres.sql"
      sed 's/\r$//' "$SCRIPT_DIR/analytical-views.sql"
    } | postgres_psql -q -v bench_rows="$rows"
  else
    {
      sed 's/\r$//' "$SCRIPT_DIR/setup-cloudberry.sql"
      sed 's/\r$//' "$SCRIPT_DIR/analytical-views.sql"
    } | cloudberry_psql -q -v bench_rows="$rows"
  fi
}

run_engine() {
  local engine=$1
  local output
  echo "running warm-cache analytical suite on $engine" >&2
  if [[ $engine == postgres ]]; then
    output=$(sed 's/\r$//' "$SCRIPT_DIR/analytical-queries.sql" | postgres_psql -q)
  else
    output=$(sed 's/\r$//' "$SCRIPT_DIR/analytical-queries.sql" | cloudberry_psql -q)
  fi
  printf '%s\n' "$output" | awk -v engine="$engine" '
    /^BENCH\|/ {
      split($0, marker, "|")
      query = marker[2]
      run = marker[3]
      next
    }
    $1 == "Execution" && $2 == "Time:" && run > 0 {
      printf "%s,%s,%s,%s\n", engine, query, run, $3
    }
  '
}

print_metadata() {
  local postgres_meta cloudberry_rows cloudberry_bytes
  postgres_meta=$(postgres_psql -Atq -c \
    "SELECT count(*) || ',' || pg_total_relation_size('analytics_bench.fact_sales') FROM analytics_bench.fact_sales")
  cloudberry_rows=$(printf '%s\n' \
    'SELECT count(*) FROM analytics_bench.fact_sales;' \
    | cloudberry_psql -Atq)
  cloudberry_bytes=$(printf '%s\n' \
    "SELECT pg_total_relation_size('analytics_bench.fact_sales');" \
    | cloudberry_psql -Atq)
  echo "engine,row_count,total_relation_bytes"
  echo "postgres,$postgres_meta"
  echo "cloudberry,$cloudberry_rows,$cloudberry_bytes"
}

verify_results() {
  local views=(
    q1_scan_aggregate
    q2_filtered_group
    q3_wide_column_scan
    q4_top_customers
    q5_dimension_join
    q6_point_range
  )
  local view sql postgres_hash cloudberry_hash status
  local mismatches=0
  echo "query,postgres_sha256,cloudberry_sha256,status"
  for view in "${views[@]}"; do
    sql="SELECT * FROM analytics_bench.$view;"
    postgres_hash=$(postgres_psql -Atq -F, -c "$sql" | sha256sum | awk '{print $1}')
    cloudberry_hash=$(printf '%s\n' "$sql" | cloudberry_psql -Atq -F, \
      | sha256sum | awk '{print $1}')
    status=match
    if [[ $postgres_hash != "$cloudberry_hash" ]]; then
      status=mismatch
      mismatches=$((mismatches + 1))
    fi
    echo "$view,$postgres_hash,$cloudberry_hash,$status"
  done
  if (( mismatches > 0 )); then
    echo "$mismatches benchmark result set(s) differ between engines" >&2
    return 1
  fi
}

summarize() {
  awk -F, '
    {
      key = $1 SUBSEP $2
      count[key]++
      value[key, count[key]] = $4 + 0
    }
    END {
      for (key in count) {
        for (i = 1; i <= count[key]; i++) {
          for (j = i + 1; j <= count[key]; j++) {
            if (value[key, j] < value[key, i]) {
              temporary = value[key, i]
              value[key, i] = value[key, j]
              value[key, j] = temporary
            }
          }
        }
        middle = int((count[key] + 1) / 2)
        median = value[key, middle]
        split(key, parts, SUBSEP)
        printf "%s,%s,%.3f,%.3f,%.3f\n", parts[1], parts[2], median, value[key, 1], value[key, count[key]]
      }
    }
  ' | sort -t, -k2,2 -k1,1
}

case ${1:-} in
  setup)
    BENCH_ROWS=${2:-$BENCH_ROWS}
    if [[ ! $BENCH_ROWS =~ ^[1-9][0-9]*$ ]] || (( BENCH_ROWS < 1000 )); then
      echo "rows must be an integer of at least 1000" >&2
      exit 1
    fi
    require_container "$POSTGRES_CONTAINER"
    require_container "$CLOUDBERRY_CONTAINER"
    ensure_databases
    setup_engine postgres "$BENCH_ROWS"
    setup_engine cloudberry "$BENCH_ROWS"
    ;;
  run)
    require_container "$POSTGRES_CONTAINER"
    require_container "$CLOUDBERRY_CONTAINER"
    raw_results=$(run_engine postgres; run_engine cloudberry)
    print_metadata
    echo
    verify_results
    echo
    echo "engine,query,run,execution_ms"
    printf '%s\n' "$raw_results"
    echo
    echo "engine,query,median_ms,min_ms,max_ms"
    printf '%s\n' "$raw_results" | summarize
    ;;
  clean)
    require_container "$POSTGRES_CONTAINER"
    require_container "$CLOUDBERRY_CONTAINER"
    postgres_psql -q -c 'DROP SCHEMA IF EXISTS analytics_bench CASCADE'
    printf '%s\n' 'DROP SCHEMA IF EXISTS analytics_bench CASCADE;' | cloudberry_psql -q
    ;;
  *)
    usage
    exit 2
    ;;
esac
