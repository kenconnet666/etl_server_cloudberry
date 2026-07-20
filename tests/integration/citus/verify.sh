#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(CDPATH='' cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly COMPOSE_FILE="${SCRIPT_DIR}/compose.yaml"
readonly PROJECT_NAME="pg2cb-citus-it"
readonly TEST_USER="postgres"
readonly TEST_DATABASE="source"
readonly TEST_PASSWORD="pg2cb_citus_test_only"
readonly MIN_REPLICATION_SLOTS=32
readonly MIN_WAL_SENDERS=32

if docker compose version >/dev/null 2>&1; then
    COMPOSE_COMMAND=(docker compose)
elif command -v docker-compose >/dev/null 2>&1 && docker-compose version >/dev/null 2>&1; then
    COMPOSE_COMMAND=(docker-compose)
else
    printf 'Docker Compose v2 is required (docker compose or docker-compose).\n' >&2
    exit 1
fi

compose() {
    "${COMPOSE_COMMAND[@]}" --project-name "${PROJECT_NAME}" --file "${COMPOSE_FILE}" "$@"
}

known_project_resource_exists() {
    local resource

    for resource in pg2cb-citus-it-coordinator pg2cb-citus-it-worker1 pg2cb-citus-it-worker2; do
        if docker container inspect "${resource}" >/dev/null 2>&1; then
            return 0
        fi
    done

    for resource in \
        pg2cb-citus-it_coordinator-data \
        pg2cb-citus-it_worker1-data \
        pg2cb-citus-it_worker2-data; do
        if docker volume inspect "${resource}" >/dev/null 2>&1; then
            return 0
        fi
    done

    docker network inspect pg2cb-citus-it-network >/dev/null 2>&1
}

docker info >/dev/null
compose config --quiet

project_existed=0
if [[ -n "$(docker ps --all --quiet --filter "label=com.docker.compose.project=${PROJECT_NAME}")" ]] ||
   [[ -n "$(docker volume ls --quiet --filter "label=com.docker.compose.project=${PROJECT_NAME}")" ]] ||
   [[ -n "$(docker network ls --quiet --filter "label=com.docker.compose.project=${PROJECT_NAME}")" ]] ||
   known_project_resource_exists; then
    project_existed=1
fi

created_by_this_run=0
if (( project_existed == 0 )); then
    created_by_this_run=1
fi

capture_failure() {
    local exit_code=$?
    trap - EXIT
    set +e

    local timestamp artifact_dir
    timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
    artifact_dir="${SCRIPT_DIR}/artifacts/failed-${timestamp}-$$"
    mkdir -p "${artifact_dir}"

    compose ps --all >"${artifact_dir}/compose-ps.txt" 2>&1 || true
    compose logs --no-color >"${artifact_dir}/compose.log" 2>&1 || true
    docker ps --all --filter "label=com.docker.compose.project=${PROJECT_NAME}" \
        >"${artifact_dir}/docker-ps.txt" 2>&1 || true

    if (( created_by_this_run == 1 )); then
        compose down --volumes --remove-orphans >"${artifact_dir}/cleanup.log" 2>&1 || true
        printf 'Verification failed. Logs retained at %s; resources created by this run were removed.\n' \
            "${artifact_dir}" >&2
    else
        printf 'Verification failed. Logs retained at %s; the pre-existing cluster was left intact.\n' \
            "${artifact_dir}" >&2
    fi

    exit "${exit_code}"
}
trap capture_failure EXIT

assert_equal() {
    local actual=$1 expected=$2 description=$3
    if [[ "${actual}" != "${expected}" ]]; then
        printf 'Assertion failed: %s (expected %q, got %q)\n' \
            "${description}" "${expected}" "${actual}" >&2
        return 1
    fi
}

assert_matches() {
    local actual=$1 pattern=$2 description=$3
    if [[ ! "${actual}" =~ ${pattern} ]]; then
        printf 'Assertion failed: %s (%q does not match %q)\n' \
            "${description}" "${actual}" "${pattern}" >&2
        return 1
    fi
}

assert_at_least() {
    local actual=$1 minimum=$2 description=$3
    if [[ ! "${actual}" =~ ^[0-9]+$ ]] || (( actual < minimum )); then
        printf 'Assertion failed: %s (expected at least %s, got %q)\n' \
            "${description}" "${minimum}" "${actual}" >&2
        return 1
    fi
}

node_sql() {
    local service=$1 query=$2
    compose exec --no-TTY "${service}" \
        psql -X --username "${TEST_USER}" --dbname "${TEST_DATABASE}" \
        --set ON_ERROR_STOP=1 --tuples-only --no-align --quiet --command "${query}"
}

wait_for_cluster() {
    local deadline=$((SECONDS + 180))
    local service container_id health all_healthy
    local services=(worker1 worker2 coordinator)

    while (( SECONDS < deadline )); do
        all_healthy=1
        for service in "${services[@]}"; do
            container_id="$(compose ps --quiet "${service}")"
            if [[ -z "${container_id}" ]]; then
                all_healthy=0
                continue
            fi

            health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${container_id}")"
            case "${health}" in
                healthy)
                    ;;
                unhealthy|exited|dead)
                    printf 'Service %s entered terminal state %s.\n' "${service}" "${health}" >&2
                    return 1
                    ;;
                *)
                    all_healthy=0
                    ;;
            esac
        done

        if (( all_healthy == 1 )); then
            return 0
        fi
        sleep 2
    done

    printf 'Timed out waiting for the Citus cluster to become healthy.\n' >&2
    return 1
}

compose pull --quiet
compose up --detach
wait_for_cluster

compose exec --no-TTY coordinator \
    psql -X --username "${TEST_USER}" --dbname "${TEST_DATABASE}" \
    --set ON_ERROR_STOP=1 --quiet \
    --file /docker-entrypoint-initdb.d/010-pg2cb-cluster.sql >/dev/null

for service in coordinator worker1 worker2; do
    settings="$(node_sql "${service}" "
        SELECT (current_setting('server_version_num')::integer / 10000)::text
            || '|' || (SELECT extversion FROM pg_extension WHERE extname = 'citus')
            || '|' || current_setting('wal_level')
            || '|' || current_setting('max_replication_slots')
            || '|' || current_setting('max_wal_senders')
            || '|' || current_setting('citus.enable_change_data_capture');
    ")"
    IFS='|' read -r pg_major citus_version wal_level replication_slots wal_senders cdc_enabled <<<"${settings}"

    assert_equal "${pg_major}" "18" "${service} PostgreSQL major version"
    assert_matches "${citus_version}" '^14\.1([.-]|$)' "${service} Citus extension version"
    assert_equal "${wal_level}" "logical" "${service} wal_level"
    assert_at_least "${replication_slots}" "${MIN_REPLICATION_SLOTS}" "${service} max_replication_slots"
    assert_at_least "${wal_senders}" "${MIN_WAL_SENDERS}" "${service} max_wal_senders"
    assert_equal "${cdc_enabled}" "on" "${service} Citus CDC setting"

    coordinator_node="$(node_sql "${service}" "
        SELECT groupid::text
            || '|' || nodename
            || '|' || nodeport::text
            || '|' || noderole::text
            || '|' || isactive::text
            || '|' || shouldhaveshards::text
        FROM pg_dist_node
        WHERE groupid = 0;
    ")"
    assert_equal "${coordinator_node}" "0|coordinator|5432|primary|true|false" \
        "${service} coordinator registration"
done

workers="$(node_sql coordinator "
    SELECT count(*)::text || '|' || coalesce(string_agg(nodename, ',' ORDER BY nodename), '')
    FROM pg_dist_node
    WHERE groupid > 0 AND noderole = 'primary' AND isactive;
")"
assert_equal "${workers}" "2|worker1,worker2" "active primary worker registration"

distribution="$(node_sql coordinator "
    SELECT count(DISTINCT shard.shardid)::text
        || '|' || count(*)::text
        || '|' || count(DISTINCT node.nodename)::text
    FROM pg_dist_shard AS shard
    JOIN pg_dist_placement AS placement USING (shardid)
    JOIN pg_dist_node AS node USING (groupid)
    WHERE shard.logicalrelid = 'integration.accounts'::regclass
      AND placement.shardstate = 1
      AND node.noderole = 'primary'
      AND node.isactive;
")"
assert_equal "${distribution}" "8|8|2" "eight shards placed across both workers"

compose exec --no-TTY coordinator \
    psql -X --username "${TEST_USER}" --dbname "${TEST_DATABASE}" \
    --set ON_ERROR_STOP=1 --quiet >/dev/null <<'SQL'
BEGIN;
TRUNCATE TABLE integration.accounts;

INSERT INTO integration.accounts (
    tenant_id,
    id,
    email,
    amount,
    active,
    payload,
    updated_at
)
SELECT value,
       value,
       format('tenant-%s@example.test', value),
       value::numeric / 10,
       true,
       jsonb_build_object('version', 1, 'tenant', value),
       timestamptz '2026-01-01 00:00:00+00' + value * interval '1 second'
FROM generate_series(1, 256) AS value;

UPDATE integration.accounts
SET active = false,
    amount = amount + 100,
    payload = jsonb_set(payload, '{version}', '2'::jsonb),
    updated_at = timestamptz '2026-02-01 00:00:00+00'
WHERE id % 3 = 0;

DELETE FROM integration.accounts
WHERE id % 5 = 0;
COMMIT;
SQL

crud_result="$(node_sql coordinator "
    SELECT count(*)::text
        || '|' || count(*) FILTER (WHERE NOT active)::text
        || '|' || count(*) FILTER (WHERE id % 5 = 0)::text
        || '|' || count(*) FILTER (
            WHERE ((id % 3 = 0) IS DISTINCT FROM ((payload ->> 'version')::integer = 2))
               OR amount <> id::numeric / 10 + CASE WHEN id % 3 = 0 THEN 100 ELSE 0 END
               OR updated_at <> CASE
                    WHEN id % 3 = 0 THEN timestamptz '2026-02-01 00:00:00+00'
                    ELSE timestamptz '2026-01-01 00:00:00+00' + id * interval '1 second'
                  END
        )::text
    FROM integration.accounts;
")"
assert_equal "${crud_result}" "205|68|0|0" "coordinator insert/update/delete content"

routed_workers="$(node_sql coordinator "
    WITH routed_shards AS (
        SELECT DISTINCT get_shard_id_for_distribution_column(
            'integration.accounts'::regclass,
            tenant_id
        ) AS shardid
        FROM integration.accounts
    ), routed_nodes AS (
        SELECT DISTINCT node.nodename
        FROM routed_shards
        JOIN pg_dist_placement AS placement USING (shardid)
        JOIN pg_dist_node AS node USING (groupid)
        WHERE placement.shardstate = 1
          AND node.noderole = 'primary'
          AND node.isactive
    )
    SELECT count(*)::text || '|' || string_agg(nodename, ',' ORDER BY nodename)
    FROM routed_nodes;
")"
assert_equal "${routed_workers}" "2|worker1,worker2" "tenant routing reaches both workers"

trap - EXIT
compose ps
printf '\nCitus integration verification passed.\n'
printf 'Coordinator DSN: postgresql://%s:%s@127.0.0.1:55440/%s\n' \
    "${TEST_USER}" "${TEST_PASSWORD}" "${TEST_DATABASE}"
printf 'The verified cluster remains running under Compose project %s.\n' "${PROJECT_NAME}"
