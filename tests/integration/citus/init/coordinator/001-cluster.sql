\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS citus;

SELECT citus_set_coordinator_host('coordinator', 5432);

SELECT citus_add_node('worker1', 5432)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_dist_node
    WHERE nodename = 'worker1' AND nodeport = 5432
);

SELECT citus_add_node('worker2', 5432)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_dist_node
    WHERE nodename = 'worker2' AND nodeport = 5432
);

CREATE SCHEMA IF NOT EXISTS integration;

CREATE TABLE IF NOT EXISTS integration.accounts (
    tenant_id bigint NOT NULL,
    id bigint NOT NULL,
    email text NOT NULL,
    amount numeric(18, 2) NOT NULL,
    active boolean NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

SET citus.shard_count = 8;

SELECT create_distributed_table('integration.accounts', 'tenant_id')
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_dist_partition
    WHERE logicalrelid = 'integration.accounts'::regclass
);
