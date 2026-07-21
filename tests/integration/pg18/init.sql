-- PostgreSQL 18 source initialization
-- This script is executed once when the container is first started

-- Create test schemas and tables for integration tests
CREATE SCHEMA IF NOT EXISTS integration;

-- Simple test table with primary key
CREATE TABLE integration.test_simple (
    id integer PRIMARY KEY,
    name text NOT NULL,
    value numeric,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Table with composite primary key
CREATE TABLE integration.test_composite_pk (
    tenant_id integer NOT NULL,
    record_id integer NOT NULL,
    data jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, record_id)
);

-- Table for testing various data types
CREATE TABLE integration.test_types (
    id integer PRIMARY KEY,
    col_text text,
    col_varchar varchar(100),
    col_int2 smallint,
    col_int4 integer,
    col_int8 bigint,
    col_numeric numeric(10, 2),
    col_float4 real,
    col_float8 double precision,
    col_bool boolean,
    col_date date,
    col_time time,
    col_timetz time with time zone,
    col_timestamp timestamp,
    col_timestamptz timestamptz,
    col_bytea bytea,
    col_json json,
    col_jsonb jsonb,
    col_uuid uuid
);

-- Insert sample data
INSERT INTO integration.test_simple (id, name, value)
VALUES
    (1, 'alpha', 10.5),
    (2, 'beta', 20.0),
    (3, 'gamma', 30.25);

-- Configure logical replication
-- These settings are already in postgresql.conf via command args,
-- but verify they're set correctly
SELECT setting FROM pg_settings WHERE name = 'wal_level';
SELECT setting FROM pg_settings WHERE name = 'max_replication_slots';
SELECT setting FROM pg_settings WHERE name = 'max_wal_senders';
