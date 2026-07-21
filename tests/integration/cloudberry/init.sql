-- Apache Cloudberry 2.1 target initialization
-- This script is executed once when the container is first started

-- Create test schema
CREATE SCHEMA IF NOT EXISTS integration;

-- Note: Actual target tables will be created by the ETL service
-- during initial snapshot. This init script just prepares the database.

-- Verify Cloudberry version
SELECT version();
