-- Create a separate database for the Iceberg REST catalog if it does not already exist.
-- PostgreSQL has no native CREATE DATABASE IF NOT EXISTS.
-- This file is executed via psql by docker-entrypoint-initdb.d, so \gexec is available:
-- the SELECT produces the CREATE DATABASE string only when the database is absent,
-- then \gexec executes it.
SELECT 'CREATE DATABASE iceberg_catalog'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'iceberg_catalog'
)\gexec
