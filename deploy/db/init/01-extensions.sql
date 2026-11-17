-- Runs once, on first initialisation of an empty data volume (FR-DATA-4).
-- This init SQL is the only place that creates the extensions: the Alembic migrations in api/ do not.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;
