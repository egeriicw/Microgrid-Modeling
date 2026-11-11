-- Runs once, on first initialisation of an empty data volume (FR-DATA-4).
-- Alembic migrations also issue CREATE EXTENSION IF NOT EXISTS, so this is belt and braces.
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;
