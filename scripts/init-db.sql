-- Initialize databases for WMATA ETL Pipeline
-- This script runs on PostgreSQL container startup
-- Note: wmata_airflow is created automatically by POSTGRES_DB env var

-- Create the WMATA ETL database if it doesn't exist
SELECT 'CREATE DATABASE wmata_etl'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'wmata_etl')\gexec

-- Grant permissions (safe to run multiple times)
GRANT ALL PRIVILEGES ON DATABASE wmata_etl TO postgres;