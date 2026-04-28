-- =============================================================================
-- WMATA ETL Pipeline - Database Schema (Medallion Architecture)
-- =============================================================================
-- Runs on PostgreSQL container startup after init-db.sql
-- Implements Bronze > Silver > Gold layered data architecture
-- =============================================================================

\connect wmata_etl;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- BRONZE LAYER - Raw ingested data (as-is from WMATA API)
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_predictions (
    id SERIAL PRIMARY KEY,
    station_code VARCHAR(10),
    destination_code VARCHAR(10),
    destination_name VARCHAR(100),
    line VARCHAR(10),
    station_name VARCHAR(100),
    minutes_to_arrival VARCHAR(10),
    car_count VARCHAR(10),
    raw_json JSONB,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_raw_station
    ON bronze.raw_predictions(station_code);
CREATE INDEX IF NOT EXISTS idx_bronze_raw_extracted
    ON bronze.raw_predictions(extracted_at);
CREATE INDEX IF NOT EXISTS idx_bronze_raw_line
    ON bronze.raw_predictions(line);

-- =============================================================================
-- SILVER LAYER - Cleaned, validated, typed data
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.cleaned_predictions (
    id SERIAL PRIMARY KEY,
    station_code VARCHAR(10) NOT NULL,
    destination_code VARCHAR(10),
    destination_name VARCHAR(100),
    line VARCHAR(10) NOT NULL,
    line_name VARCHAR(20),
    station_name VARCHAR(100) NOT NULL,
    minutes_to_arrival INTEGER,
    car_count INTEGER,
    is_arriving BOOLEAN DEFAULT FALSE,
    is_boarding BOOLEAN DEFAULT FALSE,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_cleaned_station
    ON silver.cleaned_predictions(station_code);
CREATE INDEX IF NOT EXISTS idx_silver_cleaned_extracted
    ON silver.cleaned_predictions(extracted_at);
CREATE INDEX IF NOT EXISTS idx_silver_cleaned_line
    ON silver.cleaned_predictions(line);

-- =============================================================================
-- GOLD LAYER - Aggregated business metrics
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.station_wait_times (
    station_code VARCHAR(10) NOT NULL,
    line VARCHAR(10) NOT NULL,
    station_name VARCHAR(100),
    avg_wait_minutes NUMERIC(5,2),
    min_wait_minutes INTEGER,
    max_wait_minutes INTEGER,
    train_count INTEGER,
    calculated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (station_code, line, calculated_at)
);

CREATE INDEX IF NOT EXISTS idx_gold_wait_times_calculated
    ON gold.station_wait_times(calculated_at);
CREATE INDEX IF NOT EXISTS idx_gold_wait_times_line
    ON gold.station_wait_times(line);

CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'running',
    records_extracted INTEGER DEFAULT 0,
    records_cleaned INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB
);

-- =============================================================================
-- DIMENSION TABLE - Station reference data
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.dim_stations (
    station_code VARCHAR(10) PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    line_codes VARCHAR(20) NOT NULL,
    corridor VARCHAR(60),
    lat NUMERIC(9,6),
    lng NUMERIC(9,6)
);

-- =============================================================================
-- GOLD VIEWS - Analytics-ready
-- =============================================================================

CREATE OR REPLACE VIEW gold.latest_station_wait_times AS
SELECT DISTINCT ON (station_code, line)
    station_code, line, station_name,
    avg_wait_minutes, min_wait_minutes, max_wait_minutes,
    train_count, calculated_at
FROM gold.station_wait_times
ORDER BY station_code, line, calculated_at DESC;

CREATE OR REPLACE VIEW gold.hourly_wait_averages AS
SELECT
    station_code, line,
    DATE_TRUNC('hour', calculated_at) AS hour,
    AVG(avg_wait_minutes)::NUMERIC(5,2) AS avg_wait,
    MIN(min_wait_minutes) AS min_wait,
    MAX(max_wait_minutes) AS max_wait,
    SUM(train_count) AS total_trains
FROM gold.station_wait_times
GROUP BY station_code, line, DATE_TRUNC('hour', calculated_at);

CREATE OR REPLACE VIEW gold.line_performance AS
SELECT
    line,
    COUNT(DISTINCT station_code) AS stations_reporting,
    AVG(avg_wait_minutes)::NUMERIC(5,2) AS system_avg_wait,
    MIN(min_wait_minutes) AS system_min_wait,
    MAX(max_wait_minutes) AS system_max_wait,
    SUM(train_count) AS total_trains,
    MAX(calculated_at) AS last_updated
FROM gold.station_wait_times
WHERE calculated_at > NOW() - INTERVAL '1 hour'
GROUP BY line;
