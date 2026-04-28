# Data Dictionary

Complete column-level reference for every table in the WMATA ETL pipeline.
Tables follow the **Medallion Architecture**: Bronze → Silver → Gold.

---

## Bronze Layer — `bronze.raw_predictions`

Raw data exactly as received from the WMATA API. No cleaning or typing applied.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | `SERIAL` | No | Auto-increment primary key |
| `station_code` | `VARCHAR(10)` | Yes | WMATA station code (e.g. `A01`, `C05`) |
| `destination_code` | `VARCHAR(10)` | Yes | Destination station code |
| `destination_name` | `VARCHAR(100)` | Yes | Human-readable destination name |
| `line` | `VARCHAR(10)` | Yes | Line code — may include invalid values like `No`, `--` |
| `station_name` | `VARCHAR(100)` | Yes | Human-readable station name |
| `minutes_to_arrival` | `VARCHAR(10)` | Yes | Raw string — may be numeric, `BRD`, `ARR`, or `---` |
| `car_count` | `VARCHAR(10)` | Yes | Raw string — number of cars or `-` |
| `raw_json` | `JSONB` | Yes | Full original API record as JSON |
| `extracted_at` | `TIMESTAMPTZ` | No | UTC timestamp when the API was called |
| `loaded_at` | `TIMESTAMPTZ` | Yes | Auto-set on insert (`DEFAULT NOW()`) |

**Indexes**: `station_code`, `extracted_at`, `line`

---

## Silver Layer — `silver.cleaned_predictions`

Cleaned and typed predictions. Invalid records (empty station, non-passenger lines) are excluded.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `id` | `SERIAL` | No | Auto-increment primary key |
| `station_code` | `VARCHAR(10)` | No | Validated WMATA station code |
| `destination_code` | `VARCHAR(10)` | Yes | Destination station code |
| `destination_name` | `VARCHAR(100)` | Yes | Human-readable destination name |
| `line` | `VARCHAR(10)` | No | One of: `RD`, `BL`, `OR`, `SV`, `GR`, `YL` |
| `line_name` | `VARCHAR(20)` | Yes | Full name: Red, Blue, Orange, Silver, Green, Yellow |
| `station_name` | `VARCHAR(100)` | No | Validated station name |
| `minutes_to_arrival` | `INTEGER` | Yes | Numeric wait time in minutes (NULL if non-numeric) |
| `car_count` | `INTEGER` | Yes | Number of cars (NULL if non-numeric) |
| `is_arriving` | `BOOLEAN` | Yes | `TRUE` if `minutes_to_arrival = 0` |
| `is_boarding` | `BOOLEAN` | Yes | `TRUE` if original value was `BRD` |
| `extracted_at` | `TIMESTAMPTZ` | No | UTC timestamp from extraction |
| `cleaned_at` | `TIMESTAMPTZ` | Yes | Auto-set on insert (`DEFAULT NOW()`) |

**Indexes**: `station_code`, `extracted_at`, `line`

---

## Gold Layer — `gold.station_wait_times`

Aggregated station-level wait time metrics. One row per station + line + pipeline run.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `station_code` | `VARCHAR(10)` | No | WMATA station code (PK) |
| `line` | `VARCHAR(10)` | No | Line code (PK) |
| `station_name` | `VARCHAR(100)` | Yes | Station display name |
| `avg_wait_minutes` | `NUMERIC(5,2)` | Yes | Average wait time across all predictions |
| `min_wait_minutes` | `INTEGER` | Yes | Minimum wait time observed |
| `max_wait_minutes` | `INTEGER` | Yes | Maximum wait time observed |
| `train_count` | `INTEGER` | Yes | Number of predictions in the aggregation window |
| `calculated_at` | `TIMESTAMPTZ` | No | UTC timestamp of aggregation (PK) |
| `created_at` | `TIMESTAMPTZ` | Yes | Auto-set on insert (`DEFAULT NOW()`) |

**Primary Key**: `(station_code, line, calculated_at)` — enables idempotent upserts via `ON CONFLICT`.

**Indexes**: `calculated_at`, `line`

---

## Gold Layer — `gold.pipeline_runs`

Pipeline observability table. One row per DAG execution.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `run_id` | `VARCHAR(50)` | No | Unique run identifier (PK), format: `YYYYMMDD_HHMMSS` |
| `started_at` | `TIMESTAMPTZ` | No | UTC timestamp when extraction began |
| `completed_at` | `TIMESTAMPTZ` | Yes | UTC timestamp when load completed (NULL if running/failed) |
| `status` | `VARCHAR(20)` | Yes | `running`, `success`, or `failed` |
| `records_extracted` | `INTEGER` | Yes | Count of raw records from API |
| `records_cleaned` | `INTEGER` | Yes | Count after Silver-layer cleaning |
| `records_loaded` | `INTEGER` | Yes | Count of rows upserted to Gold |
| `error_message` | `TEXT` | Yes | Error details if `status = 'failed'` |
| `metadata` | `JSONB` | Yes | Extra context: silver_rows, QC pass count, execution_time_ms |

---

## Gold Layer — `gold.dim_stations`

Static dimension table mapping WMATA station codes to human-readable names, line assignments, geographic corridors, and coordinates. Seeded on container init via `scripts/seed-stations.sql`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `station_code` | `VARCHAR(10)` | No | WMATA station code (PK), e.g. `A01` |
| `station_name` | `VARCHAR(100)` | No | Full station name, e.g. "Metro Center" |
| `line_codes` | `VARCHAR(20)` | No | Comma-separated line codes, e.g. `RD` or `BL,OR,SV` |
| `corridor` | `VARCHAR(60)` | Yes | Geographic corridor, e.g. "Red - Maryland" |
| `lat` | `NUMERIC(9,6)` | Yes | Latitude (WGS 84) |
| `lng` | `NUMERIC(9,6)` | Yes | Longitude (WGS 84) |

**Primary Key**: `station_code`

**Seed**: 91 stations across all 6 lines, loaded via `ON CONFLICT DO UPDATE` (idempotent).

---

## Gold Views

### `gold.latest_station_wait_times`

Most recent metrics per station + line. Uses `DISTINCT ON` to pick the latest `calculated_at`.

### `gold.hourly_wait_averages`

Hourly rollup of wait times grouped by station + line + hour.

### `gold.line_performance`

System-wide line performance for the last hour: avg/min/max wait, station count, train count.

---

## Data Flow Summary

```
WMATA API
   │
   ▼
bronze.raw_predictions     ← Raw strings, JSONB, no validation
   │
   ▼
silver.cleaned_predictions ← Typed integers, valid lines only, deduped
   │
   ▼
gold.station_wait_times    ← Aggregated per station+line+run
gold.pipeline_runs         ← One row per ETL execution (observability)
gold.dim_stations          ← Static reference: 91 stations with corridors + lat/lng
```

## Key Relationships

- **Lineage**: `run_id` links `gold.pipeline_runs` → enriched aggregates (via metadata)
- **Temporal**: `extracted_at` (Bronze/Silver) → `calculated_at` (Gold) → `started_at`/`completed_at` (pipeline_runs)
- **Dedup key**: Gold uses `(station_code, line, calculated_at)` composite PK for idempotent upserts
