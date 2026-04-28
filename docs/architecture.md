# WMATA ETL Pipeline — End-to-End Flow

## Architecture Overview

```
WMATA API → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated) → Dashboard
                ↑                ↑                    ↑                ↑
            Python ETL      Python ETL        Python ETL       Streamlit
```

---

## Step 1: Data Ingestion (Bronze Layer)

**Trigger:** Airflow DAG runs every 5 minutes

```
Airflow Scheduler → DAG: wmata_rail_predictions_etl → Task: extract_predictions
```

- **API Client** (`src/clients/wmata_client.py`) — Calls WMATA StationPrediction API with retries and rate limiting
- **Data Model** (`src/models/predictions.py`) — `TrainPrediction` dataclass for structured parsing
- **DAG Task** (`dags/wmata_etl_dag.py`) — Extracts all station predictions, saves raw data to bronze
- **Output:** Raw JSON → `bronze.raw_predictions` table (no transformations, string types preserved)

---

## Step 2: Data Transformation (Silver Layer)

### Python Path (Airflow Task)

- **Transformer** (`src/core/transformer.py`)
  - `transform_predictions()` — Cleans nulls, coerces numeric types, deduplicates
  - `aggregate_station_metrics()` — Groups by station + line, computes avg/min/max wait times

- **Silver Persistence** (`src/core/loader.py`)
  - `insert_cleaned_predictions()` — Writes cleaned DataFrame to `silver.cleaned_predictions`
  - Adds `line_name`, `is_arriving`, `is_boarding` flags
  - Enriches with `run_id` for lineage tracking via `enrich_with_metadata()`
- **Output:** `silver.cleaned_predictions` — typed, validated, clean records

---

## Step 3: Data Quality Checks

### Python Quality Checks (`src/core/quality_checks.py`)

| Check | Description |
|-------|-------------|
| Schema validation | Required fields present |
| Null rate | `avg_wait_minutes`, `station_code` not null |
| Wait time range | Values between 0–60 minutes |
| Valid stations | Known WMATA station codes |
| Valid lines | RD, BL, OR, SV, GR, YL only |
| Data freshness | Extracted within last 15 minutes |
| Completeness | Minimum 50 stations reporting |

**If checks fail** → DAG raises `ValueError`, pipeline run is marked failed in `gold.pipeline_runs`.

---

## Step 4: Load to Gold Layer

### Python Loader (`src/core/loader.py`)

- Upserts to `gold.station_wait_times` using `ON CONFLICT` (idempotent)
- Connection pooling via SQLAlchemy
- Automatic transaction management with rollback on failure

### Pipeline Observability

- `record_pipeline_run()` — Writes start record on extraction
- `update_pipeline_run()` — Updates with final status, record counts, and metadata on completion or failure
- **Output:** `gold.pipeline_runs` — full audit trail per DAG execution

---

## Step 5: Data Visualization

**Streamlit Dashboard** (`dashboard/app.py`) → `http://localhost:8501`

| Component | Description |
|-----------|-------------|
| KPI Cards | Stations reporting, avg wait, total trains, last update |
| Bar Chart | Average wait time by line (color-coded) |
| Pie Chart | Train distribution by line |
| Trend Line | Hourly wait time trends |
| Station Table | Detailed per-station metrics |
| Pipeline Health | Recent pipeline run statuses |

---

## Step 6: Orchestration (Airflow)

**DAG:** `wmata_rail_predictions_etl` — Runs every 5 minutes during operating hours

```
extract_predictions (Bronze)
       ↓
transform_data (Silver)
       ↓
quality_check
       ↓
load_to_database (Gold)
```

- **Schedule:** `*/5 5-23 * * *` — every 5 min from 5 AM to 11:59 PM (skips midnight–5 AM when Metro is closed)
- **Retries:** 2 attempts with 1-minute delay
- **Timeout:** 4 minutes per task
- **Max active runs:** 1 (prevents overlap)

---

## Database Schema (Medallion Architecture)

```
┌─────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│      BRONZE          │    │      SILVER          │    │       GOLD          │
│                     │    │                     │    │                     │
│ raw_predictions     │───→│ cleaned_predictions │───→│ station_wait_times  │
│ (VARCHAR types,     │    │ (INTEGER types,     │    │ (aggregated,        │
│  raw JSON, no       │    │  nulls removed,     │    │  upserted,          │
│  validation)        │    │  typed, validated)  │    │  analytics-ready)   │
│                     │    │                     │    │                     │
│                     │    │                     │    │ pipeline_runs       │
│                     │    │                     │    │                     │
│                     │    │                     │    │ Views:              │
│                     │    │                     │    │  latest_wait_times  │
│                     │    │                     │    │  hourly_averages    │
│                     │    │                     │    │  line_performance   │
└─────────────────────┘    └─────────────────────┘    └─────────────────────┘
```

---

## Infrastructure

| Service | Port | Config |
|---------|------|--------|
| Airflow UI | `8080` | `docker/docker-compose.yml` |
| Streamlit Dashboard | `8501` | `docker/Dockerfile.dashboard` |
| PostgreSQL | `5432` | `docker/docker-compose.yml` |

---

## Quick Start

```bash
# 1. Set up environment
cp .env.example .env   # Fill in WMATA_API_KEY and database credentials

# 2. Initialize Airflow
make init

# 3. Start all services
make up

# 4. Launch dashboard
make dashboard

# 5. Run tests
make test
```

---

## Project Checklist

- [x] **Data Warehouse** — PostgreSQL with medallion schemas
- [x] **Data Ingestion** — WMATA API client with retries
- [x] **Data Modeling** — Python-based transformation with Silver layer persistence
- [x] **Medallion Architecture** — Bronze → Silver → Gold with full lineage
- [x] **Testing & Data Quality** — pytest (unit + integration) + 8 automated quality checks
- [x] **Data Visualization** — Streamlit dashboard with Plotly charts
- [x] **Orchestration (Airflow)** — Scheduled DAG with task dependencies
