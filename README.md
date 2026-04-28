# WMATA Rail Predictions ETL Pipeline

Production-grade ETL pipeline that ingests real-time train arrival predictions from the Washington Metropolitan Area Transit Authority (WMATA) API, transforms them into station-level wait-time metrics using a **Medallion Architecture** (Bronze → Silver → Gold), and loads the results into PostgreSQL — all orchestrated by Apache Airflow on a 5-minute schedule.

A **Streamlit dashboard** provides live analytics at a glance.

[![CI](https://github.com/DelphinKdl/metro-transit-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/DelphinKdl/metro-transit-etl-pipeline/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Airflow 2.x](https://img.shields.io/badge/airflow-2.x-017CEE.svg)](https://airflow.apache.org/)

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Dashboard](#dashboard)
- [Data Quality](#data-quality)
- [Development](#development)
- [Documentation](#documentation)
- [Tech Stack](#tech-stack)
- [License](#license)

---

## Architecture

```
WMATA API ──▶ Extract ──▶ Bronze (raw) ──▶ Transform ──▶ Silver (cleaned) ──▶ Quality Check ──▶ Gold (aggregates)
                │              │                │               │                  │                  │
                │         PostgreSQL        pandas +        PostgreSQL          8 automated      PostgreSQL
                │       raw_predictions    filtering    cleaned_predictions    validations    station_wait_times
                │                                                                                    │
                └────────────────────── Apache Airflow (every 5 min) ────────────────────────────────┘
                                              │                                                      │
                                    gold.pipeline_runs                                    Streamlit Dashboard
                                    (observability)                                      http://localhost:8501
```

### Pipeline Flow

| Step | Task | Layer | What Happens |
|------|------|-------|-------------|
| 1 | `extract_predictions` | Bronze | Calls WMATA API, stores raw JSON in `bronze.raw_predictions`, records pipeline run start in `gold.pipeline_runs` |
| 2 | `transform_data` | Silver | Cleans data, filters invalid lines (`No`, `--`), persists to `silver.cleaned_predictions`, aggregates by station/line, enriches with `run_id` for lineage |
| 3 | `quality_check` | — | Runs 8 automated validations; blocks load if any fail; records failure in `gold.pipeline_runs` |
| 4 | `load_to_database` | Gold | Upserts station wait-time metrics to `gold.station_wait_times`, records success + metadata in `gold.pipeline_runs` |

---

## Features

- **Real-time ingestion** — Pulls predictions from WMATA API every 5 minutes during operating hours (5 AM – midnight ET)
- **Medallion Architecture** — Bronze/Silver/Gold layers in PostgreSQL with clear data lineage
- **8 automated quality checks** — Schema validation, null rates, wait-time ranges, valid stations/lines, data freshness, completeness (time-aware thresholds)
- **Idempotent upserts** — PostgreSQL `ON CONFLICT` ensures safe retries
- **Structured JSON logging** — Every pipeline step emits structured logs via structlog
- **Timezone-aware scheduling** — Configured for US/Eastern (DC local time)
- **Streamlit dashboard** — Live KPIs, line performance charts, wait-time trends, station details
- **Containerized** — Single `make up` starts Airflow + PostgreSQL + Dashboard
- **Defensive data handling** — Filters non-passenger trains, handles `BRD`/`ARR`/`---` sentinel values, tolerates new station codes
- **Pipeline observability** — Every run tracked in `gold.pipeline_runs` with status, record counts, and metadata
- **Data lineage** — `run_id` propagated from extraction through to Gold layer for full traceability
- **Batch inserts** — Parameterized bulk writes for Bronze, Silver, and Gold layers

---

## Screenshots

> Screenshots will be added once the pipeline has accumulated enough data for compelling visuals. Run `make up`, trigger a few pipeline cycles, then capture the dashboard at **http://localhost:8501**.

---

## Project Structure

```
ETL-Pipeline/
├── src/                        # All business logic
│   ├── clients/
│   │   └── wmata_client.py     # WMATA API client with retry logic
│   ├── core/
│   │   ├── transformer.py      # Data cleaning & aggregation (Silver layer)
│   │   ├── quality_checks.py   # 8 automated validation checks
│   │   └── loader.py           # PostgreSQL upsert operations (Gold layer)
│   ├── models/
│   │   └── predictions.py      # TrainPrediction dataclass
│   └── utils/
│       └── logger.py           # Structured JSON logger
├── dags/
│   └── wmata_etl_dag.py        # Airflow DAG — thin orchestration layer
├── dashboard/
│   ├── app.py                  # Streamlit dashboard
│   └── requirements.txt
├── scripts/
│   ├── schema.sql              # Medallion schema (auto-applied on init)
│   └── seed-stations.sql       # dim_stations reference data (91 stations)
├── docker/
│   ├── docker-compose.yml      # Airflow + PostgreSQL + Dashboard + pgAdmin
│   └── Dockerfile.dashboard
├── tests/
│   ├── unit/
│   └── integration/
├── docs/                       # Detailed documentation (00–10)
├── .env.example                # Environment variable template
├── pyproject.toml              # Dependencies & tool config
└── Makefile                    # All dev & deployment commands
```

---

## Quick Start

### Prerequisites

- **Docker & Docker Compose** (required)
- **Python 3.11+** (for local development only)
- **WMATA API Key** — [Get one free](https://developer.wmata.com/)

### 1. Clone & Configure

```bash
git clone https://github.com/DelphinKdl/metro-transit-etl-pipeline.git
cd metro-transit-etl-pipeline

# Create your environment file
cp .env.example .env
```

Edit `.env` with your values:

```env
WMATA_API_KEY=your_api_key_here
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=postgres
DATABASE_URL=postgresql://postgres:postgres@postgres:5432/wmata_etl
```

### 2. Initialize & Start

```bash
# Initialize Airflow (first time only — creates DB, admin user, applies schema)
make init

# Start all services (Airflow + PostgreSQL + Dashboard)
make up
```

### 3. Set API Key in Airflow

Open **http://localhost:8080** (login: `airflow` / `airflow`), then:

1. Go to **Admin → Variables**
2. Add variable — Key: `WMATA_API_KEY`, Value: your API key

### 4. Trigger the Pipeline

```bash
# Trigger manually (or wait for the automatic 5-min schedule)
make trigger
```

### 5. View Results

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | `airflow` / `airflow` |
| **Dashboard** | http://localhost:8501 | — |
| **pgAdmin** | http://localhost:5050 | `admin@admin.com` / `admin` |
| **PostgreSQL** | `localhost:5432` | from `.env` |

---

## Usage

### Makefile Commands

| Command | Description |
|---------|-------------|
| `make init` | Initialize Airflow (first time) |
| `make up` | Start all services |
| `make down` | Stop all services |
| `make clean` | Remove containers, volumes, and caches |
| `make logs` | Stream Airflow logs |
| `make trigger` | Trigger DAG manually |
| `make dag-status` | Check recent DAG run statuses |
| `make psql` | Connect to PostgreSQL shell |
| `make shell` | Open bash in Airflow container |
| `make run` | Run pipeline locally (no Docker) |
| `make test` | Run tests |
| `make test-cov` | Tests with coverage report |
| `make lint` | Run ruff + black + mypy |
| `make format` | Auto-format code |
| `make dashboard` | Start Streamlit dashboard |

### Run Locally (Without Docker)

```bash
make install
export WMATA_API_KEY=your_key
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/wmata_etl
make run
```

---

## Dashboard

The Streamlit dashboard at **http://localhost:8501** provides:

- **Dynamic headline** — Context-aware narrative (e.g. "Red Line 42% above system average")
- **KPI cards with deltas** — Avg wait vs. previous period, best/worst lines, pipeline health
- **Line performance** — Horizontal bar with system-avg benchmark + current vs. previous comparison
- **Wait time trends** — Time-series with normal-range band and rush-hour shading
- **Day × Hour heatmap** — Congestion patterns across the week
- **Station drill-down** — Top 10 longest/shortest waits with conditional coloring
- **Pipeline observability** — Layer health (Bronze/Silver/Gold counts) and recent run log

---

## Data Quality

The pipeline runs **8 automated checks** before loading data:

| Check | What It Validates | Threshold |
|-------|-------------------|-----------|
| Schema validation | All required fields present | Strict |
| Null rate (avg_wait) | Null rate below threshold | ≤ 5% |
| Null rate (station_code) | Null rate below threshold | ≤ 5% |
| Wait time range | Average wait 0–60 minutes | [0, 60] |
| Valid stations | Known WMATA station codes | ≤ 5 unknown |
| Valid lines | Only RD, BL, OR, SV, GR, YL | Strict |
| Data freshness | Records < 10 min old | < 10% stale |
| Completeness | Minimum stations reporting | Time-aware (3–40) |

If any check fails, the pipeline **stops and does not load bad data**.

See [`docs/09_DATA_QUALITY_FINDINGS.md`](docs/09_DATA_QUALITY_FINDINGS.md) for real-world API data issues discovered during development.

---

## Development

### Tests

```bash
make test             # Unit + integration tests
make test-cov         # With HTML coverage report
```

### Code Quality

```bash
make lint             # Check with ruff, black, mypy
make format           # Auto-fix formatting
```

### API Reference

```python
from src.clients.wmata_client import WMATAClient
from src.core.transformer import transform_predictions, aggregate_station_metrics
from src.core.quality_checks import run_quality_checks
from src.core.loader import upsert_to_postgres

# Extract
client = WMATAClient(api_key="your_key")
predictions = client.get_predictions("All")
raw_data = [p.to_dict() for p in predictions]

# Transform
df = transform_predictions(raw_data)
aggregates = aggregate_station_metrics(df)

# Validate
qc_result = run_quality_checks(aggregates)

# Load
if qc_result["passed"]:
    upsert_to_postgres(aggregates)
```

---

## Documentation

| Doc | Topic |
|-----|-------|
| [`00_CODE_MAP.md`](docs/00_CODE_MAP.md) | Codebase overview & file map |
| [`01_PROJECT_OVERVIEW.md`](docs/01_PROJECT_OVERVIEW.md) | Goals, design decisions, tech stack |
| [`02_EXTRACT_LAYER.md`](docs/02_EXTRACT_LAYER.md) | WMATA API client, retry logic, data models |
| [`03_TRANSFORM_LAYER.md`](docs/03_TRANSFORM_LAYER.md) | Cleaning, filtering, aggregation |
| [`04_QUALITY_CHECKS.md`](docs/04_QUALITY_CHECKS.md) | All 8 quality checks explained |
| [`05_LOAD_LAYER.md`](docs/05_LOAD_LAYER.md) | PostgreSQL upsert, connection pooling |
| [`06_ORCHESTRATION.md`](docs/06_ORCHESTRATION.md) | Airflow DAG, scheduling, task dependencies |
| [`07_CONFIGURATION.md`](docs/07_CONFIGURATION.md) | Environment variables, Pydantic settings |
| [`08_DOCKER_SETUP.md`](docs/08_DOCKER_SETUP.md) | Docker Compose, services, volumes |
| [`09_DATA_QUALITY_FINDINGS.md`](docs/09_DATA_QUALITY_FINDINGS.md) | Real-world API data issues & fixes |
| [`10_DATA_DICTIONARY.md`](docs/10_DATA_DICTIONARY.md) | Column-level reference for all tables |
| [`architecture.md`](docs/architecture.md) | End-to-end architecture diagram |

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Language** | Python 3.11 |
| **Orchestration** | Apache Airflow 2.x |
| **Database** | PostgreSQL 15 |
| **Dashboard** | Streamlit + Plotly |
| **Containerization** | Docker Compose |
| **Data Validation** | Custom quality gate framework |
| **Logging** | structlog (JSON) |
| **Testing** | pytest + coverage |
| **Code Quality** | ruff, black, mypy |
| **Data Source** | [WMATA Real-Time API](https://developer.wmata.com/) |

---

## License

MIT License. See [LICENSE](LICENSE) for details.

## WMATA API

Usage of the WMATA API is subject to the [WMATA Developer License Agreement](https://developer.wmata.com/license).
