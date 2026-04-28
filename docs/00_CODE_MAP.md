# Code Map — Where Each Part Lives

This document maps each ETL component to its exact file location.

---

## Quick Reference

| Part | What It Does | Code File |
|------|--------------|-----------|
| **Extract** | Fetch data from WMATA API | `src/clients/wmata_client.py` |
| **Transform** | Clean and aggregate data | `src/core/transformer.py` |
| **Quality Checks** | Validate data before loading | `src/core/quality_checks.py` |
| **Load** | Insert into PostgreSQL | `src/core/loader.py` |
| **Models** | Data structures | `src/models/predictions.py` |
| **Orchestration** | Schedule and monitor pipeline | `dags/wmata_etl_dag.py` |
| **Configuration** | Manage settings | `config/settings.py` |
| **Main Entry** | Run pipeline locally | `src/main.py` |

---

## Detailed File Map

### 1. CLIENTS (API Wrappers)
```
src/clients/
├── __init__.py              # Package exports
└── wmata_client.py          # ⭐ MAIN FILE: WMATAClient class
```

**Key Classes/Functions in `wmata_client.py`**:
- `WMATAClient` — API client class with retry logic
- `WMATAClient.get_predictions()` — Fetch predictions
- `WMATAClient._parse_minutes()` — Parse "ARR" → 0
- `get_all_predictions()` — Convenience function

---

### 2. MODELS (Data Structures)
```
src/models/
├── __init__.py              # Package exports
└── predictions.py           # ⭐ MAIN FILE: TrainPrediction dataclass
```

**Key Classes in `predictions.py`**:
- `TrainPrediction` — Data structure for one prediction
- `TrainPrediction.to_dict()` — Serialize to dictionary
- `TrainPrediction.from_dict()` — Deserialize from dictionary

---

### 3. CORE (Business Logic)
```
src/core/
├── __init__.py              # Package exports
├── transformer.py           # ⭐ Data transformation
├── quality_checks.py        # ⭐ Data validation checks
└── loader.py                # ⭐ PostgreSQL upsert
```

**Key Functions in `transformer.py`**:
- `transform_predictions()` — Convert raw predictions to DataFrame
- `aggregate_station_metrics()` — Group by station, calculate avg/min/max
- `enrich_with_metadata()` — Add pipeline metadata

**Key Functions in `quality_checks.py`**:
- `check_schema()` — Verify required columns exist
- `check_null_rate()` — Ensure not too many nulls
- `check_wait_time_range()` — Validate wait times are realistic
- `check_valid_stations()` — Verify station codes
- `check_valid_lines()` — Verify line codes
- `check_data_freshness()` — Ensure data is not stale
- `run_quality_checks()` — Run all checks

**Key Functions in `loader.py`**:
- `DatabaseLoader` — Database connection manager
- `DatabaseLoader.upsert_station_metrics()` — Upsert aggregates
- `upsert_to_postgres()` — Convenience function

---

### 4. UTILS (Shared Utilities)
```
src/utils/
├── __init__.py              # Package exports
└── logger.py                # ⭐ Structured logging
```

**Key Functions in `logger.py`**:
- `configure_logging()` — Set up structlog
- `get_logger()` — Get a logger instance

---

### 5. DAGS (Airflow Orchestration)
```
dags/
├── __init__.py              # Package marker
└── wmata_etl_dag.py         # ⭐ MAIN FILE: Airflow DAG
```

**Key Components in `wmata_etl_dag.py`**:
- `@dag` decorator — DAG definition
- `extract_predictions()` — Call WMATA API
- `transform_data()` — Clean data
- `quality_check()` — Validate data
- `load_to_database()` — Insert to PostgreSQL

---

### 6. CONFIGURATION
```
config/
├── __init__.py              # Package exports
└── settings.py              # ⭐ MAIN FILE: Pydantic Settings
```

**Key Classes in `settings.py`**:
- `WMATASettings` — API configuration
- `DatabaseSettings` — PostgreSQL configuration
- `PipelineSettings` — Pipeline behavior
- `get_settings()` — Get cached settings

---

### 7. SCRIPTS (Database)
```
scripts/
├── init-db.sql              # Create database
└── schema.sql               # ⭐ Table definitions
```

**Key SQL in `schema.sql`**:
- `raw_predictions` table — Store every API response
- `station_wait_times` table — Aggregated metrics
- `ON CONFLICT DO UPDATE` — Upsert pattern

---

### 8. TESTS
```
tests/
├── __init__.py
├── unit/
│   ├── test_transformer.py  # Tests for Transform layer
│   └── test_quality_checks.py # Tests for QC layer
└── integration/
```

---

### 9. DOCKER & INFRASTRUCTURE
```
docker/
└── docker-compose.yml       # ⭐ Airflow + PostgreSQL containers

(root)
├── Makefile                 # ⭐ Easy commands (make up, make down)
├── pyproject.toml           # ⭐ Dependencies & tooling
├── .env.example             # Environment variables template
└── .env                     # Your local config (gitignored)
```

---

## Visual Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AIRFLOW (dags/)                              │
│                    wmata_etl_dag.py orchestrates:                    │
└─────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐          ┌───────────────┐          ┌───────────────┐
│    EXTRACT    │          │   TRANSFORM   │          │     LOAD      │
│               │          │               │          │               │
│ src/clients/  │    →     │ src/core/     │    →     │ src/core/     │
│               │          │               │          │               │
│ wmata_client  │          │ transformer   │          │ loader.py     │
│     .py       │          │ quality_      │          │               │
│               │          │ checks.py     │          │ scripts/      │
│               │          │               │          │ schema.sql    │
└───────────────┘          └───────────────┘          └───────────────┘
        │                           │                           │
        ▼                           ▼                           ▼
   WMATA API                 pandas DataFrame              PostgreSQL
```

---

## How to Read Each File

Open these files in order to understand the full pipeline:

1. **Start here**: `src/models/predictions.py` — See the data model
2. **Then**: `src/clients/wmata_client.py` — See how we fetch data
3. **Then**: `src/core/transformer.py` — See how we clean data
4. **Then**: `src/core/quality_checks.py` — See how we validate
5. **Then**: `src/core/loader.py` — See how we store data
6. **Then**: `dags/wmata_etl_dag.py` — See how it all connects
7. **Finally**: `src/main.py` — See how to run it locally

---

*This file is your reference. Keep it open while learning the codebase.*
