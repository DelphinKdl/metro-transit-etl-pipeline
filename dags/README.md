# 04 The DAG

## Overview
This module contains the Apache Airflow DAG that orchestrates the ETL pipeline on a 5-minute schedule.

## DAG Structure

```
wmata_rail_predictions_etl
├── extract_predictions      # Fetch from WMATA API
├── transform_predictions    # Clean and aggregate
├── quality_checks          # Validate data quality
└── load_to_postgres        # Upsert to database
```

## Schedule
- **Interval**: Every 5 minutes (`*/5 * * * *`)
- **Catchup**: Disabled (no backfill on deploy)
- **Max Active Runs**: 1 (prevent overlap)

## Files

- `wmata_etl_dag.py` - Main DAG definition
- `operators/` - Custom operators if needed

## Running Locally

```bash
# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize database
airflow db init

# Copy DAG to Airflow
cp wmata_etl_dag.py $AIRFLOW_HOME/dags/

# Start scheduler
airflow scheduler

# Start webserver (separate terminal)
airflow webserver --port 8080
```

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| TaskFlow API | Cleaner Python-native syntax |
| XCom for data passing | Small payloads between tasks |
| Idempotent tasks | Safe retries on failure |
| SLA monitoring | Alert if pipeline falls behind |
