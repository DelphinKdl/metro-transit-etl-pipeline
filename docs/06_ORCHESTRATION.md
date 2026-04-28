# Part 6: Orchestration — Airflow DAG

## What is Orchestration?

**Orchestration** is NOT the same as ETL. It's the layer that:
- **Schedules** when the pipeline runs
- **Monitors** success/failure
- **Retries** on failure
- **Alerts** when things go wrong
- **Tracks** execution history

**File**: `dags/wmata_etl_dag.py`

---

## Airflow vs ETL — The Key Distinction

| Airflow Does | Airflow Does NOT Do |
|--------------|---------------------|
| Schedule tasks | Parse API responses |
| Retry on failure | Transform data |
| Track dependencies | Write SQL queries |
| Send alerts | Connect to databases |
| Log execution | Business logic |

**Interview Talking Point**:
> "Airflow is an orchestrator, not an ETL tool. It schedules and monitors tasks, but the actual data processing happens in Python code that Airflow calls. This separation of concerns makes the pipeline easier to test and maintain."

---

## The DAG Structure

### What is a DAG?

**DAG** = **D**irected **A**cyclic **G**raph

```
extract → transform → quality_check → load
   │          │            │           │
   └──────────┴────────────┴───────────┘
              Dependencies flow one way (no cycles)
```

---

## Code Breakdown

### 1. Default Arguments

**Location**: Lines 29-37

```python
default_args = {
    'owner': 'data-engineering',      # Who owns this DAG
    'depends_on_past': False,         # Don't wait for previous runs
    'email_on_failure': True,         # Send email if task fails
    'email_on_retry': False,          # Don't email on retry
    'retries': 2,                     # Retry failed tasks 2 times
    'retry_delay': timedelta(minutes=1),  # Wait 1 min between retries
    'execution_timeout': timedelta(minutes=4),  # Kill if takes >4 min
}
```

| Setting | Value | Why |
|---------|-------|-----|
| `retries: 2` | Retry twice | API might have temporary issues |
| `retry_delay: 1 min` | Wait between retries | Give API time to recover |
| `execution_timeout: 4 min` | Max task duration | Pipeline runs every 5 min, must finish in time |

---

### 2. DAG Definition

**Location**: Lines 40-49

```python
@dag(
    dag_id='wmata_rail_predictions_etl',
    default_args=default_args,
    description='ETL pipeline for WMATA rail predictions',
    schedule_interval='*/5 * * * *',   # Every 5 minutes
    start_date=days_ago(1),
    catchup=False,                      # Don't backfill
    max_active_runs=1,                  # Only one run at a time
    tags=['wmata', 'etl', 'rail', 'predictions'],
)
```

### Schedule Interval (Cron Expression)

```
*/5 * * * *
 │  │ │ │ │
 │  │ │ │ └── Day of week (0-6)
 │  │ │ └──── Month (1-12)
 │  │ └────── Day of month (1-31)
 │  └──────── Hour (0-23)
 └────────── Minute (0-59)

*/5 = Every 5 minutes
```

| Setting | Value | Why |
|---------|-------|-----|
| `schedule_interval='*/5 * * * *'` | Every 5 min | WMATA data updates frequently |
| `catchup=False` | Don't backfill | We only want current data |
| `max_active_runs=1` | One at a time | Prevent overlapping runs |

---

### 3. Task 1: Extract

**Location**: Lines 61-88

```python
@task()
def extract_predictions() -> List[Dict[str, Any]]:
    api_key = Variable.get("WMATA_API_KEY")
    client = WMATAClient(api_key=api_key)
    
    predictions = client.get_predictions("All")
    
    # Convert to dicts for XCom serialization
    return [
        {
            'car_count': p.car_count,
            'destination': p.destination,
            ...
        }
        for p in predictions
    ]
```

**Key Points**:
- `Variable.get("WMATA_API_KEY")` — Gets API key from Airflow Variables (secure storage)
- Returns list of dicts (not dataclass) — Airflow needs JSON-serializable data for **XCom**

### What is XCom?

**XCom** = Cross-Communication between tasks

```
Task A → returns data → XCom storage → Task B reads data
```

Airflow stores task outputs in its database so downstream tasks can access them.

---

### 4. Task 2: Transform

**Location**: Lines 90-108

```python
@task()
def transform_data(raw_predictions: List[Dict[str, Any]]) -> Dict[str, Any]:
    cleaned = transform_predictions(raw_predictions)
    aggregates = aggregate_station_metrics(cleaned)
    
    return {
        'cleaned_count': len(cleaned),
        'aggregate_count': len(aggregates),
        'aggregates': aggregates
    }
```

**Key Points**:
- Input comes from `extract_predictions()` via XCom
- Calls our Transform layer functions
- Returns metadata + aggregates for next task

---

### 5. Task 3: Quality Check

**Location**: Lines 110-129

```python
@task()
def quality_check(transform_result: Dict[str, Any]) -> Dict[str, Any]:
    qc_results = run_quality_checks(transform_result['aggregates'])
    
    if not qc_results['passed']:
        raise ValueError(f"Quality checks failed: {qc_results['failures']}")
    
    return {
        **transform_result,
        'qc_results': qc_results
    }
```

**Key Points**:
- If QC fails → `raise ValueError` → Task fails → Airflow retries
- If all retries fail → DAG run marked as failed → Alert sent
- Data does NOT proceed to Load if QC fails

---

### 6. Task 4: Load

**Location**: Lines 131-147

```python
@task()
def load_to_database(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    result = upsert_to_postgres(validated_data['aggregates'])
    
    return {
        'rows_upserted': result['rows_affected'],
        'execution_time_ms': result['execution_time_ms']
    }
```

**Key Points**:
- Only runs if `quality_check` passed
- Calls our Load layer function
- Returns statistics for logging

---

### 7. Task Dependencies

**Location**: Lines 149-153

```python
# Define task dependencies
raw_data = extract_predictions()
transformed = transform_data(raw_data)
validated = quality_check(transformed)
load_result = load_to_database(validated)
```

This creates the DAG:

```
extract_predictions() 
        │
        ▼
transform_data(raw_data)
        │
        ▼
quality_check(transformed)
        │
        ▼
load_to_database(validated)
```

---

## The TaskFlow API

We use Airflow's **TaskFlow API** (the `@task` decorator). This is the modern way to write DAGs.

### Old Way (Operators)

```python
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_function,
    dag=dag
)
transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_function,
    dag=dag
)
extract_task >> transform_task  # Set dependency
```

### New Way (TaskFlow)

```python
@task()
def extract():
    return data

@task()
def transform(data):
    return transformed

raw = extract()
result = transform(raw)  # Dependency is implicit
```

**Benefits of TaskFlow**:
- Less boilerplate
- Type hints work
- Dependencies are implicit from function calls
- Easier to read and maintain

---

## What Happens When the DAG Runs

```
Every 5 minutes:
    │
    ├── 1. Airflow scheduler triggers DAG
    │
    ├── 2. extract_predictions() runs
    │       └── Calls WMATA API
    │       └── Returns ~150 predictions
    │       └── Stores in XCom
    │
    ├── 3. transform_data() runs
    │       └── Reads from XCom
    │       └── Cleans and aggregates
    │       └── Returns ~85 station aggregates
    │
    ├── 4. quality_check() runs
    │       └── Validates data
    │       └── If fail → retry up to 2 times
    │       └── If still fail → DAG fails, alert sent
    │
    └── 5. load_to_database() runs
            └── Upserts to PostgreSQL
            └── DAG marked as success
```

---

## Viewing in Airflow UI

When you run Airflow, you can see:

1. **DAG List** — All your DAGs
2. **Graph View** — Visual task dependencies
3. **Tree View** — Run history
4. **Task Logs** — Detailed execution logs
5. **XCom** — Data passed between tasks

**URL**: http://localhost:8080 (after Docker is running)

---

## Error Handling Flow

```
Task fails
    │
    ├── Retry 1 (after 1 min)
    │       └── Success? → Continue
    │       └── Fail? → Retry 2
    │
    ├── Retry 2 (after 1 min)
    │       └── Success? → Continue
    │       └── Fail? → Mark task as failed
    │
    └── All retries exhausted
            └── DAG run marked as failed
            └── email_on_failure triggers
            └── Next scheduled run still happens
```

---

## Interview Talking Points

> "I use Airflow's TaskFlow API for cleaner code. Dependencies are implicit from function calls, and XCom handles data passing between tasks automatically."

> "The DAG runs every 5 minutes with a 4-minute timeout. This ensures each run completes before the next one starts, preventing overlap."

> "If quality checks fail, the task raises an exception. Airflow retries twice, and if it still fails, the DAG is marked failed and an alert is sent. This prevents bad data from reaching production."

> "I set `catchup=False` because we only care about current train predictions, not historical backfills."

---

## Key Takeaways

| Concept | Implementation |
|---------|----------------|
| **Scheduling** | `schedule_interval='*/5 * * * *'` |
| **Retries** | `retries=2, retry_delay=1 min` |
| **Timeout** | `execution_timeout=4 min` |
| **No Overlap** | `max_active_runs=1` |
| **No Backfill** | `catchup=False` |
| **Task Dependencies** | TaskFlow API with implicit deps |
| **Data Passing** | XCom (automatic with TaskFlow) |
| **Failure Handling** | Raise exception → retry → alert |

---

*Next: Part 7 — Configuration (Pydantic Settings)*
