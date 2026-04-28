# Part 5: The Load Layer — PostgreSQL Upsert

## What is the Load Layer?

The **Load** layer takes validated data and writes it to the database. Key requirements:
- **Idempotent** — Running twice doesn't create duplicates
- **Atomic** — All-or-nothing transactions
- **Fast** — Efficient bulk operations

**Files**:
- `src/core/loader.py` — Python code
- `scripts/schema.sql` — Database schema

---

## The Database Schema

### Table 1: raw_predictions (Audit/Staging)

```sql
CREATE TABLE IF NOT EXISTS raw_predictions (
    id SERIAL PRIMARY KEY,
    station_code VARCHAR(10) NOT NULL,
    destination_code VARCHAR(10),
    line VARCHAR(10),
    minutes_to_arrival INTEGER,
    car_count INTEGER,
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    raw_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

**Purpose**: Store every raw API response for:
- Auditing (what did we receive?)
- Replay (re-process historical data)
- Debugging (what went wrong?)

---

### Table 2: station_wait_times (Production)

```sql
CREATE TABLE IF NOT EXISTS station_wait_times (
    station_code VARCHAR(10) NOT NULL,
    line VARCHAR(10) NOT NULL,
    station_name VARCHAR(100),
    avg_wait_minutes NUMERIC(5,2),
    min_wait_minutes INTEGER,
    max_wait_minutes INTEGER,
    train_count INTEGER,
    calculated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (station_code, line, calculated_at)  -- Composite key!
);
```

**Purpose**: Store aggregated metrics for analysis.

**The Composite Primary Key**:
```sql
PRIMARY KEY (station_code, line, calculated_at)
```

This means: One row per station + line + time window. This enables the upsert pattern.

---

### Table 3: pipeline_runs (Metadata)

```sql
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(50) PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'running',
    records_processed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB
);
```

**Purpose**: Track pipeline execution history.

---

## The Upsert Pattern

### What is Upsert?

**Upsert** = **UP**date or in**SERT**

| Scenario | Action |
|----------|--------|
| Record doesn't exist | INSERT new row |
| Record already exists | UPDATE existing row |

### The SQL

```sql
INSERT INTO station_wait_times (
    station_code, line, station_name, avg_wait_minutes,
    min_wait_minutes, max_wait_minutes, train_count, calculated_at
) VALUES (
    :station_code, :line, :station_name, :avg_wait_minutes,
    :min_wait_minutes, :max_wait_minutes, :train_count, :calculated_at
)
ON CONFLICT (station_code, line, calculated_at) 
DO UPDATE SET
    station_name = EXCLUDED.station_name,
    avg_wait_minutes = EXCLUDED.avg_wait_minutes,
    min_wait_minutes = EXCLUDED.min_wait_minutes,
    max_wait_minutes = EXCLUDED.max_wait_minutes,
    train_count = EXCLUDED.train_count
```

### How ON CONFLICT Works

```
INSERT attempt
     │
     ├── No conflict? → INSERT new row
     │
     └── Conflict on (station_code, line, calculated_at)?
              │
              └── DO UPDATE → Update existing row with new values
```

**EXCLUDED** refers to the values we tried to insert.

### Why This Matters

| Without Upsert | With Upsert |
|----------------|-------------|
| Pipeline fails on retry | Pipeline succeeds on retry |
| Duplicate rows | No duplicates |
| Manual cleanup needed | Self-healing |

**Interview Talking Point**:
> "I use PostgreSQL's ON CONFLICT for idempotent writes. If the pipeline runs twice for the same time window, it updates the existing record instead of failing or creating duplicates. This makes the pipeline safe to retry."

---

## The Python Code

### DatabaseLoader Class

**Location**: `04_load_tests/loader.py` lines 21-113

```python
class DatabaseLoader:
    def __init__(self, connection_string: str):
        self.engine = create_engine(
            connection_string,
            pool_size=5,          # Keep 5 connections ready
            max_overflow=10,      # Allow 10 more under load
            pool_pre_ping=True    # Check connection health
        )
        self.Session = sessionmaker(bind=self.engine)
```

**Connection Pooling**:
| Setting | Purpose |
|---------|---------|
| `pool_size=5` | Keep 5 connections open |
| `max_overflow=10` | Allow up to 15 total under load |
| `pool_pre_ping=True` | Test connection before using |

---

### Session Management (Context Manager)

```python
@contextmanager
def get_session(self) -> Session:
    session = self.Session()
    try:
        yield session
        session.commit()      # Success → commit
    except Exception:
        session.rollback()    # Error → rollback
        raise
    finally:
        session.close()       # Always close
```

**Why Context Manager?**
- Automatic commit on success
- Automatic rollback on error
- Automatic cleanup (close connection)

**Usage**:
```python
with self.get_session() as session:
    session.execute(...)
    # Auto-commits if no error
    # Auto-rollbacks if error
```

---

### The Upsert Method

```python
def upsert_station_metrics(self, aggregates: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not aggregates:
        return {'rows_affected': 0, 'execution_time_ms': 0}
    
    start_time = time.time()
    
    upsert_sql = text("""
        INSERT INTO station_wait_times (...)
        VALUES (...)
        ON CONFLICT (station_code, line, calculated_at) 
        DO UPDATE SET ...
    """)
    
    with self.get_session() as session:
        for agg in aggregates:
            session.execute(upsert_sql, {...})
    
    execution_time_ms = (time.time() - start_time) * 1000
    
    return {
        'rows_affected': len(aggregates),
        'execution_time_ms': round(execution_time_ms, 2)
    }
```

**Returns**:
```python
{
    'rows_affected': 85,
    'execution_time_ms': 42.5
}
```

---

## The Views (Pre-computed Queries)

### View 1: latest_station_wait_times

```sql
CREATE OR REPLACE VIEW latest_station_wait_times AS
SELECT DISTINCT ON (station_code, line)
    station_code, line, station_name,
    avg_wait_minutes, min_wait_minutes, max_wait_minutes,
    train_count, calculated_at
FROM station_wait_times
ORDER BY station_code, line, calculated_at DESC;
```

**What it does**: Returns the most recent data for each station + line.

**Usage**:
```sql
SELECT * FROM latest_station_wait_times WHERE line = 'RD';
```

---

### View 2: hourly_wait_averages

```sql
CREATE OR REPLACE VIEW hourly_wait_averages AS
SELECT 
    station_code, line,
    DATE_TRUNC('hour', calculated_at) AS hour,
    AVG(avg_wait_minutes) AS avg_wait,
    MIN(min_wait_minutes) AS min_wait,
    MAX(max_wait_minutes) AS max_wait,
    SUM(train_count) AS total_trains
FROM station_wait_times
GROUP BY station_code, line, DATE_TRUNC('hour', calculated_at);
```

**What it does**: Aggregates data by hour for trend analysis.

**Usage**:
```sql
SELECT * FROM hourly_wait_averages 
WHERE station_code = 'A01' 
ORDER BY hour DESC LIMIT 24;
```

---

## Indexes

```sql
CREATE INDEX idx_raw_predictions_station ON raw_predictions(station_code);
CREATE INDEX idx_raw_predictions_extracted ON raw_predictions(extracted_at);
CREATE INDEX idx_station_wait_times_calculated ON station_wait_times(calculated_at);
CREATE INDEX idx_station_wait_times_line ON station_wait_times(line);
```

**Why indexes?**
| Query Pattern | Index Used |
|---------------|------------|
| Filter by station | `idx_raw_predictions_station` |
| Filter by time | `idx_raw_predictions_extracted` |
| Filter by line | `idx_station_wait_times_line` |

Without indexes, queries scan entire tables. With indexes, they jump directly to matching rows.

---

## Convenience Functions

```python
def get_loader(connection_string: Optional[str] = None) -> DatabaseLoader:
    """Get or create a database loader instance."""
    global _loader
    if _loader is None:
        conn_str = connection_string or os.getenv("DATABASE_URL", "...")
        _loader = DatabaseLoader(conn_str)
    return _loader

def upsert_to_postgres(aggregates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Convenience function to upsert aggregates."""
    loader = get_loader()
    return loader.upsert_station_metrics(aggregates)
```

**Why singleton pattern?**
- Reuse database connections
- Don't create new connection pool for every call

---

## How It Fits in the Pipeline

```
Extract → Transform → Quality Checks → LOAD
                                        │
                                        ├── upsert_station_metrics()
                                        │   └── INSERT ... ON CONFLICT DO UPDATE
                                        │
                                        └── Returns: {rows_affected: 85, execution_time_ms: 42}
```

---

## Interview Talking Points

> "I use PostgreSQL's ON CONFLICT clause for idempotent upserts. This means the pipeline is safe to retry — running it twice for the same data won't create duplicates."

> "I implemented connection pooling with SQLAlchemy to reuse database connections. This reduces latency and prevents connection exhaustion under load."

> "I created database views for common query patterns. The `latest_station_wait_times` view gives analysts instant access to current data without writing complex queries."

> "I use a context manager for session management. This ensures transactions are committed on success and rolled back on failure, preventing partial writes."

---

## Key Takeaways

| Concept | Implementation |
|---------|----------------|
| **Idempotency** | `ON CONFLICT DO UPDATE` |
| **Atomicity** | Transaction commit/rollback |
| **Connection Pooling** | SQLAlchemy engine with pool_size |
| **Session Management** | Context manager pattern |
| **Query Optimization** | Indexes on filter columns |
| **Pre-computed Queries** | Database views |

---

*Next: Part 6 — Orchestration (Airflow DAG)*
