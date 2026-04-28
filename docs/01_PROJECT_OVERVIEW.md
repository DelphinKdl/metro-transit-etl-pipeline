# Part 1: Project Overview — What Problem Are We Solving?

## The Real-World Scenario

Imagine you work at a transit agency (like WMATA). Every day, thousands of trains run across the system. You need to answer questions like:

- "What's the average wait time at Metro Center?"
- "Which stations have the longest delays?"
- "How does service quality change throughout the day?"

**The problem**: The raw data comes from sensors and APIs in real-time, but it's messy, inconsistent, and disappears after a few minutes.

**The solution**: Build an ETL pipeline that:
1. **Captures** the data before it disappears
2. **Cleans** it (handles missing values, bad formats)
3. **Aggregates** it (raw predictions → station-level metrics)
4. **Stores** it in a database for analysis

---

## What WMATA's API Gives Us

Every time you call the API, you get something like this:

```json
{
  "Trains": [
    {
      "LocationCode": "A01",
      "LocationName": "Metro Center",
      "Line": "RD",
      "Destination": "Shady Grove",
      "Min": "3"
    },
    {
      "LocationCode": "A01",
      "LocationName": "Metro Center", 
      "Line": "RD",
      "Destination": "Glenmont",
      "Min": "ARR"
    },
    {
      "LocationCode": "A01",
      "LocationName": "Metro Center",
      "Line": "BL",
      "Destination": "Largo",
      "Min": "---"
    }
  ]
}
```

**Problems with this raw data**:

| Raw Value | Problem | Solution |
|-----------|---------|----------|
| `"Min": "3"` | String, not number | Parse to integer |
| `"Min": "ARR"` | Means "arriving now" | Convert to 0 |
| `"Min": "BRD"` | Means "boarding" | Convert to 0 |
| `"Min": "---"` | Means "no data" | Convert to NULL |
| Data disappears | No historical record | Store in database |

---

## What Our Pipeline Produces

After our ETL pipeline processes this data:

| station_code | line | avg_wait_min | min_wait | max_wait | train_count | timestamp |
|--------------|------|--------------|----------|----------|-------------|-----------|
| A01 | RD | 1.5 | 0 | 3 | 2 | 2026-04-09 08:00:00 |
| A01 | BL | NULL | NULL | NULL | 0 | 2026-04-09 08:00:00 |

**What we achieved**:
- Parsed "ARR" → 0, "---" → NULL
- Aggregated by station + line
- Calculated avg/min/max wait times
- Stored with timestamp for historical analysis

---

## The ETL Pattern

```
EXTRACT          TRANSFORM              LOAD
   │                 │                    │
   ▼                 ▼                    ▼
WMATA API  →  Clean + Aggregate  →  PostgreSQL
(raw JSON)    (pandas DataFrame)    (SQL tables)
```

### What Each Stage Does

| Stage | Input | Output | Tool |
|-------|-------|--------|------|
| **Extract** | API endpoint | Raw JSON | `requests` library |
| **Transform** | Raw JSON | Clean DataFrame | `pandas` library |
| **Load** | DataFrame | Database rows | `sqlalchemy` library |

---

## Why This Matters for Interviews

When you explain this project, you're demonstrating:

1. **Understanding of data problems** — Real data is messy
2. **ETL fundamentals** — The core pattern of data engineering
3. **Production thinking** — Handling edge cases, errors, retries
4. **Tool proficiency** — Python, pandas, SQL, Docker, Airflow

---

## Key Terminology

| Term | Definition |
|------|------------|
| **ETL** | Extract, Transform, Load — the process of moving data from source to destination |
| **Pipeline** | A series of data processing steps that run automatically |
| **Idempotent** | Running the same operation multiple times produces the same result |
| **Upsert** | INSERT or UPDATE — if record exists, update it; otherwise insert |
| **Orchestration** | Scheduling and monitoring pipelines (Airflow does this) |

---

*Next: Part 2 — The Extract Layer (WMATA API Client)*
