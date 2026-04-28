# 05 Interview Script

## Overview
This document provides talking points and explanations for discussing this ETL pipeline in technical interviews.

## Elevator Pitch (30 seconds)

> "I built a real-time ETL pipeline that pulls train arrival predictions from WMATA's API every 5 minutes, transforms them into station-level wait time aggregates, and loads them into PostgreSQL using idempotent upserts. It mirrors the exact pattern used in production transit systems: raw vehicle data → clean source of truth."

## What This Pipeline Does & Why It Mirrors Their Work

| Pipeline Component | Production Equivalent | Why It Matters |
|-------------------|----------------------|----------------|
| WMATA API polling | Vehicle GPS/AVL feeds | Real-time data ingestion |
| 5-minute schedule | Frequent batch processing | Balance freshness vs. load |
| Station aggregates | Derived metrics | Transform raw → actionable |
| PostgreSQL upserts | Idempotent loading | Safe retries, no duplicates |
| Airflow DAG | Production orchestration | Industry-standard tooling |
| Quality checks | Data validation | Catch issues before production |

## Technical Deep Dives

### 1. Why 5-Minute Intervals?

**Question**: "Why did you choose 5-minute polling?"

**Answer**: 
- WMATA API rate limits (50k calls/day free tier)
- Predictions change slowly (trains take minutes to arrive)
- Balances data freshness vs. API costs
- Matches typical transit reporting intervals

### 2. Upsert Pattern

**Question**: "How do you handle duplicate data?"

**Answer**:
```sql
INSERT INTO station_wait_times (...)
ON CONFLICT (station_code, line, calculated_at) 
DO UPDATE SET avg_wait_minutes = EXCLUDED.avg_wait_minutes, ...
```
- Primary key on (station, line, timestamp)
- Idempotent: safe to retry on failure
- No duplicates even with overlapping runs

### 3. Quality Checks

**Question**: "How do you ensure data quality?"

**Answer**:
- **Schema validation**: Required fields present
- **Range checks**: Wait times 0-60 minutes
- **Freshness**: Data < 10 minutes old
- **Completeness**: 80%+ stations reporting
- **Fail-fast**: Pipeline stops on QC failure

### 4. Error Handling

**Question**: "What happens when the API fails?"

**Answer**:
- Retry with exponential backoff (3 attempts)
- Circuit breaker pattern for sustained failures
- Alerting via Airflow email/Slack
- Raw data stored for replay

### 5. Scalability

**Question**: "How would you scale this?"

**Answer**:
- **Horizontal**: Multiple workers, partitioned by station
- **Vertical**: Batch size tuning, connection pooling
- **Storage**: Time-series partitioning, data retention policies
- **Real-time**: Kafka for streaming if needed

## Common Interview Questions

### Architecture Questions

1. **"Walk me through the data flow."**
   - API → Extract (raw JSON) → Transform (pandas) → QC → Load (PostgreSQL)

2. **"Why Airflow over cron?"**
   - Dependency management, retries, backfills, monitoring UI

3. **"Why PostgreSQL over NoSQL?"**
   - Structured data, ACID compliance, SQL analytics, upsert support

### Code Questions

1. **"Show me the transformation logic."**
   - Point to `03_transform_qc/transformer.py`
   - Explain groupby aggregation, null handling

2. **"How do you test this?"**
   - Unit tests for each module
   - Integration tests with test database
   - Mock API responses

### Behavioral Questions

1. **"Tell me about a challenge you faced."**
   - Handling WMATA's inconsistent "Min" field (ARR, BRD, ---, numbers)
   - Solution: Robust parsing with explicit null handling

2. **"How would you debug a data quality issue?"**
   - Check raw_predictions table for original data
   - Review QC failure logs
   - Compare against WMATA's live data

## Key Metrics to Mention

- **Latency**: < 30 seconds from API call to database
- **Reliability**: Idempotent, retry-safe
- **Coverage**: All 91 WMATA stations
- **Freshness**: Data always < 5 minutes old

## Technologies Used

| Category | Technology | Why |
|----------|------------|-----|
| Language | Python 3.9+ | Industry standard for data |
| Orchestration | Apache Airflow | Production-grade scheduling |
| Database | PostgreSQL | ACID, upserts, analytics |
| Data Processing | pandas | Fast, expressive transforms |
| API Client | requests | Simple, reliable HTTP |
| Testing | pytest | Standard Python testing |

## Questions to Ask Them

1. "What does your current ETL architecture look like?"
2. "How do you handle data quality monitoring?"
3. "What's your approach to backfills and replay?"
4. "How do you balance real-time vs. batch processing?"
