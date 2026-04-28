# Part 4: Quality Checks — Data Validation

## What Are Quality Checks?

Quality checks (QC) validate data **before** loading it into the database. They answer:
- Is the data complete?
- Is the data valid?
- Is the data fresh?

**File**: `src/core/quality_checks.py`

---

## Why Quality Checks Matter

| Without QC | With QC |
|------------|---------|
| Bad data silently enters database | Bad data is caught and rejected |
| Downstream reports are wrong | Only clean data is loaded |
| Hard to debug issues | Clear error messages |
| Users lose trust | Data quality is guaranteed |

**Interview Talking Point**:
> "I implement quality gates in my pipelines. If data fails validation, the pipeline stops and alerts us rather than loading garbage into production."

---

## The QCResult Data Structure

```python
@dataclass
class QCResult:
    """Result of a quality check."""
    check_name: str      # e.g., "null_rate_avg_wait_minutes"
    passed: bool         # True or False
    message: str         # Human-readable explanation
    value: Any = None    # Actual measured value
    threshold: Any = None # Expected threshold
```

**Example**:
```python
QCResult(
    check_name="null_rate_avg_wait_minutes",
    passed=True,
    message="Null rate 2.50% <= 5.00%",
    value=0.025,
    threshold=0.05
)
```

---

## The 8 Quality Checks

### 1. Schema Validation
**Location**: Lines 87-122

```python
def check_schema(aggregates: List[Dict[str, Any]]) -> QCResult:
    required_fields = {
        'station_code', 'line', 'avg_wait_minutes', 
        'min_wait_minutes', 'max_wait_minutes', 'train_count'
    }
```

| Question | Answer |
|----------|--------|
| What it checks | Are all required fields present? |
| Passes when | All required fields exist in every record |
| Fails when | Any field is missing |

---

### 2. Null Rate Check
**Location**: Lines 48-84

```python
def check_null_rate(aggregates, field, threshold=0.05):
    null_count = sum(1 for agg in aggregates if agg.get(field) is None)
    null_rate = null_count / len(aggregates)
    passed = null_rate <= threshold
```

| Question | Answer |
|----------|--------|
| What it checks | Is the null rate below 5%? |
| Passes when | ≤5% of values are null |
| Fails when | >5% of values are null |

**Example**:
- 100 records, 3 nulls → 3% null rate → ✅ PASS
- 100 records, 10 nulls → 10% null rate → ❌ FAIL

---

### 3. Wait Time Range Check
**Location**: Lines 125-166

```python
def check_wait_time_range(aggregates, min_val=0, max_val=60):
    for agg in aggregates:
        avg = agg.get('avg_wait_minutes')
        if avg is not None and (avg < min_val or avg > max_val):
            out_of_range.append(...)
```

| Question | Answer |
|----------|--------|
| What it checks | Are wait times realistic (0-60 min)? |
| Passes when | All wait times are 0-60 minutes |
| Fails when | Any wait time is negative or >60 min |

**Why**: A wait time of -5 or 999 minutes is clearly wrong data.

---

### 4. Valid Stations Check
**Location**: Lines 169-200

```python
KNOWN_STATIONS = {
    'A01', 'A02', 'A03', ...  # ~90 stations
}

def check_valid_stations(aggregates):
    for agg in aggregates:
        station = agg.get('station_code')
        if station and station not in KNOWN_STATIONS:
            invalid_stations.add(station)
```

| Question | Answer |
|----------|--------|
| What it checks | Are station codes real WMATA stations? |
| Passes when | ≤5 unknown station codes |
| Fails when | >5 unknown station codes |

**Why**: Station code "XYZ" doesn't exist — it's bad data.

---

### 5. Valid Lines Check
**Location**: Lines 203-233

```python
VALID_LINES = {'RD', 'BL', 'OR', 'SV', 'GR', 'YL'}

def check_valid_lines(aggregates):
    for agg in aggregates:
        line = agg.get('line')
        if line and line not in VALID_LINES:
            invalid_lines.add(line)
```

| Question | Answer |
|----------|--------|
| What it checks | Are line codes valid (RD, BL, OR, SV, GR, YL)? |
| Passes when | All lines are valid |
| Fails when | Any unknown line code |

---

### 6. Data Freshness Check
**Location**: Lines 236-278

```python
def check_data_freshness(aggregates, max_age_minutes=10):
    now = datetime.now(timezone.utc)
    max_age = timedelta(minutes=max_age_minutes)
    
    for agg in aggregates:
        if now - extracted_at > max_age:
            stale_count += 1
```

| Question | Answer |
|----------|--------|
| What it checks | Is the data recent (not stale)? |
| Passes when | <10% of data is older than 10 minutes |
| Fails when | ≥10% of data is stale |

**Why**: Loading 2-hour-old data as "current" is misleading.

---

### 7. Completeness Check
**Location**: Lines 281-313

```python
def check_completeness(aggregates, min_stations=50):
    unique_stations = set(agg.get('station_code') for agg in aggregates)
    station_count = len(unique_stations)
    passed = station_count >= min_stations
```

| Question | Answer |
|----------|--------|
| What it checks | Are enough stations reporting? |
| Passes when | ≥50 stations have data |
| Fails when | <50 stations (system might be down) |

**Why**: If only 5 stations report, something is wrong with the API or system.

---

### 8. Null Rate for Station Code
Same as #2, but specifically for `station_code` field.

---

## The Main Function: run_quality_checks()

**Location**: Lines 316-368

```python
def run_quality_checks(aggregates: List[Dict[str, Any]]) -> Dict[str, Any]:
    checks = [
        check_schema(aggregates),
        check_null_rate(aggregates, 'avg_wait_minutes'),
        check_null_rate(aggregates, 'station_code'),
        check_wait_time_range(aggregates),
        check_valid_stations(aggregates),
        check_valid_lines(aggregates),
        check_data_freshness(aggregates),
        check_completeness(aggregates),
    ]
    
    failures = [c for c in checks if not c.passed]
    
    return {
        'passed': len(failures) == 0,
        'total_checks': len(checks),
        'passed_checks': len(checks) - len(failures),
        'failed_checks': len(failures),
        'failures': [...],
        'all_results': [...],
        'checked_at': datetime.now(timezone.utc).isoformat()
    }
```

**Returns**:
```python
{
    'passed': True,           # Overall pass/fail
    'total_checks': 8,        # How many checks ran
    'passed_checks': 8,       # How many passed
    'failed_checks': 0,       # How many failed
    'failures': [],           # Details of failures
    'all_results': [...],     # All check results
    'checked_at': '2026-04-09T07:00:00+00:00'
}
```

---

## How It Fits in the Pipeline

```
Extract → Transform → QUALITY CHECKS → Load
                           │
                           ├── All pass? → Continue to Load
                           │
                           └── Any fail? → STOP pipeline, alert
```

In the DAG (`02_the_dag/wmata_etl_dag.py`):
```python
@task()
def quality_check_task(transform_result: Dict[str, Any]) -> Dict[str, Any]:
    aggregates = transform_result['aggregates']
    qc_result = run_quality_checks(aggregates)
    
    if not qc_result['passed']:
        raise ValueError(f"Quality checks failed: {qc_result['failures']}")
    
    return qc_result
```

---

## Example Output

**All Checks Pass**:
```
quality_checks_completed passed=True total=8 failures=0
```

**Some Checks Fail**:
```
quality_checks_completed passed=False total=8 failures=2

Failures:
- null_rate_avg_wait_minutes: Null rate 12.50% > 5.00%
- completeness: 35 stations reporting (min: 50)
```

---

## Interview Talking Points

> "I implement 8 quality checks including schema validation, null rate thresholds, value range checks, and data freshness. If any check fails, the pipeline stops rather than loading bad data."

> "The completeness check catches partial outages — if only 35 of 90 stations are reporting, something is wrong with the source system."

> "Each check returns a structured result with the actual value and threshold, making debugging easy."

---

## Key Takeaways

| Check Type | What It Catches |
|------------|-----------------|
| **Schema** | Missing fields |
| **Null Rate** | Too much missing data |
| **Range** | Impossible values |
| **Valid Codes** | Unknown stations/lines |
| **Freshness** | Stale data |
| **Completeness** | Partial outages |

---

*Next: Part 5 — Load Layer (PostgreSQL Upsert)*
