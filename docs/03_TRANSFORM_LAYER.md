# Part 3: The Transform Layer — Data Cleaning & Aggregation

## What is the Transform Layer?

The **Transform** layer takes raw data from the Extract layer and:
1. **Cleans** it (handle nulls, parse types, remove duplicates)
2. **Validates** it (ensure data makes sense)
3. **Aggregates** it (group by station, calculate metrics)

**File**: `src/core/transformer.py`

---

## The Data Flow

```
Input: List of TrainPrediction objects (from Extract)
   │
   ▼
┌─────────────────────────────────────┐
│  transform_predictions()            │
│  - Convert to DataFrame             │
│  - Parse numeric fields             │
│  - Remove invalid rows              │
│  - Deduplicate                      │
└─────────────────────────────────────┘
   │
   ▼
┌─────────────────────────────────────┐
│  aggregate_station_metrics()        │
│  - Group by station + line          │
│  - Calculate avg/min/max wait       │
│  - Count trains                     │
└─────────────────────────────────────┘
   │
   ▼
Output: List of station aggregates (ready for Load)
```

---

## Function 1: transform_predictions()

**Location**: `src/core/transformer.py`

### What It Does

```python
def transform_predictions(raw_predictions: List[Dict[str, Any]]) -> pd.DataFrame:
```

Takes raw prediction dictionaries and returns a cleaned pandas DataFrame.

### Step-by-Step Breakdown

#### Step 1: Handle Empty Input
```python
if not raw_predictions:
    logger.warning("empty_predictions_received")
    return pd.DataFrame()
```
**Why**: Don't crash on empty data (like at 2 AM when Metro is closed).

---

#### Step 2: Convert to DataFrame
```python
df = pd.DataFrame(raw_predictions)
```
**Why**: pandas makes data manipulation easy and fast.

**Before** (list of dicts):
```python
[
    {"station_code": "A01", "line": "RD", "minutes_to_arrival": 3, ...},
    {"station_code": "A01", "line": "RD", "minutes_to_arrival": 8, ...},
]
```

**After** (DataFrame):
| station_code | line | minutes_to_arrival | ... |
|--------------|------|-------------------|-----|
| A01 | RD | 3 | ... |
| A01 | RD | 8 | ... |

---

#### Step 3: Parse Numeric Fields
```python
df['minutes_to_arrival'] = pd.to_numeric(df['minutes_to_arrival'], errors='coerce')
df['car_count'] = pd.to_numeric(df['car_count'], errors='coerce')
```

**What `errors='coerce'` does**:
| Input | Output |
|-------|--------|
| `3` | `3` |
| `"3"` | `3` |
| `None` | `NaN` |
| `"abc"` | `NaN` |

**Why**: Ensures all values are numbers or NaN (not strings).

---

#### Step 4: Remove Invalid Rows
```python
# Remove rows with no station code
df = df[df['station_code'].notna() & (df['station_code'] != '')]

# Remove rows with no line
df = df[df['line'].notna() & (df['line'] != '')]
```

**Why**: A prediction without a station or line is useless.

---

#### Step 5: Deduplicate
```python
df = df.drop_duplicates(
    subset=['station_code', 'line', 'destination_code', 'minutes_to_arrival', 'extracted_at'],
    keep='first'
)
```

**Why**: The API sometimes returns duplicate entries. We keep only unique combinations.

---

#### Step 6: Log What Happened
```python
logger.info(
    "predictions_transformed",
    initial_count=initial_count,
    final_count=final_count,
    dropped=initial_count - final_count
)
```

**Example log**:
```
predictions_transformed initial_count=150 final_count=142 dropped=8
```

---

## Function 2: aggregate_station_metrics()

**Location**: `03_transform_qc/transformer.py` lines 67-108

### What It Does

```python
def aggregate_station_metrics(df: pd.DataFrame) -> List[Dict[str, Any]]:
```

Groups predictions by station + line and calculates wait time statistics.

### The Aggregation

```python
aggregates = valid_df.groupby(['station_code', 'line']).agg(
    station_name=('station_name', 'first'),
    avg_wait_minutes=('minutes_to_arrival', 'mean'),
    min_wait_minutes=('minutes_to_arrival', 'min'),
    max_wait_minutes=('minutes_to_arrival', 'max'),
    train_count=('minutes_to_arrival', 'count'),
    extracted_at=('extracted_at', 'max')
).reset_index()
```

### Before vs After

**Before** (cleaned DataFrame):
| station_code | line | minutes_to_arrival |
|--------------|------|-------------------|
| A01 | RD | 3 |
| A01 | RD | 8 |
| A01 | RD | 12 |
| A01 | BL | 5 |
| B01 | RD | 2 |

**After** (aggregated):
| station_code | line | avg_wait | min_wait | max_wait | train_count |
|--------------|------|----------|----------|----------|-------------|
| A01 | RD | 7.67 | 3 | 12 | 3 |
| A01 | BL | 5.00 | 5 | 5 | 1 |
| B01 | RD | 2.00 | 2 | 2 | 1 |

### What Each Metric Means

| Metric | Calculation | Business Meaning |
|--------|-------------|------------------|
| `avg_wait_minutes` | Mean of all wait times | Average passenger wait |
| `min_wait_minutes` | Minimum wait time | Next train arriving |
| `max_wait_minutes` | Maximum wait time | Longest wait if you miss trains |
| `train_count` | Count of predictions | How many trains coming |

---

## Function 3: enrich_with_metadata()

**Location**: `03_transform_qc/transformer.py` lines 111-129

```python
def enrich_with_metadata(aggregates, run_id=None):
    for agg in aggregates:
        agg['run_id'] = run_id or datetime.now().strftime('%Y%m%d%H%M%S')
        agg['pipeline_version'] = '1.0.0'
    return aggregates
```

**Why**: Adds tracking information so you know:
- When this data was processed (`run_id`)
- Which version of the pipeline created it (`pipeline_version`)

---

## Line Code Reference

```python
LINE_CODES = {
    'RD': 'Red',
    'BL': 'Blue',
    'OR': 'Orange',
    'SV': 'Silver',
    'GR': 'Green',
    'YL': 'Yellow'
}
```

WMATA uses 2-letter codes. This mapping helps with human-readable output.

---

## Why Use pandas?

| Feature | Benefit |
|---------|---------|
| **Vectorized operations** | Fast processing of thousands of rows |
| **Built-in aggregation** | `groupby().agg()` is powerful |
| **Null handling** | `NaN` values handled automatically |
| **Type coercion** | `pd.to_numeric()` handles messy data |
| **Industry standard** | Every data engineer knows pandas |

---

## Interview Talking Points

> "I use pandas for the transform layer because it handles vectorized operations efficiently. For example, cleaning 10,000 predictions takes milliseconds because pandas operations are optimized in C."

> "The aggregation uses groupby with multiple aggregation functions in a single pass — this is more efficient than calculating each metric separately."

> "I add metadata like run_id and pipeline_version for data lineage. This helps debug issues by tracing data back to the exact pipeline run that created it."

---

## Key Takeaways

| Concept | Implementation |
|---------|----------------|
| **Data Cleaning** | Remove nulls, parse types, deduplicate |
| **Aggregation** | Group by station + line, calculate metrics |
| **Null Safety** | Handle empty input, filter invalid rows |
| **Observability** | Log counts before/after transformation |
| **Metadata** | Add run_id for data lineage |

---

*Next: Part 4 — Quality Checks (Data Validation)*
