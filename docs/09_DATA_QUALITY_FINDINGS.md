# Part 9: Data Quality Findings — Real-World API Issues

## Overview

During development of the WMATA Metro ETL pipeline, we encountered several real-world data quality issues from the [WMATA real-time train prediction API](https://developer.wmata.com/docs/services/547636a6f9182302184cda78/operations/547636a6f918230da855363f). These findings shaped our defensive data engineering approach and demonstrate why quality gates are essential in production pipelines.

**Key takeaway**: Real-time transit APIs are messy. Building a robust pipeline means anticipating — and handling — data that doesn't match the documentation.

---

## Issue #1: Non-Serializable Timestamps

| Detail | Value |
|--------|-------|
| **Layer** | Transform → Orchestration |
| **Severity** | 🔴 Pipeline crash |
| **Symptom** | Airflow task marked `failed` with `executor_state=success` |

**Root Cause**: The `aggregate_station_metrics()` function returns Pandas `Timestamp` objects:

```python
'extracted_at': Timestamp('2026-04-21 02:25:54+0000', tz='UTC')
'calculated_at': Timestamp('2026-04-21 02:28:44+0000', tz='UTC')
```

Airflow XCom serializes task return values as JSON. Pandas `Timestamp` is not JSON-serializable, causing a silent serialization failure.

**Fix**: Convert all Timestamp values to ISO 8601 strings before returning from Airflow tasks:

```python
for agg in aggregates:
    for key, val in agg.items():
        if hasattr(val, 'isoformat'):
            agg[key] = val.isoformat()
```

**Lesson**: Always sanitize return types when passing data between orchestrator tasks. Pandas types ≠ Python stdlib types.

---

## Issue #2: Non-Standard Line Code `"No"`

| Detail | Value |
|--------|-------|
| **Layer** | API → Quality Check |
| **Severity** | 🟡 QC failure |
| **Symptom** | `Invalid lines: {'No'}` |

**Root Cause**: WMATA API returns `"No"` as the line code for non-revenue or no-passenger trains. This value is not documented in the official API spec.

**Sample record**:
```json
{
  "line": "No",
  "station_code": "A01",
  "destination": "No Passenger",
  "minutes_to_arrival": 0
}
```

**Fix**: Filter to known passenger lines upstream in the transformer, not at validation:

```python
VALID_PASSENGER_LINES = {"RD", "BL", "OR", "SV", "GR", "YL"}
df = df[df["line"].isin(VALID_PASSENGER_LINES)]
```

**Lesson**: Filter bad data at the source. Don't expand allow-lists to accommodate garbage — remove it early.

---

## Issue #3: Placeholder Line Code `"--"`

| Detail | Value |
|--------|-------|
| **Layer** | API → Quality Check |
| **Severity** | 🟡 QC failure |
| **Symptom** | `Invalid lines: {'--'}` |

**Root Cause**: WMATA API returns `"--"` when line information is unavailable. This typically occurs for trains that are out of service or transitioning between lines.

**Fix**: Same upstream filter as Issue #2 — only valid passenger lines pass through the transformer.

**Lesson**: Expect multiple sentinel values from real-time APIs. A single "invalid" placeholder would be too easy — real APIs use several.

---

## Issue #4: Undocumented Station Codes (Silver Line Extension)

| Detail | Value |
|--------|-------|
| **Layer** | API → Quality Check |
| **Severity** | 🟡 QC failure |
| **Symptom** | `7 unknown station codes: ['C11', 'N07', 'N08', 'N09', 'N10', 'N11', 'N12']` |

**Root Cause**: The Silver Line Phase 2 extension opened new stations that were not in our initial validation set. WMATA added these stations to the API without versioning changes.

**Stations added**:
| Code | Station |
|------|---------|
| C11 | Potomac Yard |
| N07 | Reston Town Center |
| N08 | Herndon |
| N09 | Innovation Center |
| N10 | Washington Dulles International Airport |
| N11 | Loudoun Gateway |
| N12 | Ashburn |

**Fix**: Updated the `KNOWN_STATIONS` reference set and added a tolerance threshold (allow up to 5 unknown stations before failing).

**Lesson**: Transit systems evolve. Validation reference sets require maintenance. Build in tolerance for new additions.

---

## Issue #5: Timezone Mismatch (UTC vs Eastern)

| Detail | Value |
|--------|-------|
| **Layer** | Orchestration |
| **Severity** | 🟠 Wrong schedule / confusing logs |
| **Symptom** | Logs showed `2026-04-21 02:25:32 UTC` instead of DC local time |

**Root Cause**: Airflow defaults to UTC. The DAG schedule `*/5 5-23 * * *` (intended for 5 AM–midnight Eastern) was being interpreted as UTC, meaning the pipeline ran from 1 AM–7 PM Eastern instead.

**Fix**:
1. Set Airflow timezone: `AIRFLOW__CORE__DEFAULT_TIMEZONE: 'US/Eastern'`
2. Use timezone-aware start date: `pendulum.datetime(2024, 1, 1, tz="US/Eastern")`

**Lesson**: Always set timezone explicitly in orchestrators for region-specific pipelines. UTC is a safe default for global systems, but transit operates on local time.

---

## Issue #6: Late-Night Completeness Failures

| Detail | Value |
|--------|-------|
| **Layer** | Quality Check |
| **Severity** | 🟡 Recurring QC failures at night |
| **Symptom** | Completeness check required 50+ stations, but only ~5–10 report after midnight |

**Root Cause**: WMATA operates reduced service late at night and is fully closed from ~midnight to 5 AM on weekdays. Fewer stations have active predictions during these hours.

**Fix**: Made the completeness threshold time-aware:

```python
hour = datetime.now(timezone.utc).hour
if 11 <= hour <= 23:       # Daytime (7 AM - 7 PM ET)
    min_stations = 40
elif 9 <= hour <= 10 or hour == 0:  # Shoulder hours
    min_stations = 20
else:                       # Late night / early morning
    min_stations = 3
```

**Lesson**: Static thresholds don't work for time-varying systems. Quality checks must be context-aware — what's "complete" at 10 AM is very different from 2 AM.

---

## Issue #7: Special `minutes_to_arrival` Values

| Detail | Value |
|--------|-------|
| **Layer** | API → Transform |
| **Severity** | 🟠 Bad aggregates if not handled |
| **Symptom** | Non-numeric values in a field expected to be numeric |

**Root Cause**: WMATA embeds train status indicators in the `Min` (minutes to arrival) field:

| API Value | Meaning | Our Mapping |
|-----------|---------|-------------|
| `"BRD"` | Train is boarding | `0` minutes |
| `"ARR"` | Train is arriving | `0` minutes |
| `"---"` | No data available | `NaN` (excluded from aggregates) |
| `"1"`, `"5"`, `"12"` | Actual minutes | Parsed as integer |
| `""` | Empty string | `NaN` (excluded) |

**Fix**: Handle in the WMATA client and transformer:
```python
# In client — map to numeric
"BRD" → 0, "ARR" → 0, "---" → None

# In transformer — coerce remaining
df["minutes_to_arrival"] = pd.to_numeric(df["minutes_to_arrival"], errors="coerce")
```

**Lesson**: Real-time transit APIs embed status indicators in numeric fields. Always inspect actual API responses — don't trust the schema docs alone.

---

## Issue #8: Null `destination_code` Fields

| Detail | Value |
|--------|-------|
| **Layer** | API |
| **Severity** | 🟢 Informational |
| **Symptom** | `"destination_code": null` on some records |

**Sample**:
```json
{
  "car_count": 6,
  "destination": "Shady Grv",
  "destination_code": null,
  "line": "RD",
  "station_code": "A05"
}
```

**Root Cause**: Some trains (short-turn, out-of-service, or with abbreviated destinations) have no destination code, even though they have a destination name.

**Fix**: Allow null in `destination_code`; enforce non-null only on critical fields (`station_code`, `line`).

**Lesson**: Not all fields will be populated for every record. Define which fields are truly required vs. optional based on your analytics needs.

---

## Summary

| # | Issue | Layer | Severity | Resolution Strategy |
|---|-------|-------|----------|---------------------|
| 1 | Non-serializable Timestamps | Transform → Orchestration | 🔴 Crash | ISO string conversion |
| 2 | `"No"` line code | API → Transform | 🟡 QC fail | Upstream passenger-line filter |
| 3 | `"--"` line code | API → Transform | 🟡 QC fail | Upstream passenger-line filter |
| 4 | Missing station codes | API → QC | 🟡 QC fail | Updated reference set + tolerance |
| 5 | UTC timezone default | Orchestration | 🟠 Wrong schedule | Explicit timezone config |
| 6 | Static completeness threshold | QC | 🟡 Night failures | Time-aware thresholds |
| 7 | String sentinel values | API → Transform | 🟠 Bad data | Parser with known value mapping |
| 8 | Null destination codes | API | 🟢 Info | Allow nullable fields |

---

## Key Engineering Principles Applied

1. **Filter early, validate late** — Remove known-bad data in the transformer; validate what remains in quality checks.
2. **Fail loud** — Quality checks raise exceptions, not warnings. Bad data never silently enters the database.
3. **Context-aware thresholds** — Static rules break with time-varying systems. Our checks adapt to time of day.
4. **Tolerance for evolution** — Station lists and line codes change. We allow small deviations before failing.
5. **Test with real data** — Unit tests with mocked data missed every issue above. Always test with live API responses.

---

*Document generated from real debugging sessions during pipeline development. All issues were encountered with live WMATA API data.*
