# Part 2: The Extract Layer — WMATA API Client

## What is the Extract Layer?

The **Extract** layer is responsible for:
- Connecting to the data source (WMATA API)
- Fetching raw data
- Handling errors gracefully
- Converting raw JSON into structured Python objects

**Files**: 
- `src/clients/wmata_client.py` — API client
- `src/models/predictions.py` — Data model

---

## The Code Breakdown

### 1. The Data Structure (TrainPrediction)

```python
@dataclass
class TrainPrediction:
    """Represents a single train prediction."""
    car_count: Optional[int]
    destination: str
    destination_code: str
    line: str
    station_code: str
    station_name: str
    minutes_to_arrival: Optional[int]
    raw_minutes: str
    extracted_at: datetime
```

**Why use a dataclass?**
- Type safety — you know exactly what fields exist
- IDE autocomplete — `prediction.line` instead of `prediction["line"]`
- Immutability — harder to accidentally modify data
- Self-documenting — the code shows the data structure

---

### 2. The Client Class (WMATAClient)

```python
class WMATAClient:
    BASE_URL = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction"
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("WMATA_API_KEY")
        if not self.api_key:
            raise ValueError("WMATA API key required.")
        
        self.session = self._create_session()
```

**Key Design Decisions**:

| Decision | Why |
|----------|-----|
| API key from env var | Security — never hardcode secrets |
| `requests.Session` | Reuses connections, faster for multiple calls |
| Fail fast if no key | Better to crash immediately than fail silently later |

---

### 3. Retry Logic (Production-Critical)

```python
def _create_session(self) -> requests.Session:
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,                              # Try 3 times
        backoff_factor=1,                     # Wait 1s, 2s, 4s between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these errors
        allowed_methods=["GET"]               # Only retry GET requests
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    return session
```

**What this handles**:

| HTTP Code | Meaning | What Happens |
|-----------|---------|--------------|
| 429 | Too Many Requests | Wait and retry |
| 500 | Server Error | Wait and retry |
| 502 | Bad Gateway | Wait and retry |
| 503 | Service Unavailable | Wait and retry |
| 504 | Gateway Timeout | Wait and retry |

**Interview Talking Point**:
> "I implemented exponential backoff for API retries. If WMATA's API returns a 503, we wait 1 second, then 2 seconds, then 4 seconds before giving up. This handles temporary outages gracefully."

---

### 4. Rate Limiting (Be a Good API Citizen)

```python
def _rate_limit(self) -> None:
    elapsed = time.time() - self._last_request_time
    if elapsed < self._min_request_interval:
        time.sleep(self._min_request_interval - elapsed)
    self._last_request_time = time.time()
```

**Why rate limit ourselves?**
- WMATA has API limits (10 requests/second)
- Getting banned = pipeline breaks
- Being a good API citizen

---

### 5. Parsing Raw Data

```python
@staticmethod
def _parse_minutes(minutes_str: str) -> Optional[int]:
    """
    WMATA returns:
    - "ARR" = arriving (convert to 0)
    - "BRD" = boarding (convert to 0)
    - "---" = no data (convert to None)
    - "3"   = 3 minutes (convert to int)
    """
    if minutes_str in ("ARR", "BRD"):
        return 0
    if minutes_str == "---" or not minutes_str:
        return None
    try:
        return int(minutes_str)
    except ValueError:
        return None
```

**This is where "messy data" gets cleaned**:

| Raw Input | Parsed Output | Why |
|-----------|---------------|-----|
| `"ARR"` | `0` | Train is here, 0 minutes wait |
| `"BRD"` | `0` | Train is boarding, 0 minutes wait |
| `"---"` | `None` | No data available |
| `"3"` | `3` | 3 minutes until arrival |
| `"abc"` | `None` | Invalid data, treat as missing |

---

## The Flow

```
1. WMATAClient() created
   └── Reads API key from .env
   └── Creates session with retry logic

2. client.get_predictions("All")
   └── Rate limit check (don't spam API)
   └── HTTP GET request
   └── Parse JSON response
   └── Convert to TrainPrediction objects

3. Returns List[TrainPrediction]
   └── Clean, typed data ready for Transform layer
```

---

## Error Handling

```python
try:
    response = self.session.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    
except requests.exceptions.RequestException as e:
    logger.error("api_request_failed", error=str(e))
    raise
```

**What can go wrong**:

| Error | Cause | Handling |
|-------|-------|----------|
| `Timeout` | API too slow | Retry (via HTTPAdapter) |
| `ConnectionError` | Network down | Retry, then fail |
| `HTTPError 401` | Bad API key | Fail immediately |
| `HTTPError 429` | Rate limited | Retry with backoff |
| `JSONDecodeError` | Invalid response | Fail with error log |

---

## Logging (Observability)

```python
import structlog
logger = structlog.get_logger()

logger.info("fetching_predictions", station_code=station_code)
logger.info("predictions_fetched", count=len(predictions))
logger.error("api_request_failed", error=str(e))
```

**Why structured logging?**
- Machine-readable (JSON format)
- Easy to search in log aggregators (Datadog, Splunk)
- Context attached to each log entry

**Example output**:
```
2026-04-09 02:49:18 [info] fetching_predictions station_code=A01
2026-04-09 02:49:18 [info] predictions_fetched count=0
```

---

## Testing the Extract Layer

You already ran this successfully:

```bash
python test_api.py
```

Output:
```
Testing WMATA API connection...
----------------------------------------
2026-04-09 02:49:18 [info] fetching_predictions station_code=A01
2026-04-09 02:49:18 [info] predictions_fetched count=0
✅ SUCCESS! Got 0 predictions for Metro Center
```

---

## Key Takeaways

| Concept | Implementation |
|---------|----------------|
| **Type Safety** | `@dataclass` for TrainPrediction |
| **Retry Logic** | `urllib3.Retry` with exponential backoff |
| **Rate Limiting** | Self-imposed delay between requests |
| **Error Handling** | Try/except with logging |
| **Clean Parsing** | Handle all edge cases ("ARR", "---", etc.) |
| **Observability** | Structured logging with `structlog` |

---

*Next: Part 3 — The Transform Layer (Data Cleaning & Aggregation)*
