# Part 7: Configuration — Pydantic Settings

## What is Configuration Management?

Configuration management handles:
- **Environment variables** (API keys, database URLs)
- **Defaults** (timeout values, batch sizes)
- **Validation** (ensure values are correct types and ranges)
- **Environment-specific settings** (dev vs prod)

**File**: `config/settings.py`

---

## Why Pydantic Settings?

| Old Way (os.getenv) | Pydantic Settings |
|---------------------|-------------------|
| `os.getenv("API_KEY")` returns string or None | Type-safe, validated |
| No validation | Built-in validation |
| No defaults structure | Organized with defaults |
| Easy to misspell env vars | IDE autocomplete |
| No documentation | Self-documenting |

**Interview Talking Point**:
> "I use Pydantic Settings for configuration management. It provides type safety, validation, and automatic loading from .env files. This catches configuration errors at startup rather than runtime."

---

## The Settings Classes

### 1. WMATASettings

**Location**: Lines 16-29

```python
class WMATASettings(BaseSettings):
    """WMATA API configuration."""
    
    api_key: str = Field(..., description="WMATA API key")
    timeout: int = Field(default=30, ge=1, le=120)
    max_retries: int = Field(default=3, ge=0, le=10)
    base_url: str = Field(
        default="https://api.wmata.com/StationPrediction.svc/json/GetPrediction"
    )
    
    model_config = SettingsConfigDict(
        env_prefix="WMATA_",
        extra="ignore"
    )
```

**Key Features**:

| Feature | Code | Meaning |
|---------|------|---------|
| Required field | `api_key: str = Field(...)` | Must be provided, no default |
| Range validation | `ge=1, le=120` | Greater/equal 1, less/equal 120 |
| Env prefix | `env_prefix="WMATA_"` | Reads `WMATA_API_KEY`, `WMATA_TIMEOUT` |

**Environment Variables**:
```bash
WMATA_API_KEY=your_key_here
WMATA_TIMEOUT=30
WMATA_MAX_RETRIES=3
```

---

### 2. DatabaseSettings

**Location**: Lines 32-54

```python
class DatabaseSettings(BaseSettings):
    """PostgreSQL database configuration."""
    
    host: str = Field(default="localhost")
    port: int = Field(default=5432, ge=1, le=65535)
    name: str = Field(default="wmata_etl", alias="db")
    user: str = Field(default="postgres")
    password: str = Field(default="")
    
    # Alternative: full connection string
    url: Optional[str] = Field(default=None, alias="database_url")
    
    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        extra="ignore"
    )
    
    @property
    def connection_string(self) -> str:
        """Generate SQLAlchemy connection string."""
        if self.url:
            return self.url
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
```

**Key Features**:

| Feature | Code | Meaning |
|---------|------|---------|
| Default values | `default="localhost"` | Use if not provided |
| Alias | `alias="db"` | Can use `POSTGRES_DB` or `POSTGRES_NAME` |
| Computed property | `connection_string` | Build URL from parts |
| Optional override | `url: Optional[str]` | Use full URL if provided |

**Environment Variables**:
```bash
# Option 1: Individual settings
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=wmata_etl
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secret

# Option 2: Full URL (overrides individual settings)
DATABASE_URL=postgresql://postgres:secret@localhost:5432/wmata_etl
```

---

### 3. PipelineSettings

**Location**: Lines 57-81

```python
class PipelineSettings(BaseSettings):
    """Pipeline execution configuration."""
    
    poll_interval_minutes: int = Field(default=5, ge=1, le=60)
    batch_size: int = Field(default=1000, ge=1, le=10000)
    log_level: str = Field(default="INFO")
    environment: str = Field(default="development")
    
    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        allowed = {"development", "staging", "production"}
        if v.lower() not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v.lower()
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()
```

**Custom Validators**:
- `validate_environment` — Only allows "development", "staging", "production"
- `validate_log_level` — Only allows valid Python log levels

**What Happens on Invalid Input**:
```python
# If you set ENVIRONMENT=invalid
# Pydantic raises:
# ValidationError: environment must be one of {'development', 'staging', 'production'}
```

---

### 4. Main Settings Class

**Location**: Lines 84-103

```python
class Settings(BaseSettings):
    """Main settings class that aggregates all configuration."""
    
    wmata: WMATASettings = Field(default_factory=WMATASettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
```

**Key Features**:
- Aggregates all settings into one object
- Automatically loads from `.env` file
- Nested access: `settings.wmata.api_key`

---

## Caching with lru_cache

**Location**: Lines 106-114

```python
@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
```

**Why Cache?**
- Reading `.env` file is I/O
- Parsing and validating takes CPU
- Settings don't change during runtime
- Cache = read once, use many times

**To Reload Settings** (e.g., in tests):
```python
get_settings.cache_clear()
```

---

## Convenience Functions

**Location**: Lines 117-131

```python
def get_database_url() -> str:
    """Get database connection string."""
    return get_settings().database.connection_string

def get_wmata_api_key() -> str:
    """Get WMATA API key."""
    return get_settings().wmata.api_key

def is_production() -> bool:
    """Check if running in production environment."""
    return get_settings().pipeline.environment == "production"
```

**Usage**:
```python
from config.settings import get_database_url, is_production

db_url = get_database_url()
if is_production():
    # Use production settings
```

---

## How It Works Together

```
.env file
    │
    ▼
Settings() loads env vars
    │
    ├── WMATASettings (WMATA_* vars)
    ├── DatabaseSettings (POSTGRES_* vars)
    └── PipelineSettings (other vars)
    │
    ▼
Validation runs
    │
    ├── Type checking (str, int, etc.)
    ├── Range checking (ge=1, le=120)
    └── Custom validators (environment, log_level)
    │
    ▼
Settings object ready to use
    │
    ▼
get_settings() caches and returns
```

---

## The .env File

```bash
# .env

# WMATA API Configuration
WMATA_API_KEY=your_actual_api_key_here
WMATA_TIMEOUT=30
WMATA_MAX_RETRIES=3

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=wmata_etl
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Pipeline Configuration
ENVIRONMENT=development
LOG_LEVEL=INFO
```

---

## Error Messages

Pydantic gives clear error messages:

```python
# Missing required field
ValidationError: 1 validation error for WMATASettings
api_key
  Field required [type=missing]

# Invalid type
ValidationError: 1 validation error for DatabaseSettings
port
  Input should be a valid integer [type=int_parsing]

# Out of range
ValidationError: 1 validation error for WMATASettings
timeout
  Input should be less than or equal to 120 [type=less_than_equal]

# Invalid environment
ValidationError: 1 validation error for PipelineSettings
environment
  Value error, environment must be one of {'development', 'staging', 'production'}
```

---

## Interview Talking Points

> "I use Pydantic Settings for type-safe configuration. It validates settings at startup, so configuration errors are caught immediately rather than causing runtime failures."

> "The settings are cached with `lru_cache` to avoid re-reading the .env file on every access. This improves performance and ensures consistency."

> "I use environment prefixes like `WMATA_` and `POSTGRES_` to namespace configuration. This prevents conflicts and makes it clear which settings belong to which component."

> "Custom validators ensure only valid values are accepted. For example, the environment must be 'development', 'staging', or 'production' — anything else raises a clear error."

---

## Key Takeaways

| Concept | Implementation |
|---------|----------------|
| **Type Safety** | Pydantic validates types automatically |
| **Validation** | `ge=`, `le=`, custom validators |
| **Env Loading** | `env_prefix`, `env_file=".env"` |
| **Defaults** | `Field(default=...)` |
| **Caching** | `@lru_cache()` on `get_settings()` |
| **Nested Config** | `settings.wmata.api_key` |
| **Computed Properties** | `connection_string` property |

---

*Next: Part 8 — Docker Setup*
