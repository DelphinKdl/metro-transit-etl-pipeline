# Part 8: Docker Setup

## What is Docker?

Docker packages applications with their dependencies into **containers**. This ensures:
- Same environment everywhere (dev, staging, prod)
- No "works on my machine" problems
- Easy setup for new developers
- Isolated services

**Files**:
- `docker/docker-compose.yml` — Defines all services
- `Makefile` — Easy commands

---

## The Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose                          │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  PostgreSQL │  │   Airflow   │  │      Airflow        │  │
│  │             │  │  Webserver  │  │     Scheduler       │  │
│  │  Port 5432  │  │  Port 8080  │  │   (runs DAGs)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
│         │                │                   │              │
│         └────────────────┴───────────────────┘              │
│                          │                                  │
│                   Shared Network                            │
└─────────────────────────────────────────────────────────────┘
```

---

## docker-compose.yml Breakdown

### 1. YAML Anchors (DRY Principle)

**Location**: Lines 3-31

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:2.9.0-python3.11
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    ...
```

**What are anchors?**
- `&airflow-common` — Define a reusable block
- `<<: *airflow-common` — Include that block

This avoids repeating the same config for webserver and scheduler.

---

### 2. PostgreSQL Service

**Location**: Lines 35-52

```yaml
postgres:
  image: postgres:15
  environment:
    POSTGRES_USER: postgres
    POSTGRES_PASSWORD: postgres
    POSTGRES_DB: airflow
  volumes:
    - postgres-db-volume:/var/lib/postgresql/data
    - ./03_load/sql/init-db.sql:/docker-entrypoint-initdb.d/01-init-db.sql
    - ./03_load/sql/schema.sql:/docker-entrypoint-initdb.d/02-schema.sql
  healthcheck:
    test: ["CMD", "pg_isready", "-U", "postgres"]
  ports:
    - "5432:5432"
```

| Setting | Purpose |
|---------|---------|
| `image: postgres:15` | Use PostgreSQL 15 |
| `volumes: postgres-db-volume` | Persist data between restarts |
| `volumes: ./03_load/sql/...` | Auto-run SQL on first start |
| `healthcheck` | Other services wait until DB is ready |
| `ports: "5432:5432"` | Expose to host machine |

**Auto-initialization**:
Files in `/docker-entrypoint-initdb.d/` run automatically on first start:
1. `01-init-db.sql` — Creates `wmata_etl` database
2. `02-schema.sql` — Creates tables

---

### 3. Airflow Webserver

**Location**: Lines 54-70

```yaml
airflow-webserver:
  <<: *airflow-common
  command: webserver
  ports:
    - "8080:8080"
  healthcheck:
    test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
  depends_on:
    postgres:
      condition: service_healthy
    airflow-init:
      condition: service_completed_successfully
```

| Setting | Purpose |
|---------|---------|
| `<<: *airflow-common` | Include common config |
| `command: webserver` | Run Airflow webserver |
| `ports: "8080:8080"` | UI at http://localhost:8080 |
| `depends_on` | Wait for DB and init to complete |

---

### 4. Airflow Scheduler

**Location**: Lines 72-86

```yaml
airflow-scheduler:
  <<: *airflow-common
  command: scheduler
  depends_on:
    postgres:
      condition: service_healthy
    airflow-init:
      condition: service_completed_successfully
```

The scheduler:
- Reads DAG files
- Triggers tasks on schedule
- Manages task execution

---

### 5. Airflow Init

**Location**: Lines 88-106

```yaml
airflow-init:
  <<: *airflow-common
  entrypoint: /bin/bash
  command:
    - -c
    - |
      mkdir -p /sources/logs /sources/dags /sources/plugins
      chown -R "${AIRFLOW_UID:-50000}:0" /sources/{logs,dags,plugins}
      exec /entrypoint airflow version
  environment:
    <<: *airflow-common-env
    _AIRFLOW_DB_MIGRATE: 'true'
    _AIRFLOW_WWW_USER_CREATE: 'true'
    _AIRFLOW_WWW_USER_USERNAME: airflow
    _AIRFLOW_WWW_USER_PASSWORD: airflow
```

This runs **once** on first start:
- Creates directories
- Sets permissions
- Initializes Airflow database
- Creates admin user (airflow/airflow)

---

### 6. Volume Mounts

```yaml
volumes:
  - ./04_orchestration:/opt/airflow/dags
  - ./01_extract:/opt/airflow/dags/extract
  - ./02_transform_qc:/opt/airflow/dags/transform_qc
  - ./03_load:/opt/airflow/dags/load
  - ./logs:/opt/airflow/logs
```

| Host Path | Container Path | Purpose |
|-----------|----------------|---------|
| `./04_orchestration` | `/opt/airflow/dags` | DAG files |
| `./01_extract` | `/opt/airflow/dags/extract` | Extract module |
| `./02_transform_qc` | `/opt/airflow/dags/transform_qc` | Transform module |
| `./03_load` | `/opt/airflow/dags/load` | Load module |
| `./logs` | `/opt/airflow/logs` | Task logs |

**Why mount code?** Changes to your code are immediately visible in the container without rebuilding.

---

## The Makefile

### Available Commands

```bash
make help      # Show all commands
make init      # First-time setup
make up        # Start services
make down      # Stop services
make logs      # View logs
make shell     # Open container shell
make psql      # Connect to PostgreSQL
make test      # Run tests
make clean     # Remove everything
```

### Command Details

#### `make init`
```makefile
init:
	mkdir -p logs plugins
	echo "AIRFLOW_UID=$$(id -u)" > .env
	docker compose up airflow-init
```
- Creates directories
- Sets your user ID (for file permissions)
- Runs Airflow initialization

#### `make up`
```makefile
up:
	docker compose up -d
```
- Starts all services in background (`-d` = detached)

#### `make psql`
```makefile
psql:
	docker compose exec postgres psql -U postgres -d wmata_etl
```
- Opens PostgreSQL shell in the container
- Connects to `wmata_etl` database

#### `make trigger-dag`
```makefile
trigger-dag:
	docker compose exec airflow-scheduler airflow dags trigger wmata_rail_predictions_etl
```
- Manually triggers the DAG (useful for testing)

---

## Environment Variables

The `.env` file is loaded by Docker Compose:

```bash
# .env
AIRFLOW_UID=501                    # Your user ID (set by make init)
WMATA_API_KEY=your_key_here        # WMATA API key
```

These are passed to containers via:
```yaml
environment:
  WMATA_API_KEY: ${WMATA_API_KEY:-your_api_key_here}
```

The `:-` syntax means: use `WMATA_API_KEY` if set, otherwise use `your_api_key_here`.

---

## Startup Sequence

```
1. make init
   └── Creates directories
   └── Sets AIRFLOW_UID
   └── Runs airflow-init container
       └── Migrates Airflow database
       └── Creates admin user

2. make up
   └── Starts postgres
       └── Waits for healthcheck
       └── Runs init-db.sql
       └── Runs schema.sql
   └── Starts airflow-webserver
       └── Waits for postgres
       └── Starts web UI on :8080
   └── Starts airflow-scheduler
       └── Waits for postgres
       └── Starts scheduling DAGs
```

---

## Accessing Services

| Service | URL/Command | Credentials |
|---------|-------------|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| PostgreSQL | `make psql` | postgres / postgres |
| Logs | `make logs` | — |
| Shell | `make shell` | — |

---

## Common Issues

### Permission Errors
```bash
# Fix: Set correct UID
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### Port Already in Use
```bash
# Check what's using port 8080
lsof -i :8080

# Or change port in docker-compose.yml
ports:
  - "8081:8080"  # Use 8081 instead
```

### Container Won't Start
```bash
# View logs
docker compose logs postgres
docker compose logs airflow-webserver

# Restart fresh
make clean
make init
make up
```

---

## Interview Talking Points

> "I use Docker Compose to run Airflow and PostgreSQL together. This ensures consistent environments and makes it easy for anyone to run the pipeline with just `make up`."

> "The Makefile provides a simple interface — developers don't need to remember Docker commands. `make init` sets up everything, `make up` starts services."

> "I mount code directories as volumes so changes are reflected immediately without rebuilding containers. This speeds up development."

> "The PostgreSQL container auto-initializes the schema on first start using SQL files in `docker-entrypoint-initdb.d/`. No manual setup required."

---

## Key Takeaways

| Concept | Implementation |
|---------|----------------|
| **Multi-service** | Docker Compose orchestrates 4 containers |
| **Auto-init** | SQL files run on first PostgreSQL start |
| **Health checks** | Services wait for dependencies |
| **Volume mounts** | Code changes without rebuild |
| **Easy commands** | Makefile wraps Docker commands |
| **Credentials** | Airflow UI: airflow/airflow |

---

## Ready to Run!

You now understand all the components. Next step: **Start the pipeline!**

```bash
# 1. Initialize (first time only)
make init

# 2. Start services
make up

# 3. Open Airflow UI
open http://localhost:8080
```

---

*Next: Part 9 — Running and Testing the Pipeline*
