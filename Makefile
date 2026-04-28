# WMATA ETL Pipeline - Makefile
# Professional commands for development and deployment

.PHONY: help install dev init up down logs shell psql test lint format clean dashboard

# Default target
help:
	@echo "WMATA ETL Pipeline"
	@echo "=================="
	@echo ""
	@echo "Development:"
	@echo "  make install    - Install dependencies"
	@echo "  make dev        - Install with dev dependencies"
	@echo "  make test       - Run tests"
	@echo "  make lint       - Run linters (ruff, black, mypy)"
	@echo "  make format     - Format code with black"
	@echo ""
	@echo "Docker:"
	@echo "  make init       - Initialize Airflow (first time)"
	@echo "  make up         - Start all services"
	@echo "  make down       - Stop all services"
	@echo "  make logs       - View logs"
	@echo "  make shell      - Open container shell"
	@echo "  make psql       - Connect to PostgreSQL"
	@echo "  make clean      - Remove containers and volumes"
	@echo ""
	@echo "Pipeline:"
	@echo "  make run        - Run pipeline locally"
	@echo "  make trigger    - Trigger DAG manually"
	@echo ""
	@echo "Dashboard:"
	@echo "  make dashboard  - Start Streamlit dashboard"
	@echo ""
	@echo "URLs:"
	@echo "  Airflow UI:     http://localhost:8080"
	@echo "  Dashboard:      http://localhost:8501"
	@echo "  PostgreSQL:     localhost:5432"

# Install dependencies
install:
	pip install -e .

# Install with dev dependencies
dev:
	pip install -e ".[dev]"
	pre-commit install

# Run pipeline locally (without Airflow)
run:
	python -m src.main

# Run tests
test:
	pytest tests/ -v

# Run tests with coverage
test-cov:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Lint code
lint:
	ruff check src/ tests/
	black --check src/ tests/
	mypy src/ --ignore-missing-imports

# Format code
format:
	black src/ tests/
	ruff check --fix src/ tests/

# Initialize Airflow (run once)
init:
	@echo "Creating directories..."
	mkdir -p logs plugins
	@echo "Checking .env file..."
	@if [ ! -f .env ]; then \
		echo "ERROR: .env file not found!"; \
		echo "Copy .env.example to .env and fill in your values:"; \
		echo "  cp .env.example .env"; \
		exit 1; \
	fi
	@echo "Adding AIRFLOW_UID to .env..."
	@grep -q "^AIRFLOW_UID=" .env || echo "AIRFLOW_UID=$$(id -u)" >> .env
	@echo "Initializing Airflow..."
	cd docker && docker compose --env-file ../.env up airflow-init
	@echo ""
	@echo "✅ Initialization complete!"
	@echo "Run 'make up' to start services"

# Start all services
up:
	cd docker && docker compose --env-file ../.env up -d
	@echo ""
	@echo "✅ Services starting..."
	@echo "Airflow UI:  http://localhost:8080  (airflow / airflow)"
	@echo "Dashboard:   http://localhost:8501"

# Stop all services
down:
	cd docker && docker compose --env-file ../.env down

# View logs
logs:
	cd docker && docker compose --env-file ../.env logs -f airflow-scheduler airflow-webserver

# Open shell in Airflow container
shell:
	cd docker && docker compose --env-file ../.env exec airflow-scheduler bash

# Connect to PostgreSQL
psql:
	cd docker && docker compose --env-file ../.env exec postgres psql -U $${POSTGRES_USER:-postgres} -d wmata_etl

# Clean up everything
clean:
	cd docker && docker compose --env-file ../.env down -v --remove-orphans
	rm -rf logs/*.log
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	@echo "✅ Cleaned up"

# Trigger DAG manually
trigger:
	cd docker && docker compose --env-file ../.env exec airflow-scheduler airflow dags trigger wmata_rail_predictions_etl

# List DAGs
list-dags:
	cd docker && docker compose --env-file ../.env exec airflow-scheduler airflow dags list

# Check DAG status
dag-status:
	cd docker && docker compose --env-file ../.env exec airflow-scheduler airflow dags list-runs -d wmata_rail_predictions_etl

# ---- Dashboard commands ----

# Start Streamlit dashboard
dashboard:
	cd docker && docker compose --env-file ../.env up -d dashboard
	@echo "Dashboard: http://localhost:8501"
