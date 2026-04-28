"""
Integration test: end-to-end pipeline flow.

Requires a running PostgreSQL instance (e.g. the Docker Compose stack).
Run with:  pytest tests/integration/ -v -m integration

Skip automatically when the database is unreachable.
"""

import os
import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _db_url() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql://airflow:airflow@localhost:5432/wmata_etl",
    )


def _engine():
    return create_engine(_db_url(), pool_pre_ping=True)


def _db_is_reachable() -> bool:
    try:
        engine = _engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _db_is_reachable(),
    reason="PostgreSQL not reachable – skipping integration tests",
)


@pytest.fixture(scope="module")
def engine():
    return _engine()


@pytest.fixture(scope="module")
def run_id():
    return datetime.now(timezone.utc).strftime("test_%Y%m%d_%H%M%S")


# ---------------------------------------------------------------------------
# Sample data (mirrors what the WMATA API returns after .to_dict())
# ---------------------------------------------------------------------------

SAMPLE_RAW = [
    {
        "station_code": "A01",
        "destination_code": "B08",
        "destination_name": "Shady Grove",
        "line": "RD",
        "station_name": "Metro Center",
        "minutes_to_arrival": "3",
        "car_count": "8",
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    },
    {
        "station_code": "A01",
        "destination_code": "A15",
        "destination_name": "Glenmont",
        "line": "RD",
        "station_name": "Metro Center",
        "minutes_to_arrival": "7",
        "car_count": "6",
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    },
    {
        "station_code": "C01",
        "destination_code": "J03",
        "destination_name": "Franconia-Springfield",
        "line": "BL",
        "station_name": "Metro Center",
        "minutes_to_arrival": "5",
        "car_count": "8",
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    },
    # Invalid record – should be dropped by transformer
    {
        "station_code": "",
        "destination_code": "",
        "destination_name": "",
        "line": "No",
        "station_name": "",
        "minutes_to_arrival": "",
        "car_count": "",
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    },
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """Walk through Bronze → Silver → Quality → Gold in sequence."""

    def test_01_bronze_insert(self, engine, run_id):
        """Raw predictions land in bronze.raw_predictions."""
        from src.core.loader import DatabaseLoader

        loader = DatabaseLoader(str(engine.url))
        result = loader.upsert_raw_predictions(SAMPLE_RAW)

        assert result["rows_affected"] == len(SAMPLE_RAW)

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM bronze.raw_predictions")
            ).fetchone()
            assert row.cnt > 0

    def test_02_transform(self):
        """Transformer cleans and drops invalid rows."""
        from src.core.transformer import transform_predictions

        cleaned = transform_predictions(SAMPLE_RAW)

        # The invalid record (line='No', empty station) should be dropped
        assert len(cleaned) == 3
        assert set(cleaned["line"].unique()) == {"RD", "BL"}

    def test_03_silver_insert(self, engine, run_id):
        """Cleaned predictions persist to silver.cleaned_predictions."""
        from src.core.transformer import transform_predictions
        from src.core.loader import DatabaseLoader

        cleaned = transform_predictions(SAMPLE_RAW)
        loader = DatabaseLoader(str(engine.url))
        result = loader.insert_cleaned_predictions(cleaned, run_id=run_id)

        assert result["rows_affected"] == 3

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM silver.cleaned_predictions")
            ).fetchone()
            assert row.cnt > 0

    def test_04_quality_checks(self):
        """Quality checks pass on valid aggregated data."""
        from src.core.transformer import (
            transform_predictions,
            aggregate_station_metrics,
        )
        from src.core.quality_checks import run_quality_checks

        cleaned = transform_predictions(SAMPLE_RAW)
        aggregates = aggregate_station_metrics(cleaned)
        qc = run_quality_checks(aggregates)

        assert qc["passed"] is True
        assert qc["failed_checks"] == 0

    def test_05_gold_insert(self, engine, run_id):
        """Aggregated metrics land in gold.station_wait_times."""
        from src.core.transformer import (
            transform_predictions,
            aggregate_station_metrics,
            enrich_with_metadata,
        )
        from src.core.loader import DatabaseLoader

        cleaned = transform_predictions(SAMPLE_RAW)
        aggregates = aggregate_station_metrics(cleaned)
        aggregates = enrich_with_metadata(aggregates, run_id=run_id)
        loader = DatabaseLoader(str(engine.url))
        result = loader.upsert_station_metrics(aggregates)

        assert result["rows_affected"] > 0

        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM gold.station_wait_times")
            ).fetchone()
            assert row.cnt > 0

    def test_06_pipeline_runs_tracking(self, engine, run_id):
        """Pipeline run observability rows are written."""
        from src.core.loader import DatabaseLoader

        loader = DatabaseLoader(str(engine.url))
        loader.record_pipeline_run(run_id, records_extracted=4)
        loader.update_pipeline_run(
            run_id=run_id,
            status="success",
            records_cleaned=3,
            records_loaded=2,
        )

        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT status, records_extracted, records_cleaned, records_loaded "
                    "FROM gold.pipeline_runs WHERE run_id = :rid"
                ),
                {"rid": run_id},
            ).fetchone()

        assert row is not None
        assert row.status == "success"
        assert row.records_extracted == 4
        assert row.records_cleaned == 3
        assert row.records_loaded == 2

    def test_07_data_lineage(self):
        """Enriched aggregates carry run_id and pipeline_version."""
        from src.core.transformer import (
            transform_predictions,
            aggregate_station_metrics,
            enrich_with_metadata,
        )

        cleaned = transform_predictions(SAMPLE_RAW)
        aggregates = aggregate_station_metrics(cleaned)
        enriched = enrich_with_metadata(aggregates, run_id="test_123")

        for row in enriched:
            assert row["run_id"] == "test_123"
            assert row["pipeline_version"] == "1.0.0"
