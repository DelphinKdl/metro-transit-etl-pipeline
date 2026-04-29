"""
PostgreSQL Loader Module.

Handles upserting validated data into PostgreSQL with idempotent operations.
Uses connection pooling and transaction management for reliability.
"""

import json
import os
import time
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DatabaseLoader:
    """
    Handles PostgreSQL database operations.

    Features:
        - Connection pooling
        - Automatic transaction management
        - Idempotent upserts
        - Structured logging
    """

    def __init__(self, connection_string: str):
        """
        Initialize database loader.

        Args:
            connection_string: PostgreSQL connection string.
        """
        self.engine: Engine = create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        self.Session = sessionmaker(bind=self.engine)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_station_metrics(self, aggregates: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Upsert station metrics into PostgreSQL.

        Uses ON CONFLICT for idempotent writes. Safe to retry.

        Args:
            aggregates: List of station aggregate dictionaries.

        Returns:
            Dictionary with operation statistics.
        """
        if not aggregates:
            return {"rows_affected": 0, "execution_time_ms": 0}

        start_time = time.time()

        upsert_sql = text("""
            INSERT INTO gold.station_wait_times (
                station_code, line, station_name, avg_wait_minutes,
                min_wait_minutes, max_wait_minutes, train_count, calculated_at
            ) VALUES (
                :station_code, :line, :station_name, :avg_wait_minutes,
                :min_wait_minutes, :max_wait_minutes, :train_count, :calculated_at
            )
            ON CONFLICT (station_code, line, calculated_at)
            DO UPDATE SET
                station_name = EXCLUDED.station_name,
                avg_wait_minutes = EXCLUDED.avg_wait_minutes,
                min_wait_minutes = EXCLUDED.min_wait_minutes,
                max_wait_minutes = EXCLUDED.max_wait_minutes,
                train_count = EXCLUDED.train_count
        """)

        params = [
            {
                "station_code": agg["station_code"],
                "line": agg["line"],
                "station_name": agg.get("station_name", ""),
                "avg_wait_minutes": agg["avg_wait_minutes"],
                "min_wait_minutes": int(agg["min_wait_minutes"]),
                "max_wait_minutes": int(agg["max_wait_minutes"]),
                "train_count": agg["train_count"],
                "calculated_at": agg.get("calculated_at", datetime.now(UTC)),
            }
            for agg in aggregates
        ]

        with self.get_session() as session:
            for batch in self._batch(params):
                session.execute(upsert_sql, batch)

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(
            "station_metrics_upserted",
            rows=len(aggregates),
            execution_time_ms=round(execution_time_ms, 2),
        )

        return {
            "rows_affected": len(aggregates),
            "execution_time_ms": round(execution_time_ms, 2),
        }

    def insert_cleaned_predictions(
        self,
        cleaned_df: Any,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Persist cleaned predictions to Silver layer.

        Args:
            cleaned_df: pandas DataFrame of cleaned predictions.
            run_id: Pipeline run identifier for lineage.

        Returns:
            Dictionary with operation statistics.
        """
        import pandas as pd

        if cleaned_df is None or (hasattr(cleaned_df, "empty") and cleaned_df.empty):
            return {"rows_affected": 0, "execution_time_ms": 0}

        start_time = time.time()

        insert_sql = text("""
            INSERT INTO silver.cleaned_predictions (
                station_code, destination_code, destination_name,
                line, line_name, station_name,
                minutes_to_arrival, car_count,
                is_arriving, is_boarding,
                extracted_at
            ) VALUES (
                :station_code, :destination_code, :destination_name,
                :line, :line_name, :station_name,
                :minutes_to_arrival, :car_count,
                :is_arriving, :is_boarding,
                :extracted_at
            )
        """)

        from src.core.transformer import LINE_CODES

        rows = []
        for _, row in cleaned_df.iterrows():
            mins = row.get("minutes_to_arrival")
            rows.append(
                {
                    "station_code": row.get("station_code"),
                    "destination_code": row.get("destination_code"),
                    "destination_name": row.get("destination_name", ""),
                    "line": row.get("line"),
                    "line_name": LINE_CODES.get(row.get("line"), row.get("line")),
                    "station_name": row.get("station_name", ""),
                    "minutes_to_arrival": int(mins) if pd.notna(mins) else None,
                    "car_count": int(row["car_count"]) if pd.notna(row.get("car_count")) else None,
                    "is_arriving": mins == 0 if pd.notna(mins) else False,
                    "is_boarding": (
                        str(row.get("minutes_to_arrival_raw", "")).upper() == "BRD"
                        if "minutes_to_arrival_raw" in row.index
                        else False
                    ),
                    "extracted_at": row.get("extracted_at"),
                }
            )

        with self.get_session() as session:
            for batch in self._batch(rows):
                session.execute(insert_sql, batch)

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(
            "cleaned_predictions_inserted",
            rows=len(rows),
            execution_time_ms=round(execution_time_ms, 2),
        )

        return {
            "rows_affected": len(rows),
            "execution_time_ms": round(execution_time_ms, 2),
        }

    def record_pipeline_run(
        self,
        run_id: str,
        records_extracted: int = 0,
    ) -> None:
        """
        Record the start of a pipeline run in gold.pipeline_runs.

        Args:
            run_id: Unique pipeline run identifier.
            records_extracted: Number of records extracted.
        """
        sql = text("""
            INSERT INTO gold.pipeline_runs (
                run_id, started_at, status, records_extracted
            ) VALUES (
                :run_id, :started_at, 'running', :records_extracted
            )
            ON CONFLICT (run_id) DO NOTHING
        """)

        with self.get_session() as session:
            session.execute(
                sql,
                {
                    "run_id": run_id,
                    "started_at": datetime.now(UTC),
                    "records_extracted": records_extracted,
                },
            )

        logger.info("pipeline_run_started", run_id=run_id)

    def update_pipeline_run(
        self,
        run_id: str,
        status: str = "success",
        records_cleaned: int = 0,
        records_loaded: int = 0,
        error_message: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Update a pipeline run record on completion.

        Args:
            run_id: Pipeline run identifier.
            status: Final status (success, failed, warning).
            records_cleaned: Number of records after cleaning.
            records_loaded: Number of records loaded to gold.
            error_message: Error message if failed.
            metadata: Additional metadata as JSON.
        """
        sql = text("""
            UPDATE gold.pipeline_runs SET
                completed_at = :completed_at,
                status = :status,
                records_cleaned = :records_cleaned,
                records_loaded = :records_loaded,
                error_message = :error_message,
                metadata = :metadata
            WHERE run_id = :run_id
        """)

        with self.get_session() as session:
            session.execute(
                sql,
                {
                    "run_id": run_id,
                    "completed_at": datetime.now(UTC),
                    "status": status,
                    "records_cleaned": records_cleaned,
                    "records_loaded": records_loaded,
                    "error_message": error_message,
                    "metadata": json.dumps(metadata) if metadata else None,
                },
            )

        logger.info(
            "pipeline_run_updated",
            run_id=run_id,
            status=status,
        )

    def upsert_raw_predictions(self, predictions: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Insert raw predictions for auditing/replay.

        Args:
            predictions: List of raw prediction dictionaries.

        Returns:
            Dictionary with operation statistics.
        """
        if not predictions:
            return {"rows_affected": 0, "execution_time_ms": 0}

        start_time = time.time()

        insert_sql = text("""
            INSERT INTO bronze.raw_predictions (
                station_code, destination_code, line,
                minutes_to_arrival, car_count, extracted_at, raw_json
            ) VALUES (
                :station_code, :destination_code, :line,
                :minutes_to_arrival, :car_count, :extracted_at, :raw_json
            )
        """)

        params = [
            {
                "station_code": pred.get("station_code"),
                "destination_code": pred.get("destination_code"),
                "line": pred.get("line"),
                "minutes_to_arrival": pred.get("minutes_to_arrival"),
                "car_count": pred.get("car_count"),
                "extracted_at": pred.get("extracted_at"),
                "raw_json": json.dumps(pred),
            }
            for pred in predictions
        ]

        with self.get_session() as session:
            for batch in self._batch(params):
                session.execute(insert_sql, batch)

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(
            "raw_predictions_inserted",
            rows=len(predictions),
            execution_time_ms=round(execution_time_ms, 2),
        )

        return {
            "rows_affected": len(predictions),
            "execution_time_ms": round(execution_time_ms, 2),
        }

    @staticmethod
    def _batch(params: list[dict], size: int = 500) -> Generator:
        """Yield successive batches for bulk execution."""
        for i in range(0, len(params), size):
            chunk = params[i : i + size]
            yield chunk


# Module-level singleton
_loader: DatabaseLoader | None = None


def get_loader(connection_string: str | None = None) -> DatabaseLoader:
    """
    Get or create a database loader instance.

    Uses singleton pattern to reuse connections. If a different
    connection_string is provided after initial creation, the
    singleton is replaced with a new instance.

    Args:
        connection_string: Optional connection string override.

    Returns:
        DatabaseLoader instance.
    """
    global _loader

    conn_str = connection_string or os.getenv("DATABASE_URL")

    if conn_str and _loader is not None:
        current_url = str(_loader.engine.url)
        if current_url != conn_str:
            logger.warning(
                "replacing_database_loader",
                reason="connection_string changed",
            )
            _loader.engine.dispose()
            _loader = DatabaseLoader(conn_str)
            return _loader

    if _loader is None:
        if not conn_str:
            raise ValueError(
                "DATABASE_URL environment variable required. "
                "Set it or pass connection_string parameter."
            )
        _loader = DatabaseLoader(conn_str)

    return _loader


def upsert_to_postgres(aggregates: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Convenience function to upsert aggregates to PostgreSQL.

    Args:
        aggregates: List of station aggregate dictionaries.

    Returns:
        Dictionary with operation statistics.
    """
    loader = get_loader()
    return loader.upsert_station_metrics(aggregates)
