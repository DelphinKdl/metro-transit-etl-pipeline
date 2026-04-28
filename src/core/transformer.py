"""
Data Transformation Module.

Handles cleaning, validation, and aggregation of WMATA rail predictions.
"""

from datetime import UTC, datetime
from typing import Any

import pandas as pd

from src.utils.logger import get_logger

logger = get_logger(__name__)

# Line code mappings
LINE_CODES = {
    "RD": "Red",
    "BL": "Blue",
    "OR": "Orange",
    "SV": "Silver",
    "GR": "Green",
    "YL": "Yellow",
}

VALID_PASSENGER_LINES = set(LINE_CODES.keys())


def transform_predictions(raw_predictions: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Transform raw prediction dictionaries into a cleaned DataFrame.

    Performs:
        - Type coercion for numeric fields
        - Removal of invalid records (missing station_code or line)
        - Deduplication

    Args:
        raw_predictions: List of raw prediction dictionaries from API.

    Returns:
        Cleaned pandas DataFrame.
    """
    if not raw_predictions:
        logger.warning("empty_predictions_received")
        return pd.DataFrame()

    df = pd.DataFrame(raw_predictions)

    # Parse extracted_at if it's a string
    if "extracted_at" in df.columns and df["extracted_at"].dtype == "object":
        df["extracted_at"] = pd.to_datetime(df["extracted_at"])

    # Clean numeric fields
    if "minutes_to_arrival" in df.columns:
        df["minutes_to_arrival"] = pd.to_numeric(df["minutes_to_arrival"], errors="coerce")
    if "car_count" in df.columns:
        df["car_count"] = pd.to_numeric(df["car_count"], errors="coerce")

    # Remove invalid records
    initial_count = len(df)
    df = df[df["station_code"].notna() & (df["station_code"] != "")]
    df = df[df["line"].notna() & (df["line"] != "")]

    # Filter to valid passenger lines only (removes 'No', '--', etc.)
    df = df[df["line"].isin(VALID_PASSENGER_LINES)]

    # Deduplicate
    dedup_cols = [
        c
        for c in ["station_code", "line", "destination_code", "minutes_to_arrival", "extracted_at"]
        if c in df.columns
    ]
    df = df.drop_duplicates(subset=dedup_cols, keep="first")

    final_count = len(df)
    logger.info(
        "predictions_transformed",
        initial_count=initial_count,
        final_count=final_count,
        dropped=initial_count - final_count,
    )

    return df


def aggregate_station_metrics(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Aggregate predictions into station-level wait time metrics.

    Groups by station and line, calculating:
        - Average wait time
        - Min/max wait time
        - Train count

    Args:
        df: Cleaned predictions DataFrame.

    Returns:
        List of station aggregate dictionaries.
    """
    if df.empty:
        return []

    # Filter to only rows with valid wait times
    valid_df = df[df["minutes_to_arrival"].notna()].copy()

    if valid_df.empty:
        logger.warning("no_valid_wait_times")
        return []

    # Group by station and line
    aggregates = (
        valid_df.groupby(["station_code", "line"])
        .agg(
            station_name=("station_name", "first"),
            avg_wait_minutes=("minutes_to_arrival", "mean"),
            min_wait_minutes=("minutes_to_arrival", "min"),
            max_wait_minutes=("minutes_to_arrival", "max"),
            train_count=("minutes_to_arrival", "count"),
            extracted_at=("extracted_at", "max"),
        )
        .reset_index()
    )

    # Round average
    aggregates["avg_wait_minutes"] = aggregates["avg_wait_minutes"].round(2)

    # Add calculated_at timestamp
    aggregates["calculated_at"] = datetime.now(UTC)

    # Convert to list of dicts
    result = aggregates.to_dict("records")

    logger.info("station_metrics_aggregated", station_count=len(result))

    return list(result)


def enrich_with_metadata(
    aggregates: list[dict[str, Any]],
    run_id: str | None = None,
    pipeline_version: str = "1.0.0",
) -> list[dict[str, Any]]:
    """
    Enrich aggregates with pipeline metadata.

    Args:
        aggregates: List of station aggregate dictionaries.
        run_id: Optional pipeline run identifier.
        pipeline_version: Version string for the pipeline.

    Returns:
        Enriched aggregates with metadata fields.
    """
    generated_run_id = run_id or datetime.now(UTC).strftime("%Y%m%d%H%M%S")

    for agg in aggregates:
        agg["run_id"] = generated_run_id
        agg["pipeline_version"] = pipeline_version

    return aggregates


def get_line_name(code: str) -> str:
    """Get full line name from code."""
    return LINE_CODES.get(code, code)
