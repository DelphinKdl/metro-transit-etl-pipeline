"""
WMATA ETL Pipeline.

A production-grade ETL pipeline for WMATA rail predictions.
"""

from src.clients import WMATAClient
from src.core import (
    aggregate_station_metrics,
    run_quality_checks,
    transform_predictions,
    upsert_to_postgres,
)
from src.models import TrainPrediction

__version__ = "1.0.0"

__all__ = [
    "WMATAClient",
    "TrainPrediction",
    "transform_predictions",
    "aggregate_station_metrics",
    "run_quality_checks",
    "upsert_to_postgres",
]
