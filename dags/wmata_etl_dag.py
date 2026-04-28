"""
WMATA Rail Predictions ETL DAG.

Orchestrates the extraction, transformation, and loading of real-time
rail predictions from WMATA API to PostgreSQL every 5 minutes.

This DAG is intentionally thin - all business logic lives in src/.
"""

import sys
import os
import json
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.clients.wmata_client import WMATAClient
from src.core.transformer import transform_predictions, aggregate_station_metrics, enrich_with_metadata
from src.core.quality_checks import run_quality_checks
from src.core.loader import upsert_to_postgres, get_loader


default_args = {
    "owner": "Delphin-K",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=4),
}


@dag(
    dag_id="wmata_rail_predictions_etl",
    default_args=default_args,
    description="ETL pipeline for WMATA rail predictions",
    schedule_interval="*/5 5-23 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="US/Eastern"),
    catchup=False,
    max_active_runs=1,
    tags=["wmata", "etl", "rail", "predictions"],
)
def wmata_rail_predictions_etl():
    """
    WMATA Rail Predictions ETL Pipeline.
    
    Tasks:
        1. extract_predictions - Fetch from WMATA API
        2. transform_data - Clean and aggregate
        3. quality_check - Validate data quality
        4. load_to_database - Upsert to PostgreSQL
    """
    
    @task()
    def extract_predictions() -> Dict[str, Any]:
        """Extract predictions from WMATA API (Bronze layer)."""
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        api_key = Variable.get("WMATA_API_KEY")
        client = WMATAClient(api_key=api_key)
        predictions = client.get_predictions("All")
        
        # Convert to dicts for XCom serialization
        raw_data = [p.to_dict() for p in predictions]
        
        # Save sample to file for debugging
        sample_path = "/opt/airflow/logs/last_extract_sample.json"
        with open(sample_path, "w") as f:
            json.dump(raw_data[:5], f, indent=2, default=str)
        
        # Load raw data to Bronze layer
        loader = get_loader()
        loader.upsert_raw_predictions(raw_data)
        
        # Record pipeline run start
        loader.record_pipeline_run(run_id, records_extracted=len(raw_data))
        
        return {"raw_data": raw_data, "run_id": run_id}
    
    @task()
    def transform_data(extract_result: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw predictions and persist to Silver layer."""
        raw_predictions = extract_result["raw_data"]
        run_id = extract_result["run_id"]
        
        cleaned = transform_predictions(raw_predictions)
        
        # Persist cleaned data to Silver layer
        loader = get_loader()
        silver_result = loader.insert_cleaned_predictions(cleaned, run_id=run_id)
        
        aggregates = aggregate_station_metrics(cleaned)
        
        # Enrich with run_id for lineage tracking
        aggregates = enrich_with_metadata(aggregates, run_id=run_id)
        
        # Convert Timestamps to ISO strings for XCom JSON serialization
        for agg in aggregates:
            for key, val in agg.items():
                if hasattr(val, 'isoformat'):
                    agg[key] = val.isoformat()
        
        return {
            "cleaned_count": len(cleaned),
            "silver_rows": silver_result["rows_affected"],
            "aggregate_count": len(aggregates),
            "aggregates": aggregates,
            "run_id": run_id,
        }
    
    @task()
    def quality_check(transform_result: Dict[str, Any]) -> Dict[str, Any]:
        """Run quality checks on transformed data."""
        qc_results = run_quality_checks(transform_result["aggregates"])
        
        if not qc_results["passed"]:
            # Record failure in pipeline_runs
            loader = get_loader()
            loader.update_pipeline_run(
                run_id=transform_result["run_id"],
                status="failed",
                records_cleaned=transform_result["cleaned_count"],
                error_message=str(qc_results["failures"]),
            )
            raise ValueError(f"Quality checks failed: {qc_results['failures']}")
        
        return {
            **transform_result,
            "qc_results": qc_results,
        }
    
    @task()
    def load_to_database(validated_data: Dict[str, Any]) -> Dict[str, Any]:
        """Load validated data to Gold layer in PostgreSQL."""
        result = upsert_to_postgres(validated_data["aggregates"])
        
        # Record successful pipeline completion
        loader = get_loader()
        loader.update_pipeline_run(
            run_id=validated_data["run_id"],
            status="success",
            records_cleaned=validated_data["cleaned_count"],
            records_loaded=result["rows_affected"],
            metadata={
                "silver_rows": validated_data["silver_rows"],
                "aggregate_count": validated_data["aggregate_count"],
                "qc_checks_passed": validated_data["qc_results"]["total_checks"],
                "execution_time_ms": result["execution_time_ms"],
            },
        )
        
        return {
            "run_id": validated_data["run_id"],
            "rows_upserted": result["rows_affected"],
            "execution_time_ms": result["execution_time_ms"],
        }
    
    # Define task dependencies (DAG structure)
    # Bronze: extract -> Silver: transform -> Quality Gate -> Gold: load
    extract_result = extract_predictions()
    transformed = transform_data(extract_result)
    validated = quality_check(transformed)
    load_result = load_to_database(validated)


# Instantiate the DAG
dag = wmata_rail_predictions_etl()
