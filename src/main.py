"""
WMATA ETL Pipeline - Main Entry Point.

Run the ETL pipeline locally without Airflow.
"""

from datetime import UTC, datetime

from dotenv import load_dotenv

from src.clients.wmata_client import WMATAClient
from src.core.loader import upsert_to_postgres
from src.core.quality_checks import run_quality_checks
from src.core.transformer import aggregate_station_metrics, transform_predictions
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_pipeline():
    """
    Execute the full ETL pipeline.

    Steps:
    1. Extract predictions from WMATA API
    2. Transform into station aggregates
    3. Run quality checks
    4. Load to PostgreSQL
    """
    load_dotenv()

    run_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    logger.info("pipeline_started", run_id=run_id)

    try:
        # Step 1: Extract
        logger.info("step_extract_started")
        client = WMATAClient()
        predictions = client.get_predictions("All")

        # Convert to dicts for processing
        raw_data = [p.to_dict() for p in predictions]
        logger.info("step_extract_completed", record_count=len(raw_data))

        # Step 2: Transform
        logger.info("step_transform_started")
        df = transform_predictions(raw_data)
        aggregates = aggregate_station_metrics(df)
        logger.info("step_transform_completed", aggregate_count=len(aggregates))

        # Step 3: Quality Checks
        logger.info("step_qc_started")
        qc_result = run_quality_checks(aggregates)

        if not qc_result["passed"]:
            logger.error("step_qc_failed", failures=qc_result["failures"])
            raise ValueError(f"Quality checks failed: {qc_result['failures']}")

        logger.info("step_qc_completed", checks_passed=qc_result["passed_checks"])

        # Step 4: Load
        logger.info("step_load_started")
        load_result = upsert_to_postgres(aggregates)
        logger.info("step_load_completed", **load_result)

        logger.info(
            "pipeline_completed",
            run_id=run_id,
            records_extracted=len(raw_data),
            records_loaded=load_result["rows_affected"],
        )

        return {
            "success": True,
            "run_id": run_id,
            "records_extracted": len(raw_data),
            "records_loaded": load_result["rows_affected"],
        }

    except Exception as e:
        logger.error("pipeline_failed", run_id=run_id, error=str(e))
        raise


if __name__ == "__main__":
    result = run_pipeline()
    print("\nPipeline completed successfully!")
    print(f"Run ID: {result['run_id']}")
    print(f"Records extracted: {result['records_extracted']}")
    print(f"Records loaded: {result['records_loaded']}")
