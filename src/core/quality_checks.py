"""
Data Quality Check Module.

Validates data quality at each stage of the ETL pipeline.
Implements defensive checks to prevent bad data from reaching production.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class QCResult:
    """Result of a quality check."""

    check_name: str
    passed: bool
    message: str
    value: Any = None
    threshold: Any = None


# Known WMATA stations (subset for validation)
KNOWN_STATIONS: set[str] = {
    "A01",
    "A02",
    "A03",
    "A04",
    "A05",
    "A06",
    "A07",
    "A08",
    "A09",
    "A10",
    "A11",
    "A12",
    "A13",
    "A14",
    "A15",  # Red Line
    "B01",
    "B02",
    "B03",
    "B04",
    "B05",
    "B06",
    "B07",
    "B08",
    "B09",
    "B10",
    "B11",
    "B35",  # Red Line (cont)
    "C01",
    "C02",
    "C03",
    "C04",
    "C05",
    "C06",
    "C07",
    "C08",
    "C09",
    "C10",
    "C11",
    "C12",
    "C13",
    "C14",
    "C15",  # Blue/Orange/Silver
    "D01",
    "D02",
    "D03",
    "D04",
    "D05",
    "D06",
    "D07",
    "D08",
    "D09",
    "D10",
    "D11",
    "D12",
    "D13",  # Orange/Silver
    "E01",
    "E02",
    "E03",
    "E04",
    "E05",
    "E06",
    "E07",
    "E08",
    "E09",
    "E10",
    "F01",
    "F02",
    "F03",
    "F04",
    "F05",
    "F06",
    "F07",
    "F08",
    "F09",
    "F10",
    "F11",  # Green/Yellow
    "G01",
    "G02",
    "G03",
    "G04",
    "G05",  # Green
    "J02",
    "J03",  # Blue
    "K01",
    "K02",
    "K03",
    "K04",
    "K05",
    "K06",
    "K07",
    "K08",  # Silver
    "N01",
    "N02",
    "N03",
    "N04",
    "N06",  # Silver
    "N07",
    "N08",
    "N09",
    "N10",
    "N11",
    "N12",  # Silver Line Extension
}

VALID_LINES: set[str] = {"RD", "BL", "OR", "SV", "GR", "YL"}


def check_null_rate(
    aggregates: list[dict[str, Any]], field: str, threshold: float = 0.05
) -> QCResult:
    """
    Check that null rate for a field is below threshold.

    Args:
        aggregates: List of aggregate dictionaries.
        field: Field name to check.
        threshold: Maximum allowed null rate (0-1).

    Returns:
        QCResult with pass/fail status.
    """
    if not aggregates:
        return QCResult(
            check_name=f"null_rate_{field}",
            passed=False,
            message="No data to check",
            value=None,
            threshold=threshold,
        )

    null_count = sum(1 for agg in aggregates if agg.get(field) is None)
    null_rate = null_count / len(aggregates)

    passed = null_rate <= threshold

    return QCResult(
        check_name=f"null_rate_{field}",
        passed=passed,
        message=f"Null rate {null_rate:.2%} {'<=' if passed else '>'} {threshold:.2%}",
        value=null_rate,
        threshold=threshold,
    )


def check_schema(aggregates: list[dict[str, Any]]) -> QCResult:
    """
    Validate that all required fields are present.

    Args:
        aggregates: List of aggregate dictionaries.

    Returns:
        QCResult with pass/fail status.
    """
    required_fields = {
        "station_code",
        "line",
        "avg_wait_minutes",
        "min_wait_minutes",
        "max_wait_minutes",
        "train_count",
    }

    if not aggregates:
        return QCResult(
            check_name="schema_validation",
            passed=False,
            message="No data to validate",
        )

    missing_fields: set[str] = set()
    for agg in aggregates:
        for field in required_fields:
            if field not in agg:
                missing_fields.add(field)

    passed = len(missing_fields) == 0

    return QCResult(
        check_name="schema_validation",
        passed=passed,
        message=(
            f"Missing fields: {missing_fields}" if missing_fields else "All required fields present"
        ),
        value=list(missing_fields),
    )


def check_wait_time_range(
    aggregates: list[dict[str, Any]], min_val: int = 0, max_val: int = 60
) -> QCResult:
    """
    Check that wait times are within expected range.

    Args:
        aggregates: List of aggregate dictionaries.
        min_val: Minimum expected wait time.
        max_val: Maximum expected wait time.

    Returns:
        QCResult with pass/fail status.
    """
    if not aggregates:
        return QCResult(
            check_name="wait_time_range",
            passed=False,
            message="No data to check",
        )

    out_of_range = []
    for agg in aggregates:
        avg = agg.get("avg_wait_minutes")
        if avg is not None and (avg < min_val or avg > max_val):
            out_of_range.append(
                {
                    "station": agg.get("station_code"),
                    "line": agg.get("line"),
                    "value": avg,
                }
            )

    passed = len(out_of_range) == 0

    return QCResult(
        check_name="wait_time_range",
        passed=passed,
        message=f"{len(out_of_range)} values out of range [{min_val}, {max_val}]",
        value=out_of_range[:5],  # Limit to first 5
        threshold=(min_val, max_val),
    )


def check_valid_stations(aggregates: list[dict[str, Any]]) -> QCResult:
    """
    Check that station codes are valid WMATA stations.

    Args:
        aggregates: List of aggregate dictionaries.

    Returns:
        QCResult with pass/fail status.
    """
    if not aggregates:
        return QCResult(
            check_name="valid_stations",
            passed=False,
            message="No data to check",
        )

    invalid_stations: set[str] = set()
    for agg in aggregates:
        station = agg.get("station_code")
        if station and station not in KNOWN_STATIONS:
            invalid_stations.add(station)

    # Allow some unknown stations (new stations, etc.)
    passed = len(invalid_stations) <= 5

    return QCResult(
        check_name="valid_stations",
        passed=passed,
        message=f"{len(invalid_stations)} unknown station codes",
        value=list(invalid_stations),
    )


def check_valid_lines(aggregates: list[dict[str, Any]]) -> QCResult:
    """
    Check that line codes are valid WMATA lines.

    Args:
        aggregates: List of aggregate dictionaries.

    Returns:
        QCResult with pass/fail status.
    """
    if not aggregates:
        return QCResult(
            check_name="valid_lines",
            passed=False,
            message="No data to check",
        )

    invalid_lines: set[str] = set()
    for agg in aggregates:
        line = agg.get("line")
        if line and line not in VALID_LINES:
            invalid_lines.add(line)

    passed = len(invalid_lines) == 0

    return QCResult(
        check_name="valid_lines",
        passed=passed,
        message=f"Invalid lines: {invalid_lines}" if invalid_lines else "All lines valid",
        value=list(invalid_lines),
    )


def check_data_freshness(aggregates: list[dict[str, Any]], max_age_minutes: int = 10) -> QCResult:
    """
    Check that data is fresh (not stale).

    Args:
        aggregates: List of aggregate dictionaries.
        max_age_minutes: Maximum allowed data age in minutes.

    Returns:
        QCResult with pass/fail status.
    """
    if not aggregates:
        return QCResult(
            check_name="data_freshness",
            passed=False,
            message="No data to check",
        )

    now = datetime.now(UTC)
    max_age = timedelta(minutes=max_age_minutes)

    stale_count = 0
    for agg in aggregates:
        extracted_at = agg.get("extracted_at")
        if extracted_at:
            if isinstance(extracted_at, str):
                extracted_at = datetime.fromisoformat(extracted_at.replace("Z", "+00:00"))
            if now - extracted_at > max_age:
                stale_count += 1

    stale_rate = stale_count / len(aggregates) if aggregates else 0
    passed = stale_rate < 0.1  # Less than 10% stale

    return QCResult(
        check_name="data_freshness",
        passed=passed,
        message=f"{stale_rate:.1%} of data is stale (>{max_age_minutes} min old)",
        value=stale_rate,
        threshold=max_age_minutes,
    )


def check_completeness(
    aggregates: list[dict[str, Any]], min_stations: int | None = None
) -> QCResult:
    """
    Check that a minimum number of stations are reporting.
    Threshold is time-aware: lower at night when fewer trains run.

    Args:
        aggregates: List of aggregate dictionaries.
        min_stations: Minimum expected stations (auto-detected if None).

    Returns:
        QCResult with pass/fail status.
    """
    if not aggregates:
        return QCResult(
            check_name="completeness",
            passed=False,
            message="No data to check",
        )

    if min_stations is None:
        hour = datetime.now(UTC).hour
        # WMATA peak: 7-9 AM, 4-7 PM ET (11-13, 20-23 UTC)
        if 11 <= hour <= 23:
            min_stations = 40
        elif 9 <= hour <= 10 or hour == 0:
            min_stations = 20
        else:
            min_stations = 3  # Late night / early morning

    unique_stations = {agg.get("station_code") for agg in aggregates}
    station_count = len(unique_stations)

    passed = station_count >= min_stations

    return QCResult(
        check_name="completeness",
        passed=passed,
        message=f"{station_count} stations reporting (min: {min_stations})",
        value=station_count,
        threshold=min_stations,
    )


def run_quality_checks(
    aggregates: list[dict[str, Any]],
    fail_on_empty: bool = True,
) -> dict[str, Any]:
    """
    Run all quality checks on aggregated data.

    Args:
        aggregates: List of aggregate dictionaries.
        fail_on_empty: If True, fail when no data is provided.

    Returns:
        Dictionary with overall pass/fail and individual check results.
    """
    if not aggregates and fail_on_empty:
        return {
            "passed": False,
            "total_checks": 0,
            "passed_checks": 0,
            "failed_checks": 1,
            "failures": [{"check": "data_present", "message": "No data to validate"}],
            "all_results": [],
            "checked_at": datetime.now(UTC).isoformat(),
        }

    checks = [
        check_schema(aggregates),
        check_null_rate(aggregates, "avg_wait_minutes"),
        check_null_rate(aggregates, "station_code"),
        check_wait_time_range(aggregates),
        check_valid_stations(aggregates),
        check_valid_lines(aggregates),
        check_data_freshness(aggregates),
        check_completeness(aggregates),
    ]

    failures = [c for c in checks if not c.passed]

    result = {
        "passed": len(failures) == 0,
        "total_checks": len(checks),
        "passed_checks": len(checks) - len(failures),
        "failed_checks": len(failures),
        "failures": [
            {"check": c.check_name, "message": c.message, "value": c.value} for c in failures
        ],
        "all_results": [
            {
                "check": c.check_name,
                "passed": c.passed,
                "message": c.message,
                "value": c.value,
                "threshold": c.threshold,
            }
            for c in checks
        ],
        "checked_at": datetime.now(UTC).isoformat(),
    }

    logger.info(
        "quality_checks_completed",
        passed=result["passed"],
        total=result["total_checks"],
        failures=result["failed_checks"],
    )

    return result
