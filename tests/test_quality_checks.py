"""
Tests for Quality Checks
"""

from datetime import UTC, datetime, timedelta

from src.core.quality_checks import (
    check_data_freshness,
    check_null_rate,
    check_schema,
    check_valid_lines,
    check_valid_stations,
    check_wait_time_range,
    run_quality_checks,
)


class TestCheckNullRate:
    """Test suite for check_null_rate function."""

    def test_no_nulls(self):
        """Test with no null values."""
        data = [{"field": 1}, {"field": 2}, {"field": 3}]
        result = check_null_rate(data, "field")
        assert result.passed is True

    def test_all_nulls(self):
        """Test with all null values."""
        data = [{"field": None}, {"field": None}]
        result = check_null_rate(data, "field", threshold=0.05)
        assert result.passed is False

    def test_empty_data(self):
        """Test with empty data."""
        result = check_null_rate([], "field")
        assert result.passed is False


class TestCheckSchema:
    """Test suite for check_schema function."""

    def test_valid_schema(self):
        """Test with all required fields present."""
        data = [
            {
                "station_code": "A01",
                "line": "RD",
                "avg_wait_minutes": 5.0,
                "min_wait_minutes": 2,
                "max_wait_minutes": 8,
                "train_count": 3,
            }
        ]
        result = check_schema(data)
        assert result.passed is True

    def test_missing_field(self):
        """Test with missing required field."""
        data = [
            {
                "station_code": "A01",
                "line": "RD",
                # missing avg_wait_minutes
                "min_wait_minutes": 2,
                "max_wait_minutes": 8,
                "train_count": 3,
            }
        ]
        result = check_schema(data)
        assert result.passed is False


class TestCheckWaitTimeRange:
    """Test suite for check_wait_time_range function."""

    def test_valid_range(self):
        """Test with values in valid range."""
        data = [{"avg_wait_minutes": 5}, {"avg_wait_minutes": 10}, {"avg_wait_minutes": 30}]
        result = check_wait_time_range(data)
        assert result.passed is True

    def test_out_of_range(self):
        """Test with values out of range."""
        data = [{"station_code": "A01", "line": "RD", "avg_wait_minutes": 100}]
        result = check_wait_time_range(data, min_val=0, max_val=60)
        assert result.passed is False


class TestCheckValidStations:
    """Test suite for check_valid_stations function."""

    def test_valid_stations(self):
        """Test with valid station codes."""
        data = [{"station_code": "A01"}, {"station_code": "B02"}, {"station_code": "C03"}]
        result = check_valid_stations(data)
        assert result.passed is True

    def test_some_invalid_stations(self):
        """Test with some invalid station codes (should pass if <= 5)."""
        data = [{"station_code": "A01"}, {"station_code": "INVALID1"}, {"station_code": "INVALID2"}]
        result = check_valid_stations(data)
        assert result.passed is True  # <= 5 invalid allowed


class TestCheckValidLines:
    """Test suite for check_valid_lines function."""

    def test_valid_lines(self):
        """Test with valid line codes."""
        data = [{"line": "RD"}, {"line": "BL"}, {"line": "OR"}]
        result = check_valid_lines(data)
        assert result.passed is True

    def test_invalid_line(self):
        """Test with invalid line code."""
        data = [{"line": "XX"}]
        result = check_valid_lines(data)
        assert result.passed is False


class TestCheckDataFreshness:
    """Test suite for check_data_freshness function."""

    def test_fresh_data(self):
        """Test with fresh data."""
        now = datetime.now(UTC)
        data = [{"extracted_at": now.isoformat()}]
        result = check_data_freshness(data, max_age_minutes=10)
        assert result.passed is True

    def test_stale_data(self):
        """Test with stale data."""
        old = datetime.now(UTC) - timedelta(hours=1)
        data = [{"extracted_at": old.isoformat()}]
        result = check_data_freshness(data, max_age_minutes=10)
        assert result.passed is False


class TestRunQualityChecks:
    """Test suite for run_quality_checks function."""

    def test_all_checks_pass(self):
        """Test with data that passes all checks."""
        from src.core.quality_checks import KNOWN_STATIONS

        now = datetime.now(UTC)
        stations = sorted(KNOWN_STATIONS)[:60]
        data = [
            {
                "station_code": code,
                "line": "RD",
                "station_name": f"Station {code}",
                "avg_wait_minutes": 5.0,
                "min_wait_minutes": 2,
                "max_wait_minutes": 8,
                "train_count": 3,
                "extracted_at": now.isoformat(),
            }
            for code in stations
        ]

        result = run_quality_checks(data)

        assert result["passed"] is True
        assert result["failed_checks"] == 0

    def test_empty_data_fails(self):
        """Test that empty data fails checks."""
        result = run_quality_checks([])
        assert result["passed"] is False
