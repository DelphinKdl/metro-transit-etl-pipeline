"""Unit tests for quality checks module."""

from datetime import UTC, datetime, timedelta

from src.core.quality_checks import (
    check_completeness,
    check_data_freshness,
    check_null_rate,
    check_schema,
    check_valid_lines,
    check_valid_stations,
    check_wait_time_range,
    run_quality_checks,
)


class TestCheckNullRate:
    """Tests for check_null_rate function."""

    def test_empty_data_fails(self):
        """Empty data should fail."""
        result = check_null_rate([], "avg_wait_minutes")
        assert not result.passed

    def test_no_nulls_passes(self):
        """No nulls should pass."""
        data = [{"avg_wait_minutes": 5}, {"avg_wait_minutes": 10}]
        result = check_null_rate(data, "avg_wait_minutes")
        assert result.passed

    def test_high_null_rate_fails(self):
        """High null rate should fail."""
        data = [{"avg_wait_minutes": None}] * 10 + [{"avg_wait_minutes": 5}]
        result = check_null_rate(data, "avg_wait_minutes", threshold=0.05)
        assert not result.passed


class TestCheckSchema:
    """Tests for check_schema function."""

    def test_all_fields_present_passes(self):
        """All required fields present should pass."""
        data = [
            {
                "station_code": "A01",
                "line": "RD",
                "avg_wait_minutes": 5,
                "min_wait_minutes": 2,
                "max_wait_minutes": 10,
                "train_count": 3,
            }
        ]
        result = check_schema(data)
        assert result.passed

    def test_missing_field_fails(self):
        """Missing required field should fail."""
        data = [{"station_code": "A01", "line": "RD"}]
        result = check_schema(data)
        assert not result.passed


class TestCheckWaitTimeRange:
    """Tests for check_wait_time_range function."""

    def test_valid_range_passes(self):
        """Values in range should pass."""
        data = [{"avg_wait_minutes": 5}, {"avg_wait_minutes": 30}]
        result = check_wait_time_range(data)
        assert result.passed

    def test_out_of_range_fails(self):
        """Values out of range should fail."""
        data = [{"avg_wait_minutes": 100}]
        result = check_wait_time_range(data, max_val=60)
        assert not result.passed


class TestCheckValidStations:
    """Tests for check_valid_stations function."""

    def test_known_stations_pass(self):
        """Known station codes should pass."""
        data = [{"station_code": "A01"}, {"station_code": "B02"}]
        result = check_valid_stations(data)
        assert result.passed

    def test_many_unknown_stations_fail(self):
        """Many unknown stations should fail."""
        data = [{"station_code": f"XX{i}"} for i in range(10)]
        result = check_valid_stations(data)
        assert not result.passed


class TestCheckValidLines:
    """Tests for check_valid_lines function."""

    def test_valid_lines_pass(self):
        """Valid line codes should pass."""
        data = [{"line": "RD"}, {"line": "BL"}, {"line": "OR"}]
        result = check_valid_lines(data)
        assert result.passed

    def test_invalid_line_fails(self):
        """Invalid line code should fail."""
        data = [{"line": "XX"}]
        result = check_valid_lines(data)
        assert not result.passed


class TestCheckDataFreshness:
    """Tests for check_data_freshness function."""

    def test_fresh_data_passes(self):
        """Fresh data should pass."""
        now = datetime.now(UTC)
        data = [{"extracted_at": now}]
        result = check_data_freshness(data)
        assert result.passed

    def test_stale_data_fails(self):
        """Stale data should fail."""
        old_time = datetime.now(UTC) - timedelta(hours=1)
        data = [{"extracted_at": old_time}]
        result = check_data_freshness(data, max_age_minutes=10)
        assert not result.passed


class TestCheckCompleteness:
    """Tests for check_completeness function."""

    def test_enough_stations_passes(self):
        """Enough unique stations should pass."""
        data = [{"station_code": f"A{i:02d}"} for i in range(60)]
        result = check_completeness(data, min_stations=50)
        assert result.passed

    def test_too_few_stations_fails(self):
        """Too few stations should fail."""
        data = [{"station_code": "A01"}]
        result = check_completeness(data, min_stations=50)
        assert not result.passed


class TestRunQualityChecks:
    """Tests for run_quality_checks function."""

    def test_empty_data_fails(self):
        """Empty data should fail overall."""
        result = run_quality_checks([])
        assert not result["passed"]

    def test_returns_all_check_results(self):
        """Should return results for all checks."""
        data = [
            {
                "station_code": "A01",
                "line": "RD",
                "avg_wait_minutes": 5,
                "min_wait_minutes": 2,
                "max_wait_minutes": 10,
                "train_count": 3,
                "extracted_at": datetime.now(UTC),
            }
        ]
        result = run_quality_checks(data)
        assert "all_results" in result
        assert len(result["all_results"]) > 0
