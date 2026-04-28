"""
Tests for Data Transformer
"""

from datetime import UTC, datetime

import pandas as pd

from src.core.transformer import (
    aggregate_station_metrics,
    get_line_name,
    transform_predictions,
)


class TestTransformPredictions:
    """Test suite for transform_predictions function."""

    def test_transform_empty_list(self):
        """Test transforming empty list returns empty DataFrame."""
        result = transform_predictions([])
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_transform_valid_predictions(self):
        """Test transforming valid predictions."""
        raw = [
            {
                "station_code": "A01",
                "line": "RD",
                "destination": "Shady Grove",
                "destination_code": "A15",
                "station_name": "Metro Center",
                "minutes_to_arrival": 5,
                "car_count": 8,
                "raw_minutes": "5",
                "extracted_at": datetime.now(UTC).isoformat(),
            }
        ]

        result = transform_predictions(raw)

        assert len(result) == 1
        assert result.iloc[0]["station_code"] == "A01"
        assert result.iloc[0]["line"] == "RD"

    def test_transform_removes_null_station(self):
        """Test that null station codes are removed."""
        raw = [
            {"station_code": "A01", "line": "RD", "minutes_to_arrival": 5},
            {"station_code": None, "line": "RD", "minutes_to_arrival": 3},
            {"station_code": "", "line": "RD", "minutes_to_arrival": 2},
        ]

        result = transform_predictions(raw)

        assert len(result) == 1
        assert result.iloc[0]["station_code"] == "A01"

    def test_transform_removes_null_line(self):
        """Test that null line codes are removed."""
        raw = [
            {"station_code": "A01", "line": "RD", "minutes_to_arrival": 5},
            {"station_code": "A02", "line": None, "minutes_to_arrival": 3},
            {"station_code": "A03", "line": "", "minutes_to_arrival": 2},
        ]

        result = transform_predictions(raw)

        assert len(result) == 1


class TestAggregateStationMetrics:
    """Test suite for aggregate_station_metrics function."""

    def test_aggregate_empty_dataframe(self):
        """Test aggregating empty DataFrame returns empty list."""
        result = aggregate_station_metrics(pd.DataFrame())
        assert result == []

    def test_aggregate_single_station(self):
        """Test aggregating single station."""
        df = pd.DataFrame(
            [
                {
                    "station_code": "A01",
                    "line": "RD",
                    "station_name": "Metro Center",
                    "minutes_to_arrival": 5,
                    "extracted_at": datetime.now(UTC),
                },
                {
                    "station_code": "A01",
                    "line": "RD",
                    "station_name": "Metro Center",
                    "minutes_to_arrival": 10,
                    "extracted_at": datetime.now(UTC),
                },
            ]
        )

        result = aggregate_station_metrics(df)

        assert len(result) == 1
        assert result[0]["station_code"] == "A01"
        assert result[0]["avg_wait_minutes"] == 7.5
        assert result[0]["min_wait_minutes"] == 5
        assert result[0]["max_wait_minutes"] == 10
        assert result[0]["train_count"] == 2

    def test_aggregate_multiple_lines(self):
        """Test aggregating multiple lines at same station."""
        df = pd.DataFrame(
            [
                {
                    "station_code": "A01",
                    "line": "RD",
                    "station_name": "Metro Center",
                    "minutes_to_arrival": 5,
                    "extracted_at": datetime.now(UTC),
                },
                {
                    "station_code": "A01",
                    "line": "BL",
                    "station_name": "Metro Center",
                    "minutes_to_arrival": 8,
                    "extracted_at": datetime.now(UTC),
                },
            ]
        )

        result = aggregate_station_metrics(df)

        assert len(result) == 2
        station_lines = {(r["station_code"], r["line"]) for r in result}
        assert ("A01", "RD") in station_lines
        assert ("A01", "BL") in station_lines


class TestGetLineName:
    """Test suite for get_line_name function."""

    def test_known_lines(self):
        """Test known line codes."""
        assert get_line_name("RD") == "Red"
        assert get_line_name("BL") == "Blue"
        assert get_line_name("OR") == "Orange"
        assert get_line_name("SV") == "Silver"
        assert get_line_name("GR") == "Green"
        assert get_line_name("YL") == "Yellow"

    def test_unknown_line(self):
        """Test unknown line code returns code itself."""
        assert get_line_name("XX") == "XX"
