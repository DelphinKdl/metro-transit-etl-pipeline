"""Unit tests for transformer module."""

from datetime import UTC, datetime

from src.core.transformer import (
    aggregate_station_metrics,
    get_line_name,
    transform_predictions,
)


class TestTransformPredictions:
    """Tests for transform_predictions function."""

    def test_empty_input_returns_empty_dataframe(self):
        """Empty input should return empty DataFrame."""
        result = transform_predictions([])
        assert result.empty

    def test_removes_rows_without_station_code(self):
        """Rows without station_code should be removed."""
        now = datetime.now(UTC)
        raw = [
            {
                "station_code": "A01",
                "line": "RD",
                "minutes_to_arrival": 5,
                "car_count": 8,
                "destination_code": "B01",
                "extracted_at": now,
                "station_name": "Test",
            },
            {
                "station_code": "",
                "line": "RD",
                "minutes_to_arrival": 3,
                "car_count": 8,
                "destination_code": "B01",
                "extracted_at": now,
                "station_name": "Test",
            },
            {
                "station_code": None,
                "line": "RD",
                "minutes_to_arrival": 2,
                "car_count": 8,
                "destination_code": "B01",
                "extracted_at": now,
                "station_name": "Test",
            },
        ]
        result = transform_predictions(raw)
        assert len(result) == 1
        assert result.iloc[0]["station_code"] == "A01"

    def test_removes_rows_without_line(self):
        """Rows without line should be removed."""
        now = datetime.now(UTC)
        raw = [
            {
                "station_code": "A01",
                "line": "RD",
                "minutes_to_arrival": 5,
                "car_count": 8,
                "destination_code": "B01",
                "extracted_at": now,
                "station_name": "Test",
            },
            {
                "station_code": "A02",
                "line": "",
                "minutes_to_arrival": 3,
                "car_count": 8,
                "destination_code": "B01",
                "extracted_at": now,
                "station_name": "Test",
            },
        ]
        result = transform_predictions(raw)
        assert len(result) == 1

    def test_coerces_numeric_fields(self):
        """Numeric fields should be coerced properly."""
        now = datetime.now(UTC)
        raw = [
            {
                "station_code": "A01",
                "line": "RD",
                "minutes_to_arrival": "5",
                "car_count": "8",
                "destination_code": "B01",
                "extracted_at": now,
                "station_name": "Test",
            }
        ]
        result = transform_predictions(raw)
        assert result.iloc[0]["minutes_to_arrival"] == 5
        assert result.iloc[0]["car_count"] == 8


class TestAggregateStationMetrics:
    """Tests for aggregate_station_metrics function."""

    def test_empty_dataframe_returns_empty_list(self):
        """Empty DataFrame should return empty list."""
        import pandas as pd

        result = aggregate_station_metrics(pd.DataFrame())
        assert result == []

    def test_aggregates_by_station_and_line(self):
        """Should aggregate by station_code and line."""
        import pandas as pd

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


class TestGetLineName:
    """Tests for get_line_name function."""

    def test_known_line_codes(self):
        """Known line codes should return full names."""
        assert get_line_name("RD") == "Red"
        assert get_line_name("BL") == "Blue"
        assert get_line_name("OR") == "Orange"

    def test_unknown_line_code_returns_code(self):
        """Unknown line codes should return the code itself."""
        assert get_line_name("XX") == "XX"
