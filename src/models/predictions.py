"""
Data models for WMATA rail predictions.

Defines the core data structures used throughout the pipeline.
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class TrainPrediction:
    """
    Represents a single train arrival prediction.

    Attributes:
        car_count: Number of cars (6 or 8), None if unknown
        destination: Human-readable destination name
        destination_code: WMATA station code for destination
        line: Line code (RD, BL, OR, SV, GR, YL)
        station_code: WMATA station code where train is predicted
        station_name: Human-readable station name
        minutes_to_arrival: Minutes until arrival, 0 for ARR/BRD, None if unknown
        raw_minutes: Original string from API (e.g., "ARR", "BRD", "5")
        extracted_at: UTC timestamp when data was fetched
    """

    car_count: int | None
    destination: str
    destination_code: str
    line: str
    station_code: str
    station_name: str
    minutes_to_arrival: int | None
    raw_minutes: str
    extracted_at: datetime

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "car_count": self.car_count,
            "destination": self.destination,
            "destination_code": self.destination_code,
            "line": self.line,
            "station_code": self.station_code,
            "station_name": self.station_name,
            "minutes_to_arrival": self.minutes_to_arrival,
            "raw_minutes": self.raw_minutes,
            "extracted_at": self.extracted_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TrainPrediction":
        """Create from dictionary."""
        from datetime import datetime

        extracted_at = data.get("extracted_at")
        if isinstance(extracted_at, str):
            extracted_at = datetime.fromisoformat(extracted_at)

        return cls(
            car_count=data.get("car_count"),
            destination=data.get("destination", ""),
            destination_code=data.get("destination_code", ""),
            line=data.get("line", ""),
            station_code=data.get("station_code", ""),
            station_name=data.get("station_name", ""),
            minutes_to_arrival=data.get("minutes_to_arrival"),
            raw_minutes=data.get("raw_minutes", ""),
            extracted_at=extracted_at,
        )
