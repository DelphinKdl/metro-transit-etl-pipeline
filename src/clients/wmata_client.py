"""
WMATA Rail Predictions API Client.

Handles authentication, rate limiting, retries, and response parsing
for the WMATA StationPrediction API.
"""

import os
import time
from datetime import UTC, datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.models.predictions import TrainPrediction
from src.utils.logger import get_logger

logger = get_logger(__name__)


class WMATAClient:
    """
    Client for WMATA Rail Predictions API.

    Features:
        - Automatic retry with exponential backoff
        - Rate limiting (10 req/sec)
        - Structured logging
        - Response parsing to typed models

    Example:
        >>> client = WMATAClient()
        >>> predictions = client.get_predictions("A01")
        >>> for p in predictions:
        ...     print(f"{p.line}: {p.minutes_to_arrival} min")
    """

    BASE_URL = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction"

    def __init__(self, api_key: str | None = None, timeout: int = 30):
        """
        Initialize WMATA API client.

        Args:
            api_key: WMATA API key. Falls back to WMATA_API_KEY env var.
            timeout: Request timeout in seconds.

        Raises:
            ValueError: If no API key is provided or found in environment.
        """
        self.api_key = api_key or os.getenv("WMATA_API_KEY")
        if not self.api_key:
            raise ValueError("WMATA API key required. Set WMATA_API_KEY env var or pass api_key.")

        self.timeout = timeout
        self.session = self._create_session()
        self._last_request_time = 0.0
        self._min_request_interval = 0.1  # 10 requests/second max

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry logic."""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "api_key": self.api_key,
                "Accept": "application/json",
            }
        )

        return session

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def get_predictions(self, station_code: str = "All") -> list[TrainPrediction]:
        """
        Fetch train predictions for a station or all stations.

        Args:
            station_code: Station code (e.g., "A01") or "All" for all stations.

        Returns:
            List of TrainPrediction objects.

        Raises:
            requests.RequestException: If API request fails after retries.
        """
        self._rate_limit()

        url = f"{self.BASE_URL}/{station_code}"
        extracted_at = datetime.now(UTC)

        logger.info("fetching_predictions", station_code=station_code)

        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            predictions = self._parse_predictions(data, extracted_at)
            logger.info("predictions_fetched", count=len(predictions))

            return predictions

        except requests.exceptions.RequestException as e:
            logger.error("api_request_failed", error=str(e), station_code=station_code)
            raise

    def _parse_predictions(
        self, data: dict[str, Any], extracted_at: datetime
    ) -> list[TrainPrediction]:
        """Parse API response into TrainPrediction objects."""
        predictions = []

        for train in data.get("Trains", []):
            minutes_raw = train.get("Min", "")
            minutes = self._parse_minutes(minutes_raw)
            car_count = self._parse_car_count(train.get("Car", ""))

            prediction = TrainPrediction(
                car_count=car_count,
                destination=train.get("DestinationName", ""),
                destination_code=train.get("DestinationCode", ""),
                line=train.get("Line", ""),
                station_code=train.get("LocationCode", ""),
                station_name=train.get("LocationName", ""),
                minutes_to_arrival=minutes,
                raw_minutes=minutes_raw,
                extracted_at=extracted_at,
            )
            predictions.append(prediction)

        return predictions

    @staticmethod
    def _parse_minutes(minutes_str: str) -> int | None:
        """
        Parse minutes string to integer.

        WMATA returns:
            - "ARR" = arriving (returns 0)
            - "BRD" = boarding (returns 0)
            - "---" = no data (returns None)
            - Number string = minutes
        """
        if minutes_str in ("ARR", "BRD"):
            return 0
        if minutes_str == "---" or not minutes_str:
            return None
        try:
            return int(minutes_str)
        except ValueError:
            return None

    @staticmethod
    def _parse_car_count(car_str: str) -> int | None:
        """Parse car count string to integer."""
        if car_str in ("-", "") or not car_str:
            return None
        try:
            return int(car_str)
        except ValueError:
            return None


def get_all_predictions() -> list[TrainPrediction]:
    """
    Convenience function to fetch all predictions.

    Returns:
        List of all current train predictions.
    """
    client = WMATAClient()
    return client.get_predictions("All")
