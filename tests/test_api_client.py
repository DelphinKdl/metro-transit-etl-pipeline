"""
Tests for WMATA API Client
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone

from src.clients.wmata_client import WMATAClient
from src.models.predictions import TrainPrediction


class TestWMATAClient:
    """Test suite for WMATAClient."""
    
    def test_init_with_api_key(self):
        """Test client initialization with API key."""
        client = WMATAClient(api_key="test_key")
        assert client.api_key == "test_key"
    
    def test_init_without_api_key_raises(self):
        """Test that missing API key raises ValueError."""
        with patch.dict('os.environ', {}, clear=True):
            with pytest.raises(ValueError, match="WMATA API key required"):
                WMATAClient()
    
    def test_parse_minutes_numeric(self):
        """Test parsing numeric minutes."""
        assert WMATAClient._parse_minutes("5") == 5
        assert WMATAClient._parse_minutes("0") == 0
        assert WMATAClient._parse_minutes("30") == 30
    
    def test_parse_minutes_arriving(self):
        """Test parsing ARR and BRD as 0."""
        assert WMATAClient._parse_minutes("ARR") == 0
        assert WMATAClient._parse_minutes("BRD") == 0
    
    def test_parse_minutes_no_data(self):
        """Test parsing no data as None."""
        assert WMATAClient._parse_minutes("---") is None
        assert WMATAClient._parse_minutes("") is None
    
    def test_parse_car_count(self):
        """Test parsing car count."""
        assert WMATAClient._parse_car_count("8") == 8
        assert WMATAClient._parse_car_count("6") == 6
        assert WMATAClient._parse_car_count("-") is None
        assert WMATAClient._parse_car_count("") is None
    
    @patch('requests.Session.get')
    def test_get_predictions_success(self, mock_get):
        """Test successful API call."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "Trains": [
                {
                    "Car": "8",
                    "Destination": "Shady Grove",
                    "DestinationCode": "A15",
                    "DestinationName": "Shady Grove",
                    "Group": "1",
                    "Line": "RD",
                    "LocationCode": "A01",
                    "LocationName": "Metro Center",
                    "Min": "3"
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        client = WMATAClient(api_key="test_key")
        predictions = client.get_predictions("A01")
        
        assert len(predictions) == 1
        assert predictions[0].station_code == "A01"
        assert predictions[0].line == "RD"
        assert predictions[0].minutes_to_arrival == 3
        assert predictions[0].car_count == 8


class TestTrainPrediction:
    """Test suite for TrainPrediction dataclass."""
    
    def test_train_prediction_creation(self):
        """Test creating a TrainPrediction."""
        pred = TrainPrediction(
            car_count=8,
            destination="Shady Grove",
            destination_code="A15",
            line="RD",
            station_code="A01",
            station_name="Metro Center",
            minutes_to_arrival=5,
            raw_minutes="5",
            extracted_at=datetime.now(timezone.utc)
        )
        
        assert pred.car_count == 8
        assert pred.line == "RD"
        assert pred.minutes_to_arrival == 5
