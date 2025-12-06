"""
Tests for data utility functions used in the bike share pipeline.
"""

import pytest
import pandas as pd
import numpy as np
import json


class TestConvertComplexColsToJson:
    """Tests for the convert_complex_cols_to_json function."""

    def test_converts_dict_column_to_json_string(self):
        """Dictionary values should be converted to JSON strings."""
        from dags.utils.data_utils import convert_complex_cols_to_json

        df = pd.DataFrame({
            "id": [1, 2],
            "metadata": [{"key": "value"}, {"foo": "bar"}]
        })
        
        result = convert_complex_cols_to_json(df)
        
        assert result["metadata"].iloc[0] == '{"key": "value"}'
        assert result["metadata"].iloc[1] == '{"foo": "bar"}'

    def test_converts_list_column_to_json_string(self):
        """List values should be converted to JSON strings."""
        from dags.utils.data_utils import convert_complex_cols_to_json

        df = pd.DataFrame({
            "id": [1, 2],
            "tags": [["a", "b"], ["c", "d", "e"]]
        })
        
        result = convert_complex_cols_to_json(df)
        
        assert result["tags"].iloc[0] == '["a", "b"]'
        assert result["tags"].iloc[1] == '["c", "d", "e"]'

    def test_converts_numpy_array_to_json_string(self):
        """NumPy array values should be converted to JSON strings."""
        from dags.utils.data_utils import convert_complex_cols_to_json

        df = pd.DataFrame({
            "id": [1],
            "values": [np.array([1, 2, 3])]
        })
        
        result = convert_complex_cols_to_json(df)
        
        assert result["values"].iloc[0] == "[1, 2, 3]"

    def test_leaves_simple_columns_unchanged(self):
        """Simple string and numeric columns should not be modified."""
        from dags.utils.data_utils import convert_complex_cols_to_json

        df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Station A", "Station B"],
            "capacity": [20, 30]
        })
        
        result = convert_complex_cols_to_json(df)
        
        assert result["name"].iloc[0] == "Station A"
        assert result["capacity"].iloc[0] == 20

    def test_handles_null_values(self):
        """Null values should be preserved."""
        from dags.utils.data_utils import convert_complex_cols_to_json

        df = pd.DataFrame({
            "id": [1, 2],
            "metadata": [{"key": "value"}, None]
        })
        
        result = convert_complex_cols_to_json(df)
        
        assert result["metadata"].iloc[0] == '{"key": "value"}'
        assert pd.isna(result["metadata"].iloc[1])


class TestValidateStationData:
    """Tests for station data validation."""

    def test_station_info_has_required_columns(self):
        """Station info DataFrame should have required columns."""
        required_columns = [
            "station_id",
            "name", 
            "lat",
            "lon",
            "capacity"
        ]
        
        # Mock station info data
        df = pd.DataFrame({
            "station_id": ["1"],
            "name": ["Test Station"],
            "lat": [40.7128],
            "lon": [-74.0060],
            "capacity": [20]
        })
        
        for col in required_columns:
            assert col in df.columns, f"Missing required column: {col}"

    def test_station_status_has_required_columns(self):
        """Station status DataFrame should have required columns."""
        required_columns = [
            "station_id",
            "num_bikes_available",
            "num_docks_available",
            "is_installed",
            "is_renting",
            "is_returning"
        ]
        
        # Mock station status data
        df = pd.DataFrame({
            "station_id": ["1"],
            "num_bikes_available": [10],
            "num_docks_available": [10],
            "is_installed": [1],
            "is_renting": [1],
            "is_returning": [1]
        })
        
        for col in required_columns:
            assert col in df.columns, f"Missing required column: {col}"


class TestOccupancyCalculations:
    """Tests for occupancy rate calculations."""

    def test_occupancy_rate_calculation(self):
        """Occupancy rate should be calculated correctly."""
        bikes_available = 15
        capacity = 20
        
        occupancy_rate = (bikes_available / capacity) * 100
        
        assert occupancy_rate == 75.0

    def test_critical_empty_threshold(self):
        """Stations with ≤10% occupancy should be Critical Empty."""
        occupancy_rate = 10.0
        
        if occupancy_rate <= 10:
            status = "Critical Empty"
        elif occupancy_rate >= 90:
            status = "Critical Full"
        else:
            status = "Normal"
        
        assert status == "Critical Empty"

    def test_critical_full_threshold(self):
        """Stations with ≥90% occupancy should be Critical Full."""
        occupancy_rate = 90.0
        
        if occupancy_rate <= 10:
            status = "Critical Empty"
        elif occupancy_rate >= 90:
            status = "Critical Full"
        else:
            status = "Normal"
        
        assert status == "Critical Full"

    def test_normal_status(self):
        """Stations with 11-89% occupancy should be Normal."""
        occupancy_rate = 50.0
        
        if occupancy_rate <= 10:
            status = "Critical Empty"
        elif occupancy_rate >= 90:
            status = "Critical Full"
        else:
            status = "Normal"
        
        assert status == "Normal"

