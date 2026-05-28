"""Tests for ID helper functions."""

from __future__ import annotations

from datannurpy import build_dataset_id, build_variable_id
from datannurpy.utils import iso_to_timestamp


class TestBuildDatasetId:
    """Test build_dataset_id function."""

    def test_basic(self):
        """build_dataset_id should create folder---dataset format."""
        result = build_dataset_id("folder", "my_dataset")
        assert result == "folder---my_dataset"

    def test_sanitizes_name(self):
        """build_dataset_id should sanitize the dataset name."""
        result = build_dataset_id("src", "My File (v2)")
        assert result == "src---My File _v2_"


class TestBuildVariableId:
    """Test build_variable_id function."""

    def test_basic(self):
        """build_variable_id should create folder---dataset---variable format."""
        result = build_variable_id("folder", "dataset", "my_var")
        assert result == "folder---dataset---my_var"

    def test_sanitizes_names(self):
        """build_variable_id should sanitize dataset and variable names."""
        result = build_variable_id("src", "My File", "Var (1)")
        assert result == "src---My File---Var _1_"


class TestIsoToTimestamp:
    """Test iso_to_timestamp function."""

    def test_none_returns_none(self):
        """iso_to_timestamp(None) should return None."""
        assert iso_to_timestamp(None) is None

    def test_empty_string_returns_none(self):
        """iso_to_timestamp('') should return None."""
        assert iso_to_timestamp("") is None

    def test_datetime_format(self):
        """iso_to_timestamp should parse YYYY/MM/DDTHH:MM:SS."""
        result = iso_to_timestamp("2024/06/15T12:00:00")
        assert isinstance(result, int)
        assert result > 0

    def test_date_only_format(self):
        """iso_to_timestamp should parse YYYY/MM/DD."""
        result = iso_to_timestamp("2024/06/15")
        assert isinstance(result, int)
        assert result > 0

    def test_unrecognized_format_returns_none(self):
        """iso_to_timestamp should return None for unrecognized strings."""
        assert iso_to_timestamp("not-a-date") is None
