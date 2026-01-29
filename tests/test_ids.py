"""Tests for ID helper functions."""

from __future__ import annotations

from datannurpy import build_dataset_id, build_variable_id


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
