"""Shared fixtures for datannurpy tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from datannurpy import Catalog, Folder

# Common paths
DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


@pytest.fixture(scope="module")
def data_dir() -> Path:
    """Return the data directory path."""
    return DATA_DIR


@pytest.fixture(scope="module")
def csv_dir() -> Path:
    """Return the CSV directory path."""
    return CSV_DIR


@pytest.fixture(scope="module")
def full_catalog() -> Catalog:
    """Scan DATA_DIR once, reuse across read-only tests."""
    catalog = Catalog()
    catalog.add_folder(DATA_DIR, Folder(id="test", name="Test"))
    return catalog
