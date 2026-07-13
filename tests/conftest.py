"""Shared fixtures for datannurpy tests."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pytest

from datannurpy import Catalog, EntityMetadata


def empty_geo_scan(*args: Any, **kwargs: Any) -> Any:
    """A ``scan_geo_vector`` stub that logs a ✗ and returns an empty layer scan —
    the shared way tests simulate an internally-failed geo layer."""
    from datannurpy.utils.log import log_error

    log_error(kwargs.get("path_label", "layer"), RuntimeError("bad"), True)
    return [], None, None, None, None


# Pre-install DuckDB extensions to avoid lock conflicts with parallel workers
duckdb.execute("INSTALL delta")
duckdb.execute("INSTALL iceberg")

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
    catalog.add_folder(DATA_DIR, metadata=EntityMetadata(id="test", name="Test"))
    return catalog
