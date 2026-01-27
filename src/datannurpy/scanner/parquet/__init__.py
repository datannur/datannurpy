"""Parquet dataset discovery and scanning module.

Handles various Parquet-based formats:
- Simple Parquet files (.parquet, .pq)
- Delta Lake tables
- Hive-partitioned datasets
"""

from __future__ import annotations

from .discovery import (
    DatasetType,
    DiscoveryResult,
    ParquetDatasetInfo,
    discover_parquet_datasets,
)
from .core import DatasetMetadata, scan_parquet, scan_parquet_dataset

__all__ = [
    "DatasetType",
    "DatasetMetadata",
    "DiscoveryResult",
    "ParquetDatasetInfo",
    "discover_parquet_datasets",
    "scan_parquet",
    "scan_parquet_dataset",
]
