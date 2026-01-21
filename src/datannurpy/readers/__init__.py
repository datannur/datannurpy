"""Data readers for extracting metadata from files and databases."""

from .csv import scan_csv
from .database import (
    SYSTEM_SCHEMAS,
    connect,
    list_schemas,
    list_tables,
    scan_table,
)
from .excel import scan_excel

__all__ = [
    "scan_csv",
    "scan_excel",
    "scan_table",
    "connect",
    "list_tables",
    "list_schemas",
    "SYSTEM_SCHEMAS",
]
