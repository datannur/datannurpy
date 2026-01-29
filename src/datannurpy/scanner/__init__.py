"""File and database scanners for extracting metadata."""

from .csv import read_csv, scan_csv
from .database import (
    SYSTEM_SCHEMAS,
    connect,
    list_schemas,
    list_tables,
    scan_table,
)
from .excel import read_excel, scan_excel
from .statistical import read_statistical, scan_statistical

__all__ = [
    # Read functions (return DataFrame)
    "read_csv",
    "read_excel",
    "read_statistical",
    # Scan functions (return Variables + stats)
    "scan_csv",
    "scan_excel",
    "scan_statistical",
    "scan_table",
    # Database utilities
    "connect",
    "list_tables",
    "list_schemas",
    "SYSTEM_SCHEMAS",
]
