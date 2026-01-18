"""Data readers for extracting metadata from files."""

from .csv import scan_csv
from .excel import scan_excel

__all__ = ["scan_csv", "scan_excel"]
