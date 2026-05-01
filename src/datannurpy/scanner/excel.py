"""Excel reader using pandas + openpyxl/xlrd."""

from __future__ import annotations

import codecs
from collections.abc import Sequence
from datetime import time as dt_time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ibis
import pyarrow as pa

from ..schema import Variable
from ..utils import log_error, log_warn
from .utils import build_variables

if TYPE_CHECKING:
    import pandas as pd

_MIDNIGHT = dt_time(0, 0)

_MAX_PREVIEW_ROWS = 10
_XLS_SNIFF_BYTES = 256
_HTML_XLS_MESSAGE = (
    "invalid .xls file: HTML content detected; likely an export/report renamed .xls"
)


def _looks_like_html_xls_content(header_bytes: bytes) -> bool:
    """Detect HTML content in a file mislabeled with the .xls extension."""
    probe = header_bytes.replace(b"\x00", b"").lstrip(b" \t\r\n")
    for bom in (codecs.BOM_UTF8, codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
        if probe.startswith(bom):
            probe = probe[len(bom) :]
            break
    probe = probe.lstrip(b" \t\r\n").lower()
    return probe.startswith(
        (b"<!doctype html", b"<html", b"<head", b"<body", b"<table")
    )


def _read_file_header(path: str | Path, max_bytes: int = _XLS_SNIFF_BYTES) -> bytes:
    """Read the first bytes of a local file for lightweight format sniffing."""
    with open(path, "rb") as f:
        return f.read(max_bytes)


def _warn_html_xls(file_name: str, quiet: bool) -> None:
    """Emit a clear warning for HTML reports renamed with the .xls extension."""
    log_warn(f"{file_name}: {_HTML_XLS_MESSAGE}; skipped as untreatable", quiet)


def is_valid_tabular_dataset(rows: Sequence[tuple[object, ...]]) -> tuple[bool, str]:
    """Check if first rows look like a raw tabular dataset (xlsx or csv)."""
    if not rows:
        return False, "empty sheet"

    header = rows[0]
    if not header:
        return False, "empty header row"

    # 1. Starts at A1
    if header[0] is None:
        return False, "header does not start at column A"

    # 2. Continuous (no None gaps)
    last_filled = max(i for i, v in enumerate(header) if v is not None)
    for i in range(last_filled + 1):
        if header[i] is None:
            return False, "empty cells in header row"

    header_width = last_filled + 1

    # 3. All unique
    values = [header[i] for i in range(header_width)]
    if len(values) != len(set(values)):
        return False, "duplicate column names"

    # 4. All text
    for v in values:
        if not isinstance(v, str):
            return False, "non-text values in header row"

    # 5. Width stable (data rows don't exceed header width)
    for row in rows[1:]:
        if not row:
            continue
        for i in range(len(row) - 1, -1, -1):
            if row[i] is not None:
                if i >= header_width:
                    return False, "data wider than header row"
                break

    return True, ""


def read_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    quiet: bool = False,
) -> pd.DataFrame | None:
    """Read an Excel file into a pandas DataFrame."""
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if file_path.stat().st_size == 0:
        return None

    engine = "xlrd" if suffix == ".xls" else "openpyxl"

    if suffix == ".xls" and _looks_like_html_xls_content(_read_file_header(file_path)):
        _warn_html_xls(file_path.name, quiet)
        return None

    try:
        import pandas as pd

        df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
        if df.empty:
            return None
        return df
    except Exception as e:
        log_error(file_path.name, e, quiet)
        return None


def _read_preview_rows(source: Path | Any) -> list[tuple[object, ...]]:
    """Read first rows from xlsx using openpyxl read-only streaming."""
    import openpyxl

    wb = openpyxl.load_workbook(source, read_only=True, data_only=True)
    ws = wb.active
    rows: list[tuple[object, ...]] = []
    if ws is not None:  # pragma: no branch
        for row in ws.iter_rows(max_row=_MAX_PREVIEW_ROWS, values_only=True):
            rows.append(row)
    wb.close()
    return rows


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int | None, pa.Table | None]:
    """Scan an Excel file and return (variables, row_count, freq_table)."""
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    if suffix == ".xls" and _looks_like_html_xls_content(_read_file_header(file_path)):
        _warn_html_xls(file_path.name, quiet)
        return [], None, None

    # Pre-read validation for .xlsx (streaming, avoids full read if invalid)
    if suffix != ".xls":
        try:
            rows = _read_preview_rows(file_path)
        except Exception as e:
            log_error(file_path.name, e, quiet)
            return [], None, None
        valid, reason = is_valid_tabular_dataset(rows)
        if not valid:
            log_warn(
                f"{file_path.name}: not a valid tabular dataset ({reason}); "
                "skipped as untreatable",
                quiet,
            )
            return [], None, None

    df = read_excel(path, sheet_name=sheet_name, quiet=quiet)
    if df is None:
        return [], 0, None

    # Post-read validation for .xls (no streaming available)
    if suffix == ".xls":
        header_row = tuple(df.columns)
        data_rows = [
            tuple(row) for row in df.head(_MAX_PREVIEW_ROWS).itertuples(index=False)
        ]
        valid, reason = is_valid_tabular_dataset([header_row, *data_rows])
        if not valid:
            log_warn(
                f"{file_path.name}: not a valid tabular dataset ({reason}); "
                "skipped as untreatable",
                quiet,
            )
            return [], None, None

    # Pandas reads Excel dates as datetime64 even for date-only cells.
    # Detect columns where all values are at midnight and convert to date.
    import pandas as pd

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            non_null = df[col].dropna()
            if len(non_null) > 0 and (non_null.dt.time == _MIDNIGHT).all():
                df[col] = df[col].dt.date

    con = ibis.duckdb.connect()
    try:
        try:
            table = con.create_table("excel_data", df)
        except pa.ArrowTypeError:
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype("string")
            table = con.create_table("excel_data", df)
        row_count: int = table.count().execute()

        variables, freq_table = build_variables(
            table,
            nb_rows=row_count,
            dataset_id=dataset_id,
            infer_stats=infer_stats,
            freq_threshold=freq_threshold,
        )
        return variables, row_count, freq_table
    finally:
        con.disconnect()
        con.disconnect()
