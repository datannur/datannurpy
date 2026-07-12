"""Excel/OpenDocument spreadsheet reader using pandas + openpyxl/xlrd/odf."""

from __future__ import annotations

import codecs
import io
import posixpath
import warnings
from collections.abc import Sequence
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from datetime import time as dt_time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import ibis
import pyarrow as pa

from ..preview import preview_from_pandas
from ..schema import Variable
from ..utils import log_debug, log_error, log_warn
from .utils import build_variables

if TYPE_CHECKING:
    from collections.abc import Iterator

    import pandas as pd
    import polars as pl

_MIDNIGHT = dt_time(0, 0)

# Suffix-specific pandas engines; anything else (.xlsx/.xlsm) reads via openpyxl.
# Neither engine can stream a preview, so these suffixes validate after the full
# pandas read instead of through the pre-read streaming check.
_PANDAS_ENGINES: dict[str, Literal["xlrd", "odf"]] = {".xls": "xlrd", ".ods": "odf"}

_MAX_PREVIEW_ROWS = 10
_XLS_SNIFF_BYTES = 256
_HTML_XLS_MESSAGE = (
    "invalid .xls file: HTML content detected; likely an export/report renamed .xls"
)
_XLSX_MAIN_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_XLSX_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


@dataclass(frozen=True)
class _SharedStringRef:
    index: int


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


def _display_label(file_path: Path, path_label: str | None) -> str:
    """Return the user-facing path label for scanner messages."""
    return path_label or file_path.name


def _warn_html_xls(file_name: str, quiet: bool) -> None:
    """Emit a clear warning for HTML reports renamed with the .xls extension."""
    log_warn(f"{file_name}: {_HTML_XLS_MESSAGE}; skipped as untreatable", quiet)


def _local_name(tag: str) -> str:
    """Return the local XML name for a possibly namespaced tag."""
    return tag.rsplit("}", 1)[-1]


def _first_worksheet_path(zf: ZipFile) -> str:
    """Return the first worksheet path from an xlsx workbook."""
    with zf.open("xl/workbook.xml") as workbook_file:
        workbook_root = ET.parse(workbook_file).getroot()
    sheet = workbook_root.find(f"{_XLSX_MAIN_NS}sheets/{_XLSX_MAIN_NS}sheet")
    if sheet is None:
        raise ValueError("xlsx workbook has no sheets")
    rel_id = sheet.attrib.get(f"{_XLSX_REL_NS}id")
    if rel_id is None:
        raise ValueError("xlsx first sheet has no relationship id")

    with zf.open("xl/_rels/workbook.xml.rels") as rels_file:
        rels_root = ET.parse(rels_file).getroot()
    target: str | None = None
    for rel in rels_root:
        if rel.attrib.get("Id") == rel_id:
            target = rel.attrib.get("Target")
            break
    if target is None:
        raise ValueError("xlsx first sheet relationship is missing")

    if target.startswith("/"):
        return posixpath.normpath(target.lstrip("/"))
    return posixpath.normpath(posixpath.join("xl", target))


def _column_index(cell_ref: str | None, fallback: int) -> int:
    """Convert an Excel cell reference to a one-based column index."""
    if not cell_ref:
        return fallback
    index = 0
    for char in cell_ref:
        if not char.isalpha():
            break
        index = index * 26 + ord(char.upper()) - ord("A") + 1
    return index or fallback


def _element_text(element: ET.Element) -> str:
    """Return concatenated text from an XML element."""
    return "".join(element.itertext())


def _coerce_xlsx_number(value: str) -> int | float | str:
    """Coerce a raw xlsx numeric value to a Python scalar."""
    try:
        as_float = float(value)
    except ValueError:
        return value
    if as_float.is_integer():
        return int(as_float)
    return as_float


def _xlsx_child_text(cell: ET.Element, local_name: str) -> str | None:
    """Return text for the first direct child with the given local XML name."""
    for child in cell:
        if _local_name(child.tag) == local_name:
            return child.text or ""
    return None


def _read_xlsx_cell_value(
    cell: ET.Element,
    shared_string_indices: set[int],
) -> object:
    """Read a preview cell value from worksheet XML."""
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        for child in cell:
            if _local_name(child.tag) == "is":
                return _element_text(child)
        return ""

    raw_value = _xlsx_child_text(cell, "v")
    if raw_value is None:
        return None
    if cell_type == "s":
        index = int(raw_value)
        shared_string_indices.add(index)
        return _SharedStringRef(index)
    if cell_type == "b":
        return raw_value in {"1", "true", "TRUE"}
    if cell_type in {"str", "e"}:
        return raw_value
    return _coerce_xlsx_number(raw_value)


def _read_xlsx_sheet_preview(
    zf: ZipFile,
    worksheet_path: str,
) -> tuple[list[tuple[object, ...]], set[int]]:
    """Read preview rows from worksheet XML and collect shared string indices."""
    rows_by_index: dict[int, dict[int, object]] = {}
    shared_string_indices: set[int] = set()
    fallback_row_index = 0

    with zf.open(worksheet_path) as worksheet_file:
        for _event, element in ET.iterparse(worksheet_file, events=("end",)):
            if _local_name(element.tag) != "row":
                continue

            fallback_row_index += 1
            row_index = int(element.attrib.get("r", fallback_row_index))
            if row_index > _MAX_PREVIEW_ROWS:
                break

            cells: dict[int, object] = {}
            fallback_col_index = 0
            for cell in element:
                if _local_name(cell.tag) != "c":
                    continue
                fallback_col_index += 1
                col_index = _column_index(cell.attrib.get("r"), fallback_col_index)
                value = _read_xlsx_cell_value(cell, shared_string_indices)
                if value is not None:
                    cells[col_index] = value
            rows_by_index[row_index] = cells
            element.clear()

    if not rows_by_index:
        return [], shared_string_indices

    rows: list[tuple[object, ...]] = []
    last_row = min(max(rows_by_index), _MAX_PREVIEW_ROWS)
    for row_index in range(1, last_row + 1):
        cells = rows_by_index.get(row_index, {})
        if not cells:
            rows.append(())
            continue
        last_col = max(cells)
        rows.append(tuple(cells.get(col_index) for col_index in range(1, last_col + 1)))
    return rows, shared_string_indices


def _read_xlsx_shared_strings(zf: ZipFile, needed: set[int]) -> dict[int, str]:
    """Read only the shared strings referenced by preview rows."""
    if not needed:
        return {}

    strings: dict[int, str] = {}
    current_index = 0
    with zf.open("xl/sharedStrings.xml") as shared_strings_file:
        for _event, element in ET.iterparse(shared_strings_file, events=("end",)):
            if _local_name(element.tag) != "si":
                continue
            if current_index in needed:
                strings[current_index] = _element_text(element)
                if needed.issubset(strings):
                    break
            current_index += 1
            element.clear()

    missing = needed - set(strings)
    if missing:
        raise ValueError("xlsx shared string table is incomplete")
    return strings


def _resolve_xlsx_shared_strings(
    rows: Sequence[tuple[object, ...]],
    shared_strings: dict[int, str],
) -> list[tuple[object, ...]]:
    """Replace shared string references in preview rows."""
    resolved_rows: list[tuple[object, ...]] = []
    for row in rows:
        resolved_row: list[object] = []
        for value in row:
            if isinstance(value, _SharedStringRef):
                resolved_row.append(shared_strings[value.index])
            else:
                resolved_row.append(value)
        resolved_rows.append(tuple(resolved_row))
    return resolved_rows


def _read_xlsx_preview_rows_fast(path: Path) -> list[tuple[object, ...]]:
    """Read first xlsx rows through minimal ZIP/XML parsing."""
    with ZipFile(path) as zf:
        worksheet_path = _first_worksheet_path(zf)
        rows, needed_strings = _read_xlsx_sheet_preview(zf, worksheet_path)
        shared_strings = _read_xlsx_shared_strings(zf, needed_strings)
    return _resolve_xlsx_shared_strings(rows, shared_strings)


@contextmanager
def _capture_excel_diagnostics(label: str, quiet: bool) -> Iterator[None]:
    """Capture third-party Excel parser diagnostics and log them as debug details."""
    stdout = io.StringIO()
    with warnings.catch_warnings(record=True) as captured_warnings:
        warnings.simplefilter("always")
        with redirect_stdout(stdout):
            try:
                yield
            finally:
                diagnostics = [str(w.message) for w in captured_warnings]
                diagnostics.extend(stdout.getvalue().splitlines())
                for message in filter(None, (d.strip() for d in diagnostics)):
                    log_debug(f"{label}: Excel parser diagnostic: {message}", quiet)


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


def _read_excel_frame(
    file_path: Path, sheet_name: str | int, label: str, quiet: bool
) -> pd.DataFrame:
    """Read a spreadsheet into a DataFrame with the suffix's engine, raising on
    failure — callers choose what a read error means (``read_excel`` returns
    None for metadata loading; ``scan_excel`` reports a scan error)."""
    import pandas as pd

    engine: Literal["xlrd", "openpyxl", "odf"] = _PANDAS_ENGINES.get(
        file_path.suffix.lower(), "openpyxl"
    )
    with _capture_excel_diagnostics(label, quiet):
        return pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)


def read_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    quiet: bool = False,
    path_label: str | None = None,
) -> pd.DataFrame | None:
    """Read an Excel file into a pandas DataFrame."""
    file_path = Path(path)
    label = _display_label(file_path, path_label)
    suffix = file_path.suffix.lower()

    if file_path.stat().st_size == 0:
        return None

    if suffix == ".xls" and _looks_like_html_xls_content(_read_file_header(file_path)):
        _warn_html_xls(label, quiet)
        return None

    try:
        df = _read_excel_frame(file_path, sheet_name, label, quiet)
        if df.empty:
            return None
        return df
    except Exception as e:
        log_error(label, e, quiet)
        return None


def _read_preview_rows(
    source: Path | Any, *, quiet: bool = False, path_label: str | None = None
) -> list[tuple[object, ...]]:
    """Read first rows from xlsx using a fast local path with openpyxl fallback."""
    if isinstance(source, Path) and source.suffix.lower() == ".xlsx":
        try:
            return _read_xlsx_preview_rows_fast(source)
        except Exception as e:
            log_debug(
                f"{path_label or source.name}: fast xlsx preview failed: {e}", quiet
            )

    import openpyxl

    label = path_label or getattr(source, "name", "excel file")
    with _capture_excel_diagnostics(str(label), quiet):
        wb = openpyxl.load_workbook(source, read_only=True, data_only=True)
    ws = wb.active
    rows: list[tuple[object, ...]] = []
    if ws is not None:  # pragma: no branch
        rows.extend(ws.iter_rows(max_row=_MAX_PREVIEW_ROWS, values_only=True))
    wb.close()
    return rows


def scan_excel(
    path: str | Path,
    *,
    sheet_name: str | int = 0,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    preview_rows: int = 0,
    return_preview: bool = False,
    quiet: bool = False,
    path_label: str | None = None,
) -> tuple[Any, ...]:
    """Scan an Excel file and return (variables, row_count, freq_table)."""
    file_path = Path(path)
    label = _display_label(file_path, path_label)
    suffix = file_path.suffix.lower()

    def result(
        variables: list[Variable],
        nb_row: int | None,
        freq_table: pa.Table | None,
        preview_df: pl.DataFrame | None,
    ) -> tuple[Any, ...]:
        if return_preview:
            return variables, nb_row, freq_table, preview_df
        return variables, nb_row, freq_table

    # Before any parsing: a 0-byte file is "empty", not a read error (the xlsx
    # pre-read validation below would otherwise raise BadZipFile on it).
    if file_path.stat().st_size == 0:
        return result([], 0, None, None)

    if suffix == ".xls" and _looks_like_html_xls_content(_read_file_header(file_path)):
        _warn_html_xls(label, quiet)
        return result([], None, None, None)

    # Pre-read validation for .xlsx (streaming, avoids full read if invalid)
    if suffix not in _PANDAS_ENGINES:
        try:
            rows = _read_preview_rows(file_path, quiet=quiet, path_label=label)
        except Exception as e:
            log_error(label, e, quiet)
            return result([], None, None, None)
        valid, reason = is_valid_tabular_dataset(rows)
        if not valid:
            log_warn(
                f"{label}: not a valid tabular dataset ({reason}); "
                "skipped as untreatable",
                quiet,
            )
            return result([], None, None, None)

    # Read via the raising helper (not ``read_excel``) so a read failure — a
    # real scan error — stays distinguishable from a genuinely empty sheet.
    try:
        df = _read_excel_frame(file_path, sheet_name, label, quiet)
    except Exception as e:
        log_error(label, e, quiet)
        return result([], None, None, None)
    if df.empty:
        return result([], 0, None, None)

    # Post-read validation for .xls/.ods (no streaming available)
    if suffix in _PANDAS_ENGINES:
        header_row = tuple(df.columns)
        data_rows = [
            tuple(row) for row in df.head(_MAX_PREVIEW_ROWS).itertuples(index=False)
        ]
        valid, reason = is_valid_tabular_dataset([header_row, *data_rows])
        if not valid:
            log_warn(
                f"{label}: not a valid tabular dataset ({reason}); "
                "skipped as untreatable",
                quiet,
            )
            return result([], None, None, None)

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
        preview_df = preview_from_pandas(df, preview_rows, label=label, quiet=quiet)
        return result(variables, row_count, freq_table, preview_df)
    finally:
        con.disconnect()
