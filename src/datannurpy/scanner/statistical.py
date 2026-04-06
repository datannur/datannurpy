"""Statistical file reader (SAS, SPSS, Stata) using pyreadstat and Ibis/DuckDB."""

from __future__ import annotations

import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ibis
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ..schema import Variable
from ..utils import log_error, log_warn
from .utils import build_variables

if TYPE_CHECKING:
    from collections.abc import Generator

    import pandas as pd


@dataclass
class StatisticalMetadata:
    """Metadata extracted from statistical file."""

    description: str | None = None


def _get_reader(suffix: str) -> Any:
    """Return pyreadstat reader for a suffix."""
    import pyreadstat

    return {
        ".sas7bdat": pyreadstat.read_sas7bdat,
        ".sav": pyreadstat.read_sav,
        ".dta": pyreadstat.read_dta,
    }[suffix]


def _apply_labels(
    variables: list[Variable], column_labels: dict[str, str | None]
) -> None:
    """Apply column labels from statistical file metadata to variables."""
    for var in variables:
        label = column_labels.get(var.name or var.id)
        if label:
            var.description = label


_READSTAT_TYPE_MAP: dict[str, str] = {"double": "float", "string": "string"}


def _apply_types(variables: list[Variable], meta_types: dict[str, str]) -> None:
    """Apply readstat variable types to variables."""
    for var in variables:
        raw = meta_types.get(var.name)
        if raw:
            var.type = _READSTAT_TYPE_MAP.get(raw, raw)


def convert_float_to_int(df: pd.DataFrame) -> pd.DataFrame:
    """Convert float columns that contain only integer values to int64."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == np.float64:
            # Check if all non-null values are integers
            non_null = df[col].dropna()
            if len(non_null) > 0 and (non_null == non_null.astype(np.int64)).all():
                # Convert to nullable Int64 to preserve NaN as <NA>
                df[col] = df[col].astype("Int64")
    return df


def read_statistical(path: str | Path, *, quiet: bool = False) -> pd.DataFrame | None:
    """Read a statistical file (SAS/SPSS/Stata) into a pandas DataFrame."""
    file_path = Path(path)
    try:
        reader = _get_reader(file_path.suffix.lower())
    except ImportError:
        log_warn(
            "pyreadstat is required for SAS/SPSS/Stata support. "
            "Install it with: pip install datannurpy[stat]",
            quiet,
        )
        return None
    except KeyError:
        return None

    try:
        df, _ = reader(file_path)
        return convert_float_to_int(df)
    except Exception as e:
        log_error(file_path.name, e, quiet)
        return None


def scan_statistical(
    path: str | Path,
    *,
    dataset_id: str,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
    sample_size: int | None = None,
    quiet: bool = False,
) -> tuple[list[Variable], int, int | None, pa.Table | None, StatisticalMetadata]:
    """Scan a statistical file (SAS/SPSS/Stata) and return (variables, row_count, actual_sample_size, freq_table, metadata)."""
    file_path = Path(path)
    try:
        reader = _get_reader(file_path.suffix.lower())
    except ImportError as e:
        msg = (
            "pyreadstat is required for SAS/SPSS/Stata support. "
            "Install it with: pip install datannurpy[stat]"
        )
        raise ImportError(msg) from e

    # Read metadata only (0 RAM, instant)
    try:
        _, meta = reader(file_path, metadataonly=True)
    except Exception as e:
        log_error(file_path.name, e, quiet)
        return [], 0, None, None, StatisticalMetadata()

    column_labels: dict[str, str | None] = meta.column_names_to_labels
    stat_metadata = StatisticalMetadata(description=meta.file_label or None)

    if not infer_stats or (meta.number_rows or 0) == 0:
        variables = [
            Variable(id=f"{dataset_id}---{col}", name=col, dataset_id=dataset_id)
            for col in meta.column_names
        ]
        _apply_labels(variables, column_labels)
        _apply_types(variables, meta.readstat_variable_types)
        row_count = meta.number_rows or 0
        if not infer_stats:
            return variables, row_count, None, None, stat_metadata
        return variables, 0, None, None, stat_metadata

    try:
        with _stat_to_parquet(reader, file_path) as tmp_path:
            variables, row_count, actual_sample_size, freq_table = _build_from_parquet(
                tmp_path,
                dataset_id=dataset_id,
                freq_threshold=freq_threshold,
                sample_size=sample_size,
            )
        _apply_labels(variables, column_labels)
        return variables, row_count, actual_sample_size, freq_table, stat_metadata
    except Exception as e:
        log_error(file_path.name, e, quiet)
        return [], 0, None, None, StatisticalMetadata()


def _build_from_parquet(
    parquet_path: Path,
    *,
    dataset_id: str,
    freq_threshold: int | None,
    sample_size: int | None,
) -> tuple[list[Variable], int, int | None, pa.Table | None]:
    """Read temp Parquet with DuckDB, fix types, build variables."""
    con = ibis.duckdb.connect()
    try:
        table = con.read_parquet(str(parquet_path))
        table = _fix_parquet_types(con, table, parquet_path)
        row_count = int(table.count().to_pyarrow().as_py())

        actual_sample_size: int | None = None
        if sample_size is not None and row_count > sample_size:
            source = table.get_name()
            cursor: Any = con.raw_sql(
                f"SELECT * FROM {source} USING SAMPLE reservoir({sample_size} ROWS)"
            )
            sample_arrow = cursor.fetch_arrow_table()
            actual_sample_size = len(sample_arrow)
            sample_table = ibis.memtable(sample_arrow)

            variables, freq_table = build_variables(
                sample_table,
                nb_rows=actual_sample_size,
                dataset_id=dataset_id,
                infer_stats=True,
                freq_threshold=freq_threshold,
                full_table=table,
                full_nb_rows=row_count,
            )
        else:
            variables, freq_table = build_variables(
                table,
                nb_rows=row_count,
                dataset_id=dataset_id,
                infer_stats=True,
                freq_threshold=freq_threshold,
            )

        return variables, row_count, actual_sample_size, freq_table
    finally:
        con.disconnect()


@contextmanager
def _stat_to_parquet(
    reader: Any, file_path: Path, chunksize: int = 100_000
) -> Generator[Path, None, None]:
    """Stream a statistical file to a temporary Parquet file."""
    import pyreadstat

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()

    try:
        chunks = pyreadstat.read_file_in_chunks(reader, file_path, chunksize=chunksize)
        first_df, _ = next(chunks)
        first_arrow = pa.Table.from_pandas(first_df, preserve_index=False)
        writer = pq.ParquetWriter(tmp_path, first_arrow.schema, compression="none")
        writer.write_table(first_arrow)
        for chunk_df, _ in chunks:
            writer.write_table(pa.Table.from_pandas(chunk_df, preserve_index=False))
        writer.close()
        yield tmp_path
    finally:
        tmp_path.unlink(missing_ok=True)


def _fix_parquet_types(con: Any, table: ibis.Table, parquet_path: Path) -> ibis.Table:
    """Fix types after Parquet round-trip: detect int columns, fix TIMETZ to TIME."""
    schema = table.schema()
    float_cols = [name for name, dtype in schema.items() if dtype.is_float64()]
    has_time = any(dtype.is_time() for dtype in schema.values())

    if not float_cols and not has_time:
        return table

    # Detect TIMETZ columns (Parquet TIME sets isAdjustedToUTC=true)
    timetz_cols: list[str] = []
    if has_time:
        cursor = con.raw_sql(
            f"SELECT column_name, column_type FROM "
            f"(DESCRIBE SELECT * FROM read_parquet('{parquet_path}'))"
        )
        timetz_cols = [
            row[0] for row in cursor.fetchall() if row[1] == "TIME WITH TIME ZONE"
        ]

    # Detect float64 columns that contain only integer values
    int_cols: list[str] = []
    if float_cols:
        checks = ", ".join(
            f"bool_and({col} IS NULL OR {col} = CAST({col} AS BIGINT)) AS {col}"
            for col in float_cols
        )
        cursor = con.raw_sql(f"SELECT {checks} FROM read_parquet('{parquet_path}')")
        row = cursor.fetchone()
        int_cols = [col for col, is_int in zip(float_cols, row) if is_int]

    if not timetz_cols and not int_cols:
        return table

    # Build SQL view with proper casts
    col_exprs = []
    for name in table.columns:
        if name in timetz_cols:
            col_exprs.append(f'"{name}"::TIME AS "{name}"')
        elif name in int_cols:
            col_exprs.append(f'"{name}"::BIGINT AS "{name}"')
        else:
            col_exprs.append(f'"{name}"')

    view_sql = f"SELECT {', '.join(col_exprs)} FROM read_parquet('{parquet_path}')"
    con.raw_sql(f"CREATE OR REPLACE VIEW _stat_fixed AS {view_sql}")
    return con.table("_stat_fixed")
