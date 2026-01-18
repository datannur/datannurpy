"""Common utilities for readers."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ..entities import Variable

# Default extensions to scan
DEFAULT_EXTENSIONS = {".csv", ".xlsx", ".xls"}

# Supported file formats: suffix -> delivery_format
SUPPORTED_FORMATS: dict[str, str] = {
    ".csv": "csv",
    ".xlsx": "excel",
    ".xls": "excel",
}


def get_mtime_iso(path: Path) -> str:
    """Get file modification time as YYYY/MM/DD."""
    mtime = path.stat().st_mtime
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return dt.strftime("%Y/%m/%d")


def find_files(
    root: Path,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    recursive: bool,
) -> list[Path]:
    """Find files matching include/exclude patterns."""
    if include is None:
        pattern = "**/*" if recursive else "*"
        candidates = [
            f for f in root.glob(pattern) if f.suffix.lower() in DEFAULT_EXTENSIONS
        ]
    else:
        candidates = []
        for pat in include:
            if recursive and not pat.startswith("**"):
                pat = f"**/{pat}"
            candidates.extend(root.glob(pat))

    candidates = [f for f in candidates if f.is_file()]

    if exclude:
        excluded = set()
        for pat in exclude:
            pat = pat.rstrip("/")
            target = root / pat
            # If it's a directory, exclude all files inside
            if target.is_dir():
                for f in candidates:
                    if target.resolve() in f.resolve().parents:
                        excluded.add(f.resolve())
            # Otherwise use glob for patterns with wildcards
            elif "*" in pat:
                for f in root.glob(f"**/{pat}" if recursive else pat):
                    excluded.add(f.resolve())
            # Exact file match
            elif target.is_file():
                excluded.add(target.resolve())
        candidates = [f for f in candidates if f.resolve() not in excluded]

    return candidates


def find_subdirs(root: Path, files: list[Path]) -> set[Path]:
    """Find subdirectories containing files."""
    subdirs: set[Path] = set()
    for f in files:
        parent = f.parent
        while parent != root:
            subdirs.add(parent)
            parent = parent.parent
    return subdirs


def polars_type_to_str(dtype: pl.DataType) -> str:
    """Convert Polars dtype to string."""
    type_map: dict[type, str] = {
        pl.Int8: "integer",
        pl.Int16: "integer",
        pl.Int32: "integer",
        pl.Int64: "integer",
        pl.UInt8: "integer",
        pl.UInt16: "integer",
        pl.UInt32: "integer",
        pl.UInt64: "integer",
        pl.Float32: "float",
        pl.Float64: "float",
        pl.Boolean: "boolean",
        pl.String: "string",
        pl.Utf8: "string",
        pl.Date: "date",
        pl.Datetime: "datetime",
        pl.Time: "time",
        pl.Duration: "duration",
        pl.Categorical: "categorical",
        pl.Null: "null",
    }

    for polars_type, type_str in type_map.items():
        if isinstance(dtype, polars_type):
            return type_str

    return "unknown"


def build_variables(
    df: pl.DataFrame,
    *,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], pl.DataFrame | None]:
    """Build Variable entities from DataFrame, return (variables, freq_df)."""
    schema = df.schema
    nb_rows = len(df)

    # Compute stats only if needed
    stats: dict[str, tuple[int, int, int]] = {}
    if infer_stats:
        stats_df = df.select(
            [pl.col(col).n_unique().alias(f"{col}__distinct") for col in schema.names()]
            + [
                pl.col(col).null_count().alias(f"{col}__missing")
                for col in schema.names()
            ]
        )
        for col in schema.names():
            nb_distinct = stats_df[f"{col}__distinct"][0]
            nb_missing = stats_df[f"{col}__missing"][0]
            nb_duplicate = nb_rows - nb_distinct
            stats[col] = (nb_distinct, nb_duplicate, nb_missing)

    # Compute freq if threshold is set
    freq_df: pl.DataFrame | None = None
    if freq_threshold is not None and stats:
        eligible_cols = [
            col
            for col, (nb_distinct, _, _) in stats.items()
            if nb_distinct <= freq_threshold
        ]
        if eligible_cols:
            freq_dfs: list[pl.DataFrame] = []
            for col in eligible_cols:
                # Use column name as variable_id placeholder, will be updated in catalog
                vc = (
                    df.select(pl.col(col).value_counts(sort=True))
                    .unnest(col)
                    .rename({col: "value", "count": "freq"})
                    .with_columns(pl.lit(col).alias("variable_id"))
                    .select(["variable_id", "value", "freq"])
                )
                # Cast value to string for consistency
                vc = vc.with_columns(pl.col("value").cast(pl.Utf8))
                freq_dfs.append(vc)
            if freq_dfs:
                freq_df = pl.concat(freq_dfs)

    variables = [
        Variable(
            id=col_name,
            name=col_name,
            dataset_id=dataset_id,
            type=polars_type_to_str(col_type),
            nb_distinct=stats[col_name][0] if stats else None,
            nb_duplicate=stats[col_name][1] if stats else None,
            nb_missing=stats[col_name][2] if stats else None,
        )
        for col_name, col_type in schema.items()
    ]

    return variables, freq_df
