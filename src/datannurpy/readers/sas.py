"""SAS reader using pyreadstat and Ibis/DuckDB."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import ibis
import numpy as np
import pandas as pd

from ..entities import Variable
from ._utils import build_variables

try:
    import pyreadstat

    HAS_PYREADSTAT = True
except ImportError:
    HAS_PYREADSTAT = False

if TYPE_CHECKING:
    import pyreadstat


@dataclass
class SasMetadata:
    """Metadata extracted from SAS file."""

    description: str | None = None


def _convert_float_to_int(df: pd.DataFrame) -> pd.DataFrame:
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


def scan_sas(
    path: str | Path,
    *,
    dataset_id: str | None = None,
    infer_stats: bool = True,
    freq_threshold: int | None = None,
) -> tuple[list[Variable], int, ibis.Table | None, SasMetadata]:
    """Scan a SAS file (.sas7bdat) and return (variables, row_count, freq_table, metadata)."""
    if not HAS_PYREADSTAT:
        raise ImportError(
            "pyreadstat is required for SAS support. "
            "Install it with: pip install datannurpy[sas]"
        )

    file_path = Path(path)

    # Read data and metadata using pyreadstat
    df, meta = pyreadstat.read_sas7bdat(file_path)
    column_labels: dict[str, str | None] = meta.column_names_to_labels

    # Extract dataset-level metadata
    sas_metadata = SasMetadata(description=meta.file_label or None)

    # Convert float columns that are actually integers
    df = _convert_float_to_int(df)

    # Convert to Ibis table via DuckDB for stats computation
    con = ibis.duckdb.connect()
    table = con.create_table("sas_data", df)

    row_count: int = table.count().execute()

    variables, freq_table = build_variables(
        table,
        nb_rows=row_count,
        dataset_id=dataset_id,
        infer_stats=infer_stats,
        freq_threshold=freq_threshold,
    )

    # Apply SAS labels as variable descriptions
    for var in variables:
        label = column_labels.get(var.name or var.id)
        if label:
            var.description = label

    return variables, row_count, freq_table, sas_metadata
