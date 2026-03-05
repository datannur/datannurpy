"""Catalog for managing datasets and variables."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa

from .add_database import add_database
from .add_dataset import add_dataset
from .add_folder import add_folder
from .add_metadata import add_metadata
from .exporter.app import export_app
from .finalize import finalize, mark_dataset_modalities_seen, remove_dataset_cascade
from .schema import DatannurDB, Dataset, Variable
from .utils import ModalityManager


class Catalog(DatannurDB):
    """A catalog containing folders, datasets and variables."""

    add_folder = add_folder
    add_dataset = add_dataset
    add_database = add_database
    add_metadata = add_metadata
    export_app = export_app
    finalize = finalize
    _remove_dataset_cascade = remove_dataset_cascade
    _mark_dataset_modalities_seen = mark_dataset_modalities_seen

    def _add_variables(self, variables: list[Variable], dataset_id: str) -> None:
        """Add variables to the catalog."""
        self.variable.add_all(variables)

    def _get_variable_count(self, dataset_id: str) -> int:
        """Get the number of variables for a dataset."""
        return len(self.variable.having.dataset(dataset_id))

    def _get_dataset_by_path(self, data_path: str) -> Dataset | None:
        """Get dataset by data_path (for incremental scan)."""
        results = self.dataset.where("data_path", "==", data_path)
        return results[0] if results else None

    def __init__(
        self,
        *,
        db_path: str | Path | None = None,
        refresh: bool = False,
        freq_threshold: int = 100,
        csv_encoding: str | None = None,
        quiet: bool = False,
        _now: int | None = None,
    ) -> None:
        # Only pass path to DatannurDB if it exists (otherwise create empty)
        resolved_path = Path(db_path) if db_path is not None else None
        load_path: str | None = None
        if resolved_path is not None and resolved_path.exists():
            table_index = resolved_path / "__table__.json"
            if table_index.exists():
                load_path = str(resolved_path)

        # Initialize DatannurDB (loads existing data if path provided and exists)
        super().__init__(load_path)

        self.db_path = resolved_path
        self.refresh = refresh
        self._now = _now if _now is not None else int(time.time())
        self.freq_threshold = freq_threshold
        self.csv_encoding = csv_encoding
        self.quiet = quiet
        self._freq_tables: list[pa.Table] = []
        self.modality_manager = ModalityManager(self)

        # Flag to track if finalize() has been called (idempotent)
        self._finalized: bool = False

        # Track whether data was loaded from existing db (for finalize cleanup)
        self._loaded_from_db: bool = load_path is not None

        # Add _seen column to tables that have it as runtime field (defaults to False)
        # Only add if the table has data (not empty)
        if self._loaded_from_db:
            for table in [
                self.folder,
                self.dataset,
                self.modality,
                self.institution,
                self.tag,
                self.doc,
            ]:
                if (
                    "_seen" in table.runtime_fields
                    and not table.df.is_empty()
                    and "_seen" not in table.df.columns
                ):
                    import polars as pl

                    table._df = table._df.with_columns(pl.lit(False).alias("_seen"))
            # Rebuild modality index from loaded values
            self.modality_manager.rebuild_index()

    def export_db(
        self,
        output_dir: str | Path | None = None,
        *,
        write_js: bool = True,
        quiet: bool | None = None,
    ) -> None:
        """Write all catalog entities to JSON files."""
        self.finalize()

        if output_dir is None:
            if self.db_path is None:
                msg = "output_dir is required when db_path was not set at init"
                raise ValueError(msg)
            output_dir = self.db_path

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        table_names: list[str] = []

        for name, table in self._tables.items():
            df = table.get_persistable_df()
            if df.is_empty():
                continue
            rows = _serialize_df(df)
            _write_json(rows, output_dir / f"{name}.json")
            if write_js:
                _write_jsonjs(df, name, output_dir / f"{name}.json.js")
            table_names.append(name)

        # freq from pyarrow _freq_tables
        if self._freq_tables:
            freq_pa = pa.concat_tables(self._freq_tables)
            if len(freq_pa) > 0:
                freq_df = pl.DataFrame(freq_pa.to_pydict())
                rows = _serialize_df(freq_df)
                _write_json(rows, output_dir / "freq.json")
                if write_js:
                    _write_jsonjs(freq_df, "freq", output_dir / "freq.json.js")
                table_names.append("freq")

        # table registry
        registry = [{"name": n, "last_modif": self._now} for n in sorted(table_names)]
        registry.append({"name": "__table__", "last_modif": self._now})
        _write_json(registry, output_dir / "__table__.json")
        if write_js and table_names:
            _write_jsonjs_raw(registry, "__table__", output_dir / "__table__.json.js")

    def __repr__(self) -> str:
        return (
            f"Catalog(\n"
            f"  folders={len(self.folder.all())},\n"
            f"  datasets={len(self.dataset.all())},\n"
            f"  variables={len(self.variable.all())},\n"
            f"  modalities={len(self.modality.all())},\n"
            f"  values={len(self.value.all())},\n"
            f"  institutions={len(self.institution.all())},\n"
            f"  tags={len(self.tag.all())},\n"
            f"  docs={len(self.doc.all())}\n"
            f")"
        )


def _serialize_df(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to JSON-ready dicts (None and empty lists excluded)."""
    list_cols = {col for col in df.columns if isinstance(df.schema[col], pl.List)}
    rows: list[dict[str, Any]] = []
    for row in df.iter_rows(named=True):
        cleaned: dict[str, Any] = {}
        for key, value in row.items():
            if value is None:
                continue
            if key in list_cols:
                if value:
                    cleaned[key] = ",".join(str(v) for v in value)
            elif isinstance(value, float) and value.is_integer():
                cleaned[key] = int(value)
            else:
                cleaned[key] = value
        rows.append(cleaned)
    return rows


def _write_json(data: list[dict[str, Any]], path: Path) -> None:
    """Write list of dicts to JSON file."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _write_jsonjs(df: pl.DataFrame, table_name: str, path: Path) -> None:
    """Write DataFrame to JSON.js file (array-of-arrays format)."""
    from jsonjsdb.writer import write_table_jsonjs

    write_table_jsonjs(df, table_name, path)


def _write_jsonjs_raw(data: list[dict[str, Any]], table_name: str, path: Path) -> None:
    """Write list of dicts to JSON.js file."""
    if not data:
        return
    columns = list(data[0].keys())
    rows: list[list[Any]] = [columns]
    for record in data:
        rows.append([record.get(key) for key in columns])
    json_array = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    with open(path, "w") as f:
        f.write(f"jsonjs.data['{table_name}'] = {json_array}\n")
