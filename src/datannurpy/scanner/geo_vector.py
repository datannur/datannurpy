"""Vector geo-format reader (GeoJSON, Shapefile, …) via pyogrio.

Reads attribute columns into an Arrow table — reusing the standard
schema/stats pipeline — and the layer's spatial metadata (CRS, geometry type,
bounding box) via ``pyogrio.read_info``, mapped through the shared geo contract.
pyogrio (GDAL) ships in the optional ``geo`` extra; the import is lazy so the core
never depends on it.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import ibis
import pyarrow as pa

from ..preview import preview_from_ibis
from ..utils import log_error
from .geo import build_geo_fields
from .utils import build_variables

if TYPE_CHECKING:
    import polars as pl

    from ..schema import Variable

_INSTALL_HINT = (
    "pyogrio is required to read vector geo formats. "
    "Install it with: pip install datannurpy[geo]"
)


def _extension_types_to_storage(table: pa.Table) -> pa.Table:
    """Replace Arrow extension-typed columns by their plain storage type.

    pyogrio annotates the geometry column as ``geoarrow.wkb``; downstream the
    pipeline wants the raw WKB binary, and polars warns on the unknown extension
    type today and will materialize it as an extension dtype in polars 2.0.
    Stripping the annotation here pins the storage behavior regardless of the
    polars version and of whether the geoarrow types are registered.
    """
    fields: list[pa.Field] = []
    columns: list[pa.ChunkedArray] = []
    changed = False
    for i, field in enumerate(table.schema):
        column = table.column(i)
        if isinstance(field.type, pa.ExtensionType):
            columns.append(
                pa.chunked_array(
                    [chunk.storage for chunk in column.chunks],
                    type=field.type.storage_type,
                )
            )
            fields.append(pa.field(field.name, field.type.storage_type))
            changed = True
            continue
        if field.metadata and b"ARROW:extension:name" in field.metadata:
            field = field.remove_metadata()
            changed = True
        fields.append(field)
        columns.append(column)
    if not changed:
        return table
    schema = pa.schema(fields, metadata=table.schema.metadata)
    return pa.Table.from_arrays(columns, schema=schema)


def list_geo_layers(path: str | Path) -> list[str]:
    """Return the layer names of a vector container (e.g. a File Geodatabase)."""
    try:
        from pyogrio import list_layers
    except ImportError as e:
        raise ImportError(_INSTALL_HINT) from e
    return [str(name) for name in list_layers(Path(path))[:, 0]]


def scan_geo_vector(
    path: str | Path,
    *,
    dataset_id: str,
    layer: str | None = None,
    freq_threshold: int | None = None,
    preview_rows: int = 0,
    return_preview: bool = False,
    quiet: bool = False,
    path_label: str | None = None,
) -> tuple[list[Variable], int, Any, dict[str, Any] | None, pl.DataFrame | None]:
    """Scan a vector file/layer into (variables, nb_row, freq_table, geo, preview).

    ``layer`` selects a layer inside a multi-layer container (default: the first);
    ``geo`` is ``{crs, geometry_type, bbox}`` (or ``None`` on read failure); the
    geometry column itself is kept as a binary variable and skipped from stats.
    """
    try:
        from pyogrio import read_info
        from pyogrio.raw import read_arrow
    except ImportError as e:
        raise ImportError(_INSTALL_HINT) from e

    file_path = Path(path)
    label = path_label or file_path.name
    try:
        info = read_info(file_path, layer=layer)
        _, arrow = read_arrow(file_path, layer=layer)
    except Exception as e:
        log_error(label, e, quiet)
        return [], 0, None, None, None

    arrow = _extension_types_to_storage(arrow)

    table = ibis.memtable(arrow)
    nb_row = arrow.num_rows
    variables, freq_table = build_variables(
        table,
        nb_rows=nb_row,
        dataset_id=dataset_id,
        infer_stats=True,
        freq_threshold=freq_threshold,
    )
    preview = (
        preview_from_ibis(table, preview_rows, label=label, quiet=quiet)
        if return_preview
        else None
    )
    geo = build_geo_fields(
        info["crs"] or None, info["geometry_type"], info["total_bounds"]
    )
    return variables, nb_row, freq_table, geo, preview
