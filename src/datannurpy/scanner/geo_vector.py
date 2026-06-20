"""Vector geo-format reader (GeoJSON, …) via pyogrio.

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


def scan_geo_vector(
    path: str | Path,
    *,
    dataset_id: str,
    freq_threshold: int | None = None,
    preview_rows: int = 0,
    return_preview: bool = False,
    quiet: bool = False,
    path_label: str | None = None,
) -> tuple[list[Variable], int, Any, dict[str, Any] | None, pl.DataFrame | None]:
    """Scan a vector file into (variables, nb_row, freq_table, geo, preview).

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
        info = read_info(file_path)
        _, arrow = read_arrow(file_path)
    except Exception as e:
        log_error(label, e, quiet)
        return [], 0, None, None, None

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
