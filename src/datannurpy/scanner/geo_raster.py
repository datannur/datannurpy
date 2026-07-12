"""Raster geo-format reader (GeoTIFF, …) via rasterio.

Each band becomes a Variable (``type="band"``) carrying its value statistics
(min/max/mean/std); the dataset gets ``crs``/``bbox`` plus ``spatial_resolution`` —
the pixel size, in metres for a projected CRS. rasterio (GDAL) ships in the optional
``geo`` extra; the import is lazy so the core never depends on it.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ..schema import Variable
from ..utils import log_error
from .geo import build_geo_fields

_INSTALL_HINT = (
    "rasterio is required to read raster geo formats. "
    "Install it with: pip install datannurpy[geo]"
)


def scan_geo_raster(
    path: str | Path,
    *,
    dataset_id: str,
    quiet: bool = False,
    path_label: str | None = None,
) -> tuple[list[Variable], int | None, dict[str, Any] | None, float | None]:
    """Scan a raster into ``(variables, nb_row, geo, spatial_resolution)``.

    One Variable per band (``type="band"``) carries the band statistics; ``nb_row``
    is the pixel count. ``geo`` and ``spatial_resolution`` are ``None`` on read
    failure; ``spatial_resolution`` is also ``None`` for a non-projected CRS.
    """
    try:
        import rasterio
    except ImportError as e:
        raise ImportError(_INSTALL_HINT) from e

    file_path = Path(path)
    label = path_label or file_path.name
    try:
        with rasterio.open(file_path) as raster:
            crs = _crs_string(raster.crs)
            geo = build_geo_fields(crs, None, tuple(raster.bounds))
            projected = raster.crs is not None and raster.crs.is_projected
            spatial_resolution = abs(raster.res[0]) if projected else None
            nb_row = raster.width * raster.height
            variables = [
                _band_variable(dataset_id, i, desc, stat)
                for i, (desc, stat) in enumerate(
                    zip(raster.descriptions, raster.stats(approx=True)), start=1
                )
            ]
    except Exception as e:
        log_error(label, e, quiet)
        return [], None, None, None
    return variables, nb_row, geo, spatial_resolution


def _crs_string(crs: Any) -> str | None:
    """Authority string (e.g. ``"EPSG:2056"``) for a rasterio CRS, or ``None``."""
    authority = crs.to_authority() if crs is not None else None
    return f"{authority[0]}:{authority[1]}" if authority else None


def _band_variable(
    dataset_id: str, index: int, description: str | None, stat: Any
) -> Variable:
    """Build the Variable for one raster band from its statistics."""
    return Variable(
        id=f"{dataset_id}---band_{index}",
        name=description or f"band_{index}",
        dataset_id=dataset_id,
        type="band",
        min=stat.min,
        max=stat.max,
        mean=stat.mean,
        std=stat.std,
    )
