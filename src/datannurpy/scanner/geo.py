"""Shared geo-metadata primitives and the GeoParquet reader.

Format-agnostic helpers (geometry-type normalization, WGS84 bounding-box
reprojection) live here so every geo reader — GeoPackage, GeoParquet, … — shares
one contract: a native ``crs`` (e.g. ``"EPSG:2056"``), an OGC ``geometry_type``,
and a WGS84 ``"west,south,east,north"`` ``bbox`` string.

CRS and geometry type need no extra dependency; reprojecting a non-WGS84 bounding
box to WGS84 uses pyproj (optional ``geo`` extra) and degrades to ``None`` without it.
"""

from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from os import PathLike

    from pyproj import Transformer

# OGC Simple Features geometry types we expose. The abstract "geometry" supertype
# and curve/surface extensions fall outside this set and are dropped (left null) so
# the consumer always sees a known value or nothing.
_GEOMETRY_TYPES = frozenset(
    {
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
    }
)


def normalize_geometry_type(value: object) -> str | None:
    """Lower-case a geometry-type name, keeping only known OGC values."""
    if value is None:
        return None
    geom = str(value).strip().lower()
    return geom if geom in _GEOMETRY_TYPES else None


def wgs84_bbox(
    crs: str | None,
    min_x: Any,
    min_y: Any,
    max_x: Any,
    max_y: Any,
    *,
    cache: dict[str, Any],
) -> str | None:
    """Reproject a native bounding box to WGS84 ``"west,south,east,north"``.

    A WGS84 (EPSG:4326) box passes through unchanged (no pyproj needed); any other
    CRS is reprojected via a pyproj transformer memoized in ``cache`` (one per CRS).
    Returns ``None`` if the box is incomplete, the CRS is unknown, pyproj is
    unavailable, or the reprojection fails.
    """
    try:
        bounds = (float(min_x), float(min_y), float(max_x), float(max_y))
    except (TypeError, ValueError):
        return None
    if any(not math.isfinite(value) for value in bounds):
        return None  # incomplete or empty layer (inf/NaN extent)

    if crs == "EPSG:4326":
        # Native coordinates are already lon/lat in [west, south, east, north].
        return _format_bbox(bounds)
    if crs is None:
        return None

    if crs not in cache:
        cache[crs] = _wgs84_transformer(crs)
    transformer = cache[crs]
    if transformer is None:
        return None
    try:
        projected = transformer.transform_bounds(*bounds)
    except Exception:
        return None
    if any(not math.isfinite(value) for value in projected):
        return None
    return _format_bbox(projected)


def _format_bbox(values: tuple[float, float, float, float]) -> str:
    """Format a (west, south, east, north) tuple as a comma-separated string."""
    return ",".join(f"{round(value, 6)}" for value in values)


def build_geo_fields(
    crs: str | None, geometry_type: object, bounds: Any
) -> dict[str, Any]:
    """Assemble the ``{crs, geometry_type, bbox}`` dataset contract from raw values.

    ``bounds`` is a native ``(min_x, min_y, max_x, max_y)`` sequence reprojected to a
    WGS84 ``bbox`` string, or ``None`` when no extent is available.
    """
    return {
        "crs": crs,
        "geometry_type": normalize_geometry_type(geometry_type),
        "bbox": wgs84_bbox(crs, *bounds, cache={}) if bounds is not None else None,
    }


def _wgs84_transformer(crs: str) -> Transformer | None:
    """Build a pyproj transformer from ``crs`` to WGS84, or None if unavailable."""
    try:
        from pyproj import Transformer
    except ImportError:
        return None
    try:
        return Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    except Exception:
        return None


def extract_geoparquet_geo(path: str | PathLike[str]) -> dict[str, Any] | None:
    """Read GeoParquet spatial metadata into ``{crs, geometry_type, bbox}``.

    GeoParquet stores a JSON ``geo`` key in the Parquet file metadata describing the
    primary geometry column (CRS as PROJJSON, geometry types, native bbox). Returns
    ``None`` when the file is plain Parquet or its geo metadata is unusable. Never
    raises.
    """
    column = _geoparquet_column(path)
    if column is None:
        return None

    # The contract carries a single geometry type; a mixed layer stays null.
    types = column.get("geometry_types")
    geometry_type = types[0] if isinstance(types, list) and len(types) == 1 else None
    bbox = column.get("bbox")
    bounds = bbox if isinstance(bbox, list) and len(bbox) == 4 else None
    return build_geo_fields(_projjson_crs(column.get("crs")), geometry_type, bounds)


def _geoparquet_column(path: str | PathLike[str]) -> dict[str, Any] | None:
    """Return the primary geometry column's GeoParquet metadata, or None."""
    try:
        import pyarrow.parquet as pq

        raw = (pq.read_metadata(path).metadata or {}).get(b"geo")
        if raw is None:
            return None
        geo = json.loads(raw)
        columns = geo["columns"]
        column = columns[geo.get("primary_column") or next(iter(columns))]
    except Exception:
        return None
    return column if isinstance(column, dict) else None


def _projjson_crs(crs: object) -> str | None:
    """Resolve a GeoParquet CRS (PROJJSON, or null = WGS84) to an authority string.

    A null/absent CRS means longitude/latitude WGS84 by spec (so its bbox passes
    through). Otherwise read the PROJJSON ``id`` member (e.g. ``EPSG:2056``) — present
    in real-world files.
    """
    if crs is None:
        return "EPSG:4326"
    if not isinstance(crs, dict):
        return None
    identifier = crs.get("id")
    if not isinstance(identifier, dict):
        return None
    authority = identifier.get("authority")
    code = identifier.get("code")
    if authority is None or code is None:
        return None
    return f"{authority}:{code}"
