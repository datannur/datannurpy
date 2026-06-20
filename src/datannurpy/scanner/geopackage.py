"""GeoPackage geo-metadata extraction (CRS, geometry type, bounding box).

A GeoPackage is a SQLite database, so datannur already scans its data tables. Its
spatial metadata lives in the standard ``gpkg_*`` tables, read here with plain SQL.
CRS and geometry type need no extra dependency; the WGS84 bounding box is reprojected
from the native CRS via pyproj (optional ``geo`` extra).
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from .database import build_table_data_path

if TYPE_CHECKING:
    import ibis
    from pyproj import Transformer

    from ..catalog import Catalog

# OGC Simple Features geometry types we expose, matching the dataset
# ``geometry_type`` contract. GeoPackage stores them upper-cased; the abstract
# "GEOMETRY" supertype and curve/surface extensions fall outside this set and are
# dropped (left null) so the consumer always sees a known value or nothing.
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

_GPKG_TABLES_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='table' AND name IN "
    "('gpkg_contents', 'gpkg_geometry_columns', 'gpkg_spatial_ref_sys')"
)


def _geo_query(has_srs: bool) -> str:
    """Per-layer query; ``gpkg_spatial_ref_sys`` is optional (else CRS is unknown)."""
    srs_cols = "s.organization, s.organization_coordsys_id" if has_srs else "NULL, NULL"
    srs_join = (
        "LEFT JOIN gpkg_spatial_ref_sys s ON COALESCE(g.srs_id, c.srs_id) = s.srs_id "
        if has_srs
        else ""
    )
    return (
        f"SELECT c.table_name, g.geometry_type_name, {srs_cols}, "
        "c.min_x, c.min_y, c.max_x, c.max_y "
        "FROM gpkg_contents c "
        "JOIN gpkg_geometry_columns g ON c.table_name = g.table_name "
        f"{srs_join}WHERE c.data_type = 'features'"
    )


def extract_geopackage_geo(con: ibis.BaseBackend) -> dict[str, dict[str, Any]]:
    """Map each GeoPackage feature layer to its ``{crs, geometry_type, bbox}``.

    ``bbox`` is the WGS84 ``"west,south,east,north"`` envelope (reprojected from the
    layer's native CRS via pyproj; ``None`` if pyproj is unavailable and the CRS is
    not already WGS84). Returns an empty dict when the connection is not a GeoPackage
    (or has no usable spatial metadata). Never raises.
    """
    raw_sql = getattr(con, "raw_sql", None)
    if raw_sql is None:
        return {}

    try:
        present = {row[0] for row in raw_sql(_GPKG_TABLES_QUERY).fetchall()}
    except Exception:
        return {}
    if "gpkg_contents" not in present or "gpkg_geometry_columns" not in present:
        return {}

    try:
        rows = raw_sql(_geo_query("gpkg_spatial_ref_sys" in present)).fetchall()
    except Exception:
        return {}

    result: dict[str, dict[str, Any]] = {}
    transformers: dict[str, Any] = {}  # one pyproj Transformer per CRS, reused
    for table_name, geom_type, organization, coordsys_id, *bounds in rows:
        if table_name is None:
            continue
        crs = _build_crs(organization, coordsys_id)
        result[str(table_name)] = {
            "crs": crs,
            "geometry_type": _normalize_geometry_type(geom_type),
            "bbox": _wgs84_bbox(crs, *bounds, cache=transformers),
        }
    return result


def apply_geopackage_geo(
    catalog: Catalog,
    con: ibis.BaseBackend,
    backend_name: str,
    db_name: str,
) -> int:
    """Attach CRS / geometry type / bbox to datasets scanned from a GeoPackage.

    Matches GeoPackage layers to already-scanned datasets by their ``data_path``.
    Returns the number of datasets enriched.
    """
    geo = extract_geopackage_geo(con)
    if not geo:
        return 0

    changed: list[Any] = []
    for table_name, meta in geo.items():
        updates = {key: value for key, value in meta.items() if value}
        if not updates:
            continue
        # GeoPackage is single-schema (schema is None for SQLite).
        data_path = build_table_data_path(backend_name, db_name, None, table_name)
        dataset = catalog.dataset.get_by("data_path", data_path)
        if dataset is None:
            continue
        for field, value in updates.items():
            setattr(dataset, field, value)
        changed.append(dataset)

    if changed:
        # One insert-or-replace rebuild instead of N per-row updates.
        catalog.dataset.upsert_all(changed)
    return len(changed)


def _build_crs(organization: object, coordsys_id: object) -> str | None:
    """Build an authority CRS string (e.g. "EPSG:2056") from gpkg_spatial_ref_sys."""
    if organization is None or coordsys_id is None:
        return None
    org = str(organization).strip().upper()
    if not org or org == "NONE":
        return None
    return f"{org}:{coordsys_id}"


def _normalize_geometry_type(geom_type_name: object) -> str | None:
    """Lower-case the GeoPackage geometry type, keeping only known OGC values."""
    if geom_type_name is None:
        return None
    geom = str(geom_type_name).strip().lower()
    return geom if geom in _GEOMETRY_TYPES else None


def _wgs84_bbox(
    crs: str | None,
    min_x: Any,
    min_y: Any,
    max_x: Any,
    max_y: Any,
    *,
    cache: dict[str, Any],
) -> str | None:
    """Reproject the native bounding box to WGS84 ``"west,south,east,north"``.

    A WGS84 (EPSG:4326) layer passes through unchanged (no pyproj needed); any other
    CRS is reprojected via a pyproj transformer memoized in ``cache`` (one per CRS).
    Returns ``None`` if the box is incomplete, the CRS is unknown, pyproj is
    unavailable, or the reprojection fails.
    """
    try:
        bounds = (float(min_x), float(min_y), float(max_x), float(max_y))
    except (TypeError, ValueError):
        return None

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
