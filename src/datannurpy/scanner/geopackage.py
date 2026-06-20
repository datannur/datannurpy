"""GeoPackage geo-metadata extraction (CRS, geometry type, bounding box).

A GeoPackage is a SQLite database, so datannur already scans its data tables. Its
spatial metadata lives in the standard ``gpkg_*`` tables, read here with plain SQL.
CRS and geometry type need no extra dependency; the WGS84 bounding box is reprojected
from the native CRS via pyproj (optional ``geo`` extra).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .database import build_table_data_path
from .geo import normalize_geometry_type, wgs84_bbox

if TYPE_CHECKING:
    import ibis

    from ..catalog import Catalog

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
            "geometry_type": normalize_geometry_type(geom_type),
            "bbox": wgs84_bbox(crs, *bounds, cache=transformers),
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
