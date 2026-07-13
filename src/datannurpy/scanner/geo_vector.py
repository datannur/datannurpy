"""Vector geo-format reader (GeoJSON, Shapefile, …) via pyogrio.

Reads attribute columns into an Arrow table — reusing the standard
schema/stats pipeline — and the layer's spatial metadata (CRS, geometry type,
bounding box) via ``pyogrio.read_info``, mapped through the shared geo contract.
pyogrio (GDAL) ships in the optional ``geo`` extra; the import is lazy so the core
never depends on it.
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ibis
import pyarrow as pa

from ..preview import preview_from_ibis
from ..utils import log_debug, log_error, log_warn
from .geo import build_geo_fields
from .utils import build_variables

if TYPE_CHECKING:
    import polars as pl

    from ..schema import Variable

_INSTALL_HINT = (
    "pyogrio is required to read vector geo formats. "
    "Install it with: pip install datannurpy[geo]"
)

# XML-based vector formats eligible for the repair-and-retry fallback below.
_XML_VECTOR_SUFFIXES = {".gpx", ".kml", ".gml"}

# Real-world exporters (portal platforms …) systematically ship XML with two
# text-escaping bugs: a bare '&' inside text (URLs with query strings) and a
# bare '<' inside values ("<25%"). Both are repairable without touching any
# well-formed construct: a '&' not starting a character/entity reference, and
# a '<' not opening markup (a tag-name start character, '/', '!' or '?').
_XML_BARE_AMP_RE = re.compile(
    rb"&(?!(?:amp|lt|gt|quot|apos|#[0-9]{1,7}|#x[0-9A-Fa-f]{1,6});)"
)
_XML_BARE_LT_RE = re.compile(rb"<(?![A-Za-z_:/!?])")


def _extension_types_to_storage(table: pa.Table) -> pa.Table:
    """Replace Arrow extension-typed columns by their plain storage type.

    pyogrio annotates the geometry column as ``geoarrow.wkb``; downstream the
    pipeline wants the raw WKB binary, and polars warns on the unknown extension
    type today and will materialize it as an extension dtype in polars 2.0.
    Stripping the annotation here pins the storage behavior regardless of the
    polars version and of whether the geoarrow types are registered.

    ``BaseExtensionType`` (not ``ExtensionType``) also matches Arrow's canonical
    extension types, which are not Python-registered — pyogrio maps a nested
    GeoJSON property to ``arrow.json`` (string storage), which ibis' memtable
    schema mapping rejects; its storage keeps the values catalogable as text.
    """
    fields: list[pa.Field] = []
    columns: list[pa.ChunkedArray] = []
    changed = False
    for i, field in enumerate(table.schema):
        column = table.column(i)
        if isinstance(field.type, pa.BaseExtensionType):
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


def _require_pyogrio() -> None:
    """Fail fast with an actionable hint when the ``geo`` extra is missing."""
    try:
        import pyogrio  # noqa: F401
    except ImportError as e:
        raise ImportError(_INSTALL_HINT) from e


def list_geo_layers(path: str | Path) -> list[str]:
    """Return the layer names of a vector container (e.g. a File Geodatabase)."""
    _require_pyogrio()
    from pyogrio import list_layers

    return [str(name) for name in list_layers(Path(path))[:, 0]]


def _read_layer(path: Path, layer: str) -> tuple[Any, pa.Table]:
    """Read one layer's spatial metadata and attribute table. ``force_total_bounds``
    only computes an extent when the driver has none at low cost (GPX …) and exits
    immediately for geometry-less layers, so the flag is free elsewhere."""
    from pyogrio import read_info
    from pyogrio.raw import read_arrow

    info = read_info(path, layer=layer, force_total_bounds=True)
    _, arrow = read_arrow(path, layer=layer)
    return info, arrow


def _select_populated_layer(
    path: Path, layer: str | None
) -> tuple[list[str], str, Any, pa.Table] | None:
    """Read the requested layer, or the first *populated* one: multi-layer
    containers scanned as a single dataset (GPX, KML folders …) keep their data
    in later layers when the leading one is empty — a track-recording GPX has no
    waypoints. None when the container exposes no layer at all (a featureless
    KML without a single Folder/Placemark …)."""
    layers = [layer] if layer is not None else list_geo_layers(path)
    if not layers:
        return None
    selected = layers[0]
    info, arrow = _read_layer(path, selected)
    for name in layers[1:]:
        if arrow.num_rows:
            break
        selected = name
        info, arrow = _read_layer(path, selected)
    return layers, selected, info, arrow


def _repair_xml_bytes(data: bytes) -> tuple[bytes, int]:
    """Escape bare ``&`` and ``<`` in malformed XML text; returns the repaired
    bytes and the number of substitutions (0 → nothing repairable)."""
    repaired, amp_count = _XML_BARE_AMP_RE.subn(b"&amp;", data)
    repaired, lt_count = _XML_BARE_LT_RE.subn(b"&lt;", repaired)
    return repaired, amp_count + lt_count


def _retry_with_repaired_xml(
    path: Path, layer: str | None, label: str, quiet: bool
) -> tuple[list[str], str, Any, pa.Table] | None:
    """Retry a failed XML vector read on a repaired temp copy — the original
    file stays untouched. Returns the successful read, or None when the file is
    not repairable this way (nothing to substitute, non-ASCII-compatible
    encoding, or the repaired copy still fails to read)."""
    try:
        data = path.read_bytes()
    except OSError:
        return None
    # The byte-level repair assumes an ASCII-compatible encoding; a UTF-16 file
    # (NUL bytes near the start) would be corrupted by ASCII substitutions.
    if b"\x00" in data[:1024]:
        return None
    repaired, substitutions = _repair_xml_bytes(data)
    if not substitutions:
        return None
    with tempfile.TemporaryDirectory() as tmp_dir:
        # Same basename: GDAL picks its driver from the extension.
        fixed = Path(tmp_dir) / path.name
        fixed.write_bytes(repaired)
        try:
            read = _select_populated_layer(fixed, layer)
        except Exception:
            return None
    if read is None:
        return None
    log_warn(f"{label}: repaired malformed XML ({substitutions} substitutions)", quiet)
    return read


def _log_multilayer(
    path: Path, label: str, layers: list[str], selected: str, quiet: bool
) -> None:
    """Surface what a multi-layer container scan did *not* read. A GPX's five
    layers are fixed facets of one recording — debug only; a GML/KML container
    holds distinct feature classes, so dropping them must be visible."""
    message = (
        f"{label}: {len(layers)} layers; scanned {selected!r} (the first non-empty one)"
    )
    if path.suffix.lower() == ".gpx":
        log_debug(message, quiet)
        return
    skipped = ", ".join(repr(name) for name in layers if name != selected)
    log_warn(f"{message} — skipped: {skipped}", quiet)


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
) -> tuple[list[Variable], int | None, Any, dict[str, Any] | None, pl.DataFrame | None]:
    """Scan a vector file/layer into (variables, nb_row, freq_table, geo, preview).

    ``layer`` selects a layer inside a multi-layer container (default: the first
    non-empty one); ``geo`` is ``{crs, geometry_type, bbox}`` (or ``None`` on read
    failure); the geometry column itself is kept as a binary variable and skipped
    from stats.
    """
    _require_pyogrio()
    file_path = Path(path)
    label = path_label or file_path.name
    try:
        read = _select_populated_layer(file_path, layer)
    except Exception as e:
        # Systematically malformed XML (unescaped '&'/'<' from real-world
        # exporters) gets one conservative repair-and-retry before failing.
        read = (
            _retry_with_repaired_xml(file_path, layer, label, quiet)
            if file_path.suffix.lower() in _XML_VECTOR_SUFFIXES
            else None
        )
        if read is None:
            log_error(label, e, quiet)
            return [], None, None, None, None
    if read is None:
        # A featureless container has no layers at all: an empty dataset, not a
        # scanner failure — report zero rows instead of failing.
        log_debug(f"{label}: no layers found", quiet)
        return [], 0, None, None, None
    layers, selected, info, arrow = read
    if len(layers) > 1:
        _log_multilayer(file_path, label, layers, selected, quiet)

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
