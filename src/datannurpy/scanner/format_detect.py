"""Resolve a dataset's delivery format from its path, an explicit override, or —
for remote sources without a usable extension — a best-effort detection cascade.

Local paths are trusted to carry a correct extension and fail fast otherwise. Remote
sources (data-portal APIs, dynamic endpoints) routinely expose no extension, so a
cascade of increasingly costly signals is tried, cheapest first:

1. an explicit ``format`` override — deterministic, zero I/O, and a perf shortcut when
   the caller already knows the format;
2. the filename extension (query string ignored);
3. the last path segment used as a format token (``.../multiplelevels/CSV``);
4. a ``?format=`` query parameter;
5. the HTTP ``Content-Type`` — one metadata request;
6. content sniffing of the first bytes — only when the scan depth already reads content.

Steps 1-5 are deterministic. Only step 6 is best-effort (it emits a ``WARN``); it is
skipped at ``depth: dataset``, which reads no content.
"""

from __future__ import annotations

import codecs
from pathlib import PurePosixPath
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, parse_qs

from ..errors import ConfigError
from ..utils.log import log_warn
from .archive import (
    unsupported_zip_error,
    zip_member_list,
    zip_scannable_member,
)
from .utils import SUPPORTED_FORMATS, is_zip, supported_format_for

if TYPE_CHECKING:
    from .filesystem import FileSystem
    from .utils import FsPath

# Accepted spellings for an explicit ``format`` / a detected token → delivery_format.
# Derived from SUPPORTED_FORMATS so the two never drift: every extension without its
# leading dot (``xlsx``, ``pq``, ``shp`` …) plus every delivery_format as its own
# identity (``excel``, ``parquet`` …).
_FORMAT_ALIASES: dict[str, str] = {
    **{ext.lstrip("."): fmt for ext, fmt in SUPPORTED_FORMATS.items()},
    **{fmt: fmt for fmt in SUPPORTED_FORMATS.values()},
}

# Canonical delivery_format names, for user-facing messages.
_VALID_FORMATS: list[str] = sorted(set(SUPPORTED_FORMATS.values()))


def _default_extensions() -> dict[str, str]:
    """One canonical extension per delivery_format (first spelling in SUPPORTED_FORMATS
    wins: excel→.xlsx, parquet→.parquet, geotiff→.tif)."""
    result: dict[str, str] = {}
    for ext, fmt in SUPPORTED_FORMATS.items():
        result.setdefault(fmt, ext)
    return result


# delivery_format → default extension, and bare token spelling → extension
# (``xls`` → ``.xls``), both derived from SUPPORTED_FORMATS so they never drift.
_DEFAULT_EXTENSION: dict[str, str] = _default_extensions()
_EXTENSION_SPELLINGS: dict[str, str] = {
    ext.lstrip("."): ext for ext in SUPPORTED_FORMATS
}

# OpenDocument spreadsheet media type — both the HTTP Content-Type and, prefixed by
# ``mimetype``, the first bytes of an ``.ods`` file (its zip stores that member first,
# uncompressed, per the ODF spec), which content sniffing relies on.
_ODS_MIMETYPE = "application/vnd.oasis.opendocument.spreadsheet"

# HTTP ``Content-Type`` → delivery_format. Deliberately limited to unambiguous media
# types: ambiguous ones (``application/vnd.ms-excel`` is sent for CSV too, generic
# ``application/octet-stream``/``application/json``/``text/html``) are left to sniffing.
_CONTENT_TYPE_FORMATS: dict[str, str] = {
    "text/csv": "csv",
    "application/csv": "csv",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "excel",
    _ODS_MIMETYPE: "ods",
    "application/parquet": "parquet",
    "application/x-parquet": "parquet",
    "application/geo+json": "geojson",
    "application/vnd.google-earth.kml+xml": "kml",
    "application/gpx+xml": "gpx",
    "image/tiff": "geotiff",
}

# On an OGC endpoint (``service=WFS`` / ``request=GetFeature``), plain JSON reliably
# means GeoJSON — unlike a generic ``?format=json`` API toggle, which stays unmapped.
_WFS_OUTPUT_FORMATS: dict[str, str] = {
    "json": "geojson",
    "application/json": "geojson",
}

_SNIFF_BYTES = 512


def normalize_format(explicit: str) -> str:
    """Map a user-supplied ``format`` (a delivery format or extension spelling) to a
    canonical delivery_format, or raise ``ConfigError`` listing the valid names."""
    fmt = _FORMAT_ALIASES.get(explicit.strip().lower().lstrip("."))
    if fmt is None:
        raise ConfigError(
            f"Unknown format: {explicit!r}. Valid formats: {', '.join(_VALID_FORMATS)}"
        )
    return fmt


def _clean_segment(path_name: str) -> str:
    """Last path segment stripped of any query string / fragment."""
    return path_name.split("?", 1)[0].split("#", 1)[0]


def format_from_extension(path_name: str) -> str | None:
    """delivery_format from a filename extension, ignoring any URL query string and
    seeing through a decompressible content-compression suffix (``sales.csv.gz`` → csv)."""
    return supported_format_for(_clean_segment(path_name))


def canonical_extension(path_name: str, delivery_format: str) -> str:
    """A filesystem-safe extension (with leading dot) for the resolved format, used to
    name the local temp copy of a downloaded remote file so suffix-sensitive readers
    (the Excel engine, pyogrio) work even when the URL has no extension or a query
    string. Prefers the URL's own extension (authoritative for ``.xls`` vs ``.xlsx``),
    then the last-segment token (``.../results/xls`` → ``.xls``), else the default."""
    segment = _clean_segment(path_name)
    suffix = PurePosixPath(segment).suffix.lower()
    if suffix in SUPPORTED_FORMATS:
        return suffix
    return (
        _EXTENSION_SPELLINGS.get(segment.lower()) or _DEFAULT_EXTENSION[delivery_format]
    )


def format_from_token(path_name: str) -> str | None:
    """delivery_format when the extension-less last segment *is* a format token, e.g.
    ``.../multiplelevels/CSV`` or ``.../results/xls`` on data-portal API endpoints."""
    return _FORMAT_ALIASES.get(_clean_segment(path_name).lower())


def _has_param(params: dict[str, list[str]], key: str, value: str) -> bool:
    """Whether the query carries ``key=value`` (both compared case-insensitively)."""
    return any(v.strip().lower() == value for v in params.get(key, []))


def format_from_query(url: str) -> str | None:
    """delivery_format from a ``?format=``/``?fmt=`` or WFS ``?outputFormat=`` query
    parameter (keys matched case-insensitively). Values may be format tokens (``csv``,
    ``xlsx``) or media types (``text/csv``); on an OGC request, ``outputFormat=json``
    additionally maps to GeoJSON."""
    query = urlsplit(url).query
    if not query:
        return None
    params = {key.lower(): values for key, values in parse_qs(query).items()}
    is_ogc = _has_param(params, "service", "wfs") or _has_param(
        params, "request", "getfeature"
    )
    for key in ("format", "fmt", "outputformat"):
        for value in params.get(key, []):
            token = value.strip().lower().lstrip(".")
            fmt = _FORMAT_ALIASES.get(token) or _CONTENT_TYPE_FORMATS.get(token)
            if fmt is None and key == "outputformat" and is_ogc:
                fmt = _WFS_OUTPUT_FORMATS.get(token)
            if fmt is not None:
                return fmt
    return None


def content_type_to_format(fs: FileSystem, path: FsPath) -> str | None:
    """delivery_format from the resource's HTTP ``Content-Type`` (one info request)."""
    try:
        info = fs.info(str(path))
    except (OSError, ValueError):  # backend exposes no usable metadata
        return None
    content_type = info.get("mimetype")
    if content_type is None:
        content_type = next(
            (v for k, v in info.items() if k.lower() == "content-type"), None
        )
    if not isinstance(content_type, str):
        return None
    return _CONTENT_TYPE_FORMATS.get(content_type.split(";", 1)[0].strip().lower())


def _looks_like_csv(header: bytes) -> bool:
    """Best-effort test that ``header`` is delimited text: at least a header line and
    one data line sharing a consistent, non-zero count of the same delimiter."""
    text = header.decode("utf-8", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()][:10]
    if len(lines) < 2:
        return False
    for delimiter in (",", ";", "\t", "|"):
        counts = [line.count(delimiter) for line in lines]
        if counts[0] >= 1 and len(set(counts)) == 1:
            return True
    return False


def sniff_format(header: bytes) -> str | None:
    """Best-effort delivery_format from the first bytes of content.

    Reliable binary signatures (Zip→xlsx/ods, OLE2→xls, ``PAR1``→parquet) resolve
    unambiguously. Text is classified as CSV only after HTML/XML and JSON are ruled out
    and a coherent delimiter is found — a genuine guess, so callers should surface it.
    """
    if header.startswith(b"PK\x03\x04"):
        # An OpenDocument zip starts with an uncompressed ``mimetype`` member, so the
        # media type sits in the first bytes; any other zip is treated as xlsx.
        if b"mimetype" + _ODS_MIMETYPE.encode() in header:
            return "ods"
        return "excel"
    if header.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "excel"
    if header.startswith(b"PAR1"):
        return "parquet"
    probe = header.replace(b"\x00", b"").lstrip(b" \t\r\n")
    for bom in (codecs.BOM_UTF8, codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
        if probe.startswith(bom):
            probe = probe[len(bom) :]
            break
    probe = probe.lstrip(b" \t\r\n")
    if not probe or probe[:1] in (b"<", b"{", b"["):
        return None  # HTML/XML or JSON — not a delimited table we can trust
    return "csv" if _looks_like_csv(probe) else None


def _sniff_remote(fs: FileSystem, path: FsPath, label: str, quiet: bool) -> str | None:
    """Read the first bytes of a remote resource and sniff its format, warning when a
    best-effort guess is used so the uncertainty is visible in the log."""
    try:
        with fs.open(str(path), "rb") as f:
            header = f.read(_SNIFF_BYTES)
    except (OSError, ValueError):
        return None
    fmt = sniff_format(header)
    if fmt is not None:
        log_warn(
            f"{label}: format not declared; detected {fmt!r} by content sniffing. "
            f"Set format: to be explicit.",
            quiet,
        )
    return fmt


def resolve_delivery_format(
    path_name: str,
    *,
    explicit_format: str | None,
    fs: FileSystem | None,
    remote_path: FsPath,
    allow_content_sniff: bool,
    quiet: bool,
) -> str:
    """Resolve the delivery_format for a dataset (see module docstring for the cascade).

    Raises ``ConfigError`` when nothing conclusive is found, with a message inviting the
    caller to set ``format:``.
    """
    # 1. Explicit override — deterministic, zero I/O.
    if explicit_format is not None:
        return normalize_format(explicit_format)

    # 2. Filename extension (query string ignored) — the only trusted signal locally.
    fmt = format_from_extension(path_name)
    if fmt is not None:
        return fmt

    # 2b. A .zip carries no format in its name — classify it by inspecting the archive's
    # members (exactly one scannable file: a Shapefile, a CSV, an Excel file …). Runs
    # for local and remote, and at every depth, since the delivery_format cannot be
    # known any other way. A .zip-named resource that is not actually an archive (a
    # misnamed endpoint serving plain data) falls through to the regular cascade.
    if is_zip(path_name):
        names = zip_member_list(remote_path, fs)
        if names is not None:
            selected = zip_scannable_member(names)
            if selected is None:
                raise unsupported_zip_error(path_name, names)
            return selected[1]

    # Auto-detection is remote-only: local extensions are reliable, so fail fast.
    if fs is None or fs.is_local:
        raise _unsupported_error(path_name)

    # 3. + 4. Free URL-shape signals: a format token as the last segment, or ?format=.
    fmt = format_from_token(path_name) or format_from_query(str(remote_path))
    if fmt is not None:
        return fmt

    # 5. HTTP Content-Type — one metadata request.
    fmt = content_type_to_format(fs, remote_path)
    if fmt is not None:
        return fmt

    # 6. Content sniffing — reads bytes, so only when the depth already reads content.
    if allow_content_sniff:
        fmt = _sniff_remote(fs, remote_path, path_name, quiet)
        if fmt is not None:
            return fmt

    # 7. Nothing conclusive → actionable failure.
    raise ConfigError(
        f"Could not detect the format of {remote_path}. "
        f"Specify it with format: (one of {', '.join(_VALID_FORMATS)})."
    )


def _unsupported_error(path_name: str) -> ConfigError:
    """The strict extension-based error, preserved for local paths."""
    suffix = PurePosixPath(_clean_segment(path_name)).suffix.lower()
    return ConfigError(
        f"Unsupported format: {suffix}. Supported: {', '.join(SUPPORTED_FORMATS.keys())}"
    )
