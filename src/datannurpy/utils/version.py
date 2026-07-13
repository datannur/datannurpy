"""Package version used to key the retry of previously failed scans."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    _VERSION = version("datannurpy")
except PackageNotFoundError:  # pragma: no cover - running from source without metadata
    _VERSION = "unknown"


def scanner_version() -> str:
    """The installed datannurpy version, recorded on datasets whose scan failed
    so a later release re-scans them (see ``Dataset.scan_failed_version``)."""
    return _VERSION


def is_stale_failure(scan_failed_version: str | None) -> bool:
    """Whether a dataset's recorded scan failure predates the running release —
    the current scanner may handle what the failing one could not, so every
    incremental-skip site treats such a dataset as changed and re-scans it once."""
    return scan_failed_version is not None and scan_failed_version != scanner_version()
