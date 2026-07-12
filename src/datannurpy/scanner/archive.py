"""Zip-archive handling — the standard distribution form of a Shapefile, and the
common ``data.csv.zip`` publication pattern of open-data portals.

A Shapefile is inherently multi-file (``.shp`` + ``.shx``/``.dbf``/``.prj`` …), so
open-data portals (IGN, Census TIGER, Eurostat, ArcGIS Hub) ship it as a single
``.zip``; tabular files (CSV, Excel, Parquet) are zipped the same way. Unlike gzip, a
``.zip`` name says nothing about its content, so the archive's central directory is
inspected to classify it. Exactly one scannable member is supported per archive;
multi-dataset archives are out of scope (each would need its own dataset identity).
"""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from ..compression import DecompressionLimitError, decompressed_cap, is_gzipped
from ..errors import ConfigError
from .utils import (
    DEFAULT_EXCLUDE_DIRS,
    DEFAULT_EXCLUDE_PREFIXES,
    supported_format_for,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from .filesystem import FileSystem
    from .utils import FsPath

_COPY_CHUNK = 1 << 20  # 1 MiB

# Junk basename prefixes inside archives: the folder-scan set plus AppleDouble
# resource forks ("._x.csv"), which macOS Finder adds under __MACOSX/ when zipping.
_JUNK_MEMBER_PREFIXES = (*DEFAULT_EXCLUDE_PREFIXES, "._")


def is_zip(name: str) -> bool:
    """Whether ``name`` denotes a zip archive (``*.zip``), ignoring any URL query
    string or fragment (``data.zip?token=…``)."""
    clean = name.split("?", 1)[0].split("#", 1)[0]
    return PurePosixPath(clean).suffix.lower() == ".zip"


def _member_basename(name: str) -> str:
    """A member's basename with both separator conventions honoured — some Windows
    tools write non-conformant ``\\``-separated member names."""
    return PurePosixPath(name.replace("\\", "/")).name


def _is_junk_member(name: str) -> bool:
    """Whether an archive member is packaging junk (``__MACOSX/`` resource forks,
    Office lock files …) rather than data, mirroring the folder-scan exclusions."""
    parts = PurePosixPath(name.replace("\\", "/")).parts
    if any(part in DEFAULT_EXCLUDE_DIRS for part in parts):
        return True
    return _member_basename(name).startswith(_JUNK_MEMBER_PREFIXES)


def zip_scannable_member(names: list[str]) -> tuple[str, str] | None:
    """The single scannable member of an archive as ``(name, delivery_format)``, or
    None when there isn't exactly one (zero → nothing scannable; several → ambiguous,
    out of scope). Packaging junk (``__MACOSX/`` …) is ignored.

    Shapefile sidecars (``.shx``/``.dbf``/``.prj`` …) are not supported formats
    themselves, so a zipped Shapefile resolves to its lone ``.shp`` member — which
    also wins over extra data members (a codebook CSV shipped next to the ``.shp``),
    the Shapefile being what such archives distribute."""
    candidates: list[tuple[str, str]] = []
    for name in names:
        if name.endswith("/") or _is_junk_member(name):
            continue
        fmt = supported_format_for(_member_basename(name))
        if fmt is not None:
            candidates.append((name, fmt))
    shapefiles = [c for c in candidates if c[1] == "shapefile"]
    if shapefiles:
        return shapefiles[0] if len(shapefiles) == 1 else None
    return candidates[0] if len(candidates) == 1 else None


@contextmanager
def _open_zip(path: FsPath, fs: FileSystem | None) -> Generator[zipfile.ZipFile]:
    """Open a zip archive from a local path or a remote (seekable) filesystem handle.
    Remote backends serve the central directory via range reads, so listing/extracting
    members does not require downloading the whole archive up front."""
    if fs is not None and not fs.is_local:
        with fs.open(str(path)) as handle, zipfile.ZipFile(handle) as zf:
            yield zf
    else:
        with zipfile.ZipFile(path) as zf:
            yield zf


def zip_member_list(path: FsPath, fs: FileSystem | None) -> list[str] | None:
    """The archive's member names, or None when ``path`` is not actually a zip
    (a ``.zip``-named endpoint serving plain data) — callers fall back to the
    regular format-detection cascade."""
    try:
        with _open_zip(path, fs) as zf:
            return zf.namelist()
    except zipfile.BadZipFile:
        return None


def zip_csv_member_header(
    path: FsPath, fs: FileSystem | None, max_bytes: int
) -> bytes | None:
    """The first ``max_bytes`` of a zipped CSV's single member, streamed straight
    from the archive — the schema-only fast path, decompressing only the leading
    chunks instead of extracting a potentially huge member. None when that path
    does not apply (not actually a zip, no lone plain-CSV member): callers fall
    back to full extraction."""
    try:
        with _open_zip(path, fs) as zf:
            selected = zip_scannable_member(zf.namelist())
            if selected is None:
                return None
            member, fmt = selected
            if fmt != "csv" or is_gzipped(member):
                return None
            with zf.open(member) as src:
                return src.read(max_bytes)
    except zipfile.BadZipFile:
        return None


def _extract_member(zf: zipfile.ZipFile, name: str, target: Path, budget: int) -> int:
    """Stream one member to ``target`` in bounded memory; return bytes written, raising
    once the running total would exceed ``budget`` (decompression-bomb guard)."""
    written = 0
    with zf.open(name) as src, open(target, "wb") as dst:
        while chunk := src.read(_COPY_CHUNK):
            written += len(chunk)
            if written > budget:
                raise DecompressionLimitError(
                    f"zip member {name!r} exceeds the decompression cap "
                    f"(possible zip bomb); refusing to continue"
                )
            dst.write(chunk)
    return written


@contextmanager
def local_member_from_zip(
    path: FsPath, fs: FileSystem | None, delivery_format: str
) -> Generator[Path | None]:
    """Extract the single scannable member inside ``path`` to a temp dir and yield it.

    Yields None when ``path`` is not actually a zip archive (a ``.zip``-named endpoint
    serving plain data) — the caller then scans the raw bytes as ``delivery_format``.
    A member whose format contradicts ``delivery_format`` (an explicit ``format:``
    mismatch) raises a clear ``ConfigError`` instead of scanning it as the wrong thing.

    Same-stem siblings come along (a Shapefile's ``.shx``/``.dbf``/``.prj`` sidecars),
    matched case-insensitively. Members are streamed (bounded memory) under their
    *basename* alone — so a crafted ``../`` member name cannot escape the temp dir
    (Zip Slip is structurally impossible, no path is honoured) — and the total
    extracted size is capped relative to the archive's compressed size to guard
    against a zip bomb."""
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        try:
            with _open_zip(path, fs) as zf:
                selected = zip_scannable_member(zf.namelist())
                # Normally guaranteed by prior format resolution, but an explicit
                # ``format:`` on an unclassifiable zip reaches here unresolved.
                if selected is None:
                    raise unsupported_zip_error(
                        PurePosixPath(str(path)).name, zf.namelist()
                    )
                member, member_format = selected
                if member_format != delivery_format:
                    raise ConfigError(
                        f"{PurePosixPath(str(path)).name}: the archive's data file "
                        f"({member}) is {member_format!r}, which contradicts "
                        f"format: {delivery_format!r}."
                    )
                stem = _member_basename(member).rsplit(".", 1)[0].lower()
                # Cap from the central directory's compressed sizes — no extra I/O.
                budget = decompressed_cap(sum(zi.compress_size for zi in zf.infolist()))
                for name in zf.namelist():
                    basename = _member_basename(name)
                    if name.endswith("/") or basename.rsplit(".", 1)[0].lower() != stem:
                        continue
                    budget -= _extract_member(zf, name, tmp_dir / basename, budget)
        except zipfile.BadZipFile:
            yield None
            return
        yield tmp_dir / _member_basename(member)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def unsupported_zip_error(path_name: str, members: list[str]) -> ConfigError:
    """A clear error for a ``.zip`` without exactly one scannable member."""
    listed = ", ".join(members[:10]) or "(empty)"
    return ConfigError(
        f"{path_name}: unsupported zip archive — expected exactly one scannable "
        f"data file (e.g. .shp, .csv, .xlsx, .parquet). "
        f"Members: {listed}. Extract it first, or point at a single file."
    )
