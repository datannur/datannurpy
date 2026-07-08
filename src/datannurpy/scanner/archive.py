"""Zip-archive handling — currently the standard distribution form of a Shapefile.

A Shapefile is inherently multi-file (``.shp`` + ``.shx``/``.dbf``/``.prj`` …), so
open-data portals (IGN, Census TIGER, Eurostat, ArcGIS Hub) ship it as a single
``.zip``. Unlike gzip, a ``.zip`` name says nothing about its content, so the archive's
central directory is inspected to classify it. Phase 1 supports exactly one Shapefile
per archive; multi-member archives and other zipped formats are out of scope.
"""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from ..compression import DecompressionLimitError, decompressed_cap
from ..errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import Generator

    from .filesystem import FileSystem
    from .utils import FsPath

_COPY_CHUNK = 1 << 20  # 1 MiB


def is_zip(name: str) -> bool:
    """Whether ``name`` denotes a zip archive (``*.zip``)."""
    return PurePosixPath(name).suffix.lower() == ".zip"


def zip_shapefile_member(names: list[str]) -> str | None:
    """The single ``.shp`` member of an archive, or None when there isn't exactly one
    (zero → not a Shapefile; several → ambiguous, deferred to a later phase)."""
    shps = [n for n in names if not n.endswith("/") and n.lower().endswith(".shp")]
    return shps[0] if len(shps) == 1 else None


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


def zip_member_list(path: FsPath, fs: FileSystem | None) -> list[str]:
    """The archive's member names, for a user-facing "unsupported archive" message."""
    with _open_zip(path, fs) as zf:
        return zf.namelist()


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
def local_shapefile_from_zip(path: FsPath, fs: FileSystem | None) -> Generator[Path]:
    """Extract the single Shapefile inside ``path`` to a temp dir and yield its ``.shp``.

    Members are streamed (bounded memory) under their *basename* alone — so a crafted
    ``../`` member name cannot escape the temp dir (Zip Slip is structurally impossible,
    no path is honoured) — and the total extracted size is capped relative to the
    archive's compressed size to guard against a zip bomb."""
    tmp_dir = Path(tempfile.mkdtemp())
    try:
        with _open_zip(path, fs) as zf:
            member = zip_shapefile_member(zf.namelist())
            # Normally guaranteed by prior format resolution, but an explicit
            # ``format: shapefile`` on a non-Shapefile zip reaches here unclassified.
            if member is None:
                raise unsupported_zip_error(
                    PurePosixPath(str(path)).name, zf.namelist()
                )
            stem = PurePosixPath(member).stem
            # Cap from the central directory's compressed sizes — no extra I/O.
            budget = decompressed_cap(sum(zi.compress_size for zi in zf.infolist()))
            for name in zf.namelist():
                if name.endswith("/") or PurePosixPath(name).stem != stem:
                    continue
                target = tmp_dir / PurePosixPath(name).name
                budget -= _extract_member(zf, name, target, budget)
        yield tmp_dir / PurePosixPath(member).name
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def unsupported_zip_error(path_name: str, members: list[str]) -> ConfigError:
    """A clear error for a ``.zip`` that is not a single-Shapefile archive."""
    listed = ", ".join(members[:10]) or "(empty)"
    return ConfigError(
        f"{path_name}: unsupported zip archive — expected exactly one Shapefile (.shp). "
        f"Members: {listed}. Extract it first, or point at a single file."
    )
