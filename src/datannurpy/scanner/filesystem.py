"""Filesystem abstraction using fsspec for local and remote storage."""

from __future__ import annotations

import codecs
import shutil
import tempfile
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path, PurePath, PurePosixPath
from typing import IO, TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

import fsspec

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator


def is_remote_url(path: str | Path) -> bool:
    """Check if path is a remote URL (contains ://)."""
    path_str = str(path)
    return "://" in path_str and not path_str.startswith("file://")


def _expand_home_in_options(options: dict[str, Any]) -> dict[str, Any]:
    """Expand ~ to home directory in path-like storage options."""
    result = options.copy()
    path_keys = ("key_filename", "keyfile", "private_key")
    for key in path_keys:
        if key in result and isinstance(result[key], str) and "~" in result[key]:
            result[key] = str(Path(result[key]).expanduser())
    return result


class FileSystem:
    """Unified filesystem interface for local and remote paths."""

    def __init__(self, path: str | Path, storage_options: dict[str, Any] | None = None):
        """Initialize filesystem from a path (local or remote URL)."""
        path_str = str(path)
        self._url_parts = urlsplit(path_str) if is_remote_url(path_str) else None
        # Expand ~ in path-like options (e.g., key_filename for SFTP)
        opts = _expand_home_in_options(storage_options) if storage_options else {}
        self.fs, self.root = fsspec.core.url_to_fs(path_str, **opts)
        # Normalize root path (remove trailing slash for consistency)
        self.root = self.root.rstrip("/")
        self._info_cache: dict[str, dict[str, Any]] = {}

    @property
    def is_local(self) -> bool:
        """Check if this is a local filesystem."""
        return self.fs.protocol == "file" or (
            isinstance(self.fs.protocol, tuple) and "file" in self.fs.protocol
        )

    def _full_path(self, path: str) -> str:
        """Convert relative path to full path on this filesystem."""
        if path.startswith(self.root):
            return path
        return f"{self.root}/{path}".replace("//", "/")

    def glob(self, pattern: str) -> list[str]:
        """Find files matching a glob pattern."""
        full_pattern = self._full_path(pattern)
        results = self.fs.glob(full_pattern)
        return sorted(results)

    def isdir(self, path: str) -> bool:
        """Check if path is a directory."""
        full_path = self._full_path(path)
        return bool(self.fs.isdir(full_path))

    def isfile(self, path: str) -> bool:
        """Check if path is a file."""
        full_path = self._full_path(path)
        return bool(self.fs.isfile(full_path))

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        full_path = self._full_path(path)
        return bool(self.fs.exists(full_path))

    def info(self, path: str) -> dict[str, Any]:
        """Get file/directory metadata (size, mtime, type).

        Memoized per resolved path: a single scan reads the same file's metadata
        several times (is_dir, mtime, size, format detection) and remote backends
        (e.g. HTTP) issue one network round-trip per uncached ``info`` call. A
        FileSystem instance is scoped to one scanned path/tree, so the metadata is
        stable for its lifetime.
        """
        full_path = self._full_path(path)
        cached = self._info_cache.get(full_path)
        if cached is None:
            cached = dict(self.fs.info(full_path))
            self._info_cache[full_path] = cached
        return dict(cached)

    def listdir(self, path: str) -> list[str]:
        """List directory contents (names only, not full paths)."""
        full_path = self._full_path(path)
        entries = self.fs.listdir(full_path)
        return [entry["name"].rsplit("/", 1)[-1] for entry in entries]

    def iterdir(self, path: str) -> Iterator[str]:
        """Iterate over directory contents (full paths)."""
        full_path = self._full_path(path)
        entries = self.fs.listdir(full_path)
        for entry in entries:
            yield entry["name"]

    @contextmanager
    def open(self, path: str, mode: str = "rb") -> Generator[Any, None, None]:
        """Open a file for reading/writing."""
        full_path = self._full_path(path)
        f = self.fs.open(full_path, mode)
        try:
            yield f
        finally:
            f.close()

    @contextmanager
    def _local_copy(
        self,
        path: str,
        download: Callable[[str, Path], None],
        local_name: str | None = None,
    ) -> Generator[Path, None, None]:
        """Yield a local copy of ``path``: the path itself when already local, or a
        temp copy produced by ``download(remote_path, tmp_dir)`` (auto-cleaned). The
        temp file is named ``local_name`` when given, else the remote basename."""
        full_path = self._full_path(path)
        if self.is_local:
            yield Path(full_path)
            return
        tmp_dir = Path(tempfile.mkdtemp())
        try:
            download(full_path, tmp_dir)
            yield tmp_dir / (local_name or PurePosixPath(full_path).name)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def ensure_local(
        self, path: str, local_name: str | None = None
    ) -> AbstractContextManager[Path]:
        """Ensure a single file is available locally (downloads it if remote).

        ``local_name`` overrides the temp filename — used to give a downloaded URL a
        safe name with the right extension (the raw basename may carry a query string
        or lack the extension that suffix-based readers need).
        """
        # get_file (single-file copy) writes to the exact destination path; download()
        # (= get) applies directory heuristics that, for a URL with a query string,
        # create a directory and drop the file inside it under its raw name.
        return self._local_copy(
            path,
            lambda src, tmp: self.fs.get_file(
                src, str(tmp / (local_name or PurePosixPath(src).name))
            ),
            local_name=local_name,
        )

    def ensure_local_dir(self, path: str) -> AbstractContextManager[Path]:
        """Ensure a directory is available locally (downloads it if remote)."""
        return self._local_copy(
            path,
            lambda src, tmp: self.fs.download(
                src, str(tmp / PurePosixPath(src).name), recursive=True
            ),
        )

    def ensure_local_siblings(self, path: str) -> AbstractContextManager[Path]:
        """Ensure a file and its same-stem siblings are local (e.g. shapefile parts).

        Multi-file formats such as Shapefile (``.shp`` + ``.shx``/``.dbf``/``.prj``)
        need their companion files alongside the main file to be readable.
        """
        return self._local_copy(path, self._download_siblings)

    def _download_siblings(self, src: str, tmp: Path) -> None:
        """Download ``src`` and every same-stem file beside it into ``tmp``."""
        stem = PurePosixPath(src).stem
        for sibling in self.fs.ls(PurePosixPath(src).parent.as_posix(), detail=False):
            if PurePosixPath(sibling).stem == stem:
                self.fs.get_file(sibling, str(tmp / PurePosixPath(sibling).name))

    def to_path(self, path: str) -> Path:
        """Convert filesystem path to Path object (local only)."""
        if not self.is_local:
            msg = "to_path() only works with local filesystems"
            raise ValueError(msg)
        return Path(path)

    def relative_to_root(self, path: str) -> str:
        """Get path relative to root."""
        if path.startswith(self.root):
            rel = path[len(self.root) :].lstrip("/")
            return rel if rel else "."
        return path

    def canonical_url_for_path(self, path: str | Path | PurePath) -> str | None:
        """Return a user-free remote URL for a filesystem path without touching I/O."""
        if self.is_local or self._url_parts is None:
            return None
        protocol = self._url_parts.scheme
        hostname = self._url_parts.hostname
        if not protocol or hostname is None:
            return None
        netloc = hostname
        if self._url_parts.port is not None:
            netloc = f"{netloc}:{self._url_parts.port}"

        path_str = str(path).replace("\\", "/")
        if not path_str.startswith("/"):
            path_str = self._full_path(path_str)
        return urlunsplit((protocol, netloc, path_str, "", ""))


def get_filesystem(
    path: str | Path, storage_options: dict[str, Any] | None = None
) -> FileSystem:
    """Create a FileSystem instance for the given path."""
    return FileSystem(path, storage_options)


_CHUNK_SIZE = 1_048_576  # 1 MB


def ensure_local_utf8(
    fin: IO[bytes], fout: IO[bytes], csv_encoding: str | None = None
) -> None:
    """Copy binary stream to output, ensuring UTF-8 encoding."""
    if csv_encoding:
        while chunk := fin.read(_CHUNK_SIZE):
            fout.write(chunk.decode(csv_encoding).encode("utf-8"))
        return

    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    is_utf8 = True
    carry = b""
    while chunk := fin.read(_CHUNK_SIZE):
        if is_utf8:
            try:
                decoder.decode(chunk, False)
                pending = decoder.getstate()[0]
                n = len(pending)
                if n:
                    fout.write(carry + chunk[:-n])
                    carry = chunk[-n:]
                else:
                    fout.write(carry + chunk)
                    carry = b""
            except UnicodeDecodeError:
                is_utf8 = False
                fout.write(
                    (carry + chunk).decode("cp1252", errors="replace").encode("utf-8")
                )
                carry = b""
        else:
            fout.write(chunk.decode("cp1252", errors="replace").encode("utf-8"))
    if is_utf8 and carry:
        fout.write(carry.decode("cp1252", errors="replace").encode("utf-8"))
