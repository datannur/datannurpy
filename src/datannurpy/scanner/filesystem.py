"""Filesystem abstraction using fsspec for local and remote storage."""

from __future__ import annotations

import codecs
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

import fsspec

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator


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
        # Expand ~ in path-like options (e.g., key_filename for SFTP)
        opts = _expand_home_in_options(storage_options) if storage_options else {}
        self.fs, self.root = fsspec.core.url_to_fs(path_str, **opts)
        # Normalize root path (remove trailing slash for consistency)
        self.root = self.root.rstrip("/")

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
        """Get file/directory metadata (size, mtime, type)."""
        full_path = self._full_path(path)
        return dict(self.fs.info(full_path))

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
    def ensure_local(self, path: str) -> Generator[Path, None, None]:
        """Ensure file is available locally, downloading if needed.

        For local filesystems, yields the path directly.
        For remote filesystems, downloads to a temp file and yields that path.
        The temp file is automatically cleaned up after use.
        """
        full_path = self._full_path(path)
        if self.is_local:
            yield Path(full_path)
        else:
            tmp_dir = Path(tempfile.mkdtemp())
            tmp_path = tmp_dir / Path(path).name
            try:
                self.fs.download(full_path, str(tmp_path))
                yield tmp_path
            finally:
                tmp_path.unlink(missing_ok=True)
                tmp_dir.rmdir()

    @contextmanager
    def ensure_local_dir(self, path: str) -> Generator[Path, None, None]:
        """Ensure directory is available locally, downloading if needed.

        For local filesystems, yields the path directly.
        For remote filesystems, downloads to a temp directory and yields that path.
        The temp directory is automatically cleaned up after use.
        """
        full_path = self._full_path(path)
        if self.is_local:
            yield Path(full_path)
        else:
            tmp_dir = tempfile.mkdtemp()
            local_path = Path(tmp_dir) / Path(path).name
            try:
                self.fs.download(full_path, str(local_path), recursive=True)
                yield local_path
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

    @contextmanager
    def ensure_local_partial(
        self, path: str, max_bytes: int
    ) -> Generator[Path, None, None]:
        """Download only the first N bytes of a file to a local temp file.

        Useful for reading file headers (SAS/SPSS/Stata) without full download.
        """
        full_path = self._full_path(path)
        suffix = Path(path).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = Path(tmp.name)
        try:
            with self.fs.open(full_path, "rb") as f:
                content = f.read(max_bytes)
            tmp_path.write_bytes(content)
            yield tmp_path
        finally:
            tmp_path.unlink(missing_ok=True)

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
