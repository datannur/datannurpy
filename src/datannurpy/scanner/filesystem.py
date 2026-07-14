"""Filesystem abstraction using fsspec for local and remote storage."""

from __future__ import annotations

import codecs
import importlib.util
import shutil
import tempfile
import time
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path, PurePath, PurePosixPath
from typing import IO, TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

import fsspec

from ..errors import ConfigError

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterator

# Resilience defaults for HTTP(S) sources (e.g. an unattended CI publishing a catalog
# from public URLs): a transient blip should retry rather than fail the whole run, and
# a hung endpoint should fail fast instead of blocking on aiohttp's 5-minute default.
_HTTP_MAX_RETRIES = 3
_HTTP_RETRY_BACKOFF = 0.5  # seconds, doubled each attempt (0.5s, 1s, 2s)
_HTTP_SOCK_CONNECT = 30  # seconds to establish the connection
_HTTP_SOCK_READ = 60  # seconds between received chunks (no total cap → large files ok)


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
        self._is_http = self._url_parts is not None and self._url_parts.scheme in (
            "http",
            "https",
        )
        # Expand ~ in path-like options (e.g., key_filename for SFTP)
        opts = _expand_home_in_options(storage_options) if storage_options else {}
        if self._is_http:
            _ensure_http_support()
            opts = _with_http_timeout(opts)
        self.fs, self.root = fsspec.core.url_to_fs(path_str, **opts)
        # Normalize root path (remove trailing slash for consistency)
        self.root = self.root.rstrip("/")
        self._info_cache: dict[str, dict[str, Any]] = {}
        self._listdir_cache: dict[str, list[dict[str, Any]]] = {}

    def _run_io(self, op: Callable[[], Any]) -> Any:
        """Run a remote I/O op, retrying transient failures for HTTP backends (other
        backends handle their own retries; their errors carry no HTTP status to judge)."""
        return _retry_transient(op) if self._is_http else op()

    @property
    def is_local(self) -> bool:
        """Check if this is a local filesystem."""
        return self.fs.protocol == "file" or (
            isinstance(self.fs.protocol, tuple) and "file" in self.fs.protocol
        )

    def _full_path(self, path: str) -> str:
        """Convert relative path to full path on this filesystem."""
        # fsspec normalizes its root to POSIX form even on Windows; reconcile a
        # native-separator input so the startswith/join below don't mismatch.
        path = path.replace("\\", "/")
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
            cached = dict(self._run_io(lambda: self.fs.info(full_path)))
            self._info_cache[full_path] = cached
        return dict(cached)

    def _listdir(self, full_path: str) -> list[dict[str, Any]]:
        """Directory listing (``ls(detail=True)``) memoized per directory.

        A scan walks the tree more than once — parquet discovery first, then the
        general pass — and each walk lists every directory. The contents are stable
        for this FileSystem's lifetime (scoped to one scanned tree), so one listing
        per directory serves both walks instead of a round-trip each time.
        """
        cached = self._listdir_cache.get(full_path)
        if cached is None:
            cached = list(self.fs.listdir(full_path))
            self._listdir_cache[full_path] = cached
        return cached

    def listdir(self, path: str) -> list[str]:
        """List directory contents (names only, not full paths)."""
        entries = self._listdir(self._full_path(path))
        return [entry["name"].rsplit("/", 1)[-1] for entry in entries]

    def iterdir(self, path: str) -> Iterator[str]:
        """Iterate over directory contents (full paths)."""
        for name, _info in self.iterdir_detailed(path):
            yield name

    def iterdir_detailed(self, path: str) -> Iterator[tuple[str, dict[str, Any]]]:
        """Iterate directory entries as ``(full_path, info)`` from one listing.

        ``listdir`` (``ls(detail=True)``) already returns each entry's type and
        mtime in a single round-trip; caching them here lets the later ``info()``
        lookups (mtime for skip/scan, size, ETag) reuse this listing instead of
        issuing a network round-trip per file — the dominant cost of an
        all-incremental run on remote backends (SFTP/NAS).
        """
        for entry in self._listdir(self._full_path(path)):
            name = entry["name"]
            info = dict(entry)
            self._info_cache[self._full_path(name)] = info
            yield name, info

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
            lambda src, tmp: self._run_io(
                lambda: self.fs.get_file(
                    src, str(tmp / (local_name or PurePosixPath(src).name))
                )
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
                self._run_io(
                    lambda s=sibling: self.fs.get_file(
                        s, str(tmp / PurePosixPath(s).name)
                    )
                )

    def to_path(self, path: str) -> Path:
        """Convert filesystem path to Path object (local only)."""
        if not self.is_local:
            msg = "to_path() only works with local filesystems"
            raise ValueError(msg)
        return Path(path)

    def relative_to_root(self, path: str) -> str:
        """Get path relative to root."""
        # fsspec roots are POSIX even on Windows; reconcile a native-separator input.
        path = path.replace("\\", "/")
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


def _http_backend_available() -> bool:
    """Whether the aiohttp HTTP backend is importable in this environment."""
    return importlib.util.find_spec("aiohttp") is not None


def _ensure_http_support() -> None:
    """Fail early with an actionable message when the HTTP backend is missing.

    datannur declares ``aiohttp`` only on Python >= 3.10, because aiohttp's fixes
    for its known CVEs land in 3.14+, which dropped Python 3.9. On 3.9 the package
    is therefore absent, so scanning an HTTP(S) URL raises this clear error instead
    of a raw ``ImportError`` surfacing deep inside fsspec's HTTP filesystem.
    """
    if not _http_backend_available():
        raise ConfigError(
            "Scanning an HTTP(S) URL needs the 'aiohttp' package, which datannur "
            "installs only on Python >= 3.10 (aiohttp's security fixes require it). "
            "Upgrade to Python >= 3.10, or install aiohttp manually on 3.9."
        )


def _with_http_timeout(opts: dict[str, Any]) -> dict[str, Any]:
    """Add a default socket timeout to HTTP ``client_kwargs`` so a hung endpoint fails
    fast (aiohttp's default is a 5-minute total). A user-provided timeout is kept."""
    import aiohttp

    client_kwargs = dict(opts.get("client_kwargs") or {})
    client_kwargs.setdefault(
        "timeout",
        aiohttp.ClientTimeout(
            sock_connect=_HTTP_SOCK_CONNECT, sock_read=_HTTP_SOCK_READ
        ),
    )
    return {**opts, "client_kwargs": client_kwargs}


def _is_transient(exc: BaseException) -> bool:
    """Whether a remote error is worth retrying: a transient network/server condition
    (connection reset, timeout, 5xx, 429), not a definitive 4xx (missing/forbidden)."""
    status = _http_status_in_chain(exc)
    if status is None:  # connection reset / DNS blip / timeout
        return True
    return status >= 500 or status == 429


def _retry_transient(
    op: Callable[[], Any],
    *,
    retries: int = _HTTP_MAX_RETRIES,
    backoff: float = _HTTP_RETRY_BACKOFF,
) -> Any:
    """Call ``op()``, retrying transient failures with exponential backoff. Permanent
    errors propagate immediately; the final attempt's error propagates as-is."""
    for attempt in range(retries):
        try:
            return op()
        except Exception as exc:
            if not _is_transient(exc):
                raise
            time.sleep(backoff * 2**attempt)
    return op()  # last attempt: any error propagates to the caller


def _http_status_in_chain(exc: BaseException) -> int | None:
    """The HTTP status from a remote-access error, if any. fsspec collapses HTTP
    failures into a bare FileNotFoundError but keeps the aiohttp ClientResponseError
    (carrying ``status``) in the cause/context chain."""
    cur: BaseException | None = exc
    while cur is not None:
        status = getattr(cur, "status", None)
        if isinstance(status, int):
            return status
        cur = cur.__cause__ or cur.__context__
    return None


def remote_access_error_reason(exc: BaseException) -> str | None:
    """An actionable reason for a remote-access failure — authentication for 401/403,
    a server error for 5xx — or None for a plain not-found / unreachable error the
    caller phrases itself (404, DNS, connection refused, timeout)."""
    status = _http_status_in_chain(exc)
    if status in (401, 403):
        return (
            f"authentication required (HTTP {status}); only public, unauthenticated "
            f"URLs are supported"
        )
    if status is not None and status >= 500:
        return f"server error (HTTP {status})"
    return None


def get_filesystem(
    path: str | Path, storage_options: dict[str, Any] | None = None
) -> FileSystem:
    """Create a FileSystem instance for the given path."""
    return FileSystem(path, storage_options)


_CHUNK_SIZE = 1_048_576  # 1 MB


_UTF8_BOM = b"\xef\xbb\xbf"


def _read_chunks_bom_stripped(fin: IO[bytes]) -> Iterator[bytes]:
    """Yield the stream in chunks with every leading UTF-8 BOM removed.

    DuckDB strips a single leading BOM itself, but some export pipelines
    double-encode and emit two (an opendata portal re-exporting an already-BOM'd
    file), leaving a stray BOM glued to the first column name (``\\ufeffName``).
    Stripping them here — before the file reaches DuckDB — keeps the first column's
    identity stable, so external-metadata overlays match it. A BOM only carries
    meaning at the very start, so leading ones are always safe to drop."""
    prefix = fin.read(len(_UTF8_BOM))
    while prefix == _UTF8_BOM:
        prefix = fin.read(len(_UTF8_BOM))
    first = prefix + fin.read(_CHUNK_SIZE)
    if first:
        yield first
    while chunk := fin.read(_CHUNK_SIZE):
        yield chunk


def ensure_local_utf8(
    fin: IO[bytes], fout: IO[bytes], csv_encoding: str | None = None
) -> None:
    """Copy binary stream to output, ensuring UTF-8 encoding and no leading BOM."""
    if csv_encoding:
        for chunk in _read_chunks_bom_stripped(fin):
            fout.write(chunk.decode(csv_encoding).encode("utf-8"))
        return

    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    is_utf8 = True
    carry = b""
    for chunk in _read_chunks_bom_stripped(fin):
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
