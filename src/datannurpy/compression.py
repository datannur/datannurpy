"""Transparent handling of content-compressed data sources (currently gzip).

Only single-stream gzip (``sales.csv.gz`` and friends) is handled here: gzip wraps
exactly one file, so the inner format is deterministic from the name — the compression
suffix is stripped for format resolution, and the byte stream is decompressed on the
fly (bounded against decompression bombs) so every downstream reader sees plain content.

Transport compression (HTTP ``Content-Encoding: gzip``) is deliberately out of scope:
the HTTP layer already decompresses it, and the ``.gz`` *suffix* is precisely what
distinguishes a compressed resource from a transparently-encoded transfer. Multi-member
archives (``.zip``) are a separate concern and intentionally excluded.
"""

from __future__ import annotations

import gzip
import io
from pathlib import PurePosixPath
from typing import Any, BinaryIO

# Recognised content-compression suffixes. gzip only: single-stream and therefore
# name-deterministic. ``.zip`` (ambiguous, multi-member) is handled separately.
COMPRESSION_SUFFIXES = (".gz",)

# Decompression-bomb guard: never expand a source past the larger of an absolute floor
# (so small-but-legitimate files always pass) and a generous multiple of the compressed
# size (so a pathological ratio fails fast). Real tabular data compresses well under
# this ratio; a bomb sits orders of magnitude beyond it.
_DECOMP_MIN_CAP = 1 << 30  # 1 GiB always permitted
_DECOMP_MAX_RATIO = 200


class DecompressionLimitError(Exception):
    """Raised when a compressed source expands past its allowed size (bomb guard)."""


def compression_suffix(name: str) -> str:
    """Return the trailing content-compression suffix (``.gz``) of ``name``, or ``''``."""
    suffix = PurePosixPath(name).suffix.lower()
    return suffix if suffix in COMPRESSION_SUFFIXES else ""


def is_gzipped(name: str) -> bool:
    """Whether ``name`` denotes a gzip-compressed resource (``*.gz``)."""
    return compression_suffix(name) == ".gz"


def strip_compression_suffix(name: str) -> str:
    """Drop a trailing content-compression suffix so the inner format can be resolved
    (``sales.csv.gz`` → ``sales.csv``). A no-op for uncompressed names."""
    suffix = compression_suffix(name)
    return name[: -len(suffix)] if suffix else name


def decompressed_cap(compressed_size: int) -> int:
    """Maximum decompressed byte count tolerated for a source of ``compressed_size``."""
    return max(_DECOMP_MIN_CAP, compressed_size * _DECOMP_MAX_RATIO)


class _BoundedGzipReader(io.RawIOBase):
    """A read-only gzip decompression stream that aborts past ``cap`` decompressed bytes.

    Bounding the *output* is what actually guards against a bomb: the gzip footer's
    stored size is truncated to 32 bits and trivially spoofed, so it can't be trusted.
    """

    def __init__(self, raw: BinaryIO, cap: int) -> None:
        self._gz = gzip.GzipFile(fileobj=raw, mode="rb")
        self._cap = cap
        self._seen = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any) -> int:
        chunk = self._gz.read(len(buffer))
        n = len(chunk)
        self._seen += n
        if self._seen > self._cap:
            raise DecompressionLimitError(
                f"decompressed size exceeds {self._cap} bytes "
                f"(possible gzip bomb); refusing to continue"
            )
        buffer[:n] = chunk
        return n

    def close(self) -> None:
        try:
            self._gz.close()
        finally:
            super().close()


def bounded_gzip_stream(raw: BinaryIO, cap: int) -> BinaryIO:
    """Wrap a raw binary stream in a buffered, size-bounded gzip decompressor."""
    return io.BufferedReader(_BoundedGzipReader(raw, cap))
