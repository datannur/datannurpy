"""Unit tests for the delivery-format detection cascade (scanner/format_detect.py)."""

from __future__ import annotations

import io
from contextlib import contextmanager
from typing import Any, Iterator, cast

import pytest

from datannurpy.errors import ConfigError
from datannurpy.scanner.filesystem import FileSystem
from datannurpy.scanner.format_detect import (
    content_type_to_format,
    format_from_extension,
    format_from_query,
    format_from_token,
    normalize_format,
    resolve_delivery_format,
    sniff_format,
)


class _FakeFS:
    """Minimal stand-in for ``FileSystem`` exercising info()/open() code paths."""

    def __init__(
        self,
        *,
        is_local: bool = False,
        info: dict[str, Any] | None = None,
        info_exc: Exception | None = None,
        body: bytes = b"",
        open_exc: Exception | None = None,
    ) -> None:
        self.is_local = is_local
        self._info = info or {}
        self._info_exc = info_exc
        self._body = body
        self._open_exc = open_exc

    def info(self, path: str) -> dict[str, Any]:
        if self._info_exc is not None:
            raise self._info_exc
        return self._info

    @contextmanager
    def open(self, path: str, mode: str = "rb") -> Iterator[io.BytesIO]:
        if self._open_exc is not None:
            raise self._open_exc
        yield io.BytesIO(self._body)


# --------------------------------------------------------------------------- #
# normalize_format
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("value", "expected"),
    [("xlsx", "excel"), (".CSV", "csv"), ("parquet", "parquet"), ("PQ", "parquet")],
)
def test_normalize_format_aliases(value: str, expected: str) -> None:
    assert normalize_format(value) == expected


def test_normalize_format_rejects_unknown() -> None:
    with pytest.raises(ConfigError, match="Unknown format"):
        normalize_format("json")


# --------------------------------------------------------------------------- #
# format_from_extension / _from_token / _from_query
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("path_name", "expected"),
    [("sales.csv?token=x", "csv"), ("data.xlsx", "excel"), ("CSV?x=1", None)],
)
def test_format_from_extension(path_name: str, expected: str | None) -> None:
    assert format_from_extension(path_name) == expected


@pytest.mark.parametrize(
    ("path_name", "expected"),
    [
        ("CSV?language=fr", "csv"),
        ("xls?d=1", "excel"),
        ("levels", None),
        ("a.csv", None),
    ],
)
def test_format_from_token(path_name: str, expected: str | None) -> None:
    assert format_from_token(path_name) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://x/y", None),
        ("http://x/y?format=csv", "csv"),
        ("http://x/y?fmt=xlsx", "excel"),
        ("http://x/y?format=weird", None),
        # WFS GetFeature: outputFormat (case-insensitive key) spells GeoJSON as a
        # JSON media type or plain "json" — but only on an OGC request; the same
        # value on a generic API (or a plain ?format=json) stays unmapped.
        ("http://x/wfs?request=GetFeature&outputFormat=application/json", "geojson"),
        ("http://x/wfs?SERVICE=WFS&outputformat=json", "geojson"),
        ("http://x/api?outputFormat=json", None),
        ("http://x/wfs?outputFormat=csv", "csv"),
        ("http://x/wfs?request=GetFeature&outputFormat=text/xml", None),
        ("http://x/y?format=json", None),
        # Media-type spellings resolve through the Content-Type table.
        ("http://x/y?format=text/csv", "csv"),
    ],
)
def test_format_from_query(url: str, expected: str | None) -> None:
    assert format_from_query(url) == expected


# --------------------------------------------------------------------------- #
# content_type_to_format
# --------------------------------------------------------------------------- #
def test_content_type_from_mimetype() -> None:
    fs = _FakeFS(info={"mimetype": "text/csv; charset=utf-8"})
    assert content_type_to_format(cast(FileSystem, fs), "http://x/y") == "csv"


def test_content_type_from_header_case_insensitive() -> None:
    fs = _FakeFS(info={"Content-Type": "application/parquet"})
    assert content_type_to_format(cast(FileSystem, fs), "http://x/y") == "parquet"


@pytest.mark.parametrize(
    ("content_type", "expected"),
    [
        ("application/vnd.oasis.opendocument.spreadsheet", "ods"),
        ("application/gpx+xml", "gpx"),
    ],
)
def test_content_type_new_formats(content_type: str, expected: str) -> None:
    fs = _FakeFS(info={"mimetype": content_type})
    assert content_type_to_format(cast(FileSystem, fs), "http://x/y") == expected


def test_content_type_unknown_returns_none() -> None:
    fs = _FakeFS(info={"mimetype": "application/octet-stream"})
    assert content_type_to_format(cast(FileSystem, fs), "http://x/y") is None


def test_content_type_absent_returns_none() -> None:
    fs = _FakeFS(info={"size": 10})
    assert content_type_to_format(cast(FileSystem, fs), "http://x/y") is None


def test_content_type_info_failure_returns_none() -> None:
    fs = _FakeFS(info_exc=OSError("no HEAD"))
    assert content_type_to_format(cast(FileSystem, fs), "http://x/y") is None


# --------------------------------------------------------------------------- #
# sniff_format
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (b"PK\x03\x04rest", "excel"),
        (
            b"PK\x03\x04"
            + b"\x00" * 26
            + b"mimetypeapplication/vnd.oasis.opendocument.spreadsheet",
            "ods",
        ),
        (b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1rest", "excel"),
        (b"PAR1rest", "parquet"),
        (b"<html><body>", None),
        (b'{"a": 1}', None),
        (b"   \n\t ", None),
        (b"\xef\xbb\xbfa,b\n1,2\n", "csv"),
        (b"hello world", None),
        (b"foo\nbar\n", None),
        (b"a,b\nc,d,e\n", None),
    ],
)
def test_sniff_format(header: bytes, expected: str | None) -> None:
    assert sniff_format(header) == expected


# --------------------------------------------------------------------------- #
# resolve_delivery_format cascade
# --------------------------------------------------------------------------- #
def _resolve(path_name: str, **kw: Any) -> str:
    kw.setdefault("explicit_format", None)
    kw.setdefault("fs", None)
    kw.setdefault("remote_path", path_name)
    kw.setdefault("allow_content_sniff", True)
    kw.setdefault("quiet", True)
    return resolve_delivery_format(path_name, **kw)


def test_resolve_explicit_wins() -> None:
    assert _resolve("whatever", explicit_format="xlsx") == "excel"


def test_resolve_by_extension() -> None:
    assert _resolve("a.csv") == "csv"


def test_resolve_local_unsupported_raises() -> None:
    with pytest.raises(ConfigError, match="Unsupported format"):
        _resolve("a.json")


def test_resolve_local_fs_unsupported_raises() -> None:
    with pytest.raises(ConfigError, match="Unsupported format"):
        _resolve("a.json", fs=_FakeFS(is_local=True))


def test_resolve_by_token() -> None:
    fs = _FakeFS(is_local=False)
    assert _resolve("CSV", fs=fs, remote_path="http://x/CSV") == "csv"


def test_resolve_by_query() -> None:
    fs = _FakeFS(is_local=False)
    assert (
        _resolve("download", fs=fs, remote_path="http://x/download?format=csv") == "csv"
    )


def test_resolve_by_content_type() -> None:
    fs = _FakeFS(is_local=False, info={"mimetype": "text/csv"})
    assert _resolve("download", fs=fs, remote_path="http://x/download") == "csv"


def test_resolve_by_content_sniff() -> None:
    fs = _FakeFS(is_local=False, info={}, body=b"x,y\n1,2\n3,4\n")
    assert _resolve("download", fs=fs, remote_path="http://x/download") == "csv"


def test_resolve_sniff_skipped_raises() -> None:
    fs = _FakeFS(is_local=False, info={}, body=b"x,y\n1,2\n")
    with pytest.raises(ConfigError, match="Could not detect the format"):
        _resolve(
            "download",
            fs=fs,
            remote_path="http://x/download",
            allow_content_sniff=False,
        )


def test_resolve_unconclusive_raises() -> None:
    fs = _FakeFS(is_local=False, info={}, body=b"hello")  # sniff returns None
    with pytest.raises(ConfigError, match="Could not detect the format"):
        _resolve("download", fs=fs, remote_path="http://x/download")


def test_resolve_sniff_open_failure_raises() -> None:
    fs = _FakeFS(is_local=False, info={}, open_exc=OSError("no body"))
    with pytest.raises(ConfigError, match="Could not detect the format"):
        _resolve("download", fs=fs, remote_path="http://x/download")
