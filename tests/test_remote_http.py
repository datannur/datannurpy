"""Integration tests for public HTTP(S) URLs served by a local test server.

These validate the full ``dataset:`` HTTP code path end-to-end without any external
network: a stdlib ``http.server`` serves a temp directory on an ephemeral port. A few
handler variants exercise how the ``Last-Modified`` header drives incremental scans.
"""

from __future__ import annotations

import functools
import http.server
import os
import re
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest

from datannurpy import Catalog
from datannurpy.errors import ConfigError


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    """SimpleHTTPRequestHandler with request logging silenced."""

    def log_message(self, format: str, *args: object) -> None:
        pass


class _NoLastModifiedHandler(_QuietHandler):
    """Serve files without a Last-Modified header (like a dynamic endpoint)."""

    def send_header(self, keyword: str, value: str) -> None:
        if keyword == "Last-Modified":
            return
        super().send_header(keyword, value)


class _MalformedLastModifiedHandler(_QuietHandler):
    """Serve files with an unparseable Last-Modified header."""

    def send_header(self, keyword: str, value: str) -> None:
        if keyword == "Last-Modified":
            value = "not-a-date"
        super().send_header(keyword, value)


ServeFn = Callable[..., str]


@pytest.fixture
def serve(tmp_path: Path) -> Iterator[ServeFn]:
    """Serve ``tmp_path`` over HTTP; return a factory that yields the base URL.

    Port 0 lets the OS pick a free port so tests stay parallel-safe under xdist. The
    factory takes an optional handler class to vary the Last-Modified behavior.
    """
    started: list[tuple[http.server.ThreadingHTTPServer, threading.Thread]] = []

    def _serve(handler_cls: type[_QuietHandler] = _QuietHandler) -> str:
        handler = functools.partial(handler_cls, directory=str(tmp_path))
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        started.append((server, thread))
        return f"http://127.0.0.1:{server.server_address[1]}"

    try:
        yield _serve
    finally:
        for server, thread in started:
            server.shutdown()
            server.server_close()
            thread.join()


def test_add_dataset_http_csv(serve: ServeFn, tmp_path: Path) -> None:
    """A public CSV URL is scanned like a local file, exporting the URL as data_path."""
    (tmp_path / "sales.csv").write_text("id,amount\n1,100\n2,200\n3,300\n")
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/sales.csv")

    assert len(catalog.dataset.all()) == 1
    ds = catalog.dataset.all()[0]
    assert ds.id == "sales"
    assert ds.delivery_format == "csv"
    assert ds.nb_row == 3
    # The exported data_path is the public URL, uncorrupted (no collapsed '//').
    assert ds.data_path == f"{base}/sales.csv"
    assert [v.name for v in catalog.variable.all()] == ["id", "amount"]
    # Last-Modified becomes a real date, never a fake 1970 epoch.
    assert ds.last_update_date is not None
    assert re.fullmatch(r"\d{4}/\d{2}/\d{2}T\d{2}:\d{2}:\d{2}", ds.last_update_date)


def test_add_dataset_http_query_string(serve: ServeFn, tmp_path: Path) -> None:
    """An API URL with a query string scans correctly (safe temp name via get_file) and
    gets a clean, query-free name plus a query-hashed id (unique across query variants)."""
    (tmp_path / "CSV").write_bytes(b"id,amount\n1,100\n2,200\n")
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/CSV?language=fr&levelFrom=1")

    ds = catalog.dataset.all()[0]
    assert ds.delivery_format == "csv"
    assert ds.nb_row == 2  # the download/scan is not corrupted by the query string
    assert ds.name == "CSV"  # query stripped from the default name
    assert ds.id.startswith("CSV_") and ds.id != "CSV"  # query hash keeps id unique
    assert ds.data_path == f"{base}/CSV?language=fr&levelFrom=1"


def test_add_dataset_http_query_string_ids_are_distinct(
    serve: ServeFn, tmp_path: Path
) -> None:
    """Two endpoints sharing a path but differing only by query get distinct ids."""
    (tmp_path / "CSV").write_bytes(b"a,b\n1,2\n")
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/CSV?type=a")
    catalog.add_dataset(f"{base}/CSV?type=b")

    ids = {ds.id for ds in catalog.dataset.all()}
    assert len(ids) == 2  # no collision on the shared "CSV" segment


def test_add_dataset_http_404(serve: ServeFn) -> None:
    """A missing URL fails loudly (ConfigError -> non-zero exit), like a missing file."""
    base = serve()
    catalog = Catalog(quiet=True)
    with pytest.raises(ConfigError):
        catalog.add_dataset(f"{base}/nope.csv")


def test_add_dataset_http_undetectable_format(serve: ServeFn, tmp_path: Path) -> None:
    """When no signal resolves the format, the run fails asking for an explicit format:."""
    (tmp_path / "notes.txt").write_text("hello")  # non-tabular, unknown extension
    base = serve()

    catalog = Catalog(quiet=True)
    with pytest.raises(ConfigError, match="Could not detect the format"):
        catalog.add_dataset(f"{base}/notes.txt")


def test_add_dataset_http_explicit_format(serve: ServeFn, tmp_path: Path) -> None:
    """An explicit format: scans an extension-less URL, overriding detection."""
    (tmp_path / "export").write_bytes(b"id,amount\n1,100\n2,200\n")
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/export", format="csv")

    ds = catalog.dataset.all()[0]
    assert ds.delivery_format == "csv"
    assert ds.nb_row == 2
    assert [v.name for v in catalog.variable.all()] == ["id", "amount"]


def test_add_dataset_http_format_token(serve: ServeFn, tmp_path: Path) -> None:
    """An extension-less endpoint whose last segment is a format token is auto-detected."""
    (tmp_path / "CSV").write_bytes(b"a,b\n1,2\n3,4\n")
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/CSV")

    ds = catalog.dataset.all()[0]
    assert ds.delivery_format == "csv"
    assert [v.name for v in catalog.variable.all()] == ["a", "b"]


def test_add_dataset_http_content_sniff(serve: ServeFn, tmp_path: Path) -> None:
    """A tabular body with no extension/token/format is detected by content sniffing."""
    (tmp_path / "download").write_bytes(b"x,y,z\n1,2,3\n4,5,6\n")
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/download")

    ds = catalog.dataset.all()[0]
    assert ds.delivery_format == "csv"
    assert [v.name for v in catalog.variable.all()] == ["x", "y", "z"]


def test_http_skips_when_last_modified_unchanged(
    serve: ServeFn, tmp_path: Path
) -> None:
    """An unchanged Last-Modified lets the incremental scan skip the URL."""
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    orig = csv.stat().st_mtime
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/data.csv")
    assert [v.name for v in catalog.variable.all()] == ["a", "b"]

    # Content changes but the modification time is pinned back: same Last-Modified, so
    # the run trusts the freshness signal and keeps the previous (stale) scan.
    csv.write_text("a,b,c\n1,2,3\n")
    os.utime(csv, (orig, orig))
    catalog.add_dataset(f"{base}/data.csv")

    assert len(catalog.dataset.all()) == 1
    assert [v.name for v in catalog.variable.all()] == ["a", "b"]


def test_http_rescans_when_last_modified_changes(
    serve: ServeFn, tmp_path: Path
) -> None:
    """A newer Last-Modified triggers a re-scan reflecting the new content."""
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    orig = csv.stat().st_mtime
    base = serve()

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/data.csv")
    assert [v.name for v in catalog.variable.all()] == ["a", "b"]

    csv.write_text("a,b,c\n1,2,3\n")
    os.utime(csv, (orig + 100, orig + 100))
    catalog.add_dataset(f"{base}/data.csv")

    assert len(catalog.dataset.all()) == 1  # same dataset, rebuilt
    assert [v.name for v in catalog.variable.all()] == ["a", "b", "c"]


def test_http_rescans_without_last_modified(serve: ServeFn, tmp_path: Path) -> None:
    """With no Last-Modified header there is no freshness signal, so always re-scan."""
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    base = serve(_NoLastModifiedHandler)

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/data.csv")
    ds = catalog.dataset.all()[0]
    assert ds.last_update_date is None  # no fake date when nothing is known
    assert [v.name for v in catalog.variable.all()] == ["a", "b"]

    csv.write_text("a,b,c\n1,2,3\n")
    catalog.add_dataset(f"{base}/data.csv")

    assert len(catalog.dataset.all()) == 1
    assert [v.name for v in catalog.variable.all()] == ["a", "b", "c"]


def test_http_ignores_malformed_last_modified(serve: ServeFn, tmp_path: Path) -> None:
    """An unparseable Last-Modified is treated as no signal (re-scan), not a crash."""
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    base = serve(_MalformedLastModifiedHandler)

    catalog = Catalog(quiet=True)
    catalog.add_dataset(f"{base}/data.csv")
    ds = catalog.dataset.all()[0]
    assert ds.last_update_date is None
    assert [v.name for v in catalog.variable.all()] == ["a", "b"]

    csv.write_text("a,b,c\n1,2,3\n")
    catalog.add_dataset(f"{base}/data.csv")

    assert [v.name for v in catalog.variable.all()] == ["a", "b", "c"]
