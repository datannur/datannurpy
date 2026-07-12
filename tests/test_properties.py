"""Property-based tests for the algorithmic kernels.

Each test asserts an invariant that must hold for *any* input, exercising the
id, prefix-grouping, time-series and connection-string kernels with generated
edge cases that example-based tests miss (exotic filenames, partial years,
special characters in credentials, ...).
"""

from __future__ import annotations

import os
import re
from pathlib import PurePosixPath
from urllib.parse import urlparse

from hypothesis import given, settings
from hypothesis import strategies as st

from datannurpy.scanner.database import (
    parse_connection_string,
    sanitize_connection_url,
)
from datannurpy.scanner.timeseries import group_time_series, period_sort_key
from datannurpy.scanner.utils import (
    GZIP_INNER_FORMATS,
    SUPPORTED_FORMATS,
    supported_format_for,
)
from datannurpy.utils import get_prefix_folders, get_table_prefix, sanitize_id
from datannurpy.utils.ids import build_enumeration_name

# Deterministic runs by default: no seed drift between CI runs and no
# .hypothesis/ example database left in the working tree. For a deeper local
# hunt: HYPOTHESIS_PROFILE=stress uv run pytest tests/test_properties.py
settings.register_profile("deterministic", derandomize=True, database=None)
settings.register_profile("stress", max_examples=2000, database=None, deadline=None)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "deterministic"))


# ---------------------------------------------------------------------------
# ids
# ---------------------------------------------------------------------------

_VALID_ID = re.compile(r"[a-zA-Z0-9_,\- ]*")


@given(st.text())
def test_sanitize_id_valid_idempotent_length_preserving(value: str) -> None:
    once = sanitize_id(value)
    assert _VALID_ID.fullmatch(once)
    assert len(once) == len(value)
    assert sanitize_id(once) == once


@given(st.sets(st.text(max_size=30), min_size=1, max_size=8))
def test_build_enumeration_name_counts_hidden_values(values: set[str]) -> None:
    name = build_enumeration_name(values)
    if len(values) > 3:
        assert name.endswith(f"... (+{len(values) - 3})")


# ---------------------------------------------------------------------------
# prefix grouping
# ---------------------------------------------------------------------------

_table_name = st.lists(
    st.text(alphabet="abcd12", min_size=1, max_size=3), min_size=1, max_size=4
).map("_".join)


@given(st.lists(_table_name, min_size=1, max_size=12))
def test_prefix_folders_group_some_but_not_all_tables(tables: list[str]) -> None:
    folders = get_prefix_folders(tables)
    prefixes = {f.prefix for f in folders}
    for folder in folders:
        members = sum(1 for t in tables if t.startswith(folder.prefix + "_"))
        assert 2 <= members < len(tables)
        if folder.parent_prefix is not None:
            assert folder.parent_prefix in prefixes
            assert folder.prefix.startswith(folder.parent_prefix + "_")


@given(st.lists(_table_name, min_size=1, max_size=12), _table_name)
def test_get_table_prefix_returns_most_specific_valid_prefix(
    tables: list[str], table: str
) -> None:
    valid = {f.prefix for f in get_prefix_folders(tables)}
    prefix = get_table_prefix(table, valid)
    applicable = [p for p in valid if table.startswith(p + "_")]
    if prefix is None:
        assert not applicable
    else:
        assert prefix in applicable
        assert all(len(p) <= len(prefix) for p in applicable)


# ---------------------------------------------------------------------------
# time series grouping
# ---------------------------------------------------------------------------

_segment = st.one_of(
    st.text(alphabet="abcdxyz", min_size=1, max_size=6),
    st.integers(min_value=1980, max_value=2035).map(str),
    st.integers(min_value=0, max_value=12).map(lambda m: f"{m:02d}"),
    st.integers(min_value=1, max_value=2035).map(str),
)
_rel_file = st.builds(
    lambda dirs, stem, token: "/".join([*dirs, f"{stem}_{token}.csv"]),
    st.lists(_segment, max_size=2),
    _segment,
    _segment,
)


@given(st.lists(_rel_file, min_size=1, max_size=10, unique=True))
def test_group_time_series_partitions_the_input_files(rel_paths: list[str]) -> None:
    """No file may be lost or duplicated: groups + singles == input."""
    root = PurePosixPath("/data")
    files = [(root / rel, mtime) for mtime, rel in enumerate(rel_paths)]
    groups, singles = group_time_series(files, root)

    grouped_paths = [path for group in groups for _, path in group.files]
    single_paths = [path for path, _ in singles]
    assert sorted(map(str, grouped_paths + single_paths)) == sorted(
        str(path) for path, _ in files
    )
    for group in groups:
        assert len(group.files) >= 2
        sort_keys = [period_sort_key(period) for period, _ in group.files]
        assert sort_keys == sorted(sort_keys)


# ---------------------------------------------------------------------------
# connection strings
# ---------------------------------------------------------------------------

_user = st.text(min_size=1, max_size=16).filter(lambda s: ":" not in s)
_password = st.text(min_size=1, max_size=16)


@given(_user, _password)
def test_parse_connection_string_roundtrips_credentials(
    user: str, password: str
) -> None:
    backend, kwargs = parse_connection_string(
        f"postgresql://{user}:{password}@localhost:5432/mydb"
    )
    assert backend == "postgres"
    assert kwargs["user"] == user
    assert kwargs["password"] == password
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == "5432"
    assert kwargs["database"] == "mydb"


@given(_user, _password)
def test_sanitize_connection_url_strips_credentials_and_query(
    user: str, password: str
) -> None:
    sanitized = sanitize_connection_url(
        f"mysql://{user}:{password}@db.example.com:3306/app?ssl_mode=REQUIRED"
    )
    parsed = urlparse(sanitized)
    assert parsed.username is None
    assert parsed.password is None
    assert parsed.query == ""
    assert parsed.hostname == "db.example.com"
    assert parsed.port == 3306
    assert parsed.path == "/app"


# ---------------------------------------------------------------------------
# format resolution
# ---------------------------------------------------------------------------

_stem = st.text(alphabet="abcdef123", min_size=1, max_size=8)


@given(st.sampled_from(sorted(SUPPORTED_FORMATS)), _stem)
def test_supported_format_sees_through_gzip_only_when_decompressible(
    extension: str, stem: str
) -> None:
    fmt = SUPPORTED_FORMATS[extension]
    assert supported_format_for(f"{stem}{extension}") == fmt
    assert supported_format_for(f"{stem}{extension.upper()}") == fmt
    expected_gz = fmt if fmt in GZIP_INNER_FORMATS else None
    assert supported_format_for(f"{stem}{extension}.gz") == expected_gz
