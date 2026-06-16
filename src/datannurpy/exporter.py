"""Export catalog to JSON database and web app."""

from __future__ import annotations

import fnmatch
import os
import re
import shutil
import stat
import sys
import time
import webbrowser
import zlib
from collections.abc import Iterator
from pathlib import Path, PurePath, PureWindowsPath
from typing import TYPE_CHECKING, Any

import polars as pl
from jsonjsdb.writer import export_hash_session, write_table_json_pair

from .utils.log import _write_log
from .utils.params import validate_params

if TYPE_CHECKING:
    from .catalog import Catalog

from .add_metadata import apply_metadata_tombstones, ensure_metadata_applied
from .errors import ConfigError
from .preview import apply_preview_flags, sync_preview_exports

_GZIP_CHUNK_SIZE = 1024 * 1024
_MARKDOWN_LINK_RE = re.compile(
    r"(?P<prefix>!?\[[^\]]*\]\()(?P<target><[^>\n]*>|[^)\s]+)(?P<suffix>(?:\s+[^)]*)?\))"
)
_MARKDOWN_UNCHANGED_LINK_PREFIXES = ("http://", "https://", "/", "#", "mailto:")


def _drop_empty_columns(catalog: Catalog) -> None:
    """Drop all-null/empty columns from catalog tables in place before export."""
    for table in catalog._tables.values():
        df = table._df
        if df.is_empty():
            continue
        keep = table.runtime_fields | {"id"}
        cols = [c for c in df.columns if c in keep or not _is_empty_column(df[c])]
        if len(cols) != len(df.columns):
            table._df = df.select(cols)


def _is_empty_column(col: pl.Series) -> bool:
    """Return True if every value is null, empty string or empty list."""
    if col.null_count() == col.len():
        return True
    if isinstance(col.dtype, pl.List):
        return bool(col.list.len().fill_null(0).sum() == 0)
    if col.dtype == pl.Utf8:
        return bool((col.drop_nulls().str.len_chars() == 0).all())
    return False


def _format_size(size: int) -> str:
    """Return a compact human-readable byte size."""
    if size < 1024:
        return f"{size} B"
    value = float(size) / 1024
    for unit in ["KB", "MB"]:
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def _format_percent(size: int, total: int) -> str:
    """Return a compact percentage of total size."""
    if total <= 0:
        return "0.0%"
    return f"{(size / total) * 100:.1f}%"


def _gzip_estimated_size(path: Path) -> int:
    """Return gzip-compressed size without writing a .gz file."""
    compressor = zlib.compressobj(level=9, wbits=31)
    total = 0
    with path.open("rb") as file:
        while chunk := file.read(_GZIP_CHUNK_SIZE):
            total += len(compressor.compress(chunk))
    total += len(compressor.flush())
    return total


def _table_name_from_jsonjs(path: Path) -> str:
    """Return table name from a .json.js filename."""
    return path.name[: -len(".json.js")]


def _build_export_size_report(path: Path) -> str:
    """Build a size report for exported json/json.js files."""
    names: set[str] = set()
    file_sizes: dict[str, int] = {}
    try:
        entries = list(os.scandir(path))
    except FileNotFoundError:
        return ""

    for entry in entries:
        try:
            if not entry.is_file():
                continue
            file_stat = entry.stat()
        except FileNotFoundError:
            continue

        filename = entry.name
        if filename.endswith(".json.js"):
            names.add(filename[: -len(".json.js")])
        elif filename.endswith(".json"):
            names.add(filename[: -len(".json")])
        else:
            continue
        file_sizes[filename] = file_stat.st_size

    if not names:
        return ""

    rows: list[tuple[str, int, int, int]] = []
    for name in sorted(names):
        json_path = path / f"{name}.json"
        json_filename = f"{name}.json"
        jsonjs_filename = f"{name}.json.js"
        json_size = file_sizes.get(json_filename, 0)
        jsonjs_size = file_sizes.get(jsonjs_filename, 0)
        gzip_size = (
            _gzip_estimated_size(json_path) if json_filename in file_sizes else 0
        )
        rows.append((name, json_size, jsonjs_size, gzip_size))

    rows.sort(key=lambda row: row[1] + row[2], reverse=True)
    total_json = sum(row[1] for row in rows)
    total_jsonjs = sum(row[2] for row in rows)
    total_gzip = sum(row[3] for row in rows)

    lines = [
        "\n  →  export size by table",
        "     table             json      %   json.js      %   json.gz      %",
    ]
    for name, json_size, jsonjs_size, gzip_size in rows:
        lines.append(
            f"     {name[:16]:<16} "
            f"{_format_size(json_size):>8} {_format_percent(json_size, total_json):>6} "
            f"{_format_size(jsonjs_size):>9} {_format_percent(jsonjs_size, total_jsonjs):>6} "
            f"{_format_size(gzip_size):>9} {_format_percent(gzip_size, total_gzip):>6}"
        )
    lines.append(
        f"     {'total':<16} "
        f"{_format_size(total_json):>8} {_format_percent(total_json, total_json):>6} "
        f"{_format_size(total_jsonjs):>9} {_format_percent(total_jsonjs, total_jsonjs):>6} "
        f"{_format_size(total_gzip):>9} {_format_percent(total_gzip, total_gzip):>6}"
    )
    return "\n".join(lines)


def _print_export_size_report(path: Path, *, quiet: bool) -> None:
    """Print and log export size report when not quiet."""
    if quiet:
        return
    report = _build_export_size_report(path)
    if not report:
        return
    print(report, file=sys.stderr)
    _write_log(report)


def _clean_stale_db_files(catalog: Catalog, path: Path) -> None:
    """Remove generated JSON database files that are no longer part of the export."""
    try:
        entries = list(os.scandir(path))
    except FileNotFoundError:
        return
    table_names = set(catalog._tables)
    expected_names = table_names | {"__table__", "config", "evolution"}
    for entry in entries:
        if not entry.is_file():
            continue
        name = None
        filename = entry.name
        if filename.endswith(".json.js"):
            name = filename[: -len(".json.js")]
        elif filename.endswith(".json"):
            name = filename[: -len(".json")]
        if name is not None and name not in expected_names:
            Path(entry.path).unlink()


def _is_local_markdown_doc(doc: Any) -> bool:
    """Return whether a doc points to a local markdown file."""
    doc_type = (doc.type or "").lower()
    doc_path = doc.path or ""
    if doc_type != "md" or not doc_path:
        return False
    if "://" in doc_path or doc_path.startswith("/"):
        return False
    return Path(doc_path).suffix.lower() == ".md"


def _resolve_doc_source_path(db_dir: Path, doc_path: str) -> Path:
    """Resolve a doc path from an app export or db-only export."""
    if db_dir.name == "db" and db_dir.parent.name == "data":
        return db_dir.parent.parent / doc_path
    return db_dir / doc_path


def _split_markdown_link_target(target: str) -> tuple[str, str, str]:
    """Split a Markdown link target into optional angle wrapper, path and suffix."""
    wrapper = ""
    if target.startswith("<") and target.endswith(">"):
        wrapper = "<>"
        target = target[1:-1]

    suffix_index = len(target)
    for marker in ("#", "?"):
        index = target.find(marker)
        if index != -1:
            suffix_index = min(suffix_index, index)
    return wrapper, target[:suffix_index], target[suffix_index:]


def _local_markdown_path(path: PurePath) -> str:
    """Return a browser-compatible local Markdown path."""
    value = path.as_posix()
    if isinstance(path, PureWindowsPath) and not value.startswith("//"):
        return f"/{value}"
    return value


def _local_markdown_target(base_dir: PurePath, target_path: str) -> PurePath:
    """Build a local Markdown target without canonicalizing mapped drives."""
    path = base_dir.joinpath(target_path)
    if path.is_absolute() or not isinstance(path, Path):
        return path
    return path.absolute()


def _rewrite_markdown_links(content: str, source_path: PurePath) -> str:
    """Rewrite relative Markdown links relative to the source document path."""
    base_dir = source_path.parent

    def replace(match: re.Match[str]) -> str:
        target = match.group("target")
        wrapper, target_path, target_suffix = _split_markdown_link_target(target)
        target_lower = target_path.lower()
        if target_lower.startswith(_MARKDOWN_UNCHANGED_LINK_PREFIXES):
            return match.group(0)
        if not target_path:
            return match.group(0)

        rewritten_path = _local_markdown_target(base_dir, target_path)
        rewritten = f"{_local_markdown_path(rewritten_path)}{target_suffix}"
        if wrapper:
            rewritten = f"<{rewritten}>"
        return f"{match.group('prefix')}{rewritten}{match.group('suffix')}"

    return _MARKDOWN_LINK_RE.sub(replace, content)


def _sync_markdown_doc_exports(catalog: Catalog, output_dir: str | Path) -> None:
    """Write md-doc JSON files for local markdown docs."""
    db_dir = Path(output_dir)
    md_doc_dir = db_dir / "md-doc"
    markdown_docs = []

    for doc in catalog.doc.all():
        if not _is_local_markdown_doc(doc):
            continue
        source_path = _resolve_doc_source_path(db_dir, str(doc.path))
        try:
            content = source_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        markdown_docs.append((doc, source_path, content))

    if not markdown_docs:
        return

    md_doc_dir.mkdir(parents=True, exist_ok=True)
    with export_hash_session(db_dir) as hash_session:
        for doc, source_path, content in markdown_docs:
            content = _rewrite_markdown_links(content, source_path)
            rows = pl.DataFrame({"content": [content]})
            write_table_json_pair(
                rows,
                doc.id,
                md_doc_dir,
                export_root=db_dir,
                json_path=md_doc_dir / f"{doc.id}.json",
                hash_session=hash_session,
            )


def _normalize_copy_assets(copy_assets: Any) -> list[dict[str, Any]]:
    """Normalize copy_assets rules."""
    if isinstance(copy_assets, dict):
        rules = [copy_assets]
    elif isinstance(copy_assets, list):
        rules = copy_assets
    else:
        raise ConfigError("'copy_assets' must be a mapping or list of mappings")

    normalized: list[dict[str, Any]] = []
    allowed_keys = {"from", "to", "include", "clean"}
    for rule in rules:
        if not isinstance(rule, dict):
            raise ConfigError("Each 'copy_assets' entry must be a mapping")
        unknown_keys = sorted(set(rule) - allowed_keys)
        if unknown_keys:
            raise ConfigError(f"Unknown copy_assets keys: {', '.join(unknown_keys)}")
        if "from" not in rule or "to" not in rule:
            raise ConfigError("Each 'copy_assets' entry must define 'from' and 'to'")
        source = rule["from"]
        target = rule["to"]
        if not isinstance(source, str) or not isinstance(target, str):
            raise ConfigError("'copy_assets' 'from' and 'to' must be strings")

        include = rule.get("include")
        if include is None:
            include_patterns: list[str] | None = None
        elif isinstance(include, str):
            include_patterns = [include]
        elif isinstance(include, list) and all(isinstance(p, str) for p in include):
            include_patterns = include
        else:
            raise ConfigError("'copy_assets' 'include' must be a string or list")

        clean = rule.get("clean", False)
        if not isinstance(clean, bool):
            raise ConfigError("'copy_assets' 'clean' must be a boolean")

        normalized.append(
            {
                "from": source,
                "to": target,
                "include": include_patterns,
                "clean": clean,
            }
        )
    return normalized


def _resolve_copy_source(source: str, base_dir: Path) -> Path:
    """Resolve a copy_assets source path."""
    if "://" in source:
        raise ConfigError("'copy_assets' 'from' must be a local path")
    source_path = Path(source)
    if not source_path.is_absolute():
        source_path = (base_dir / source_path).resolve()
    else:
        source_path = source_path.resolve()
    try:
        source_path.stat()
    except FileNotFoundError:
        raise ConfigError(f"copy_assets source not found: {source_path}")
    return source_path


def _resolve_copy_target(target: str, output_dir: Path) -> Path:
    """Resolve a copy_assets destination path."""
    target_path = Path(target)
    if target_path.is_absolute():
        raise ConfigError("'copy_assets' 'to' must be relative to the export directory")

    export_root = output_dir.resolve()
    resolved = (export_root / target_path).resolve()
    if resolved != export_root and export_root not in resolved.parents:
        raise ConfigError("'copy_assets' 'to' must stay within the export directory")
    return resolved


def _matches_copy_include(path: Path, include: list[str] | None) -> bool:
    """Return whether a file matches include patterns."""
    if not include:
        return True
    rel_path = path.as_posix()
    return any(
        fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(path.name, pattern)
        for pattern in include
    )


def _walk_copy_files(source: Path) -> Iterator[tuple[Path, Path, os.stat_result]]:
    """Yield source files, destination-relative paths and source stats."""
    for dirpath, dirnames, filenames in os.walk(source):
        dirnames.sort()
        for filename in sorted(filenames):
            file_path = Path(dirpath) / filename
            try:
                file_stat = file_path.stat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(file_stat.st_mode):
                yield file_path, file_path.relative_to(source), file_stat


def _iter_copy_files(
    source: Path, include: list[str] | None
) -> tuple[list[tuple[Path, Path, os.stat_result]], bool]:
    """Return source files and destination-relative paths."""
    source_stat = source.stat()
    if stat.S_ISREG(source_stat.st_mode):
        rel_path = Path(source.name)
        files = (
            [(source, rel_path, source_stat)]
            if _matches_copy_include(rel_path, include)
            else []
        )
        return files, False
    if not stat.S_ISDIR(source_stat.st_mode):
        return [], False

    return [
        (file_path, rel_path, file_stat)
        for file_path, rel_path, file_stat in _walk_copy_files(source)
        if _matches_copy_include(rel_path, include)
    ], True


def _should_copy_asset(
    source_stat: os.stat_result, destination_stat: os.stat_result | None
) -> bool:
    """Return whether a destination file should be replaced."""
    if destination_stat is None:
        return True
    if not stat.S_ISREG(destination_stat.st_mode):
        return True
    return (
        source_stat.st_size != destination_stat.st_size
        or source_stat.st_mtime > destination_stat.st_mtime
    )


def _clean_copy_target(target_root: Path, expected_files: set[Path]) -> int:
    """Remove stale files from a copy_assets destination."""
    removed = 0
    for dirpath, dirnames, filenames in os.walk(target_root, topdown=False):
        current_dir = Path(dirpath)
        for filename in filenames:
            path = current_dir / filename
            if path not in expected_files:
                path.unlink()
                removed += 1
        for dirname in dirnames:
            path = current_dir / dirname
            try:
                path.rmdir()
            except OSError:
                pass
    return removed


def _copy_assets_impl(
    rules_config: Any, output_dir: Path, *, base_dir: Path, quiet: bool
) -> None:
    """Copy configured assets into an export directory."""
    rules = _normalize_copy_assets(rules_config)
    for rule in rules:
        source = _resolve_copy_source(rule["from"], base_dir)
        target_root = _resolve_copy_target(rule["to"], output_dir)
        files, source_is_dir = _iter_copy_files(source, rule["include"])

        if source_is_dir:
            try:
                target_root.mkdir(parents=True, exist_ok=True)
            except NotADirectoryError as exc:
                raise ConfigError(
                    f"copy_assets destination parent is not a directory: {target_root.parent}"
                ) from exc

        expected_files: set[Path] = set()
        copied = 0
        for source_file, rel_path, source_stat in files:
            destination = target_root / rel_path
            expected_files.add(destination)
            try:
                destination_stat = destination.stat()
            except (FileNotFoundError, NotADirectoryError):
                destination_stat = None
            else:
                if stat.S_ISDIR(destination_stat.st_mode):
                    shutil.rmtree(destination)
                    destination_stat = None
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
            except (FileExistsError, NotADirectoryError) as exc:
                raise ConfigError(
                    f"copy_assets destination parent is not a directory: {destination.parent}"
                ) from exc
            if _should_copy_asset(source_stat, destination_stat):
                shutil.copy2(source_file, destination)
                copied += 1

        removed = (
            _clean_copy_target(target_root, expected_files) if rule["clean"] else 0
        )
        if not quiet:
            print(
                f"  →  copy_assets: {rule['from']} -> {rule['to']} "
                f"({copied} copied, {removed} removed)",
                file=sys.stderr,
            )


def _resolve_copy_base_dir(base_dir: str | Path | None) -> Path:
    """Return the base directory used for relative copy_assets sources."""
    if base_dir is None:
        return Path.cwd().resolve()
    return Path(base_dir).resolve()


@validate_params
def copy_assets(
    output_dir: str | Path,
    rules: Any,
    *,
    base_dir: str | Path | None = None,
    quiet: bool = False,
) -> None:
    """Copy local files or directories into an export directory."""
    _copy_assets_impl(
        rules,
        Path(output_dir),
        base_dir=_resolve_copy_base_dir(base_dir),
        quiet=quiet,
    )


@validate_params
def export_db(
    catalog: Catalog,
    output_dir: str | Path | None = None,
    *,
    track_evolution: bool = True,
    copy_assets: Any = None,
    base_dir: str | Path | None = None,
    export_size_report: bool = False,
    quiet: bool | None = None,
) -> None:
    """Write all catalog entities to JSON files."""
    ensure_metadata_applied(catalog)
    # Only finalize (cleanup unseen entities) if a scan was performed
    if catalog._has_scanned:
        catalog.finalize()

    path = output_dir or catalog.db_path
    if path is None:
        msg = "output_dir is required when app_path was not set at init"
        raise ConfigError(msg)
    q = quiet if quiet is not None else catalog.quiet

    # Parent relations for cascade suppression in evolution tracking
    parent_relations = {
        "dataset": "folder",
        "variable": "dataset",
        "frequency": "variable",
        "value": "enumeration",
    }
    apply_metadata_tombstones(catalog)

    if copy_assets is not None:
        export_dir = Path(path)
        copy_assets_base_dir = _resolve_copy_base_dir(base_dir)
        _copy_assets_impl(
            copy_assets,
            export_dir,
            base_dir=copy_assets_base_dir,
            quiet=q,
        )

    preview_ids = sync_preview_exports(catalog, path)
    apply_preview_flags(catalog, preview_ids)
    _sync_markdown_doc_exports(catalog, path)
    _drop_empty_columns(catalog)
    catalog.save(
        path,
        track_evolution=track_evolution,
        timestamp=catalog._now,
        parent_relations=parent_relations,
    )
    _clean_stale_db_files(catalog, Path(path))
    if export_size_report:
        _print_export_size_report(Path(path), quiet=q)


def _get_app_path() -> Path:
    """Return path to bundled app directory."""
    return Path(__file__).parent / "app"


def _is_app_initialized(output_dir: Path) -> bool:
    """Return whether an exported app already exists."""
    return (output_dir / "index.html").is_file()


def _copy_app(output_dir: Path) -> None:
    """Copy datannur app to output directory."""
    app_src = _get_app_path()
    try:
        app_items = list(os.scandir(app_src))
    except FileNotFoundError:
        raise ConfigError(
            "datannur app not found. Run `make download-app` to download it, "
            "or install datannurpy with the app bundled."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    # Copy app files to output_dir. The data directory is local app state; it is
    # created if needed but never removed or overwritten by app asset refreshes.
    for item in app_items:
        dest = output_dir / item.name
        if item.is_dir():
            if item.name == "data":
                dest.mkdir(parents=True, exist_ok=True)
                continue
            try:
                shutil.rmtree(dest)
            except FileNotFoundError:
                pass
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)


@validate_params
def export_app(
    catalog: Catalog,
    output_dir: str | Path | None = None,
    *,
    open_browser: bool = False,
    track_evolution: bool = True,
    update_app: bool = False,
    copy_assets: Any = None,
    base_dir: str | Path | None = None,
    export_size_report: bool = False,
    quiet: bool | None = None,
) -> None:
    """Export a standalone datannur visualization app with catalog data."""
    ensure_metadata_applied(catalog)
    # Only finalize (cleanup unseen entities) if a scan was performed
    if catalog._has_scanned:
        catalog.finalize()

    if output_dir is None:
        if catalog.app_path is None:
            msg = "output_dir is required when app_path was not set at init"
            raise ConfigError(msg)
        output_dir = catalog.app_path

    q = quiet if quiet is not None else catalog.quiet
    output_dir = Path(output_dir)

    start_time = time.perf_counter()
    header = f"\n[export_app] {output_dir.name}"
    if not q:
        print(header, file=sys.stderr)
    _write_log(header)

    # Install app files on first export. Later exports update data/db only unless
    # the caller explicitly requests a front-end bundle refresh.
    if update_app or not _is_app_initialized(output_dir):
        _copy_app(output_dir)

    if copy_assets is not None:
        copy_assets_base_dir = _resolve_copy_base_dir(base_dir)
        _copy_assets_impl(
            copy_assets,
            output_dir,
            base_dir=copy_assets_base_dir,
            quiet=q,
        )

    # Write to data/db/
    db_dir = output_dir / "data" / "db"
    catalog.export_db(db_dir, quiet=True, track_evolution=track_evolution)
    if export_size_report:
        _print_export_size_report(db_dir, quiet=q)

    elapsed = time.perf_counter() - start_time
    index_uri = (output_dir / "index.html").resolve().as_uri()
    summary = f"\n  →  exported in {elapsed:.1f}s: {index_uri}"
    if not q:
        print(summary, file=sys.stderr)
    _write_log(summary)

    if open_browser:
        index_path = output_dir / "index.html"
        webbrowser.open(index_path.resolve().as_uri())
