"""Export catalog to JSON database and web app."""

from __future__ import annotations

import fnmatch
import shutil
import sys
import time
import webbrowser
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .utils.log import _write_log
from .utils.params import validate_params

if TYPE_CHECKING:
    from .catalog import Catalog

from .add_metadata import ensure_metadata_applied
from .errors import ConfigError


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
    if not source_path.exists():
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


def _iter_copy_files(
    source: Path, include: list[str] | None
) -> list[tuple[Path, Path]]:
    """Return source files and destination-relative paths."""
    if source.is_file():
        rel_path = Path(source.name)
        return [(source, rel_path)] if _matches_copy_include(rel_path, include) else []

    return [
        (file_path, file_path.relative_to(source))
        for file_path in sorted(source.rglob("*"))
        if file_path.is_file()
        and _matches_copy_include(file_path.relative_to(source), include)
    ]


def _should_copy_asset(source: Path, destination: Path) -> bool:
    """Return whether a destination file should be replaced."""
    if not destination.exists() or not destination.is_file():
        return True
    source_stat = source.stat()
    destination_stat = destination.stat()
    return (
        source_stat.st_size != destination_stat.st_size
        or source_stat.st_mtime > destination_stat.st_mtime
    )


def _clean_copy_target(target_root: Path, expected_files: set[Path]) -> int:
    """Remove stale files from a copy_assets destination."""
    if not target_root.exists():
        return 0

    removed = 0
    for path in sorted(
        target_root.rglob("*"), key=lambda p: len(p.parts), reverse=True
    ):
        if path.is_file() and path.resolve() not in expected_files:
            path.unlink()
            removed += 1
        elif path.is_dir() and not any(path.iterdir()):
            path.rmdir()
    return removed


def copy_assets(
    copy_assets: Any, output_dir: Path, *, base_dir: Path, quiet: bool
) -> None:
    """Copy configured assets into an export directory."""
    rules = _normalize_copy_assets(copy_assets)
    for rule in rules:
        source = _resolve_copy_source(rule["from"], base_dir)
        target_root = _resolve_copy_target(rule["to"], output_dir)
        files = _iter_copy_files(source, rule["include"])

        if source.is_dir():
            try:
                target_root.mkdir(parents=True, exist_ok=True)
            except NotADirectoryError as exc:
                raise ConfigError(
                    f"copy_assets destination parent is not a directory: {target_root.parent}"
                ) from exc

        expected_files: set[Path] = set()
        copied = 0
        for source_file, rel_path in files:
            destination = target_root / rel_path
            expected_files.add(destination.resolve())
            if destination.exists() and destination.is_dir():
                shutil.rmtree(destination)
            try:
                destination.parent.mkdir(parents=True, exist_ok=True)
            except (FileExistsError, NotADirectoryError) as exc:
                raise ConfigError(
                    f"copy_assets destination parent is not a directory: {destination.parent}"
                ) from exc
            if _should_copy_asset(source_file, destination):
                shutil.copy2(source_file, destination)
                copied += 1

        removed = (
            _clean_copy_target(target_root, expected_files) if rule["clean"] else 0
        )
        if not quiet:
            print(
                f"  → copy_assets: {rule['from']} -> {rule['to']} "
                f"({copied} copied, {removed} removed)",
                file=sys.stderr,
            )


@validate_params
def export_db(
    catalog: Catalog,
    output_dir: str | Path | None = None,
    *,
    track_evolution: bool = True,
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

    # Parent relations for cascade suppression in evolution tracking
    parent_relations = {
        "dataset": "folder",
        "variable": "dataset",
        "frequency": "variable",
        "value": "enumeration",
    }
    catalog.save(
        path,
        track_evolution=track_evolution,
        timestamp=catalog._now,
        parent_relations=parent_relations,
    )


def _get_app_path() -> Path:
    """Return path to bundled app directory."""
    return Path(__file__).parent / "app"


def _copy_app(output_dir: Path) -> None:
    """Copy datannur app to output directory."""
    app_src = _get_app_path()
    if not app_src.exists():
        raise ConfigError(
            "datannur app not found. Run `make download-app` to download it, "
            "or install datannurpy with the app bundled."
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    # Copy app files to output_dir (merge with existing files)
    for item in app_src.iterdir():
        dest = output_dir / item.name
        if item.is_dir():
            if dest.exists():
                shutil.rmtree(dest)
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

    # Copy app files
    _copy_app(output_dir)

    # Write to data/db/
    db_dir = output_dir / "data" / "db"
    catalog.export_db(db_dir, quiet=True, track_evolution=track_evolution)

    elapsed = time.perf_counter() - start_time
    index_uri = (output_dir / "index.html").resolve().as_uri()
    summary = f"\n  → exported in {elapsed:.1f}s: {index_uri}"
    if not q:
        print(summary, file=sys.stderr)
    _write_log(summary)

    if open_browser:
        index_path = output_dir / "index.html"
        webbrowser.open(index_path.resolve().as_uri())
