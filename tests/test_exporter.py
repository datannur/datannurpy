"""Tests for exporter helpers and copy_assets."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from datannurpy.errors import ConfigError
from datannurpy.exporter import (
    _clean_copy_target,
    _normalize_copy_assets,
    _resolve_copy_source,
    _resolve_copy_target,
    _should_copy_asset,
    copy_assets,
)


class TestCopyAssetsHelpers:
    """Test copy_assets helper functions."""

    @pytest.mark.parametrize(
        ("value", "message"),
        [
            ("bad", "must be a mapping or list of mappings"),
            (["bad"], "entry must be a mapping"),
            ([{"from": "a"}], "must define 'from' and 'to'"),
            ([{"from": 1, "to": "x"}], "'from' and 'to' must be strings"),
            ([{"from": "a", "to": "b", "extra": True}], "Unknown copy_assets keys"),
            (
                [{"from": "a", "to": "b", "include": 1}],
                "'include' must be a string or list",
            ),
            ([{"from": "a", "to": "b", "clean": "yes"}], "'clean' must be a boolean"),
        ],
    )
    def test_normalize_copy_assets_errors(self, value: object, message: str):
        """Invalid copy_assets values raise ConfigError."""
        with pytest.raises(ConfigError, match=message):
            _normalize_copy_assets(value)

    def test_normalize_copy_assets_accepts_mapping(self):
        """A single mapping is normalized to a list."""
        result = _normalize_copy_assets(
            {"from": "docs", "to": "data/doc", "include": "*.pdf", "clean": True}
        )
        assert result == [
            {
                "from": "docs",
                "to": "data/doc",
                "include": ["*.pdf"],
                "clean": True,
            }
        ]

    def test_normalize_copy_assets_accepts_include_list(self):
        """Include lists are preserved as-is."""
        result = _normalize_copy_assets(
            {"from": "docs", "to": "data/doc", "include": ["*.pdf", "*.md"]}
        )
        assert result[0]["include"] == ["*.pdf", "*.md"]

    def test_resolve_copy_source_raises_for_non_local_or_missing(self, tmp_path: Path):
        """copy_assets sources must exist and be local paths."""
        with pytest.raises(ConfigError, match="must be a local path"):
            _resolve_copy_source("https://example.com/file.pdf", tmp_path)
        with pytest.raises(ConfigError, match="source not found"):
            _resolve_copy_source("missing", tmp_path)

    def test_resolve_copy_source_accepts_absolute_paths(self, tmp_path: Path):
        """Absolute copy_assets sources are accepted."""
        source = tmp_path / "guide.pdf"
        source.write_text("pdf")
        assert _resolve_copy_source(str(source.resolve()), tmp_path) == source.resolve()

    def test_resolve_copy_target_rejects_absolute_and_escaping_paths(
        self, tmp_path: Path
    ):
        """copy_assets destinations stay inside the export directory."""
        export_dir = tmp_path / "export"
        export_dir.mkdir()
        with pytest.raises(ConfigError, match="must be relative"):
            _resolve_copy_target(str((tmp_path / "absolute").resolve()), export_dir)
        with pytest.raises(ConfigError, match="must stay within"):
            _resolve_copy_target("../outside", export_dir)

    def test_should_copy_asset_uses_size_and_mtime(self, tmp_path: Path):
        """Incremental copy decisions use size and mtime."""
        source = tmp_path / "source.txt"
        source.write_text("same")
        destination = tmp_path / "dest.txt"
        destination.write_text("same")
        os.utime(destination, (source.stat().st_atime, source.stat().st_mtime + 10))

        assert not _should_copy_asset(source, destination)

        source.write_text("newer")
        assert _should_copy_asset(source, destination)

    def test_clean_copy_target_removes_stale_files(self, tmp_path: Path):
        """Stale files are removed and empty directories cleaned up."""
        target = tmp_path / "target"
        keep = target / "keep.txt"
        stale = target / "nested" / "stale.txt"
        keep.parent.mkdir(parents=True)
        stale.parent.mkdir(parents=True)
        keep.write_text("keep")
        stale.write_text("stale")

        removed = _clean_copy_target(target, {keep.resolve()})

        assert removed == 1
        assert keep.exists()
        assert not stale.exists()
        assert not stale.parent.exists()

    def test_clean_copy_target_missing_directory_is_noop(self, tmp_path: Path):
        """Cleaning a missing target does nothing."""
        assert _clean_copy_target(tmp_path / "missing", set()) == 0


class TestCopyAssets:
    """Test copy_assets execution."""

    def test_run_copy_assets_copies_include_and_clean(self, tmp_path: Path):
        """copy_assets copies filtered files and removes stale targets."""
        source = tmp_path / "assets"
        nested = source / "nested"
        nested.mkdir(parents=True)
        (source / "guide.pdf").write_text("pdf")
        (nested / "deep.pdf").write_text("deep")
        (source / "notes.txt").write_text("txt")

        export_dir = tmp_path / "export"
        stale = export_dir / "data" / "doc" / "old.pdf"
        stale.parent.mkdir(parents=True)
        stale.write_text("old")

        copy_assets(
            [{"from": "assets", "to": "data/doc", "include": "*.pdf", "clean": True}],
            export_dir,
            base_dir=tmp_path,
            quiet=True,
        )

        assert (export_dir / "data" / "doc" / "guide.pdf").read_text() == "pdf"
        assert (
            export_dir / "data" / "doc" / "nested" / "deep.pdf"
        ).read_text() == "deep"
        assert not (export_dir / "data" / "doc" / "notes.txt").exists()
        assert not stale.exists()

    def test_run_copy_assets_updates_only_when_needed(self, tmp_path: Path):
        """copy_assets skips unchanged files and updates changed ones."""
        source = tmp_path / "assets"
        source.mkdir()
        source_file = source / "data.json"
        source_file.write_text("old")

        export_dir = tmp_path / "export"
        copy_assets(
            [{"from": "assets", "to": "data/assets"}],
            export_dir,
            base_dir=tmp_path,
            quiet=True,
        )
        destination = export_dir / "data" / "assets" / "data.json"
        first_mtime = destination.stat().st_mtime

        os.utime(
            destination, (destination.stat().st_atime, destination.stat().st_mtime + 10)
        )
        copy_assets(
            [{"from": "assets", "to": "data/assets"}],
            export_dir,
            base_dir=tmp_path,
            quiet=True,
        )
        assert destination.read_text() == "old"
        assert destination.stat().st_mtime == pytest.approx(first_mtime + 10)

        source_file.write_text("new-data")
        copy_assets(
            [{"from": "assets", "to": "data/assets"}],
            export_dir,
            base_dir=tmp_path,
            quiet=True,
        )
        assert destination.read_text() == "new-data"

    def test_run_copy_assets_supports_single_file_source(self, tmp_path: Path):
        """Single files are copied into the destination directory."""
        source_file = tmp_path / "guide.md"
        source_file.write_text("guide")
        export_dir = tmp_path / "export"

        copy_assets(
            [{"from": "guide.md", "to": "data/doc"}],
            export_dir,
            base_dir=tmp_path,
            quiet=True,
        )

        assert (export_dir / "data" / "doc" / "guide.md").read_text() == "guide"

    def test_run_copy_assets_skips_single_file_when_include_does_not_match(
        self, tmp_path: Path
    ):
        """Single-file sources honor include filters."""
        source_file = tmp_path / "guide.md"
        source_file.write_text("guide")
        export_dir = tmp_path / "export"

        copy_assets(
            [{"from": "guide.md", "to": "data/doc", "include": "*.pdf"}],
            export_dir,
            base_dir=tmp_path,
            quiet=True,
        )

        assert not (export_dir / "data" / "doc" / "guide.md").exists()

    def test_run_copy_assets_replaces_conflicting_destination_directory(
        self, tmp_path: Path
    ):
        """Destination directories conflicting with files are replaced."""
        source_file = tmp_path / "guide.md"
        source_file.write_text("guide")
        destination = tmp_path / "export" / "data" / "doc" / "guide.md"
        destination.mkdir(parents=True)
        (destination / "old.txt").write_text("old")

        copy_assets(
            [{"from": "guide.md", "to": "data/doc"}],
            tmp_path / "export",
            base_dir=tmp_path,
            quiet=True,
        )

        assert destination.is_file()
        assert destination.read_text() == "guide"

    def test_run_copy_assets_raises_for_conflicting_parent(self, tmp_path: Path):
        """A file parent where a directory is needed raises ConfigError."""
        source = tmp_path / "assets"
        source.mkdir()
        (source / "doc.txt").write_text("doc")
        export_dir = tmp_path / "export"
        conflict = export_dir / "data"
        conflict.parent.mkdir(parents=True)
        conflict.write_text("not a dir")

        with pytest.raises(ConfigError, match="destination parent is not a directory"):
            copy_assets(
                [{"from": "assets", "to": "data/doc"}],
                export_dir,
                base_dir=tmp_path,
                quiet=True,
            )

    def test_run_copy_assets_raises_for_file_source_parent_conflict(
        self, tmp_path: Path
    ):
        """Single-file sources also reject file parents in the destination path."""
        source_file = tmp_path / "guide.md"
        source_file.write_text("guide")
        export_dir = tmp_path / "export"
        conflict = export_dir / "data"
        conflict.parent.mkdir(parents=True)
        conflict.write_text("not a dir")

        with pytest.raises(ConfigError, match="destination parent is not a directory"):
            copy_assets(
                [{"from": "guide.md", "to": "data/doc"}],
                export_dir,
                base_dir=tmp_path,
                quiet=True,
            )

    def test_run_copy_assets_prints_when_not_quiet(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ):
        """copy_assets reports copied rules when quiet is false."""
        source = tmp_path / "assets"
        source.mkdir()
        (source / "guide.txt").write_text("guide")

        copy_assets(
            [{"from": "assets", "to": "data/doc"}],
            tmp_path / "export",
            base_dir=tmp_path,
            quiet=False,
        )

        assert "copy_assets: assets -> data/doc" in capsys.readouterr().err
