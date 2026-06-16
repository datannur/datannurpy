"""Tests for Catalog.export_db method."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import polars as pl
import pyarrow as pa
import pytest

from datannurpy import Catalog, EntityMetadata, Folder
from datannurpy import preview as preview_mod
from datannurpy.errors import ConfigError
from datannurpy.preview import (
    _dataset_id_from_preview_file,
    _existing_preview_ids,
    _json_safe_object,
    _preview_files_exist,
    _preview_label,
    _remove_stale_preview_files,
    normalize_preview_df,
    preview_from_ibis,
    sync_preview_exports,
    validate_preview_rows,
)
from datannurpy.schema import Dataset

DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


def _make_employees_catalog() -> Catalog:
    """Scan employees.csv once."""
    catalog = Catalog()
    catalog.add_folder(
        CSV_DIR,
        metadata=EntityMetadata(id="test", name="Test", license="ODbL-1.0"),
        include=["**/employees.csv"],
    )
    catalog.dataset.update("test---employees_csv", license="CC-BY-4.0")
    return catalog


class TestFrequency:
    """Test frequency computation."""

    def test_frequency_default_enabled(self, tmp_path: Path):
        """Catalog should compute frequencies by default (threshold=100)."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\nred\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        assert (tmp_path / "output" / "frequency.json").exists()

    def test_frequency_disabled(self, tmp_path: Path):
        """Catalog(freq_threshold=0) should not compute frequencies."""
        (tmp_path / "data.csv").write_text("color\nred\nblue\n")

        catalog = Catalog(freq_threshold=0)
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        assert not (tmp_path / "output" / "frequency.json").exists()

    def test_frequency_threshold(self, tmp_path: Path):
        """freq_threshold should filter columns by nb_distinct."""
        (tmp_path / "data.csv").write_text("a,b\n1,x\n2,y\n3,z\n")

        catalog = Catalog(freq_threshold=2)
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        # Column a (integer, 3 distinct > threshold 2): no frequency rows
        # Column b (string, 3 distinct > threshold 2): pattern frequencies
        assert (tmp_path / "output" / "frequency.json").exists()
        with open(tmp_path / "output" / "frequency.json") as f:
            data = json.load(f)
        # Only string column b should have pattern frequency entries
        var_ids = {d["variable_id"] for d in data}
        assert all("b" in vid for vid in var_ids)

    def test_frequency_content(self, tmp_path: Path):
        """frequency.json should contain value counts."""
        (tmp_path / "data.csv").write_text("color\nred\nred\nblue\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        with open(tmp_path / "output" / "frequency.json") as f:
            data = json.load(f)

        values = {d["value"]: d["frequency"] for d in data}
        assert values["red"] == 2
        assert values["blue"] == 1

    def test_frequency_multiple_files(self, tmp_path: Path):
        """Frequency export should work with multiple files (union of lazy tables)."""
        # Create two separate CSV files with frequency-eligible columns
        (tmp_path / "file1.csv").write_text("status\nactive\nactive\ninactive\n")
        (tmp_path / "file2.csv").write_text("status\npending\npending\nactive\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)
        catalog.export_db(tmp_path / "output")

        # Should not raise CatalogException about missing table
        assert (tmp_path / "output" / "frequency.json").exists()

        with open(tmp_path / "output" / "frequency.json") as f:
            data = json.load(f)

        # Should have frequency data from both files
        assert len(data) > 0


class TestCatalogWrite:
    """Test Catalog.write method."""

    _shared_catalog: Catalog | None = None

    @staticmethod
    def _get_catalog() -> Catalog:
        """Return a shared catalog scanning employees.csv (created once)."""
        if TestCatalogWrite._shared_catalog is None:
            TestCatalogWrite._shared_catalog = _make_employees_catalog()
        return TestCatalogWrite._shared_catalog

    def test_write_empty_catalog(self, tmp_path):
        """export_db on empty catalog should only create __table__.json."""
        catalog = Catalog()
        catalog.export_db(tmp_path)

        # No entity files should be created (only __table__.json registry)
        assert not (tmp_path / "folder.json").exists()
        assert not (tmp_path / "dataset.json").exists()
        assert not (tmp_path / "variable.json").exists()
        # jsonjsdb always creates __table__.json as table registry
        assert (tmp_path / "__table__.json").exists()

    def test_write_creates_json_files(self, tmp_path):
        """write should create .json files for each entity type."""
        self._get_catalog().export_db(tmp_path)

        assert (tmp_path / "variable.json").exists()
        assert (tmp_path / "dataset.json").exists()
        assert (tmp_path / "folder.json").exists()

    def test_write_creates_jsonjs_files(self, tmp_path):
        """write should create .json.js files by default."""
        self._get_catalog().export_db(tmp_path)

        assert (tmp_path / "variable.json.js").exists()
        assert (tmp_path / "dataset.json.js").exists()
        assert (tmp_path / "folder.json.js").exists()

    def test_write_variable_json_content(self, tmp_path):
        """write should produce valid variable JSON."""
        self._get_catalog().export_db(tmp_path)

        with open(tmp_path / "variable.json") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 9
        assert all("id" in item for item in data)

    def test_write_dataset_json_content(self, tmp_path):
        """write should produce valid dataset JSON."""
        self._get_catalog().export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)

        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["id"] == "test---employees_csv"
        assert data[0]["folder_id"] == "test"
        assert data[0]["license"] == "CC-BY-4.0"

    def test_write_folder_json_content(self, tmp_path):
        """write should produce valid folder JSON."""
        self._get_catalog().export_db(tmp_path)

        with open(tmp_path / "folder.json") as f:
            data = json.load(f)

        assert isinstance(data, list)
        # Filter out auto-generated _enumerations folder
        user_folders = [f for f in data if f["id"] != "_enumerations"]
        assert len(user_folders) == 1
        assert user_folders[0]["id"] == "test"
        assert user_folders[0]["name"] == "Test"
        assert user_folders[0]["license"] == "ODbL-1.0"

    def test_write_jsonjs_format(self, tmp_path):
        """write should produce correct jsonjs format."""
        self._get_catalog().export_db(tmp_path)

        content = (tmp_path / "variable.json.js").read_text()

        # Should start with jsonjs.data assignment
        assert content.startswith("jsonjs.data['variable'] = ")

        # Extract JSON part and parse
        json_part = content.replace("jsonjs.data['variable'] = ", "")
        data = json.loads(json_part)

        # First element should be column names
        assert isinstance(data[0], list)
        assert "id" in data[0]
        assert "name" in data[0]

        # Remaining elements should be data rows
        assert len(data) == 10  # 1 header + 9 variables

    def test_write_creates_output_dir(self, tmp_path):
        """write should create output directory if needed."""
        self._get_catalog().export_db(tmp_path / "nested" / "path")

        assert (tmp_path / "nested" / "path" / "variable.json").exists()

    def test_write_float_to_int(self, tmp_path):
        """write should convert whole floats to ints."""
        self._get_catalog().export_db(tmp_path)

        with open(tmp_path / "variable.json") as f:
            data = json.load(f)

        # nb_distinct should be int, not float
        for item in data:
            if "nb_distinct" in item:
                assert isinstance(item["nb_distinct"], int)

    def test_write_creates_table_registry(self, tmp_path):
        """write should create __table__.json registry."""
        self._get_catalog().export_db(tmp_path)

        assert (tmp_path / "__table__.json").exists()

        with open(tmp_path / "__table__.json") as f:
            data = json.load(f)

        table_names = [t["name"] for t in data]
        assert "folder" in table_names
        assert "dataset" in table_names
        assert "variable" in table_names
        assert all("last_modif" in t for t in data)

    def test_write_organizations(self, tmp_path: Path):
        """export_db should write organization.json when organizations exist."""
        from datannurpy.schema import Organization

        catalog = Catalog()
        catalog.organization.add(Organization(id="org1", name="Organization 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "organization.json").exists()
        assert (tmp_path / "organization.json.js").exists()

        with open(tmp_path / "organization.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "org1"

    def test_preview_rows_exports_json_and_jsonjs(self, tmp_path: Path):
        """preview_rows writes bounded JSON and JSON-JS preview files."""
        data_path = tmp_path / "sales.csv"
        data_path.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")

        catalog = Catalog(depth="stat", preview_rows=2)
        catalog.add_dataset(data_path, metadata=EntityMetadata(id="sales"))
        catalog.export_db(tmp_path / "out")

        preview_path = tmp_path / "out" / "preview" / "sales.json"
        preview_js_path = tmp_path / "out" / "preview" / "sales.json.js"
        assert preview_path.exists()
        assert preview_js_path.exists()

        rows = json.loads(preview_path.read_text())
        assert rows == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        datasets = json.loads((tmp_path / "out" / "dataset.json").read_text())
        assert datasets[0]["has_preview"] == 1
        assert "preview_rows" not in datasets[0]

        content = preview_js_path.read_text()
        assert content.startswith("jsonjs.data['sales'] = ")
        assert json.loads(content.replace("jsonjs.data['sales'] = ", ""))[0] == [
            "id",
            "name",
        ]

    def test_preview_rows_false_disables_export(self, tmp_path: Path):
        """preview_rows=False disables preview files."""
        data_path = tmp_path / "sales.csv"
        data_path.write_text("id,name\n1,Alice\n")

        catalog = Catalog(depth="stat", preview_rows=False)
        catalog.add_dataset(data_path, metadata=EntityMetadata(id="sales"))
        catalog.export_db(tmp_path / "out")

        assert not (tmp_path / "out" / "preview").exists()
        datasets = json.loads((tmp_path / "out" / "dataset.json").read_text())
        assert "has_preview" not in datasets[0]

    def test_dataset_and_variable_depth_do_not_export_previews(self, tmp_path: Path):
        """Structure-only depths do not read rows for previews."""
        for depth in ("dataset", "variable"):
            data_path = tmp_path / f"{depth}.csv"
            data_path.write_text("id,name\n1,Alice\n")
            out_dir = tmp_path / f"out-{depth}"

            catalog = Catalog(depth=depth)
            catalog.add_dataset(data_path, metadata=EntityMetadata(id=depth))
            catalog.export_db(out_dir)

            assert not (out_dir / "preview").exists()

    def test_add_dataset_preview_override_disables_one_source(self, tmp_path: Path):
        """Per-source preview_rows override can disable an individual dataset."""
        public = tmp_path / "public.csv"
        private = tmp_path / "private.csv"
        public.write_text("id\n1\n")
        private.write_text("id\n2\n")

        catalog = Catalog(depth="stat", preview_rows=1)
        catalog.add_dataset(public, metadata=EntityMetadata(id="public"))
        catalog.add_dataset(
            private, metadata=EntityMetadata(id="private"), preview_rows=False
        )
        catalog.export_db(tmp_path / "out")

        assert (tmp_path / "out" / "preview" / "public.json").exists()
        assert not (tmp_path / "out" / "preview" / "private.json").exists()

    def test_invalid_preview_rows_raise_config_error(self, tmp_path: Path):
        """Invalid preview_rows values fail fast."""
        data_path = tmp_path / "sales.csv"
        data_path.write_text("id\n1\n")

        with pytest.raises(ConfigError, match="preview_rows cannot be None"):
            validate_preview_rows(None, allow_none=False)
        with pytest.raises(ConfigError, match="preview_rows must be a non-negative"):
            validate_preview_rows("bad", allow_none=True)  # type: ignore[arg-type]
        with pytest.raises(ConfigError, match="preview_rows must be >= 0"):
            Catalog(preview_rows=-1)
        with pytest.raises(ConfigError, match="preview_rows=true is ambiguous"):
            Catalog(preview_rows=True)  # type: ignore[arg-type]

        catalog = Catalog()
        with pytest.raises(ConfigError, match="preview_rows must be >= 0"):
            catalog.add_dataset(data_path, preview_rows=-1)

    def test_preview_export_removes_stale_files(self, tmp_path: Path):
        """Preview synchronization removes files for disabled or removed datasets."""
        preview_dir = tmp_path / "out" / "preview"
        preview_dir.mkdir(parents=True)
        (preview_dir / "stale.json").write_text("[]")
        (preview_dir / "stale.json.js").write_text("jsonjs.data['stale'] = []")

        catalog = Catalog(depth="stat", preview_rows=False)
        catalog.export_db(tmp_path / "out")

        assert not preview_dir.exists()

    def test_preview_helpers_cover_fallbacks_and_cleanup(self, tmp_path: Path):
        """Preview helpers normalize objects and clean non-preview paths."""

        class FakeLimitedTable:
            def __init__(self, rows: int = 0) -> None:
                self.rows = rows

            def limit(self, rows: int) -> FakeLimitedTable:
                return FakeLimitedTable(rows)

            def to_pyarrow(self) -> pa.Table:
                raise RuntimeError("arrow unavailable")

            def execute(self) -> pa.Table:
                return pa.table({"id": list(range(self.rows))})

        preview = preview_from_ibis(
            cast(Any, FakeLimitedTable()),
            1,
            label="fake",
            quiet=True,
        )
        assert preview is not None
        assert preview.to_dicts() == [{"id": 0}]

        normalized = normalize_preview_df(
            pl.DataFrame(
                {
                    "binary_value": pl.Series([None, b"abc"], dtype=pl.Binary),
                    "object_value": pl.Series([None, {"a": 1}], dtype=pl.Object),
                }
            )
        )
        assert normalized.to_dicts() == [
            {"binary_value": None, "object_value": None},
            {"binary_value": "b'abc'", "object_value": "{'a': 1}"},
        ]
        assert _json_safe_object(None) is None
        assert _json_safe_object({"b": 2}) == "{'b': 2}"

        from datannurpy.schema import Dataset

        label_catalog = Catalog()
        dataset = Dataset(id="fallback", name="Fallback")
        assert _preview_label(label_catalog, dataset) == "Fallback"
        label_catalog._dataset_preview_labels[dataset.id] = "Stored label"
        assert _preview_label(label_catalog, dataset) == "Stored label"

        preview_dir = tmp_path / "preview"
        nested = preview_dir / "nested"
        nested.mkdir(parents=True)
        (nested / "file.txt").write_text("remove me")
        (preview_dir / "keep.json").write_text("[]")
        (preview_dir / "stale.txt").write_text("remove me")

        _remove_stale_preview_files(preview_dir, {"keep"})

        assert (preview_dir / "keep.json").exists()
        assert not _preview_files_exist(preview_dir, "keep")
        (preview_dir / "keep.json.js").write_text("jsonjs.data['keep'] = []")
        assert _preview_files_exist(preview_dir, "keep")
        (preview_dir / "ignored").mkdir()
        (preview_dir / "ignored.txt").write_text("ignore me")
        (preview_dir / "partial.json").write_text("[]")
        assert _existing_preview_ids(preview_dir) == {"keep"}
        assert not nested.exists()
        assert not (preview_dir / "stale.txt").exists()
        assert _dataset_id_from_preview_file(Path("stale.txt")) is None

    def test_sync_preview_exports_scans_existing_preview_dir_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Preview sync does not probe preview files for each dataset."""

        def fail_preview_probe(preview_dir: Path, dataset_id: str) -> bool:
            raise AssertionError("preview files should not be probed")

        preview_dir = tmp_path / "preview"
        preview_dir.mkdir()
        (preview_dir / "kept.json").write_text("[]")
        (preview_dir / "kept.json.js").write_text("jsonjs.data['kept'] = []")

        catalog = Catalog()
        catalog.dataset.add(Dataset(id="kept", preview_rows=0))
        catalog.dataset.add(Dataset(id="disabled", preview_rows=0))
        catalog.dataset.add(Dataset(id="eligible", preview_rows=1))
        monkeypatch.setattr(preview_mod, "_preview_files_exist", fail_preview_probe)

        preview_ids = sync_preview_exports(catalog, tmp_path)

        assert preview_ids == {"kept"}
        assert (preview_dir / "kept.json").exists()
        assert (preview_dir / "kept.json.js").exists()

    def test_sync_preview_exports_skips_missing_preview_files(self, tmp_path: Path):
        """Preview sync skips eligible datasets when no preview data or files exist."""
        catalog = Catalog()
        catalog.dataset.add(Dataset(id="missing", preview_rows=1))

        preview_ids = sync_preview_exports(catalog, tmp_path)

        assert preview_ids == set()
        assert not (tmp_path / "preview").exists()

    def test_sync_preview_exports_avoids_file_probes_without_preview_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Preview sync avoids file probes when the preview directory is absent."""

        def fail_preview_probe(preview_dir: Path, dataset_id: str) -> bool:
            raise AssertionError("preview files should not be probed")

        catalog = Catalog()
        catalog.dataset.add(Dataset(id="eligible", preview_rows=1))
        monkeypatch.setattr(preview_mod, "_preview_files_exist", fail_preview_probe)

        preview_ids = sync_preview_exports(catalog, tmp_path)

        assert preview_ids == set()
        assert not (tmp_path / "preview").exists()

    def test_sync_preview_exports_short_circuits_when_disabled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Preview sync avoids per-dataset probes when previews are disabled."""

        def fail_preview_probe(preview_dir: Path, dataset_id: str) -> bool:
            raise AssertionError("preview files should not be probed")

        catalog = Catalog()
        catalog.dataset.add(Dataset(id="disabled", preview_rows=0))
        monkeypatch.setattr(preview_mod, "_preview_files_exist", fail_preview_probe)

        preview_ids = sync_preview_exports(catalog, tmp_path)

        assert preview_ids == set()
        assert not (tmp_path / "preview").exists()

    def test_incremental_export_preserves_existing_preview(self, tmp_path: Path):
        """Skipped unchanged datasets keep existing preview files."""
        data_path = tmp_path / "sales.csv"
        data_path.write_text("id\n1\n2\n")
        app_path = tmp_path / "app"

        catalog = Catalog(app_path=app_path, depth="stat", preview_rows=1)
        catalog.add_dataset(data_path, metadata=EntityMetadata(id="sales"))
        catalog.export_db(catalog.db_path)

        preview_path = app_path / "data" / "db" / "preview" / "sales.json"
        preview_js_path = app_path / "data" / "db" / "preview" / "sales.json.js"
        preview_path.write_text('[{"id": 99}]')
        preview_js_path.write_text("jsonjs.data['sales'] = [[\"id\"], [99]]")

        incremental = Catalog(app_path=app_path, depth="stat", preview_rows=1)
        incremental.add_dataset(data_path, metadata=EntityMetadata(id="sales"))
        incremental.export_db(incremental.db_path)

        assert json.loads(preview_path.read_text()) == [{"id": 99}]
        assert preview_js_path.read_text() == "jsonjs.data['sales'] = [[\"id\"], [99]]"
        datasets = json.loads((app_path / "data" / "db" / "dataset.json").read_text())
        assert datasets[0]["has_preview"] == 1

    def test_database_preview_uses_row_limit(self, tmp_path: Path):
        """Database previews honor the configured row limit."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        conn.executemany(
            "INSERT INTO users VALUES (?, ?)",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        )
        conn.commit()
        conn.close()

        catalog = Catalog(depth="stat", preview_rows=2)
        catalog.add_database(f"sqlite:///{db_path}", metadata=EntityMetadata(id="db"))
        catalog.export_db(tmp_path / "out")

        rows = json.loads(
            (tmp_path / "out" / "preview" / "db---users.json").read_text()
        )
        assert len(rows) == 2

    def test_write_tags(self, tmp_path: Path):
        """export_db should write tag.json when tags exist."""
        from datannurpy.schema import Tag

        catalog = Catalog()
        catalog.tag.add(Tag(id="tag1", name="Tag 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "tag.json").exists()
        assert (tmp_path / "tag.json.js").exists()

        with open(tmp_path / "tag.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "tag1"

    def test_write_docs(self, tmp_path: Path):
        """export_db should write doc.json when docs exist."""
        from datannurpy.schema import Doc

        catalog = Catalog()
        catalog.doc.add(Doc(id="doc1", name="Doc 1"))
        catalog.export_db(tmp_path)

        assert (tmp_path / "doc.json").exists()
        assert (tmp_path / "doc.json.js").exists()

        with open(tmp_path / "doc.json") as f:
            data = json.load(f)
        assert len(data) == 1
        assert data[0]["id"] == "doc1"

    def test_export_db_builds_markdown_doc_files(self, tmp_path: Path):
        """Local markdown Doc paths are compiled to md-doc JSON files."""
        from datannurpy.schema import Doc

        app_dir = tmp_path / "app"
        source_dir = app_dir / "data" / "db-source" / "md"
        source_dir.mkdir(parents=True)
        (source_dir / "guide.md").write_text("# Guide\n\nHello", encoding="utf-8")

        catalog = Catalog()
        catalog.doc.add(
            Doc(
                id="guide",
                type="md",
                path="data/db-source/md/guide.md",
            )
        )
        catalog.export_db(app_dir / "data" / "db")

        md_doc_json = app_dir / "data" / "db" / "md-doc" / "guide.json"
        assert json.loads(md_doc_json.read_text()) == [{"content": "# Guide\n\nHello"}]
        assert (app_dir / "data" / "db" / "md-doc" / "guide.json.js").exists()
        hashes = json.loads(
            (app_dir / "data" / "db" / "_meta" / "json-hashes.json").read_text()
        )
        assert "md-doc/guide.json" in hashes

    def test_export_db_builds_markdown_doc_files_in_db_export(self, tmp_path: Path):
        """Markdown Doc paths also resolve relative to db-only output dirs."""
        from datannurpy.schema import Doc

        source_dir = tmp_path / "output" / "docs"
        source_dir.mkdir(parents=True)
        (source_dir / "guide.md").write_text("# Guide", encoding="utf-8")

        catalog = Catalog()
        catalog.doc.add(Doc(id="guide", type="md", path="docs/guide.md"))
        catalog.export_db(tmp_path / "output")

        md_doc_json = tmp_path / "output" / "md-doc" / "guide.json"
        assert json.loads(md_doc_json.read_text()) == [{"content": "# Guide"}]

    def test_export_db_rewrites_relative_markdown_doc_links(self, tmp_path: Path):
        """Relative Markdown links are resolved from the source document directory."""
        from datannurpy.schema import Doc

        source_dir = tmp_path / "output" / "docs" / "example"
        source_dir.mkdir(parents=True)
        (source_dir / "readme.md").write_text(
            "![Image](images/schema.png)\n"
            "[Details](details.md)\n"
            "[Parent](../shared.md)\n"
            "[Wrapped](<assets/schema.svg>)\n"
            "[WrappedExternal](<https://example.org/file>)\n"
            "[Query](details.md?tab=1#section)\n"
            "[External](https://example.org)\n"
            "[Root](/doc/file.pdf)\n"
            "[Anchor](#section)\n"
            "[Mail](mailto:test@example.org)",
            encoding="utf-8",
        )

        catalog = Catalog()
        catalog.doc.add(Doc(id="readme", type="md", path="docs/example/readme.md"))
        catalog.export_db(tmp_path / "output")

        md_doc_json = tmp_path / "output" / "md-doc" / "readme.json"
        image_path = (source_dir / "images" / "schema.png").absolute().as_posix()
        details_path = (source_dir / "details.md").absolute().as_posix()
        parent_path = (source_dir / ".." / "shared.md").absolute().as_posix()
        wrapped_path = (source_dir / "assets" / "schema.svg").absolute().as_posix()
        assert json.loads(md_doc_json.read_text()) == [
            {
                "content": f"![Image]({image_path})\n"
                f"[Details]({details_path})\n"
                f"[Parent]({parent_path})\n"
                f"[Wrapped](<{wrapped_path}>)\n"
                "[WrappedExternal](<https://example.org/file>)\n"
                f"[Query]({details_path}?tab=1#section)\n"
                "[External](https://example.org)\n"
                "[Root](/doc/file.pdf)\n"
                "[Anchor](#section)\n"
                "[Mail](mailto:test@example.org)"
            }
        ]

    def test_export_db_skips_non_local_markdown_doc_files(self, tmp_path: Path):
        """Only existing local markdown Doc paths are compiled."""
        from datannurpy.schema import Doc

        catalog = Catalog()
        catalog.doc.add(Doc(id="pdf", type="pdf", path="docs/file.pdf"))
        catalog.doc.add(Doc(id="missing", type="md", path="docs/missing.md"))
        catalog.doc.add(Doc(id="remote", type="md", path="https://example.com/doc.md"))
        catalog.doc.add(Doc(id="empty", type="md"))
        catalog.export_db(tmp_path)

        assert not (tmp_path / "md-doc").exists()


class TestDatasetIncrementalFields:
    """Test Dataset incremental scan fields export."""

    def test_dataset_last_update_date_exported(self, tmp_path: Path):
        """last_update_date should be exported to JSON when set."""
        from datannurpy.schema import Dataset

        catalog = Catalog()
        ds = Dataset(
            id="test",
            name="Test",
            last_update_date="2024/02/01T00:00:00",
        )
        catalog.dataset.add(ds)
        catalog.export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)
        assert data[0]["last_update_date"] == "2024/02/01T00:00:00"

    def test_dataset_schema_signature_exported(self, tmp_path: Path):
        """schema_signature should be exported to JSON when set."""
        from datannurpy.schema import Dataset

        catalog = Catalog()
        ds = Dataset(
            id="test",
            name="Test",
            schema_signature="abc123hash",
        )
        catalog.dataset.add(ds)
        catalog.export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)
        assert data[0]["schema_signature"] == "abc123hash"

    def test_dataset_incremental_fields_not_exported_when_none(self, tmp_path: Path):
        """Incremental fields should not appear in JSON when None."""
        from datannurpy.schema import Dataset

        catalog = Catalog()
        ds = Dataset(id="test", name="Test")
        catalog.dataset.add(ds)
        catalog.export_db(tmp_path)

        with open(tmp_path / "dataset.json") as f:
            data = json.load(f)
        # Empty columns are stripped from the export
        assert "last_update_date" not in data[0]
        assert "schema_signature" not in data[0]


class TestSerializationEdgeCases:
    """Test edge cases in catalog export serialization."""

    def test_export_empty_frequency_tables(self, tmp_path: Path):
        """export_db with empty frequency table should not create frequency.json."""
        catalog = Catalog()
        # No frequency entries added
        catalog.export_db(tmp_path)
        assert not (tmp_path / "frequency.json").exists()


class TestEvolutionTracking:
    """Test evolution tracking in export_db."""

    def test_track_evolution_disabled(self, tmp_path: Path):
        """export_db(track_evolution=False) should not create evolution.json."""
        catalog = Catalog()
        catalog.folder.add(Folder(id="test", name="Test"))
        catalog.export_db(tmp_path, track_evolution=False)

        assert not (tmp_path / "evolution.json").exists()
        assert (tmp_path / "folder.json").exists()

    def test_track_evolution_no_changes(self, tmp_path: Path):
        """export_db should not create evolution.json when no changes detected."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"

        # First export - no evolution.json should be created (initial state)
        catalog1 = Catalog(app_path=app_dir)
        catalog1.folder.add(Folder(id="test", name="Test"))
        catalog1.export_db()
        assert not (db_dir / "evolution.json").exists()

        # Load the same data and export again (no changes)
        catalog2 = Catalog(app_path=app_dir)
        catalog2.export_db()

        # No changes = no evolution.json created
        assert not (db_dir / "evolution.json").exists()

    def test_track_evolution_with_changes(self, tmp_path: Path):
        """export_db should create evolution.json when changes are detected."""
        app_dir = tmp_path / "app"
        db_dir = app_dir / "data" / "db"

        # First export
        catalog1 = Catalog(app_path=app_dir)
        catalog1.folder.add(Folder(id="test", name="Original"))
        catalog1.export_db()
        assert not (db_dir / "evolution.json").exists()

        # Load, modify, and export again
        catalog2 = Catalog(app_path=app_dir)
        catalog2.folder.update("test", name="Modified")
        catalog2.export_db()

        # Modification should create evolution.json
        assert (db_dir / "evolution.json").exists()
        import json

        with open(db_dir / "evolution.json") as f:
            evolution = json.load(f)
        assert len(evolution) == 1
        assert evolution[0]["type"] == "update"
        assert evolution[0]["entity"] == "folder"
        assert evolution[0]["entity_id"] == "test"
        assert evolution[0]["variable"] == "name"
        assert evolution[0]["old_value"] == "Original"
        assert evolution[0]["new_value"] == "Modified"
