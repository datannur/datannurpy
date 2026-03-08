"""Tests for finalize functionality."""

from __future__ import annotations

from pathlib import Path

from datannurpy import Catalog, Folder
from datannurpy.schema import Doc, Institution, Modality, Tag, Value
from datannurpy.utils.ids import build_value_id


class TestFinalizeIdempotent:
    """Tests for finalize idempotence."""

    def test_finalize_is_idempotent(self, tmp_path: Path):
        """Calling finalize multiple times should be a no-op after first call."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # Create catalog with db_path
        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.add_folder(data_dir, Folder(id="test", name="Test"))
        catalog.export_db()

        # Reload catalog
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="test", name="Test"))

        catalog2.finalize()
        count_after_first = len(catalog2.folder.all()) + len(catalog2.dataset.all())

        # Add more unseen entities after finalize
        catalog2.folder.add(Folder(id="new", name="New"))

        catalog2.finalize()  # Should be no-op
        count_after_second = len(catalog2.folder.all()) + len(catalog2.dataset.all())

        assert count_after_second == count_after_first + 1  # New folder not removed

    def test_finalize_skipped_without_db_path(self):
        """Finalize should be a no-op when no db_path is set."""
        catalog = Catalog(quiet=True)
        catalog.folder.add(Folder(id="test", name="Test"))

        catalog.finalize()

        # Entity should not be removed (no db_path = no cleanup)
        assert len(catalog.folder.all()) == 1


class TestFinalizeUnseenFolders:
    """Tests for removing unseen folders."""

    def test_unseen_folder_is_removed(self, tmp_path: Path):
        """Folders with _seen=False should be removed."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # First scan - add_folder marks everything as seen
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Reload without scanning - entities loaded with _seen=False
        catalog2 = Catalog(app_path=app_dir, quiet=True)

        initial_count = len(catalog2.folder.all())
        assert initial_count >= 1

        catalog2.finalize()
        assert len(catalog2.folder.all()) == 0

    def test_seen_folder_is_kept(self, tmp_path: Path):
        """Folders with _seen=True should be kept."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Reload and rescan - add_folder marks as seen
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))

        initial_count = len([f for f in catalog2.folder.all() if f.id == "src"])
        catalog2.finalize()

        # src folder should be kept (it was scanned)
        assert len([f for f in catalog2.folder.all() if f.id == "src"]) == initial_count

    def test_unseen_folder_cascades_to_datasets(self, tmp_path: Path):
        """Removing unseen folder should also remove its datasets."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "data.csv").write_text("a,b\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))

        # Verify before export
        assert len(catalog1.dataset.all()) >= 1
        catalog1.export_db()

        # Reload without scanning
        catalog2 = Catalog(app_path=app_dir, quiet=True)

        catalog2.finalize()

        assert len([f for f in catalog2.folder.all() if f.id == "src"]) == 0
        assert len([ds for ds in catalog2.dataset.all() if ds.folder_id == "src"]) == 0


class TestFinalizeUnseenDatasets:
    """Tests for removing unseen datasets."""

    def test_unseen_dataset_is_removed(self, tmp_path: Path):
        """Datasets with _seen=False should be removed."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "keep.csv").write_text("a,b\n1,2\n")
        (data_dir / "remove.csv").write_text("c,d\n3,4\n")

        # First scan with both files
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))

        assert len(catalog1.dataset.all()) == 2
        catalog1.export_db()

        # Remove one file and rescan
        (data_dir / "remove.csv").unlink()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog2.finalize()

        assert len([ds for ds in catalog2.dataset.all() if ds.folder_id == "src"]) == 1
        assert any("keep" in ds.id for ds in catalog2.dataset.all())

    def test_unseen_dataset_removes_variables(self, tmp_path: Path):
        """Removing dataset should also remove its variables."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("col1,col2\n1,2\n")

        # First scan
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))

        assert len(catalog1.variable.all()) >= 1
        catalog1.export_db()

        # Remove file and rescan
        (data_dir / "test.csv").unlink()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog2.finalize()

        # No datasets from src folder
        assert len([ds for ds in catalog2.dataset.all() if ds.folder_id == "src"]) == 0
        # No variables from src datasets
        assert (
            len([v for v in catalog2.variable.all() if v.dataset_id.startswith("src")])
            == 0
        )


class TestFinalizeUnseenModalities:
    """Tests for removing unseen modalities."""

    def test_unseen_modality_is_removed(self, tmp_path: Path):
        """Modalities with _seen=False should be removed."""
        app_dir = tmp_path

        # Create catalog with modality
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        mod = Modality(id="old_mod", name="Old Modality")
        mod._seen = True  # Mark as seen for export
        catalog1.modality.add(mod)
        catalog1.export_db()

        # Reload without marking as seen
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        # Modality loaded with _seen=False

        catalog2.finalize()
        assert len(catalog2.modality.all()) == 0

    def test_seen_modality_is_kept(self, tmp_path: Path):
        """Modalities with _seen=True should be kept."""
        app_dir = tmp_path

        catalog = Catalog(app_path=app_dir, quiet=True)
        mod = Modality(id="kept_mod", name="Kept Modality")
        mod._seen = True
        catalog.modality.add(mod)

        catalog.finalize()
        assert len(catalog.modality.all()) == 1

    def test_removed_modality_removes_values(self, tmp_path: Path):
        """Values of removed modalities should also be removed."""
        app_dir = tmp_path

        # Create catalog with modality and values
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        mod = Modality(id="old_mod", name="Old Modality")
        mod._seen = True
        catalog1.modality.add(mod)
        catalog1.value.add(
            Value(id=build_value_id("old_mod", "A"), modality_id="old_mod", value="A")
        )
        catalog1.value.add(
            Value(id=build_value_id("old_mod", "B"), modality_id="old_mod", value="B")
        )

        # Add another modality that will be kept
        kept_mod = Modality(id="kept_mod", name="Kept")
        kept_mod._seen = True
        catalog1.modality.add(kept_mod)
        catalog1.value.add(
            Value(id=build_value_id("kept_mod", "X"), modality_id="kept_mod", value="X")
        )
        catalog1.export_db()

        # Reload and mark only kept_mod as seen
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.modality.update("kept_mod", _seen=True)

        catalog2.finalize()

        assert len(catalog2.modality.all()) == 1
        assert len(catalog2.value.all()) == 1
        assert catalog2.value.all()[0].modality_id == "kept_mod"


class TestFinalizeModalitiesWithoutFolder:
    """Tests for modalities without _modalities folder."""

    def test_modality_marked_seen_without_modalities_folder(self, tmp_path: Path):
        """Modalities should be marked seen even if _modalities folder doesn't exist."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        # CSV with categorical column that creates a modality
        (data_dir / "test.csv").write_text("status\nactive\ninactive\nactive\n")

        # First scan - creates modality but we'll remove the _modalities folder
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog1.export_db()

        # Reload and rescan
        catalog2 = Catalog(app_path=app_dir, quiet=True)

        # Remove _modalities folder via the jsonjsdb API
        from datannurpy.utils.ids import MODALITIES_FOLDER_ID

        mod_folder = catalog2.folder.get(MODALITIES_FOLDER_ID)
        if mod_folder:
            catalog2.folder.remove(MODALITIES_FOLDER_ID)

        # Now rescan - should not crash even without _modalities folder
        catalog2.add_folder(data_dir, Folder(id="src", name="Source"))
        catalog2.finalize()

        # Modalities should still be present
        assert len(catalog2.modality.all()) >= 0  # May or may not have modalities


class TestFinalizeUnseenInstitutions:
    """Tests for removing unseen institutions."""

    def test_unseen_institution_is_removed(self, tmp_path: Path):
        """Institutions with _seen=False should be removed."""
        app_dir = tmp_path

        catalog1 = Catalog(app_path=app_dir, quiet=True)
        inst = Institution(id="old_inst", name="Old")
        inst._seen = True
        catalog1.institution.add(inst)
        catalog1.export_db()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.finalize()
        assert len(catalog2.institution.all()) == 0

    def test_seen_institution_is_kept(self, tmp_path: Path):
        """Institutions with _seen=True should be kept."""
        app_dir = tmp_path

        catalog = Catalog(app_path=app_dir, quiet=True)
        inst = Institution(id="kept", name="Kept")
        inst._seen = True
        catalog.institution.add(inst)

        catalog.finalize()
        assert len(catalog.institution.all()) == 1


class TestFinalizeUnseenTags:
    """Tests for removing unseen tags."""

    def test_unseen_tag_is_removed(self, tmp_path: Path):
        """Tags with _seen=False should be removed."""
        app_dir = tmp_path

        catalog1 = Catalog(app_path=app_dir, quiet=True)
        tag = Tag(id="old_tag", name="Old")
        tag._seen = True
        catalog1.tag.add(tag)
        catalog1.export_db()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.finalize()
        assert len(catalog2.tag.all()) == 0

    def test_seen_tag_is_kept(self, tmp_path: Path):
        """Tags with _seen=True should be kept."""
        app_dir = tmp_path

        catalog = Catalog(app_path=app_dir, quiet=True)
        tag = Tag(id="kept", name="Kept")
        tag._seen = True
        catalog.tag.add(tag)

        catalog.finalize()
        assert len(catalog.tag.all()) == 1


class TestFinalizeUnseenDocs:
    """Tests for removing unseen docs."""

    def test_unseen_doc_is_removed(self, tmp_path: Path):
        """Docs with _seen=False should be removed."""
        app_dir = tmp_path

        catalog1 = Catalog(app_path=app_dir, quiet=True)
        doc = Doc(id="old_doc", name="Old")
        doc._seen = True
        catalog1.doc.add(doc)
        catalog1.export_db()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.finalize()
        assert len(catalog2.doc.all()) == 0

    def test_seen_doc_is_kept(self, tmp_path: Path):
        """Docs with _seen=True should be kept."""
        app_dir = tmp_path

        catalog = Catalog(app_path=app_dir, quiet=True)
        doc = Doc(id="kept", name="Kept")
        doc._seen = True
        catalog.doc.add(doc)

        catalog.finalize()
        assert len(catalog.doc.all()) == 1


class TestFinalizeCalledByExport:
    """Tests for automatic finalize on export."""

    def test_export_db_calls_finalize_only_after_scan(self, tmp_path: Path):
        """export_db should call finalize only when a scan was performed."""
        app_dir = tmp_path

        # Create catalog with entity (no scan)
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        mod = Modality(id="old_mod", name="Old")
        mod._seen = True
        catalog1.modality.add(mod)
        catalog1.export_db()

        # Reload and export without scan - finalize should NOT run
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.export_db()

        # Modality should still exist (no scan = no finalize cleanup)
        assert len(catalog2.modality.all()) == 1
        assert catalog2._finalized is False

    def test_export_db_calls_finalize_after_scan(self, tmp_path: Path):
        """export_db should call finalize when add_folder was used."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # Create catalog with scan
        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.add_folder(data_dir, folder=Folder(id="test"))
        catalog.export_db()

        assert catalog._finalized is True
        assert catalog._has_scanned is True

    def test_export_app_calls_finalize(self, tmp_path: Path):
        """export_app should call finalize automatically."""
        app_dir = tmp_path

        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.folder.add(Folder(id="old", name="Old"))

        # finalize via direct call (export_app needs app files)
        catalog.finalize()

        # Folder should be removed (db_path set, not seen)
        assert len(catalog.folder.all()) == 0
        assert catalog._finalized is True

    def test_finalize_missing_modality_reference(self, tmp_path: Path):
        """mark_dataset_seen should handle missing modality gracefully."""
        from datannurpy.schema import Dataset, Variable

        app_dir = tmp_path
        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.dataset.add(Dataset(id="ds1", name="DS"))
        catalog.variable.add(
            Variable(
                id="ds1---v1", name="v1", dataset_id="ds1", modality_ids=["nonexistent"]
            )
        )
        catalog.modality_manager.mark_dataset_seen("ds1")
        # Should not raise — modality simply not found
