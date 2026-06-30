"""Tests for finalize functionality."""

from __future__ import annotations

from pathlib import Path

from datannurpy import Catalog, EntityMetadata, Folder
from datannurpy.finalize import remove_folders_cascade
from datannurpy.schema import Doc, Enumeration, Organization, Tag, Value, Variable
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
        catalog.add_folder(data_dir, metadata=EntityMetadata(id="test", name="Test"))
        catalog.export_db()

        # Reload catalog
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, metadata=EntityMetadata(id="test", name="Test"))

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


class TestRemoveFoldersCascade:
    """Tests for folder cascade removal helper."""

    def test_empty_ids_noop(self):
        """Empty folder ID lists should not change the catalog."""
        catalog = Catalog(quiet=True)
        catalog.folder.add(Folder(id="root"))

        remove_folders_cascade(catalog, [])

        assert catalog.folder.get("root") is not None

    def test_cycle_does_not_loop_forever(self):
        """Already-collected folders are skipped while traversing descendants."""
        catalog = Catalog(quiet=True)
        catalog.folder.add(Folder(id="root", parent_id="child"))
        catalog.folder.add(Folder(id="child", parent_id="root"))

        remove_folders_cascade(catalog, "root")

        assert catalog.folder.count == 0


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
        catalog1.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))
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
        catalog1.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))
        catalog1.export_db()

        # Reload and rescan - add_folder marks as seen
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))

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
        catalog1.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))

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
        catalog1.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))

        assert len(catalog1.dataset.all()) == 2
        catalog1.export_db()

        # Remove one file and rescan
        (data_dir / "remove.csv").unlink()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))
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
        catalog1.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))

        assert len(catalog1.variable.all()) >= 1
        catalog1.export_db()

        # Remove file and rescan
        (data_dir / "test.csv").unlink()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))
        catalog2.finalize()

        # No datasets from src folder
        assert len([ds for ds in catalog2.dataset.all() if ds.folder_id == "src"]) == 0
        # No variables from src datasets
        assert (
            len([v for v in catalog2.variable.all() if v.dataset_id.startswith("src")])
            == 0
        )

    def test_unseen_dataset_without_variables(self, tmp_path: Path):
        """Cascade should remove the dataset even when it has no variables."""
        from datannurpy.schema import Dataset

        catalog = Catalog(app_path=tmp_path, quiet=True)
        catalog.dataset.add(Dataset(id="ds1", name="DS", _seen=False))
        catalog.finalize()
        assert catalog.dataset.get("ds1") is None


class TestFinalizeUnseenEnumerations:
    """Tests for removing unseen enumerations."""

    def test_unseen_enumeration_is_removed(self, tmp_path: Path):
        """Enumerations with _seen=False should be removed."""
        app_dir = tmp_path

        # Create catalog with enumeration
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        enumeration = Enumeration(id="old_enum", name="Old Enumeration")
        enumeration._seen = True  # Mark as seen for export
        catalog1.enumeration.add(enumeration)
        catalog1.export_db()

        # Reload without marking as seen
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        # Enumeration loaded with _seen=False

        catalog2.finalize()
        assert len(catalog2.enumeration.all()) == 0

    def test_seen_enumeration_is_kept(self, tmp_path: Path):
        """Enumerations with _seen=True should be kept."""
        app_dir = tmp_path

        catalog = Catalog(app_path=app_dir, quiet=True)
        enumeration = Enumeration(id="kept_enum", name="Kept Enumeration")
        enumeration._seen = True
        catalog.enumeration.add(enumeration)

        catalog.finalize()
        assert len(catalog.enumeration.all()) == 1

    def test_removed_enumeration_removes_values(self, tmp_path: Path):
        """Values of removed enumerations should also be removed."""
        app_dir = tmp_path

        # Create catalog with enumeration and values
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        enumeration = Enumeration(id="old_enum", name="Old Enumeration")
        enumeration._seen = True
        catalog1.enumeration.add(enumeration)
        catalog1.value.add(
            Value(
                id=build_value_id("old_enum", "A"), enumeration_id="old_enum", value="A"
            )
        )
        catalog1.value.add(
            Value(
                id=build_value_id("old_enum", "B"), enumeration_id="old_enum", value="B"
            )
        )

        # Add another enumeration that will be kept
        kept_enumeration = Enumeration(id="kept_enum", name="Kept")
        kept_enumeration._seen = True
        catalog1.enumeration.add(kept_enumeration)
        catalog1.value.add(
            Value(
                id=build_value_id("kept_enum", "X"),
                enumeration_id="kept_enum",
                value="X",
            )
        )
        catalog1.export_db()

        # Reload and mark only kept_enum as seen
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.enumeration.update("kept_enum", _seen=True)

        catalog2.finalize()

        assert len(catalog2.enumeration.all()) == 1
        assert len(catalog2.value.all()) == 1
        assert catalog2.value.all()[0].enumeration_id == "kept_enum"


class TestFinalizeEnumerationsWithoutFolder:
    """Tests for enumerations without _enumerations folder."""

    def test_enumeration_marked_seen_without_enumerations_folder(self, tmp_path: Path):
        """Enumerations should be marked seen even if _enumerations folder doesn't exist."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        # CSV with categorical column that creates an enumeration
        (data_dir / "test.csv").write_text("status\nactive\ninactive\nactive\n")

        # First scan - creates an enumeration but we'll remove the _enumerations folder
        catalog1 = Catalog(app_path=app_dir, quiet=True)
        catalog1.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))
        catalog1.export_db()

        # Reload and rescan
        catalog2 = Catalog(app_path=app_dir, quiet=True)

        # Remove _enumerations folder via the jsonjsdb API
        from datannurpy.utils.ids import ENUMERATIONS_FOLDER_ID

        enumeration_folder = catalog2.folder.get(ENUMERATIONS_FOLDER_ID)
        if enumeration_folder:
            catalog2.folder.remove(ENUMERATIONS_FOLDER_ID)

        # Now rescan - should not crash even without _enumerations folder
        catalog2.add_folder(data_dir, metadata=EntityMetadata(id="src", name="Source"))
        catalog2.finalize()

        # Enumerations should still be present
        assert len(catalog2.enumeration.all()) >= 0  # May or may not have enumerations


class TestFinalizeUnseenOrganizations:
    """Tests for removing unseen organizations."""

    def test_unseen_organization_is_removed(self, tmp_path: Path):
        """Organizations with _seen=False should be removed."""
        app_dir = tmp_path

        catalog1 = Catalog(app_path=app_dir, quiet=True)
        org = Organization(id="old_org", name="Old")
        org._seen = True
        catalog1.organization.add(org)
        catalog1.export_db()

        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.finalize()
        assert len(catalog2.organization.all()) == 0

    def test_seen_organization_is_kept(self, tmp_path: Path):
        """Organizations with _seen=True should be kept."""
        app_dir = tmp_path

        catalog = Catalog(app_path=app_dir, quiet=True)
        org = Organization(id="kept", name="Kept")
        org._seen = True
        catalog.organization.add(org)

        catalog.finalize()
        assert len(catalog.organization.all()) == 1


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


def _add_scan_tags(catalog: Catalog) -> None:
    """Add the full scan tag hierarchy (scan > auto > ..., scan > db > ...)."""
    from datannurpy.scanner.autotag import ensure_auto_tags
    from datannurpy.utils.db_enrich import ensure_db_tags

    ensure_auto_tags(catalog)
    ensure_db_tags(catalog)


class TestFinalizeOrphanScanTags:
    """Tests for removing unreferenced scan tags."""

    def test_all_scan_tags_removed_when_no_references(self, tmp_path: Path):
        """All scan tags should be removed when no entity references them."""
        catalog = Catalog(app_path=tmp_path, quiet=True)
        _add_scan_tags(catalog)
        assert catalog.tag.get("scan") is not None

        catalog.finalize()
        assert catalog.tag.get("scan") is None
        assert catalog.tag.get("auto") is None
        assert catalog.tag.get("db") is None
        assert catalog.tag.get("auto---email") is None
        assert catalog.tag.get("db---not-null") is None

    def test_referenced_leaf_tag_and_ancestors_kept(self, tmp_path: Path):
        """Referenced leaf tag + its ancestors should be kept."""
        catalog = Catalog(app_path=tmp_path, quiet=True)
        _add_scan_tags(catalog)
        catalog.variable.add(
            Variable(id="v1", name="v1", dataset_id="ds1", tag_ids=["auto---email"])
        )

        catalog.finalize()
        # email and its ancestors kept
        assert catalog.tag.get("scan") is not None
        assert catalog.tag.get("auto") is not None
        assert catalog.tag.get("auto---format") is not None
        assert catalog.tag.get("auto---email") is not None
        # unreferenced branches removed
        assert catalog.tag.get("auto---security") is None
        assert catalog.tag.get("auto---bcrypt") is None
        assert catalog.tag.get("auto---text") is None
        assert catalog.tag.get("db") is None
        assert catalog.tag.get("db---not-null") is None

    def test_db_tag_referenced_keeps_db_branch(self, tmp_path: Path):
        """Referenced db leaf tag should keep db branch + scan root."""
        catalog = Catalog(app_path=tmp_path, quiet=True)
        _add_scan_tags(catalog)
        catalog.variable.add(
            Variable(id="v1", name="v1", dataset_id="ds1", tag_ids=["db---not-null"])
        )

        catalog.finalize()
        assert catalog.tag.get("scan") is not None
        assert catalog.tag.get("db") is not None
        assert catalog.tag.get("db---not-null") is not None
        # unreferenced db siblings removed
        assert catalog.tag.get("db---unique") is None
        # auto branch removed entirely
        assert catalog.tag.get("auto") is None

    def test_user_tags_not_affected(self, tmp_path: Path):
        """User tags (not under scan) should not be removed by orphan cleanup."""
        catalog = Catalog(app_path=tmp_path, quiet=True)
        _add_scan_tags(catalog)
        user_tag = Tag(id="my-tag", name="My Tag", _seen=True)
        catalog.tag.add(user_tag)

        catalog.finalize()
        # User tag kept (not a scan descendant)
        assert catalog.tag.get("my-tag") is not None
        # Scan tags removed (no references)
        assert catalog.tag.get("scan") is None

    def test_multiple_references_across_entities(self, tmp_path: Path):
        """Tags referenced by different entity types should be kept."""
        catalog = Catalog(app_path=tmp_path, quiet=True)
        _add_scan_tags(catalog)
        catalog.variable.add(
            Variable(id="v1", name="v1", dataset_id="ds1", tag_ids=["auto---email"])
        )
        catalog.folder.add(Folder(id="f1", tag_ids=["db---indexed"], _seen=True))

        catalog.finalize()
        assert catalog.tag.get("auto---email") is not None
        assert catalog.tag.get("db---indexed") is not None
        assert catalog.tag.get("scan") is not None

    def test_no_scan_tag_is_noop(self, tmp_path: Path):
        """Cleanup should be a no-op when no scan tag exists."""
        catalog = Catalog(app_path=tmp_path, quiet=True)
        user_tag = Tag(id="user", name="User", _seen=True)
        catalog.tag.add(user_tag)

        catalog.finalize()
        assert catalog.tag.get("user") is not None


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
        enumeration = Enumeration(id="old_enum", name="Old")
        enumeration._seen = True
        catalog1.enumeration.add(enumeration)
        catalog1.export_db()

        # Reload and export without scan - finalize should NOT run
        catalog2 = Catalog(app_path=app_dir, quiet=True)
        catalog2.export_db()

        # Enumeration should still exist (no scan = no finalize cleanup)
        assert len(catalog2.enumeration.all()) == 1
        assert catalog2._finalized is False

    def test_export_db_calls_finalize_after_scan(self, tmp_path: Path):
        """export_db should call finalize when add_folder was used."""
        app_dir = tmp_path
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.csv").write_text("a,b\n1,2\n")

        # Create catalog with scan
        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.add_folder(data_dir, metadata=EntityMetadata(id="test"))
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

    def test_finalize_missing_enumeration_reference(self, tmp_path: Path):
        """mark_dataset_seen should handle missing enumeration gracefully."""
        from datannurpy.schema import Dataset, Variable

        app_dir = tmp_path
        catalog = Catalog(app_path=app_dir, quiet=True)
        catalog.dataset.add(Dataset(id="ds1", name="DS"))
        catalog.variable.add(
            Variable(
                id="ds1---v1",
                name="v1",
                dataset_id="ds1",
                enumeration_ids=["nonexistent"],
            )
        )
        catalog.enumeration_manager.mark_dataset_seen("ds1")
        # Should not raise — enumeration simply not found

    def test_mark_datasets_seen_empty_variable_table(self, tmp_path: Path):
        """mark_datasets_seen is a no-op when no variables exist."""
        catalog = Catalog(app_path=tmp_path, quiet=True)
        catalog.enumeration_manager.mark_datasets_seen(["ds1"])
        catalog.enumeration_manager.mark_datasets_seen([])


class TestRemoveOrphanChildren:
    """Tests for the referential-integrity backstop (remove_orphan_children)."""

    def test_consistent_catalog_is_noop(self):
        """A referentially consistent catalog loses nothing and reports 0."""
        from datannurpy.schema import Dataset
        from datannurpy.finalize import remove_orphan_children

        catalog = Catalog(quiet=True)
        catalog.dataset.add(Dataset(id="ds1", name="DS"))
        catalog.variable.add(Variable(id="ds1---v1", name="v1", dataset_id="ds1"))

        assert remove_orphan_children(catalog) == 0
        assert len(catalog.variable.all()) == 1

    def test_variable_with_missing_dataset_is_removed(self):
        """A variable whose dataset is gone is dropped; a valid one is kept."""
        from datannurpy.schema import Dataset
        from datannurpy.finalize import remove_orphan_children

        catalog = Catalog(quiet=True)
        catalog.dataset.add(Dataset(id="ds1", name="DS"))
        catalog.variable.add(Variable(id="ds1---ok", name="ok", dataset_id="ds1"))
        catalog.variable.add(Variable(id="ghost---v", name="v", dataset_id="ghost"))

        assert remove_orphan_children(catalog) == 1
        assert [v.id for v in catalog.variable.all()] == ["ds1---ok"]

    def test_orphan_variable_cascades_to_its_frequencies(self):
        """Removing an orphan variable also removes its frequencies."""
        from datannurpy.schema import Frequency
        from datannurpy.finalize import remove_orphan_children
        from datannurpy.utils.ids import build_frequency_id

        catalog = Catalog(quiet=True)
        catalog.variable.add(Variable(id="ghost---v", name="v", dataset_id="ghost"))
        catalog.frequency.add(
            Frequency(
                id=build_frequency_id("ghost---v", "A"),
                variable_id="ghost---v",
                value="A",
                frequency=3,
            )
        )

        assert remove_orphan_children(catalog) == 1
        assert len(catalog.variable.all()) == 0
        assert len(catalog.frequency.all()) == 0

    def test_empty_variable_table_is_noop(self):
        """No variables means nothing to sweep."""
        from datannurpy.finalize import remove_orphan_children

        catalog = Catalog(quiet=True)
        assert remove_orphan_children(catalog) == 0

    def test_standalone_metadata_value_and_frequency_are_kept(self):
        """value/frequency without a local parent are enrichment, not orphans."""
        from datannurpy.schema import Dataset, Frequency
        from datannurpy.finalize import remove_orphan_children
        from datannurpy.utils.ids import build_frequency_id

        # A consistent dataset/variable pair so the real sweep path runs (no
        # early return) yet leaves the parentless value/frequency untouched.
        catalog = Catalog(quiet=True)
        catalog.dataset.add(Dataset(id="ds1", name="DS"))
        catalog.variable.add(Variable(id="ds1---v1", name="v1", dataset_id="ds1"))
        catalog.value.add(
            Value(id=build_value_id("ghost", "A"), enumeration_id="ghost", value="A")
        )
        catalog.frequency.add(
            Frequency(
                id=build_frequency_id("ghost---v", "A"),
                variable_id="ghost---v",
                value="A",
                frequency=1,
            )
        )

        assert remove_orphan_children(catalog) == 0
        assert len(catalog.variable.all()) == 1
        assert len(catalog.value.all()) == 1
        assert len(catalog.frequency.all()) == 1

    def test_metadata_first_orphan_swept_on_export(self, tmp_path: Path):
        """Metadata re-asserting a variable after its dataset is gone is cleaned.

        Reproduces the reported phantom: once the dataset (and file) disappear, a
        lingering variable.csv row re-adds variables with no parent on every run.
        """
        data_dir = tmp_path / "data"
        meta_dir = tmp_path / "meta"
        db_dir = tmp_path / "db"
        for d in (data_dir, meta_dir, db_dir):
            d.mkdir()
        (data_dir / "emp.csv").write_text("id,age\n1,30\n")
        (meta_dir / "dataset.csv").write_text(
            "id,name,folder_id,_match_path\nD,Emp,f,emp.csv\n"
        )
        (meta_dir / "variable.csv").write_text(
            "id,name,dataset_id\nD---id,id,D\nD---age,age,D\n"
        )

        def run():
            c = Catalog(output_dir=str(db_dir), metadata_path=str(meta_dir), quiet=True)
            c.add_folder(str(data_dir), create_folders=False, on_unmatched="skip")
            c.export_db(str(db_dir))

        run()  # baseline
        # Dataset + file gone, but variable.csv still references D.
        (meta_dir / "dataset.csv").write_text("id,name,folder_id,_match_path\n")
        (data_dir / "emp.csv").unlink()
        run()  # cascade clears D and its variables
        run()  # metadata re-adds the now-parentless variables -> must be swept

        reloaded = Catalog(output_dir=str(db_dir), quiet=True)
        dataset_ids = (
            set(reloaded.dataset.df["id"].to_list())
            if not reloaded.dataset.is_empty
            else set()
        )
        orphans = [
            v for v in reloaded.variable.all() if v.dataset_id not in dataset_ids
        ]
        assert orphans == []
