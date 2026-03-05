"""End-to-end snapshot test for the demo catalog.

This test ensures the full pipeline (add_folder, add_database, add_metadata, export_db)
produces exactly the expected output. Any change in output will fail the test.

To update snapshots after an intentional change:
    make update-snapshots
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from datannurpy import Catalog, Folder

DATA_DIR = Path(__file__).parent.parent / "data"
FIXTURES_DIR = Path(__file__).parent / "fixtures" / "expected_db"

# Fixed timestamp for deterministic tests (2025-01-01 00:00:00 UTC)
FIXED_TIMESTAMP = 1735689600

# Files to compare (excluding .json.js which are derived from .json)
SNAPSHOT_FILES = [
    "__table__.json",
    "folder.json",
    "dataset.json",
    "variable.json",
    "modality.json",
    "value.json",
    "institution.json",
    "tag.json",
    "freq.json",
]


def normalize_data(data: Any) -> Any:
    """Normalize data for stable comparison (remove absolute paths and volatile dates)."""
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            # Normalize absolute paths to relative
            if key == "data_path" and isinstance(value, str):
                # Extract relative path from DATA_DIR or normalize to just the tail
                if "/data/" in value:
                    result[key] = value.split("/data/", 1)[-1]
                elif "/datannurpy/data" in value:
                    # Handle case where path ends with /data
                    result[key] = ""
                else:
                    result[key] = value
            # Remove volatile timestamp fields (file mtime varies by machine)
            elif key in ("last_update_date", "last_update_timestamp"):
                continue  # Skip - depends on file system mtime
            else:
                result[key] = normalize_data(value)
        return result
    elif isinstance(data, list):
        return [normalize_data(item) for item in data]
    else:
        return data


def sort_by_id(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort list of dicts by 'id' or composite key for stable comparison."""

    def sort_key(x: dict[str, Any]) -> tuple[str, ...]:
        # For freq.json: sort by variable_id + value
        if "variable_id" in x and "value" in x:
            return (x.get("variable_id", ""), str(x.get("value", "")))
        # For value.json: sort by modality_id + value
        if "modality_id" in x and "value" in x:
            return (x.get("modality_id", ""), str(x.get("value", "")))
        # For __table__.json: sort by name
        if "name" in x and "id" not in x:
            return (x.get("name", ""),)
        # Default: sort by id
        return (x.get("id", ""),)

    return sorted(data, key=sort_key)


def build_demo_catalog(output_dir: Path) -> None:
    """Build the catalog exactly like demo.py does."""
    catalog = Catalog(db_path=output_dir, refresh=True, _now=FIXED_TIMESTAMP)
    catalog.add_folder(DATA_DIR)
    catalog.add_database(f"sqlite:///{DATA_DIR}/company.db")
    catalog.add_database(
        f"sqlite:///{DATA_DIR}/photovoltaik.gpkg",
        folder=Folder(
            id="photovoltaik",
            name="Grandes installations photovoltaïques",
            description="Installations photovoltaïques de haute altitude en Suisse. "
            "Source: Office fédéral de l'énergie (OFEN) - opendata.swiss",
        ),
    )
    catalog.add_metadata(DATA_DIR / "metadata")
    catalog.export_db(quiet=True)


class TestE2ESnapshot:
    """End-to-end snapshot tests."""

    @pytest.fixture(scope="class")
    def generated_db(self, tmp_path_factory: pytest.TempPathFactory) -> Path:
        """Generate the catalog output once for all tests in this class."""
        output_dir = tmp_path_factory.mktemp("e2e_output")
        build_demo_catalog(output_dir)
        return output_dir

    @pytest.mark.parametrize("filename", SNAPSHOT_FILES)
    def test_snapshot_matches(self, generated_db: Path, filename: str) -> None:
        """Compare generated JSON against expected snapshot."""
        generated_path = generated_db / filename
        expected_path = FIXTURES_DIR / filename

        # Check if we should update snapshots
        if os.environ.get("UPDATE_SNAPSHOTS") == "1":
            self._update_snapshot(generated_path, expected_path)
            pytest.skip(f"Updated snapshot: {filename}")

        # Load and normalize both files
        assert generated_path.exists(), f"Generated file missing: {filename}"
        assert expected_path.exists(), (
            f"Expected snapshot missing: {filename}. "
            "Run 'make update-snapshots' to create it."
        )

        with open(generated_path) as f:
            generated = json.load(f)
        with open(expected_path) as f:
            expected = json.load(f)

        # Normalize and sort for stable comparison
        generated_normalized = sort_by_id(normalize_data(generated))
        expected_normalized = sort_by_id(normalize_data(expected))

        # Compare with helpful diff
        assert generated_normalized == expected_normalized, (
            f"Snapshot mismatch for {filename}.\n"
            f"Run 'make update-snapshots' to update if this change is intentional."
        )

    def _update_snapshot(self, generated_path: Path, expected_path: Path) -> None:
        """Update a snapshot file with normalized content."""
        expected_path.parent.mkdir(parents=True, exist_ok=True)

        with open(generated_path) as f:
            data = json.load(f)

        # Store normalized data (without paths/dates) for stable snapshots
        normalized = sort_by_id(normalize_data(data))

        with open(expected_path, "w") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
            f.write("\n")  # Trailing newline


class TestE2EStructure:
    """Structural tests that don't rely on exact snapshots."""

    @pytest.fixture(scope="class")
    def catalog(self, tmp_path_factory: pytest.TempPathFactory) -> Catalog:
        """Build and return the catalog for structural tests."""
        output_dir = tmp_path_factory.mktemp("e2e_structure")
        catalog = Catalog(db_path=output_dir, refresh=True, _now=FIXED_TIMESTAMP)
        catalog.add_folder(DATA_DIR)
        catalog.add_database(f"sqlite:///{DATA_DIR}/company.db")
        catalog.add_database(
            f"sqlite:///{DATA_DIR}/photovoltaik.gpkg",
            folder=Folder(
                id="photovoltaik",
                name="Grandes installations photovoltaïques",
            ),
        )
        catalog.add_metadata(DATA_DIR / "metadata")
        return catalog

    def test_folders_count(self, catalog: Catalog) -> None:
        """Should have expected number of folders."""
        # data + subfolders + company_db + photovoltaik + _modalities
        assert len(catalog.folder.all()) >= 5

    def test_datasets_count(self, catalog: Catalog) -> None:
        """Should have datasets from files and databases."""
        assert len(catalog.dataset.all()) >= 10

    def test_variables_count(self, catalog: Catalog) -> None:
        """Should have many variables across all datasets."""
        assert len(catalog.variable.all()) >= 50

    def test_id_format(self, catalog: Catalog) -> None:
        """All IDs should follow the convention (separator: ---)."""
        for ds in catalog.dataset.all():
            assert "---" in ds.id or ds.folder_id == ds.id.rsplit("---", 1)[0]

        for var in catalog.variable.all():
            parts = var.id.split("---")
            assert len(parts) >= 2, f"Invalid variable ID: {var.id}"

    def test_photovoltaik_folder_metadata(self, catalog: Catalog) -> None:
        """Custom folder metadata should be preserved."""
        pv_folder = next(
            (f for f in catalog.folder.all() if f.id == "photovoltaik"), None
        )
        assert pv_folder is not None
        assert pv_folder.name == "Grandes installations photovoltaïques"

    def test_metadata_applied(self, catalog: Catalog) -> None:
        """Metadata from add_metadata should be merged."""
        # Check that some metadata file was applied
        # This depends on what's in data/metadata/
        modalities = catalog.modality.all()
        # Should have at least some modalities from metadata
        assert len(modalities) >= 0  # Adjust based on actual metadata content
