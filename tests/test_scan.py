"""Tests for Catalog.add_folder and Catalog.add_dataset."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import ibis
import ibis.expr.datatypes as dt
from ibis.expr.datatypes.core import IntervalUnit
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyreadstat
import pytest

from datannurpy import Catalog, Folder
from datannurpy.scanner.parquet.core import scan_parquet
from datannurpy.scanner.parquet.discovery import is_hive_partitioned
from datannurpy.scanner.utils import build_variables, ibis_type_to_str

DATA_DIR = Path(__file__).parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


class TestExtractParquetMetadata:
    """Test parquet metadata extraction."""

    def test_metadata_without_description_key(self, tmp_path: Path):
        """Parquet with metadata but no description key should return None."""
        # Create field with metadata but no 'description' key
        field = pa.field("col", pa.int64(), metadata={b"other_key": b"value"})
        schema = pa.schema([field], metadata={b"other_key": b"value"})
        table = pa.table({"col": [1, 2, 3]}, schema=schema)
        path = tmp_path / "no_desc.parquet"
        pq.write_table(table, path)

        _, _, _, metadata = scan_parquet(path)
        assert metadata.description is None
        assert metadata.column_descriptions is None

    def test_partial_column_descriptions(self, tmp_path: Path):
        """Parquet with description for some columns should skip others."""
        # col1 has description, col2 does not
        field1 = pa.field("col1", pa.int64(), metadata={b"description": b"First col"})
        field2 = pa.field("col2", pa.int64())
        schema = pa.schema([field1, field2])
        table = pa.table({"col1": [1], "col2": [2]}, schema=schema)
        path = tmp_path / "partial.parquet"
        pq.write_table(table, path)

        variables, _, _, _ = scan_parquet(path)
        var_by_name = {v.name: v for v in variables}
        assert var_by_name["col1"].description == "First col"
        assert var_by_name["col2"].description is None


class TestScanDeltaExceptions:
    """Test scan_delta exception handling."""

    def test_deltalake_not_installed(self, monkeypatch):
        """Delta scan should warn if deltalake is not installed."""
        # Hide deltalake module
        monkeypatch.setitem(sys.modules, "deltalake", None)

        catalog = Catalog()
        with pytest.warns(UserWarning, match="deltalake not installed"):
            catalog.add_dataset(DATA_DIR / "test_delta", quiet=True)

    def test_deltalake_other_exception(self, monkeypatch):
        """Delta scan should warn on other deltalake errors."""

        def mock_deltatable(*args, **kwargs):
            raise RuntimeError("Some delta error")

        mock_module = MagicMock()
        mock_module.DeltaTable = mock_deltatable
        monkeypatch.setitem(sys.modules, "deltalake", mock_module)

        catalog = Catalog()
        with pytest.warns(UserWarning, match="Failed to extract Delta table metadata"):
            catalog.add_dataset(DATA_DIR / "test_delta", quiet=True)


class TestScanIcebergExceptions:
    """Test scan_iceberg exception handling."""

    def test_pyiceberg_not_installed(self, monkeypatch, tmp_path):
        """Iceberg scan should raise ImportError if pyiceberg is not installed."""
        # Create fake Iceberg structure
        metadata_dir = tmp_path / "metadata"
        metadata_dir.mkdir()
        (metadata_dir / "00000-abc.metadata.json").write_text("{}")

        # Hide pyiceberg module
        monkeypatch.setitem(sys.modules, "pyiceberg", None)
        monkeypatch.setitem(sys.modules, "pyiceberg.table", None)

        catalog = Catalog()
        with pytest.raises(ImportError, match="PyIceberg is required"):
            catalog.add_dataset(tmp_path, quiet=True)


class TestScanStatisticalExceptions:
    """Test scan_statistical exception handling."""

    def test_pyreadstat_not_installed(self, monkeypatch, tmp_path):
        """Statistical scan should raise ImportError if pyreadstat is not installed."""
        sas_file = tmp_path / "test.sas7bdat"
        sas_file.write_bytes(b"")

        # Hide pyreadstat module
        monkeypatch.setitem(sys.modules, "pyreadstat", None)

        catalog = Catalog()
        with pytest.raises(ImportError, match="pyreadstat is required"):
            catalog.add_dataset(sas_file, quiet=True)

    def test_corrupted_file(self, tmp_path):
        """Statistical scan should warn and return empty for corrupted files."""
        sas_file = tmp_path / "corrupted.sas7bdat"
        sas_file.write_bytes(b"not a valid sas file")

        catalog = Catalog()
        with pytest.warns(UserWarning, match="Could not read statistical file"):
            catalog.add_dataset(sas_file, quiet=True)

        # Dataset created but with no variables
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].nb_row == 0
        assert len(catalog.variables) == 0

    def test_column_without_label(self, tmp_path):
        """Statistical scan should handle columns without labels."""
        # Create SPSS file with no column labels
        df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
        spss_file = tmp_path / "no_labels.sav"
        pyreadstat.write_sav(df, spss_file)

        catalog = Catalog()
        catalog.add_dataset(spss_file)

        # Variables should have no descriptions (no labels in file)
        assert len(catalog.variables) == 2
        assert all(v.description is None for v in catalog.variables)


class TestIbisTypeToStr:
    """Test ibis_type_to_str function."""

    def test_unsigned_integer(self):
        """Unsigned integers should map to 'integer'."""
        assert ibis_type_to_str(dt.UInt8()) == "integer"
        assert ibis_type_to_str(dt.UInt64()) == "integer"

    def test_boolean(self):
        """Boolean should map to 'boolean'."""
        assert ibis_type_to_str(dt.Boolean()) == "boolean"

    def test_interval(self):
        """Interval should map to 'duration'."""
        assert ibis_type_to_str(dt.Interval(unit=IntervalUnit.SECOND)) == "duration"

    def test_null(self):
        """Null should map to 'null'."""
        assert ibis_type_to_str(dt.Null()) == "null"


class TestBuildVariables:
    """Test build_variables function."""

    def test_all_columns_skipped(self):
        """build_variables should handle tables where all columns are skipped."""
        # Create a table with only Binary columns (which are auto-skipped)
        table = ibis.memtable({"blob": [b"data"]})
        variables, freq_table = build_variables(
            table, nb_rows=1, infer_stats=True, skip_stats_columns={"blob"}
        )
        assert len(variables) == 1
        assert variables[0].nb_distinct is None  # No stats computed

    def test_oracle_clob_error_handled(self, monkeypatch):
        """build_variables should handle Oracle ORA-22849 error gracefully."""
        table = ibis.memtable({"col": ["a", "b"]})

        def mock_aggregate(*args, **kwargs):
            raise Exception("ORA-22849: cannot use CLOB in COUNT DISTINCT")

        # Patch at the class level since instances are immutable
        monkeypatch.setattr(
            "ibis.expr.types.relations.Table.aggregate",
            MagicMock(side_effect=mock_aggregate),
        )
        variables, freq_table = build_variables(table, nb_rows=2, infer_stats=True)

        assert len(variables) == 1
        assert variables[0].nb_distinct is None  # Stats skipped due to error

    def test_other_exception_reraised(self, monkeypatch):
        """build_variables should reraise non-Oracle exceptions."""
        table = ibis.memtable({"col": ["a", "b"]})

        def mock_aggregate(*args, **kwargs):
            raise ValueError("Some other error")

        monkeypatch.setattr(
            "ibis.expr.types.relations.Table.aggregate",
            MagicMock(side_effect=mock_aggregate),
        )

        with pytest.raises(ValueError, match="Some other error"):
            build_variables(table, nb_rows=2, infer_stats=True)


class TestCatalogRepr:
    """Test Catalog __repr__ method."""

    def test_repr(self):
        """Catalog repr should show counts."""
        catalog = Catalog()
        result = repr(catalog)
        assert "Catalog(" in result
        assert "folders=0" in result
        assert "datasets=0" in result


class TestHivePartitionDetection:
    """Test Hive partition detection."""

    def test_is_hive_partitioned_with_file(self):
        """is_hive_partitioned should return False for a file path."""
        file_path = CSV_DIR / "employees.csv"
        assert is_hive_partitioned(file_path) is False

    def test_is_hive_partitioned_without_parquet_files(self, tmp_path: Path):
        """is_hive_partitioned should return False if partition dir has no parquet files."""
        # Create a Hive-style directory without parquet files
        partition_dir = tmp_path / "year=2024"
        partition_dir.mkdir()
        (partition_dir / "data.csv").write_text("x\n1")  # CSV, not parquet

        assert is_hive_partitioned(tmp_path) is False


class TestLegacyEncoding:
    """Test scanning CSV files with legacy encodings and delimiters."""

    def test_csv_latin1_semicolon_delimiter(self):
        """CSV with latin1 encoding and semicolon delimiter should be scanned correctly."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv")

        # Should find 4 variables: nom, prénom, département, salaire
        assert len(catalog.variables) == 4, (
            f"Expected 4 variables, got {len(catalog.variables)}"
        )

        # Should have 3 data rows
        assert catalog.datasets[0].nb_row == 3, (
            f"Expected 3 rows, got {catalog.datasets[0].nb_row}"
        )

    def test_csv_explicit_encoding(self):
        """CSV scan with explicit encoding should work."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv", csv_encoding="CP1252")

        assert len(catalog.variables) == 4

    def test_csv_all_encodings_fail(self, tmp_path, monkeypatch):
        """CSV scan should warn when all encodings fail."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col\n1")

        # Make all read_csv calls fail
        def mock_read_csv(*args, **kwargs):
            raise duckdb.InvalidInputException("Mocked encoding error")

        monkeypatch.setattr("ibis.backends.duckdb.Backend.read_csv", mock_read_csv)

        catalog = Catalog()
        with pytest.warns(UserWarning, match="Could not parse CSV file"):
            catalog.add_dataset(csv_file, quiet=True)

        # Dataset created but with no variables
        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 0


class TestAddDataset:
    """Test Catalog.add_dataset method."""

    def test_add_dataset_scans_parquet_file(self):
        """add_dataset should scan a single parquet file via scan_file."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test.pq")

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "parquet"
        assert len(catalog.variables) == 3

    def test_add_dataset_scans_file(self):
        """add_dataset should scan a single file."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 9

    def test_add_dataset_with_folder(self):
        """add_dataset with folder should create folder and link."""
        catalog = Catalog()
        catalog.add_dataset(
            CSV_DIR / "employees.csv",
            folder=Folder(id="hr", name="HR Data"),
        )

        # +1 for _modalities folder (auto-created)
        assert len([f for f in catalog.folders if f.id != "_modalities"]) == 1
        assert catalog.folders[0].id == "hr"
        assert catalog.datasets[0].folder_id == "hr"
        assert catalog.datasets[0].id == "hr---employees"

    def test_add_dataset_with_folder_id(self):
        """add_dataset with folder_id should link to existing folder."""
        catalog = Catalog()
        catalog.add_folder(CSV_DIR, Folder(id="data", name="Data"), include=[])
        catalog.add_dataset(CSV_DIR / "employees.csv", folder_id="data")

        assert catalog.datasets[0].folder_id == "data"

    def test_add_dataset_reuses_folder(self):
        """add_dataset should not duplicate folder."""
        catalog = Catalog()
        folder = Folder(id="src", name="Source")
        catalog.add_dataset(CSV_DIR / "employees.csv", folder=folder)
        catalog.add_dataset(CSV_DIR / "regions_france.csv", folder=folder)

        # +1 for _modalities folder (auto-created)
        assert len([f for f in catalog.folders if f.id != "_modalities"]) == 1
        assert len(catalog.datasets) == 2

    def test_add_dataset_with_metadata(self):
        """add_dataset should accept metadata overrides."""
        catalog = Catalog()
        catalog.add_dataset(
            CSV_DIR / "employees.csv",
            name="Employés",
            description="Liste des employés",
            type="référentiel",
            link="https://example.com",
            start_date="2020/01/01",
        )

        ds = catalog.datasets[0]
        assert ds.name == "Employés"
        assert ds.description == "Liste des employés"
        assert ds.type == "référentiel"
        assert ds.link == "https://example.com"
        assert ds.start_date == "2020/01/01"

    def test_add_dataset_standalone_id(self):
        """add_dataset without folder should use filename as ID."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "employees.csv")

        assert catalog.datasets[0].id == "employees"

    def test_add_dataset_inherits_file_description(self):
        """add_dataset should use file metadata description when available."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "cars.sas7bdat")

        assert catalog.datasets[0].description == "Written by SAS"

    def test_add_dataset_explicit_description_not_overwritten(self):
        """add_dataset should keep explicit description over file metadata."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "cars.sas7bdat", description="Custom desc")

        assert catalog.datasets[0].description == "Custom desc"

    def test_add_dataset_not_found(self):
        """add_dataset should raise FileNotFoundError."""
        catalog = Catalog()
        with pytest.raises(FileNotFoundError):
            catalog.add_dataset("/nonexistent/file.csv")

    def test_add_dataset_unsupported_format(self, tmp_path: Path):
        """add_dataset should raise for unsupported formats."""
        (tmp_path / "data.json").write_text("{}")

        catalog = Catalog()
        with pytest.raises(ValueError, match="Unsupported format"):
            catalog.add_dataset(tmp_path / "data.json")

    def test_add_dataset_folder_and_folder_id_error(self):
        """add_dataset should raise if both folder and folder_id given."""
        catalog = Catalog()
        with pytest.raises(ValueError, match="Cannot specify both"):
            catalog.add_dataset(
                CSV_DIR / "employees.csv",
                folder=Folder(id="a", name="A"),
                folder_id="b",
            )

    def test_add_dataset_delta_directory(self):
        """add_dataset should scan a Delta Lake directory."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_delta")

        assert len(catalog.datasets) == 1
        ds = catalog.datasets[0]
        assert ds.id == "test_delta"
        assert ds.delivery_format == "delta"
        assert ds.nb_row == 6
        assert ds.name == "Test Delta Table"  # From Delta metadata
        assert ds.description == "A test Delta Lake table"

    def test_add_dataset_delta_with_overrides(self):
        """add_dataset on Delta should allow metadata overrides."""
        catalog = Catalog()
        catalog.add_dataset(
            DATA_DIR / "test_delta",
            name="Custom Name",
            description="Custom description",
            folder=Folder(id="sales", name="Sales"),
            quiet=True,
        )

        ds = catalog.datasets[0]
        assert ds.id == "sales---test_delta"
        assert ds.name == "Custom Name"
        assert ds.description == "Custom description"
        assert ds.folder_id == "sales"

    def test_add_dataset_hive_directory(self):
        """add_dataset should scan a Hive partitioned directory."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "test_partitioned")

        assert len(catalog.datasets) == 1
        ds = catalog.datasets[0]
        assert ds.id == "test_partitioned"
        assert ds.delivery_format == "parquet"
        assert ds.nb_row == 6

    def test_add_dataset_iceberg_directory(self):
        """add_dataset should scan an Iceberg table directory."""
        catalog = Catalog()
        catalog.add_dataset(DATA_DIR / "iceberg_warehouse" / "default" / "test_table")

        assert len(catalog.datasets) == 1
        ds = catalog.datasets[0]
        assert ds.id == "test_table"
        assert ds.delivery_format == "iceberg"
        assert ds.description == "Sample Iceberg table for testing"

    def test_add_dataset_unknown_directory(self, tmp_path: Path):
        """add_dataset should raise for unknown directory format."""
        (tmp_path / "subdir").mkdir()

        catalog = Catalog()
        with pytest.raises(ValueError, match="not a recognized Parquet format"):
            catalog.add_dataset(tmp_path / "subdir")


@pytest.fixture(scope="module")
def full_catalog():
    """Scan DATA_DIR once, reuse across read-only tests."""
    catalog = Catalog()
    catalog.add_folder(DATA_DIR, Folder(id="test", name="Test"))
    return catalog


class TestAddFolder:
    """Test Catalog.add_folder method."""

    def test_add_folder_scans_csv(self):
        """add_folder should scan CSV files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        assert len(catalog.variables) == 9

    def test_add_folder_scans_excel(self):
        """add_folder should scan Excel files."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, Folder(id="test", name="Test"), include=["*.xlsx"])
        assert len(catalog.variables) > 0

    def test_add_folder_empty_excel(self, tmp_path):
        """add_folder should handle empty Excel files (0 bytes)."""
        (tmp_path / "empty.xlsx").write_bytes(b"")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 0

    def test_add_folder_corrupted_excel(self, tmp_path):
        """add_folder should warn on corrupted Excel files."""
        (tmp_path / "corrupted.xlsx").write_bytes(b"not a real excel file")

        catalog = Catalog()
        with pytest.warns(UserWarning, match="Could not read Excel file"):
            catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 0

    def test_add_folder_empty_sheet_excel(self, tmp_path):
        """add_folder should handle Excel files with empty sheet."""
        # Create valid Excel with empty DataFrame
        pd.DataFrame().to_excel(tmp_path / "empty_sheet.xlsx", index=False)

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 0

    def test_add_folder_scans_parquet(self):
        """add_folder should scan Parquet files (.parquet extension)."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test.parquet"]
        )
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "parquet"
        assert len(catalog.variables) == 3

    def test_add_folder_scans_pq(self):
        """add_folder should scan Parquet files (.pq extension)."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test.pq"]
        )
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "parquet"
        assert len(catalog.variables) == 3

    def test_add_folder_scans_sas(self):
        """add_folder should scan SAS files (.sas7bdat extension)."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "sas"
        assert len(catalog.variables) == 4

    def test_add_folder_extracts_sas_metadata(self):
        """add_folder should extract metadata from SAS files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        # Dataset description from file label
        assert catalog.datasets[0].description == "Written by SAS"
        # Variable descriptions from column labels
        var_by_name = {v.name: v for v in catalog.variables}
        assert var_by_name["MPG"].description == "miles per gallon"
        assert var_by_name["CYL"].description == "number of cylinders"

    def test_add_folder_sas_integer_conversion(self):
        """add_folder should convert SAS float columns with integer values to integer type."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["cars.sas7bdat"]
        )
        var_by_name = {v.name: v for v in catalog.variables}
        # CYL contains only integers (3, 4, 5, 6, 8)
        assert var_by_name["CYL"].type == "integer"
        # MPG contains decimals (14.5, 16.2, etc.)
        assert var_by_name["MPG"].type == "float"

    def test_add_folder_extracts_parquet_metadata(self):
        """add_folder should extract metadata from Parquet files."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR,
            Folder(id="test", name="Test"),
            include=["test_with_metadata.parquet"],
        )
        # Dataset description from schema metadata
        assert catalog.datasets[0].description == "Table des employes de la societe"
        # Variable descriptions from column metadata
        var_by_name = {v.name: v for v in catalog.variables}
        assert var_by_name["id"].description == "Identifiant unique"
        assert var_by_name["name"].description == "Nom complet de la personne"
        assert var_by_name["age"].description == "Age en annees"

    def test_add_folder_detects_delta_table(self):
        """add_folder should detect and scan Delta Lake tables."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test_delta/**"]
        )
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "delta"
        # Name comes from Delta metadata
        assert catalog.datasets[0].name == "Test Delta Table"
        assert len(catalog.variables) == 3
        var_names = {v.name for v in catalog.variables}
        assert var_names == {"id", "name", "age"}

    def test_add_folder_extracts_delta_metadata(self):
        """add_folder should extract Delta Lake metadata."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test_delta/**"]
        )
        # Should extract name and description from Delta metadata
        ds = catalog.datasets[0]
        assert ds.description == "A test Delta Lake table"

    def test_add_folder_detects_hive_partitioned(self):
        """add_folder should detect and scan Hive-partitioned Parquet datasets."""
        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR, Folder(id="test", name="Test"), include=["test_partitioned/**"]
        )
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "hive"
        assert catalog.datasets[0].name == "test_partitioned"
        # Should have all columns including partition columns
        var_names = {v.name for v in catalog.variables}
        assert "year" in var_names
        assert "region" in var_names
        assert catalog.datasets[0].nb_row == 6

    def test_add_folder_detects_iceberg_table(self):
        """add_folder should detect and scan Iceberg tables."""
        # The Iceberg table is at iceberg_warehouse/default/test_table
        iceberg_table_path = DATA_DIR / "iceberg_warehouse" / "default" / "test_table"
        if not iceberg_table_path.exists():
            pytest.skip("iceberg_warehouse table not found")

        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR / "iceberg_warehouse",
            Folder(id="test", name="Test"),
        )
        # Should find the table inside default/test_table
        iceberg_datasets = [
            d for d in catalog.datasets if d.delivery_format == "iceberg"
        ]
        assert len(iceberg_datasets) == 1
        assert iceberg_datasets[0].name == "test_table"
        var_names = {v.name for v in catalog.variables}
        assert "id" in var_names
        assert "name" in var_names
        assert "city" in var_names
        assert "amount" in var_names
        assert iceberg_datasets[0].nb_row == 5

    def test_add_folder_extracts_iceberg_metadata(self):
        """add_folder should extract Iceberg table and column metadata."""
        iceberg_table_path = DATA_DIR / "iceberg_warehouse" / "default" / "test_table"
        if not iceberg_table_path.exists():
            pytest.skip("iceberg_warehouse table not found")

        catalog = Catalog()
        catalog.add_folder(
            DATA_DIR / "iceberg_warehouse",
            Folder(id="test", name="Test"),
        )
        # Dataset description from properties.comment
        ds = catalog.datasets[0]
        assert ds.description == "Sample Iceberg table for testing"
        # Variable descriptions from schema fields
        var_by_name = {v.name: v for v in catalog.variables}
        assert var_by_name["id"].description == "Unique identifier"
        assert var_by_name["name"].description == "Person's full name"
        assert var_by_name["city"].description == "City of residence"
        assert var_by_name["amount"].description == "Transaction amount"

    def test_add_folder_assigns_folder_id(self, full_catalog):
        """add_folder should assign folder_id to datasets."""
        # All datasets should have folder_id starting with root folder ID
        assert all(
            ds.folder_id is not None
            and (ds.folder_id == "test" or ds.folder_id.startswith("test---"))
            for ds in full_catalog.datasets
        )

    def test_add_folder_prefixes_ids(self):
        """add_folder should prefix IDs with folder ID."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="src", name="Source"), include=["employees.csv"]
        )
        assert catalog.datasets[0].id == "src---employees_csv"
        assert catalog.variables[0].id.startswith("src---employees_csv---")

    def test_add_folder_infers_stats(self):
        """add_folder should compute stats by default."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR, Folder(id="test", name="Test"), include=["employees.csv"]
        )
        assert all(v.nb_distinct is not None for v in catalog.variables)
        assert all(v.nb_missing is not None for v in catalog.variables)

    def test_add_folder_without_stats(self):
        """add_folder with infer_stats=False should skip stats."""
        catalog = Catalog()
        catalog.add_folder(
            CSV_DIR,
            Folder(id="test", name="Test"),
            include=["employees.csv"],
            infer_stats=False,
        )
        assert all(v.nb_distinct is None for v in catalog.variables)
        assert all(v.nb_missing is None for v in catalog.variables)

    def test_add_folder_ignores_unknown_formats(self, tmp_path: Path):
        """add_folder should skip files with unknown extensions when using include."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "unknown.xyz").write_text("some data")
        (tmp_path / "readme.txt").write_text("documentation")

        catalog = Catalog()
        # Use include pattern that matches all files, including unknown formats
        catalog.add_folder(tmp_path, include=["*.*"])

        # Only the CSV should be added (unknown formats are skipped)
        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].delivery_format == "csv"

    def test_add_folder_handles_empty_csv(self, tmp_path: Path):
        """add_folder should handle empty CSV files (header only)."""
        (tmp_path / "empty.csv").write_text("col1,col2\n")  # Header only, no data

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].nb_row == 0

    def test_add_folder_not_found(self):
        """add_folder should raise FileNotFoundError for missing path."""
        catalog = Catalog()
        with pytest.raises(FileNotFoundError):
            catalog.add_folder("/nonexistent/path")

    def test_add_folder_default_folder(self):
        """add_folder without folder arg should use directory name."""
        catalog = Catalog()
        catalog.add_folder(DATA_DIR, include=["employees.csv"])
        assert catalog.folders[0].id == "data"
        assert catalog.folders[0].name == "data"

    def test_add_folder_sets_type_filesystem(self, full_catalog):
        """add_folder should set type='filesystem' on all folders."""
        for folder in full_catalog.folders:
            if folder.id != "_modalities":
                assert folder.type == "filesystem"


class TestSubfolders:
    """Test recursive subfolder scanning."""

    def test_add_folder_scans_subdirs(self, tmp_path: Path):
        """add_folder should scan files in subdirectories."""
        # Create nested structure
        subdir = tmp_path / "2024" / "january"
        subdir.mkdir(parents=True)
        (subdir / "sales.csv").write_text("amount,qty\n100,5\n200,10")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].id == "src---2024---january---sales_csv"

    def test_add_folder_creates_subfolders(self, tmp_path: Path):
        """add_folder should create Folder entities for subdirectories."""
        # Create nested structure
        (tmp_path / "2024").mkdir()
        (tmp_path / "2024" / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        # +1 for _modalities folder (auto-created)
        user_folders = [f for f in catalog.folders if f.id != "_modalities"]
        assert len(user_folders) == 2  # root + 2024
        subfolder = user_folders[1]
        assert subfolder.id == "root---2024"
        assert subfolder.parent_id == "root"

    def test_add_folder_nested_subfolders(self, tmp_path: Path):
        """add_folder should handle deeply nested folders."""
        deep = tmp_path / "a" / "b" / "c"
        deep.mkdir(parents=True)
        (deep / "file.csv").write_text("col\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="x", name="X"))

        # Should have: x, x---a, x---a---b, x---a---b---c (+1 for _modalities)
        user_folders = [f for f in catalog.folders if f.id != "_modalities"]
        assert len(user_folders) == 4
        folder_ids = [f.id for f in user_folders]
        assert "x" in folder_ids
        assert "x---a" in folder_ids
        assert "x---a---b" in folder_ids
        assert "x---a---b---c" in folder_ids

    def test_add_folder_subfolder_parent_chain(self, tmp_path: Path):
        """Subfolders should have correct parent_id chain."""
        (tmp_path / "a" / "b").mkdir(parents=True)
        (tmp_path / "a" / "b" / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        folders_by_id = {f.id: f for f in catalog.folders}
        assert folders_by_id["root---a"].parent_id == "root"
        assert folders_by_id["root---a---b"].parent_id == "root---a"

    def test_add_folder_non_recursive(self, tmp_path: Path):
        """add_folder with recursive=False should not scan subdirs."""
        (tmp_path / "root.csv").write_text("x\n1")
        subdir = tmp_path / "sub"
        subdir.mkdir()
        (subdir / "nested.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, recursive=False)

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "root"

    def test_add_folder_non_recursive_with_include(self, tmp_path: Path):
        """add_folder with recursive=False and include should use glob directly."""
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["a.csv"], recursive=False)

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "a"


class TestIdSanitization:
    """Test ID generation with special characters."""

    def test_sanitize_spaces_in_filename(self, tmp_path: Path):
        """Spaces in filenames should be preserved in IDs."""
        (tmp_path / "my file.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert catalog.datasets[0].id == "src---my file_csv"

    def test_sanitize_special_chars(self, tmp_path: Path):
        """Special characters should be replaced with underscore."""
        (tmp_path / "data@2024#v1.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        assert catalog.datasets[0].id == "src---data_2024_v1_csv"

    def test_sanitize_folder_name_with_spaces(self, tmp_path: Path):
        """Folder names with spaces should be handled."""
        subdir = tmp_path / "Year 2024"
        subdir.mkdir()
        (subdir / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"))

        assert "root---Year 2024" in [f.id for f in catalog.folders]

    def test_sanitize_variable_name(self, tmp_path: Path):
        """Variable names with special chars should be sanitized."""
        (tmp_path / "data.csv").write_text("col@name,col#2\n1,2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="src", name="Source"))

        var_ids = [v.id for v in catalog.variables]
        assert "src---data_csv---col_name" in var_ids
        assert "src---data_csv---col_2" in var_ids


class TestIncludeExclude:
    """Test include/exclude glob patterns."""

    def test_include_single_pattern(self, tmp_path: Path):
        """include should filter to matching files only."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "data.xlsx").write_bytes(b"")  # Empty xlsx won't parse

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["*.csv"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "data"

    def test_include_multiple_patterns(self, tmp_path: Path):
        """include should accept multiple patterns."""
        (tmp_path / "a.csv").write_text("x\n1")
        (tmp_path / "b.csv").write_text("y\n2")
        (tmp_path / "c.txt").write_text("ignored")

        catalog = Catalog()
        catalog.add_folder(tmp_path, include=["a.csv", "b.csv"])

        assert len(catalog.datasets) == 2

    def test_exclude_pattern(self, tmp_path: Path):
        """exclude should filter out matching files."""
        (tmp_path / "keep.csv").write_text("x\n1")
        (tmp_path / "skip.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["skip.csv"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "keep"

    def test_exclude_subdirectory(self, tmp_path: Path):
        """exclude should filter out subdirectories."""
        (tmp_path / "data.csv").write_text("x\n1")
        archive = tmp_path / "archive"
        archive.mkdir()
        (archive / "old.csv").write_text("y\n2")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["archive"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "data"

    def test_exclude_nested_subdirectory(self, tmp_path: Path):
        """exclude should filter out nested subdirectories."""
        (tmp_path / "data.csv").write_text("x\n1")
        archive = tmp_path / "archive"
        archive.mkdir()
        (archive / "keep.csv").write_text("y\n2")
        tmp = archive / "tmp"
        tmp.mkdir()
        (tmp / "old.csv").write_text("z\n3")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["archive/tmp"])

        assert len(catalog.datasets) == 2
        names = {d.name for d in catalog.datasets}
        assert names == {"data", "keep"}

    def test_exclude_glob_pattern(self, tmp_path: Path):
        """exclude should support glob patterns."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "backup.csv").write_text("y\n2")
        (tmp_path / "report.txt").write_text("ignored")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["backup.*"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "data"

    def test_exclude_nonexistent_file(self, tmp_path: Path):
        """exclude with nonexistent file should be ignored."""
        (tmp_path / "data.csv").write_text("x\n1")

        catalog = Catalog()
        catalog.add_folder(tmp_path, exclude=["nonexistent.csv"])

        assert len(catalog.datasets) == 1
        assert catalog.datasets[0].name == "data"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_folder(self, tmp_path: Path):
        """Empty folder should create no datasets."""
        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="empty", name="Empty"))

        assert len(catalog.folders) == 1
        assert len(catalog.datasets) == 0
        assert len(catalog.variables) == 0

    def test_empty_csv_file(self, tmp_path: Path):
        """Empty CSV should create dataset with no variables."""
        (tmp_path / "empty.csv").write_text("")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 0

    def test_csv_headers_only(self, tmp_path: Path):
        """CSV with headers but no data should create variables."""
        (tmp_path / "headers.csv").write_text("col_a,col_b,col_c\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
        assert len(catalog.variables) == 3

    def test_not_a_directory(self, tmp_path: Path):
        """add_folder should raise for file path."""
        file = tmp_path / "file.csv"
        file.write_text("x\n1")

        catalog = Catalog()
        with pytest.raises(NotADirectoryError):
            catalog.add_folder(file)

    def test_add_folder_rejects_dataset_path(self):
        """add_folder should raise for Delta/Hive/Iceberg paths."""
        catalog = Catalog()
        with pytest.raises(ValueError, match="Use add_dataset"):
            catalog.add_folder(DATA_DIR / "test_delta")

    def test_unsupported_file_extension(self, tmp_path: Path):
        """Unsupported files should be ignored."""
        (tmp_path / "data.csv").write_text("x\n1")
        (tmp_path / "readme.txt").write_text("ignored")
        (tmp_path / "config.json").write_text("{}")

        catalog = Catalog()
        catalog.add_folder(tmp_path)

        assert len(catalog.datasets) == 1
