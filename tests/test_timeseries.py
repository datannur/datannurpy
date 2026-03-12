"""Tests for time series detection and scanning."""

from __future__ import annotations

from pathlib import Path

from datannurpy import Catalog, Folder
from datannurpy.scanner.timeseries import (
    PERIOD_PLACEHOLDER,
    build_series_dataset_name,
    compute_variable_periods,
    extract_period,
    group_time_series,
    normalize_path,
    period_sort_key,
)

DATA_DIR = Path(__file__).parent.parent / "data"
TIMESERIES_DIR = DATA_DIR / "timeseries"


class TestPeriodExtraction:
    """Test period pattern extraction from paths."""

    def test_year_only(self):
        """Extract year from filename."""
        path = Path("enquete_2024.csv")
        period = extract_period(path)
        assert period is not None
        assert period.year == 2024
        assert period.sub_period == 0
        assert period.day == 0
        assert period.to_string() == "2024"

    def test_quarter(self):
        """Extract quarter from filename."""
        path = Path("rapport_2024Q1.csv")
        period = extract_period(path)
        assert period is not None
        assert period.year == 2024
        assert period.sub_period == 13  # Q1 stored as 13
        assert period.to_string() == "2024Q1"

    def test_quarter_lowercase(self):
        """Extract lowercase quarter."""
        path = Path("rapport_2024q3.csv")
        period = extract_period(path)
        assert period is not None
        assert period.to_string() == "2024Q3"

    def test_quarter_with_t(self):
        """Extract quarter with T notation."""
        path = Path("rapport_2024-T2.csv")
        period = extract_period(path)
        assert period is not None
        assert period.to_string() == "2024Q2"

    def test_year_month(self):
        """Extract year-month from filename."""
        path = Path("data_2024-03.csv")
        period = extract_period(path)
        assert period is not None
        assert period.year == 2024
        assert period.sub_period == 3
        assert period.to_string() == "2024/03"

    def test_year_month_compact(self):
        """Extract compact year-month (YYYYMM)."""
        path = Path("export_202403.csv")
        period = extract_period(path)
        assert period is not None
        assert period.to_string() == "2024/03"

    def test_full_date(self):
        """Extract full date from filename."""
        path = Path("log_2024-03-15.csv")
        period = extract_period(path)
        assert period is not None
        assert period.to_string() == "2024/03/15"

    def test_period_in_folder(self):
        """Extract period from folder path."""
        path = Path("2024/data.csv")
        period = extract_period(path)
        assert period is not None
        assert period.to_string() == "2024"

    def test_combined_year_quarter(self):
        """Combine year from folder and quarter from filename."""
        path = Path("2024/Q1/export.csv")
        period = extract_period(path)
        assert period is not None
        assert period.to_string() == "2024Q1"

    def test_combined_year_month_day(self):
        """Combine year/month/day from nested folders."""
        path = Path("2024/03/15/export.csv")
        period = extract_period(path)
        assert period is not None
        assert period.to_string() == "2024/03/15"

    def test_no_period(self):
        """Return None if no period found."""
        path = Path("employees.csv")
        period = extract_period(path)
        assert period is None


class TestNormalizePath:
    """Test path normalization for grouping."""

    def test_normalize_year_in_filename(self):
        """Normalize year in filename."""
        path = Path("enquete_2024.csv")
        normalized = normalize_path(path)
        assert PERIOD_PLACEHOLDER in normalized
        assert normalized == f"enquete_{PERIOD_PLACEHOLDER}.csv"

    def test_normalize_year_in_folder(self):
        """Normalize year in folder name."""
        path = Path("2024/enquete.csv")
        normalized = normalize_path(path)
        assert normalized == f"{PERIOD_PLACEHOLDER}/enquete.csv"

    def test_normalize_quarter(self):
        """Normalize quarter in filename."""
        path = Path("rapport_2024Q1.csv")
        normalized = normalize_path(path)
        assert PERIOD_PLACEHOLDER in normalized

    def test_normalize_multiple_periods(self):
        """Normalize multiple periods in path."""
        path = Path("2024/Q1/export.csv")
        normalized = normalize_path(path)
        # Both year and quarter should be replaced
        assert normalized.count(PERIOD_PLACEHOLDER) == 2


class TestPeriodSortKey:
    """Test period sorting."""

    def test_sort_years(self):
        """Sort years chronologically."""
        periods = ["2023", "2021", "2024", "2020"]
        sorted_periods = sorted(periods, key=period_sort_key)
        assert sorted_periods == ["2020", "2021", "2023", "2024"]

    def test_sort_quarters(self):
        """Sort quarters chronologically."""
        periods = ["2024Q2", "2023Q4", "2024Q1", "2023Q1"]
        sorted_periods = sorted(periods, key=period_sort_key)
        assert sorted_periods == ["2023Q1", "2023Q4", "2024Q1", "2024Q2"]

    def test_sort_months(self):
        """Sort months chronologically."""
        periods = ["2024/03", "2023/12", "2024/01"]
        sorted_periods = sorted(periods, key=period_sort_key)
        assert sorted_periods == ["2023/12", "2024/01", "2024/03"]

    def test_sort_mixed(self):
        """Sort mixed period formats."""
        periods = ["2024", "2023Q4", "2024/01"]
        sorted_periods = sorted(periods, key=period_sort_key)
        # Year 2023 with Q4 (sub_period 16) comes before 2024
        # 2024 (sub_period 0) comes before 2024/01 (sub_period 1)
        assert sorted_periods == ["2023Q4", "2024", "2024/01"]


class TestGroupTimeSeries:
    """Test file grouping into time series."""

    def test_group_yearly_files(self, tmp_path: Path):
        """Group files with yearly pattern."""
        files = [
            (tmp_path / "enquete_2020.csv", 1000),
            (tmp_path / "enquete_2021.csv", 2000),
            (tmp_path / "enquete_2023.csv", 3000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 1
        assert len(singles) == 0
        assert len(series[0].files) == 3
        assert series[0].max_mtime == 3000
        # Files should be sorted by period
        assert series[0].files[0][0] == "2020"
        assert series[0].files[-1][0] == "2023"

    def test_group_multiple_series(self, tmp_path: Path):
        """Group multiple independent series."""
        files = [
            (tmp_path / "budget_2022.csv", 1000),
            (tmp_path / "budget_2023.csv", 2000),
            (tmp_path / "census_2023.csv", 3000),
            (tmp_path / "census_2024.csv", 4000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 2
        assert len(singles) == 0

    def test_single_file_not_grouped(self, tmp_path: Path):
        """Single file with period is not grouped."""
        files = [
            (tmp_path / "unique_2024.csv", 1000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 0
        assert len(singles) == 1

    def test_no_period_files(self, tmp_path: Path):
        """Files without period pattern are not grouped."""
        files = [
            (tmp_path / "employees.csv", 1000),
            (tmp_path / "departments.csv", 2000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 0
        assert len(singles) == 2


class TestComputeVariablePeriods:
    """Test variable start_date/end_date computation."""

    def test_variable_present_all_periods(self):
        """Variable present from start to end has null dates."""
        columns = {
            "2020": ["id", "name"],
            "2021": ["id", "name"],
            "2023": ["id", "name"],
        }
        periods = compute_variable_periods(columns)

        assert periods["id"] == (None, None)
        assert periods["name"] == (None, None)

    def test_variable_added_later(self):
        """Variable added in later period has start_date."""
        columns = {
            "2020": ["id", "name"],
            "2021": ["id", "name", "email"],
            "2023": ["id", "name", "email"],
        }
        periods = compute_variable_periods(columns)

        assert periods["id"] == (None, None)
        assert periods["email"] == ("2021", None)

    def test_variable_removed(self):
        """Variable removed has end_date."""
        columns = {
            "2020": ["id", "name", "old_field"],
            "2021": ["id", "name", "old_field"],
            "2023": ["id", "name"],
        }
        periods = compute_variable_periods(columns)

        assert periods["old_field"] == (None, "2021")

    def test_schema_evolution_pattern(self):
        """Test the exact pattern from the doc."""
        # enquete_2020.csv: id, nom, revenu
        # enquete_2021.csv: id, nom, revenu, email
        # enquete_2023.csv: id, nom, email  (revenu removed)
        columns = {
            "2020": ["id", "nom", "revenu"],
            "2021": ["id", "nom", "revenu", "email"],
            "2023": ["id", "nom", "email"],
        }
        periods = compute_variable_periods(columns)

        assert periods["id"] == (None, None)
        assert periods["nom"] == (None, None)
        assert periods["revenu"] == (None, "2021")  # removed after 2021
        assert periods["email"] == ("2021", None)  # added in 2021


class TestBuildSeriesDatasetName:
    """Test dataset name generation for series."""

    def test_period_in_filename(self):
        """Pattern in filename uses readable placeholder."""
        normalized = f"enquete_{PERIOD_PLACEHOLDER}.csv"
        name = build_series_dataset_name(normalized, ["2020", "2021"])
        assert name == "enquete_[YYYY]"

    def test_quarter_pattern(self):
        """Quarter pattern uses appropriate placeholder."""
        normalized = f"rapport_{PERIOD_PLACEHOLDER}.csv"
        name = build_series_dataset_name(normalized, ["2024Q1", "2024Q2"])
        assert name == "rapport_[YYYY]Q[N]"

    def test_period_in_folder_only(self):
        """Period only in folder returns clean name."""
        normalized = f"{PERIOD_PLACEHOLDER}/enquete.csv"
        name = build_series_dataset_name(normalized, ["2020", "2021"])
        assert name == "enquete"


class TestAddFolderTimeSeries:
    """Integration tests for add_folder with time series."""

    def test_yearly_series_creates_single_dataset(self):
        """Yearly series creates one dataset instead of multiple."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "yearly", Folder(id="yearly", name="Yearly"), quiet=True
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        ds = datasets[0]
        assert ds.nb_files == 3
        assert ds.start_date == "2020"
        assert ds.end_date == "2023"

    def test_quarterly_series(self):
        """Quarterly series detection."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "quarterly",
            Folder(id="quarterly", name="Quarterly"),
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_files == 3
        assert datasets[0].start_date == "2023Q1"
        assert datasets[0].end_date == "2024Q1"

    def test_schema_evolution_variables(self):
        """Variables have correct start_date/end_date."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "schema_evolution",
            Folder(id="evolution", name="Evolution"),
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 1

        variables = {v.name: v for v in catalog.variable.all()}

        # id and nom present throughout
        assert variables["id"].start_date is None
        assert variables["id"].end_date is None
        assert variables["nom"].start_date is None
        assert variables["nom"].end_date is None

        # email added in 2021
        assert variables["email"].start_date == "2021"
        assert variables["email"].end_date is None

        # revenu removed after 2021
        assert variables["revenu"].start_date is None
        assert variables["revenu"].end_date == "2021"

    def test_mixed_series_grouped_separately(self):
        """Multiple series in same folder are grouped separately."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "mixed", Folder(id="mixed", name="Mixed"), quiet=True
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 2  # budget_* and census_*

        names = {ds.name for ds in datasets if ds.name}
        # Both series should have readable names
        assert any("budget" in name.lower() for name in names)
        assert any("census" in name.lower() for name in names)

    def test_time_series_disabled(self):
        """time_series=False creates separate datasets."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "yearly",
            Folder(id="yearly", name="Yearly"),
            time_series=False,
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 3  # One per file
        for ds in datasets:
            assert ds.nb_files is None

    def test_variables_union(self):
        """All variables from all files are in the union."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "schema_evolution",
            Folder(id="evolution", name="Evolution"),
            quiet=True,
        )

        variables = catalog.variable.all()
        var_names = {v.name for v in variables}

        # Should have union of all columns
        assert var_names == {"id", "nom", "revenu", "email"}

    def test_structure_mode_with_timeseries(self):
        """Structure mode works with time series."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "yearly",
            Folder(id="yearly", name="Yearly"),
            depth="structure",
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_files == 3
        assert datasets[0].start_date == "2020"
        assert datasets[0].end_date == "2023"
        # No variables in structure mode
        assert len(catalog.variable.all()) == 0

    def test_schema_mode_with_timeseries(self):
        """Schema mode scans columns from all files."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "schema_evolution",
            Folder(id="evolution", name="Evolution"),
            depth="schema",
            quiet=True,
        )

        variables = catalog.variable.all()
        var_names = {v.name for v in variables}
        assert var_names == {"id", "nom", "revenu", "email"}


class TestPeriodEdgeCases:
    """Edge case tests for period extraction and sorting."""

    def test_period_sort_key_with_period_info(self):
        """period_sort_key works with PeriodInfo objects."""
        from datannurpy.scanner.timeseries import PeriodInfo

        info = PeriodInfo(2024, 3, 15, "2024-03-15")
        key = period_sort_key(info)
        assert key == (2024, 3, 15)

    def test_period_sort_key_full_date(self):
        """period_sort_key parses full date strings."""
        assert period_sort_key("2024/03/15") == (2024, 3, 15)

    def test_period_sort_key_compact_month(self):
        """period_sort_key parses compact month format."""
        assert period_sort_key("202403") == (2024, 3, 0)

    def test_period_sort_key_fallback(self):
        """period_sort_key handles unknown formats."""
        assert period_sort_key("some_2024_data") == (2024, 0, 0)
        assert period_sort_key("no_date") == (0, 0, 0)

    def test_compute_variable_periods_empty(self):
        """compute_variable_periods handles empty input."""
        result = compute_variable_periods({})
        assert result == {}

    def test_build_series_name_full_date(self):
        """build_series_dataset_name with full date pattern."""
        normalized = f"log_{PERIOD_PLACEHOLDER}.csv"
        name = build_series_dataset_name(normalized, ["2024/03/15", "2024/03/16"])
        assert name == "log_[YYYY/MM/DD]"

    def test_build_series_name_month_pattern(self):
        """build_series_dataset_name with month pattern."""
        normalized = f"data_{PERIOD_PLACEHOLDER}.csv"
        name = build_series_dataset_name(normalized, ["2024/03", "2024/04"])
        assert name == "data_[YYYY/MM]"

    def test_build_series_name_empty_periods(self):
        """build_series_dataset_name with no periods falls back to [YYYY]."""
        normalized = f"data_{PERIOD_PLACEHOLDER}.csv"
        name = build_series_dataset_name(normalized, [])
        assert name == "data_[YYYY]"

    def test_combine_periods_no_year(self):
        """_combine_periods returns None if no year found."""
        from datannurpy.scanner.timeseries import PeriodInfo, _combine_periods

        # Periods without years
        periods = [
            PeriodInfo(0, 13, 0, "Q1"),  # Quarter only
            PeriodInfo(0, 3, 0, "03"),  # Month only
        ]
        result = _combine_periods(periods)
        assert result is None


class TestTimeSeriesRescan:
    """Tests for time series rescan and edge cases."""

    def test_rescan_modified_timeseries(self, tmp_path: Path):
        """Rescan time series when a file changes."""
        import time

        # Create time series files
        ts_dir = tmp_path / "series"
        ts_dir.mkdir()
        (ts_dir / "data_2020.csv").write_text("id,value\n1,100\n")
        (ts_dir / "data_2021.csv").write_text("id,value\n2,200\n")

        # First scan
        catalog = Catalog()
        catalog.add_folder(ts_dir, Folder(id="test", name="Test"), quiet=True)
        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_files == 2

        # Modify a file
        time.sleep(0.1)  # Ensure mtime changes
        (ts_dir / "data_2021.csv").write_text("id,value,new_col\n2,200,x\n")

        # Rescan same catalog (refresh=True to force rescan)
        catalog.add_folder(
            ts_dir, Folder(id="test", name="Test"), quiet=True, refresh=True
        )
        datasets2 = catalog.dataset.all()
        assert len(datasets2) == 1
        # Variables should include new_col
        var_names = {v.name for v in catalog.variable.all()}
        assert "new_col" in var_names

    def test_empty_timeseries_file(self, tmp_path: Path):
        """Handle time series with empty last file."""
        ts_dir = tmp_path / "empty_series"
        ts_dir.mkdir()
        (ts_dir / "data_2020.csv").write_text("id,value\n1,100\n")
        # Create empty CSV (headers only, 0 rows)
        (ts_dir / "data_2021.csv").write_text("id,value\n")

        catalog = Catalog()
        catalog.add_folder(ts_dir, Folder(id="test", name="Test"), quiet=True)

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        # Should still have variables from schema
        assert len(catalog.variable.all()) > 0

    def test_timeseries_at_root_no_subfolders(self, tmp_path: Path):
        """Time series files directly in scanned folder (no subfolders created)."""
        # Create files directly in tmp_path (not in a subfolder)
        (tmp_path / "report_2020.csv").write_text("id,value\n1,100\n")
        (tmp_path / "report_2021.csv").write_text("id,value\n2,200\n")

        catalog = Catalog()
        catalog.add_folder(tmp_path, Folder(id="root", name="Root"), quiet=True)

        # Should have one grouped dataset
        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_files == 2
        # No child folders should be created from file paths
        # (only the scanned folder and system folders like _modalities exist)
        folder_ids = {f.id for f in catalog.folder.all()}
        assert "root" in folder_ids
        # No subfolders like "2020" or "2021" should exist
        assert not any(f.id.startswith("root---") for f in catalog.folder.all())
