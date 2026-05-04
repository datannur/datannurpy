"""Tests for time series detection and scanning."""

from __future__ import annotations

from pathlib import Path

import pytest

from datannurpy.add_folder import _canonicalize_time_series_columns
from datannurpy import Catalog, EntityMetadata
from datannurpy.scanner.timeseries import (
    PERIOD_PLACEHOLDER,
    build_series_dataset_id,
    build_series_dataset_name,
    compute_variable_periods,
    extract_period,
    group_table_time_series,
    group_time_series,
    normalize_path,
    period_sort_key,
)

DATA_DIR = Path(__file__).parent.parent / "data"
TIMESERIES_DIR = DATA_DIR / "timeseries"


def _input_files(root: Path, names: list[str]) -> list[tuple[Path, int]]:
    """Build deterministic file inputs from relative paths."""
    return [(root / name, idx * 1000) for idx, name in enumerate(names, start=1)]


def _file_series_output(
    series,
    root: Path,
) -> list[tuple[str, tuple[str, ...], str | None]]:
    """Summarize file groups as normalized path, periods, suffix."""
    _ = root
    return sorted(
        (
            group.normalized_path,
            tuple(period for period, _ in group.files),
            group.id_suffix,
        )
        for group in series
    )


def _single_file_output(singles, root: Path) -> list[str]:
    """Summarize single files as sorted relative paths."""
    return sorted(str(path.relative_to(root)) for path, _ in singles)


def _table_series_output(series) -> list[tuple[str, tuple[str, ...], str | None]]:
    """Summarize table groups as normalized name, periods, suffix."""
    return sorted(
        (
            group.normalized_name,
            tuple(period for period, _ in group.tables),
            group.id_suffix,
        )
        for group in series
    )


def _series_case(
    normalized: str,
    periods: tuple[str, ...],
    suffix: str | None = None,
) -> tuple[str, tuple[str, ...], str | None]:
    """Build an expected series snapshot."""
    return (normalized, periods, suffix)


def _single_series_case(
    names: list[str],
    normalized: str,
    periods: tuple[str, ...],
) -> tuple[
    list[str],
    list[tuple[str, tuple[str, ...], str | None]],
    list[str],
]:
    """Build a parameter case where all inputs form one series."""
    return (names, [_series_case(normalized, periods)], [])


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

    def test_full_date_compact(self):
        """Extract compact full date (YYYYMMDD)."""
        path = Path("log_20240315.csv")
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

    def test_no_year_anywhere_not_grouped(self, tmp_path: Path):
        """Files without any 4-digit year must not be grouped as a series.

        Regression: trailing two-digit suffixes were previously interpreted as
        month_only / day_only fragments and grouped, producing bogus periods.
        """
        files = [
            (tmp_path / "TABLE_LOOKUP_MONTH12.csv", 1000),
            (tmp_path / "TABLE_LOOKUP_MONTH13.csv", 2000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 0
        assert len(singles) == 2

    def test_same_bucket_without_year_becomes_singles(self, tmp_path: Path):
        """Two no-year file candidates in the same bucket must remain singles."""
        files = [
            (tmp_path / "data_11.csv", 1000),
            (tmp_path / "data_12.csv", 2000),
        ]

        series, singles = group_time_series(files, tmp_path)

        assert series == []
        assert [path.name for path, _ in singles] == ["data_11.csv", "data_12.csv"]

    def test_group_year_not_confused_with_constant_two_digit_token(
        self,
        tmp_path: Path,
    ):
        """A constant token like _22_ must not collide with year 2022."""
        files = [
            (tmp_path / "dataset_2020_segment_22_suffix.csv", 1000),
            (tmp_path / "dataset_2021_segment_22_suffix.csv", 2000),
            (tmp_path / "dataset_2022_segment_22_suffix.csv", 3000),
            (tmp_path / "dataset_2023_segment_22_suffix.csv", 4000),
            (tmp_path / "dataset_2024_segment_22_suffix.csv", 5000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 1
        assert len(singles) == 0
        assert [period for period, _ in series[0].files] == [
            "2020",
            "2021",
            "2022",
            "2023",
            "2024",
        ]

    def test_mixed_yearly_and_quarterly_files_split_by_granularity(
        self,
        tmp_path: Path,
    ):
        """Yearly and quarterly files with the same base name must not merge."""
        files = [
            (tmp_path / "data_2021.csv", 1000),
            (tmp_path / "data_2021Q1.csv", 2000),
            (tmp_path / "data_2021Q2.csv", 3000),
            (tmp_path / "data_2022.csv", 4000),
        ]

        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 2
        assert len(singles) == 0
        assert sorted([period for period, _ in group.files] for group in series) == [
            ["2021", "2022"],
            ["2021Q1", "2021Q2"],
        ]
        assert sorted(group.id_suffix or "" for group in series) == [
            "quarterly",
            "yearly",
        ]

    def test_compact_dates_in_folder_form_series(self, tmp_path: Path):
        """Compact YYYYMMDD folder segments should form a time series."""
        files = [
            (tmp_path / "dataset" / "segment" / "20210325" / "file.ext", 1000),
            (tmp_path / "dataset" / "segment" / "20211214" / "file.ext", 2000),
            (tmp_path / "dataset" / "segment" / "20240925" / "file.ext", 3000),
        ]

        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 1
        assert len(singles) == 0
        assert [period for period, _ in series[0].files] == [
            "2021/03/25",
            "2021/12/14",
            "2024/09/25",
        ]


class TestRealisticFileTimeSeriesCases:
    """Realistic file layouts expressed as input → output snapshots."""

    @pytest.mark.parametrize(
        ("names", "expected_series", "expected_singles"),
        [
            _single_series_case(
                [
                    "published/2024-12-31/data_20240101.csv",
                    "published/2024-12-31/data_20240102.csv",
                    "published/2024-12-31/data_20240103.csv",
                ],
                f"published/2024-12-31/data_{PERIOD_PLACEHOLDER}.csv",
                ("2024/01/01", "2024/01/02", "2024/01/03"),
            ),
            _single_series_case(
                [
                    "delivery/20240215/sales_202401.csv",
                    "delivery/20240215/sales_202402.csv",
                    "delivery/20240215/sales_202403.csv",
                ],
                f"delivery/20240215/sales_{PERIOD_PLACEHOLDER}.csv",
                ("2024/01", "2024/02", "2024/03"),
            ),
            _single_series_case(
                [
                    "delivery/20240215/votants_2020.csv",
                    "delivery/20240215/votants_2021.csv",
                    "delivery/20240215/votants_2022.csv",
                ],
                f"delivery/20240215/votants_{PERIOD_PLACEHOLDER}.csv",
                ("2020", "2021", "2022"),
            ),
            _single_series_case(
                [
                    "survey_2024/results_Q1.csv",
                    "survey_2024/results_Q2.csv",
                    "survey_2024/results_Q3.csv",
                ],
                f"survey_2024/results_{PERIOD_PLACEHOLDER}.csv",
                ("2024Q1", "2024Q2", "2024Q3"),
            ),
            _single_series_case(
                [
                    "2024/results_Q1.csv",
                    "2024/results_Q2.csv",
                    "2024/results_Q3.csv",
                ],
                f"2024/results_{PERIOD_PLACEHOLDER}.csv",
                ("2024Q1", "2024Q2", "2024Q3"),
            ),
            (
                [
                    "extracts/2024/01/region_A.csv",
                    "extracts/2024/02/region_A.csv",
                    "extracts/2024/01/region_B.csv",
                    "extracts/2024/02/region_B.csv",
                ],
                [
                    _series_case(
                        f"extracts/2024/{PERIOD_PLACEHOLDER}/region_A.csv",
                        ("2024/01", "2024/02"),
                    ),
                    _series_case(
                        f"extracts/2024/{PERIOD_PLACEHOLDER}/region_B.csv",
                        ("2024/01", "2024/02"),
                    ),
                ],
                [],
            ),
            (
                [
                    "archive/20071021/VOTANTS_01.xls",
                    "archive/20081130/VOTANTS_01.xls",
                    "archive/20090927/VOTANTS_04.xls",
                    "archive/20100926/VOTANTS_04.xls",
                    "archive/20110313/VOTANTS_06.xls",
                ],
                [
                    _series_case(
                        f"archive/{PERIOD_PLACEHOLDER}/VOTANTS_01.xls",
                        ("2007/10/21", "2008/11/30"),
                    ),
                    _series_case(
                        f"archive/{PERIOD_PLACEHOLDER}/VOTANTS_04.xls",
                        ("2009/09/27", "2010/09/26"),
                    ),
                ],
                ["archive/20110313/VOTANTS_06.xls"],
            ),
            (
                [
                    "archive/20071021/VOTANTS_01_part_01.xls",
                    "archive/20081130/VOTANTS_01_part_01.xls",
                    "archive/20090927/VOTANTS_04_part_02.xls",
                    "archive/20100926/VOTANTS_04_part_02.xls",
                ],
                [
                    _series_case(
                        f"archive/{PERIOD_PLACEHOLDER}/VOTANTS_01_part_01.xls",
                        ("2007/10/21", "2008/11/30"),
                    ),
                    _series_case(
                        f"archive/{PERIOD_PLACEHOLDER}/VOTANTS_04_part_02.xls",
                        ("2009/09/27", "2010/09/26"),
                    ),
                ],
                [],
            ),
            (
                [
                    "archive/20071021/VOTANTS_01.xls",
                    "archive/20081130/VOTANTS_02.xls",
                ],
                [],
                [
                    "archive/20071021/VOTANTS_01.xls",
                    "archive/20081130/VOTANTS_02.xls",
                ],
            ),
            (
                [
                    "archive/20071021/VOTANTS_01.xls",
                    "archive/20081130/ELECTEURS_01.xls",
                ],
                [],
                [
                    "archive/20071021/VOTANTS_01.xls",
                    "archive/20081130/ELECTEURS_01.xls",
                ],
            ),
            _single_series_case(
                [
                    "snapshot_2024/data/01/day15.json",
                    "snapshot_2024/data/01/day16.json",
                    "snapshot_2024/data/01/day17.json",
                ],
                f"snapshot_2024/data/01/day{PERIOD_PLACEHOLDER}.json",
                ("2024/01/15", "2024/01/16", "2024/01/17"),
            ),
            _single_series_case(
                [
                    "2024-01/day15.csv",
                    "2024-01/day16.csv",
                ],
                f"2024-01/day{PERIOD_PLACEHOLDER}.csv",
                ("2024/01/15", "2024/01/16"),
            ),
            _single_series_case(
                [
                    "202401/day15.csv",
                    "202401/day16.csv",
                ],
                f"202401/day{PERIOD_PLACEHOLDER}.csv",
                ("2024/01/15", "2024/01/16"),
            ),
            (
                [
                    "published/2024-12-31/report_01.csv",
                    "published/2024-12-31/report_02.csv",
                ],
                [],
                [
                    "published/2024-12-31/report_01.csv",
                    "published/2024-12-31/report_02.csv",
                ],
            ),
            (
                [
                    "snapshot_2024/data/day15.json",
                    "snapshot_2024/data/day16.json",
                    "snapshot_2024/data/day17.json",
                ],
                [],
                [
                    "snapshot_2024/data/day15.json",
                    "snapshot_2024/data/day16.json",
                    "snapshot_2024/data/day17.json",
                ],
            ),
            _single_series_case(
                [
                    "archive_2021/result_2020.csv",
                    "archive_2022/result_2021.csv",
                    "archive_2023/result_2022.csv",
                ],
                f"archive_{PERIOD_PLACEHOLDER}/result_{PERIOD_PLACEHOLDER}.csv",
                ("2020", "2021", "2022"),
            ),
            (
                [
                    "archive_2021/result_2020/revision_2018.csv",
                    "archive_2021/result_2022/revision_2020.csv",
                    "archive_2022/result_2021/revision_2019.csv",
                    "archive_2022/result_2023/revision_2021.csv",
                ],
                [
                    _series_case(
                        f"archive_{PERIOD_PLACEHOLDER}/"
                        f"result_{PERIOD_PLACEHOLDER}/"
                        f"revision_{PERIOD_PLACEHOLDER}.csv",
                        ("2018", "2019", "2020", "2021"),
                    ),
                ],
                [],
            ),
            (
                [
                    "archive_2021/result_2020/revision_2018.csv",
                    "archive_2021/result_2020/revision_2019.csv",
                    "archive_2022/result_2021/revision_2020.csv",
                    "archive_2022/result_2021/revision_2021.csv",
                ],
                [
                    _series_case(
                        f"archive_2021/result_2020/revision_{PERIOD_PLACEHOLDER}.csv",
                        ("2018", "2019"),
                    ),
                    _series_case(
                        f"archive_2022/result_2021/revision_{PERIOD_PLACEHOLDER}.csv",
                        ("2020", "2021"),
                    ),
                ],
                [],
            ),
        ],
    )
    def test_file_time_series_layouts(
        self,
        tmp_path: Path,
        names: list[str],
        expected_series: list[tuple[str, tuple[str, ...], str | None]],
        expected_singles: list[str],
    ):
        """Group realistic layouts into deterministic series and singles."""
        series, singles = group_time_series(_input_files(tmp_path, names), tmp_path)

        assert _file_series_output(series, tmp_path) == expected_series
        assert _single_file_output(singles, tmp_path) == expected_singles


class TestGroupLevelPeriodDetection:
    """Test group-level period detection (constant vs variable positions)."""

    def test_constant_folder_date_variable_file_date(self, tmp_path: Path):
        """Constant date in folder should not be extracted as period."""
        files = [
            (tmp_path / "old_2024_08" / "data_2018.csv", 1000),
            (tmp_path / "old_2024_08" / "data_2022.csv", 2000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 1
        assert len(singles) == 0
        group = series[0]
        periods = [p for p, _ in group.files]
        assert periods == ["2018", "2022"]
        assert "old_2024_08" in group.normalized_path
        assert group.normalized_path.count(PERIOD_PLACEHOLDER) == 1

    def test_constant_dates_in_filename(self, tmp_path: Path):
        """Constant dates in filename should not be part of period."""
        files = [
            (tmp_path / "_1970abc_xyz1970_2000.sas7bdat", 1000),
            (tmp_path / "_1980abc_xyz1970_2000.sas7bdat", 2000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 1
        group = series[0]
        periods = [p for p, _ in group.files]
        assert periods == ["1970", "1980"]
        assert "xyz1970" in group.normalized_path
        assert "2000" in group.normalized_path

    def test_constant_dates_with_duplicate_year_values(self, tmp_path: Path):
        """Variable year matching a constant year must not break position order."""
        files = [
            (tmp_path / "_1970x_abc1970_2000.sas7bdat", 100),
            (tmp_path / "_1980x_abc1970_2000.sas7bdat", 200),
            (tmp_path / "_1990x_abc1970_2000.sas7bdat", 300),
            (tmp_path / "_2000x_abc1970_2000.sas7bdat", 400),
        ]
        series, singles = group_time_series(files, tmp_path)
        assert len(series) == 1
        assert len(singles) == 0
        group = series[0]
        periods = [p for p, _ in group.files]
        assert periods == ["1970", "1980", "1990", "2000"]
        assert "abc1970" in group.normalized_path
        assert "2000" in group.normalized_path

    def test_order_violation_creates_subgroups(self, tmp_path: Path):
        """YYYY_MM folder + YYYY file with both varying → sub-group by folder."""
        files = [
            (tmp_path / "old_2024_08" / "data_2018.csv", 1000),
            (tmp_path / "old_2024_08" / "data_2022.csv", 2000),
            (tmp_path / "old_2025_01" / "data_2019.csv", 3000),
            (tmp_path / "old_2025_01" / "data_2023.csv", 4000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 2
        series.sort(key=lambda g: g.files[0][0])

        assert [p for p, _ in series[0].files] == ["2018", "2022"]
        assert "old_2024_08" in series[0].normalized_path

        assert [p for p, _ in series[1].files] == ["2019", "2023"]
        assert "old_2025_01" in series[1].normalized_path

    def test_year_month_hierarchy_both_variable(self, tmp_path: Path):
        """Year and month in folders, both variable → combine."""
        files = [
            (tmp_path / "2024" / "01" / "data.csv", 1000),
            (tmp_path / "2024" / "02" / "data.csv", 2000),
            (tmp_path / "2025" / "01" / "data.csv", 3000),
            (tmp_path / "2025" / "02" / "data.csv", 4000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 1
        group = series[0]
        periods = [p for p, _ in group.files]
        assert periods == ["2024/01", "2024/02", "2025/01", "2025/02"]

    def test_constant_year_folder_variable_month_file(self, tmp_path: Path):
        """Constant year in folder + variable month in file → include year context."""
        files = [
            (tmp_path / "2024" / "data_01.csv", 1000),
            (tmp_path / "2024" / "data_02.csv", 2000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert len(series) == 1
        group = series[0]
        periods = [p for p, _ in group.files]
        assert periods == ["2024/01", "2024/02"]

    def test_overlap_year_month_no_extra_position(self):
        """Year-month match should suppress overlapping year match."""
        from datannurpy.scanner.timeseries import _extract_period_from_segment

        matches = _extract_period_from_segment("old_2024_08")
        assert len(matches) == 1
        _, original, info = matches[0]
        assert original == "2024_08"
        assert info.year == 2024
        assert info.sub_period == 8

    def test_overlap_quarter_no_extra_position(self):
        """Quarter match should suppress overlapping year match."""
        from datannurpy.scanner.timeseries import _extract_period_from_segment

        matches = _extract_period_from_segment("rapport_2024Q1")
        assert len(matches) == 1
        assert matches[0][2].year == 2024
        assert matches[0][2].sub_period == 13

    def test_subgroup_singles_become_singles(self, tmp_path: Path):
        """Sub-groups with only 1 file become singles."""
        files = [
            (tmp_path / "old_2024_08" / "data_2018.csv", 1000),
            (tmp_path / "old_2025_01" / "data_2018.csv", 2000),
        ]
        series, singles = group_time_series(files, tmp_path)

        # Both folder dates vary, file date is constant
        # With only 1 variable position (folder date), each file gets a unique period
        # Group has 2 files with different periods → valid series
        assert len(series) == 1

    def test_subgroup_with_single_file_goes_to_singles(self, tmp_path: Path):
        """Sub-group with 1 file after split becomes a single."""
        files = [
            (tmp_path / "old_2024_08" / "data_2018.csv", 1000),
            (tmp_path / "old_2024_08" / "data_2022.csv", 2000),
            (tmp_path / "old_2025_01" / "data_2019.csv", 3000),
        ]
        series, singles = group_time_series(files, tmp_path)

        # old_2024_08 sub-group has 2 files → series
        # old_2025_01 sub-group has 1 file → single
        assert len(series) == 1
        assert len(singles) == 1
        assert [p for p, _ in series[0].files] == ["2018", "2022"]


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

    def test_build_series_dataset_id_keeps_regular_ids_stable(self):
        """Regular series IDs should keep the historical normalized-path form."""
        normalized = f"data_{PERIOD_PLACEHOLDER}.csv"
        assert (
            build_series_dataset_id(normalized, "root")
            == "root---data_---PERIOD---_csv"
        )

    def test_dataset_id_suffix_covers_series_and_daily_cases(self):
        """The internal series suffix helper should cover empty and daily cases."""
        from datannurpy.scanner.timeseries import _series_id_suffix

        assert _series_id_suffix([]) == "series"
        assert _series_id_suffix(["2024/03/15", "2024/03/16"]) == "daily"

    def test_series_id_suffix_helper_covers_month_and_quarter(self):
        """The internal series suffix helper should cover month and quarter cases."""
        from datannurpy.scanner.timeseries import _series_id_suffix

        assert _series_id_suffix(["2024/03", "2024/04"]) == "monthly"
        assert _series_id_suffix(["2024Q1", "2024Q2"]) == "quarterly"


class TestAddFolderTimeSeries:
    """Integration tests for add_folder with time series."""

    def test_canonicalize_time_series_columns_handles_empty_input(self):
        """Empty time-series column maps should stay empty."""
        assert _canonicalize_time_series_columns({}) == {}

    def test_canonicalize_time_series_columns_deduplicates_same_period_aliases(self):
        """Aliases that sanitize identically should collapse within each period."""
        columns_by_period = {
            "2020": ["id", "canonical€label", "canonical_label"],
            "2021": ["id", "canonical_label"],
        }

        assert _canonicalize_time_series_columns(columns_by_period) == {
            "2020": ["id", "canonical_label"],
            "2021": ["id", "canonical_label"],
        }

    def test_mixed_granularities_create_distinct_datasets(self, tmp_path: Path):
        """Yearly and quarterly files with the same base name must create two datasets."""
        for name in (
            "data_2021.csv",
            "data_2021Q1.csv",
            "data_2021Q2.csv",
            "data_2022.csv",
        ):
            (tmp_path / name).write_text("id,value\n1,10\n")

        catalog = Catalog(quiet=True)
        catalog.add_folder(
            tmp_path,
            metadata=EntityMetadata(id="root", name="Root"),
            quiet=True,
        )

        datasets = sorted(catalog.dataset.all(), key=lambda dataset: dataset.name or "")
        assert len(datasets) == 2
        assert len({dataset.id for dataset in datasets}) == 2
        assert [dataset.name for dataset in datasets] == [
            "data_[YYYY]",
            "data_[YYYY]Q[N]",
        ]

    def test_yearly_series_creates_single_dataset(self):
        """Yearly series creates one dataset instead of multiple."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "yearly",
            metadata=EntityMetadata(id="yearly", name="Yearly"),
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        ds = datasets[0]
        assert ds.nb_resources == 3
        assert ds.start_date == "2020"
        assert ds.end_date == "2023"

    def test_quarterly_series(self):
        """Quarterly series detection."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "quarterly",
            metadata=EntityMetadata(id="quarterly", name="Quarterly"),
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_resources == 3
        assert datasets[0].start_date == "2023Q1"
        assert datasets[0].end_date == "2024Q1"

    def test_schema_evolution_variables(self):
        """Variables have correct start_date/end_date."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "schema_evolution",
            metadata=EntityMetadata(id="evolution", name="Evolution"),
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
            TIMESERIES_DIR / "mixed",
            metadata=EntityMetadata(id="mixed", name="Mixed"),
            quiet=True,
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
            metadata=EntityMetadata(id="yearly", name="Yearly"),
            time_series=False,
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 3  # One per file
        for ds in datasets:
            assert ds.nb_resources is None

    def test_variables_union(self):
        """All variables from all files are in the union."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "schema_evolution",
            metadata=EntityMetadata(id="evolution", name="Evolution"),
            quiet=True,
        )

        variables = catalog.variable.all()
        var_names = {v.name for v in variables}

        # Should have union of all columns
        assert var_names == {"id", "nom", "revenu", "email"}

    def test_time_series_aliases_are_canonicalized_before_id_build(
        self, tmp_path: Path
    ):
        """Header aliases that sanitize to the same ID should merge under the latest label."""
        ts_dir = tmp_path / "aliases"
        ts_dir.mkdir()
        (ts_dir / "data_2020.csv").write_text("id,canonical€label\n1,foo\n")
        (ts_dir / "data_2021.csv").write_text("id,canonical_label\n2,bar\n")

        catalog = Catalog(quiet=True)
        catalog.add_folder(
            ts_dir,
            metadata=EntityMetadata(id="aliases", name="Aliases"),
        )

        variables = {v.name: v for v in catalog.variable.all()}
        assert set(variables) == {"id", "canonical_label"}
        assert variables["canonical_label"].start_date is None
        assert variables["canonical_label"].end_date is None

    def test_dataset_mode_with_timeseries(self):
        """Structure mode works with time series."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "yearly",
            metadata=EntityMetadata(id="yearly", name="Yearly"),
            depth="dataset",
            quiet=True,
        )

        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_resources == 3
        assert datasets[0].start_date == "2020"
        assert datasets[0].end_date == "2023"
        # No variables in dataset mode
        assert len(catalog.variable.all()) == 0

    def test_variable_mode_with_timeseries(self):
        """Schema mode scans columns from all files."""
        catalog = Catalog()
        catalog.add_folder(
            TIMESERIES_DIR / "schema_evolution",
            metadata=EntityMetadata(id="evolution", name="Evolution"),
            depth="variable",
            quiet=True,
        )

        variables = catalog.variable.all()
        var_names = {v.name for v in variables}
        assert var_names == {"id", "nom", "revenu", "email"}

    def test_variable_mode_reuses_latest_schema_scan(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Schema mode should not rescan the latest time-series file."""
        from datannurpy import add_folder as add_folder_mod

        ts_dir = tmp_path / "series"
        ts_dir.mkdir()
        (ts_dir / "data_2023.csv").write_text("id,value\n1,100\n")
        (ts_dir / "data_2024.csv").write_text("id,value\n2,200\n")

        calls: list[tuple[str, bool | None]] = []
        original_scan_file = add_folder_mod.scan_file

        def counting_scan_file(*args, **kwargs):
            calls.append((Path(args[0]).name, kwargs.get("schema_only")))
            return original_scan_file(*args, **kwargs)

        monkeypatch.setattr(add_folder_mod, "scan_file", counting_scan_file)

        catalog = Catalog()
        catalog.add_folder(
            ts_dir,
            metadata=EntityMetadata(id="series", name="Series"),
            depth="variable",
            quiet=True,
        )

        latest_calls = [
            schema_only for name, schema_only in calls if name == "data_2024.csv"
        ]
        assert latest_calls == [True]

    def test_time_series_persists_effective_sample_size(self, tmp_path: Path):
        """Time series datasets should keep the latest scan effective sample_size."""
        ts_dir = tmp_path / "ts"
        ts_dir.mkdir()
        for year in ("2023", "2024"):
            csv_file = ts_dir / f"sales_{year}.csv"
            csv_file.write_text(
                "id,value\n" + "".join(f"{i},{i * 10}\n" for i in range(240))
            )

        catalog = Catalog(quiet=True)
        catalog.add_folder(
            ts_dir,
            metadata=EntityMetadata(id="ts", name="TS"),
            sample_size=100,
        )

        dataset = catalog.dataset.all()[0]
        assert dataset.nb_row == 240
        assert dataset.sample_size == 100


class TestPeriodEdgeCases:
    """Edge case tests for period extraction and sorting."""

    def test_period_granularity_signature_distinguishes_all_supported_kinds(self):
        """Granularity signatures must distinguish year, quarter, month, date, and no-year placeholders."""
        from datannurpy.scanner.timeseries import (
            PeriodInfo,
            _period_granularity_signature,
        )

        signature = _period_granularity_signature(
            [
                PeriodInfo(2024, 0, 0, "2024"),
                PeriodInfo(2024, 13, 0, "2024Q1"),
                PeriodInfo(2024, 3, 0, "2024/03"),
                PeriodInfo(2024, 3, 15, "2024/03/15"),
                PeriodInfo(0, 0, 0, "QX"),
            ]
        )

        assert signature == (1, 2, 3, 4, 0)

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

    def test_combine_periods_year_plus_full_date(self):
        """_combine_periods with year + date that has sub_period and day."""
        from datannurpy.scanner.timeseries import PeriodInfo, _combine_periods

        periods = [
            PeriodInfo(2024, 0, 0, "2024"),
            PeriodInfo(0, 3, 15, "03-15"),  # sub_period and day both set
        ]
        result = _combine_periods(periods)
        assert result is not None
        assert result.to_string() == "2024/03/15"

    def test_combine_periods_year_month_day(self):
        """_combine_periods combines year, month, and day from separate infos."""
        from datannurpy.scanner.timeseries import PeriodInfo, _combine_periods

        periods = [
            PeriodInfo(2024, 0, 0, "2024"),
            PeriodInfo(0, 3, 0, "03"),
            PeriodInfo(0, 0, 15, "15"),
        ]
        result = _combine_periods(periods)
        assert result is not None
        assert result.to_string() == "2024/03/15"

    def test_combine_periods_year_with_sub_and_day(self):
        """_combine_periods with year+month info and separate day."""
        from datannurpy.scanner.timeseries import PeriodInfo, _combine_periods

        periods = [
            PeriodInfo(2024, 3, 0, "2024_03"),  # year+month (sub_period set, day=0)
            PeriodInfo(0, 0, 15, "15"),  # day only
        ]
        result = _combine_periods(periods)
        assert result is not None
        assert result.to_string() == "2024/03/15"

    def test_month_without_year_context_is_not_grouped(self, tmp_path: Path):
        """A final period without a real year should not become a series."""
        files = [
            (tmp_path / "old_2024_08" / "data_01.csv", 1000),
            (tmp_path / "old_2024_08" / "data_02.csv", 2000),
        ]
        series, singles = group_time_series(files, tmp_path)

        assert series == []
        assert [path.name for path, _ in singles] == ["data_01.csv", "data_02.csv"]

    def test_period_granularity_day(self):
        """_period_granularity returns 3 for day-level info."""
        from datannurpy.scanner.timeseries import PeriodInfo, _period_granularity

        assert _period_granularity(PeriodInfo(2024, 3, 15, "2024-03-15")) == 3
        assert _period_granularity(PeriodInfo(0, 0, 15, "15")) == 3

    def test_period_granularity_zero(self):
        """_period_granularity returns 0 for empty info."""
        from datannurpy.scanner.timeseries import PeriodInfo, _period_granularity

        assert _period_granularity(PeriodInfo(0, 0, 0, "")) == 0

    def test_refine_normalized_path_mismatch(self):
        """_refine_normalized_path returns original on mismatch."""
        from datannurpy.scanner.timeseries import PeriodInfo, _refine_normalized_path

        path = f"a_{PERIOD_PLACEHOLDER}/b_{PERIOD_PLACEHOLDER}.csv"
        # Provide only 1 position for 2 placeholders
        result = _refine_normalized_path(path, [PeriodInfo(2024, 0, 0, "2024")], [True])
        assert result == path

    def test_refine_group_no_positions(self):
        """_refine_group handles 0 period positions."""
        from datannurpy.scanner.timeseries import _refine_group

        result = _refine_group(
            "no_date.csv",
            [(Path("no_date.csv"), 1000, []), (Path("no_date.csv"), 2000, [])],
        )
        assert len(result) == 1
        assert result[0][0] == "no_date.csv"

    def test_refine_group_all_constant_positions(self):
        """_refine_group falls back when all positions constant."""
        from datannurpy.scanner.timeseries import PeriodInfo, _refine_group

        # Two files with identical date → all constant → fallback to all positions
        result = _refine_group(
            f"data_{PERIOD_PLACEHOLDER}.csv",
            [
                (Path("data_2024.csv"), 1000, [PeriodInfo(2024, 0, 0, "2024")]),
                (Path("data_2024.csv"), 2000, [PeriodInfo(2024, 0, 0, "2024")]),
            ],
        )
        assert len(result) == 1
        _, files = result[0]
        assert all(period == "2024" for period, _, _ in files)


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
        catalog.add_folder(
            ts_dir,
            metadata=EntityMetadata(id="test", name="Test"),
            quiet=True,
        )
        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_resources == 2

        # Modify a file
        time.sleep(0.1)  # Ensure mtime changes
        (ts_dir / "data_2021.csv").write_text("id,value,new_col\n2,200,x\n")

        # Rescan same catalog (refresh=True to force rescan)
        catalog.add_folder(
            ts_dir,
            metadata=EntityMetadata(id="test", name="Test"),
            quiet=True,
            refresh=True,
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
        catalog.add_folder(
            ts_dir,
            metadata=EntityMetadata(id="test", name="Test"),
            quiet=True,
        )

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
        catalog.add_folder(
            tmp_path,
            metadata=EntityMetadata(id="root", name="Root"),
            quiet=True,
        )

        # Should have one grouped dataset
        datasets = catalog.dataset.all()
        assert len(datasets) == 1
        assert datasets[0].nb_resources == 2
        # No child folders should be created from file paths
        # (only the scanned folder and system folders like _enumerations exist)
        folder_ids = {f.id for f in catalog.folder.all()}
        assert "root" in folder_ids
        # No subfolders like "2020" or "2021" should exist
        assert not any(f.id.startswith("root---") for f in catalog.folder.all())


class TestRealisticTableTimeSeriesCases:
    """Realistic table layouts expressed as input → output snapshots."""

    @pytest.mark.parametrize(
        ("tables", "expected_series", "expected_singles"),
        [
            _single_series_case(
                ["sales_fact_202401", "sales_fact_202402", "sales_fact_202403"],
                f"sales_fact_{PERIOD_PLACEHOLDER}",
                ("2024/01", "2024/02", "2024/03"),
            ),
            _single_series_case(
                [
                    "survey_2024_results_Q1",
                    "survey_2024_results_Q2",
                    "survey_2024_results_Q3",
                ],
                f"survey_2024_results_{PERIOD_PLACEHOLDER}",
                ("2024Q1", "2024Q2", "2024Q3"),
            ),
            _single_series_case(
                [
                    "published_20241231_data_20240101",
                    "published_20241231_data_20240102",
                    "published_20241231_data_20240103",
                ],
                f"published_20241231_data_{PERIOD_PLACEHOLDER}",
                ("2024/01/01", "2024/01/02", "2024/01/03"),
            ),
            (
                [
                    "published_20241231_report_01",
                    "published_20241231_report_02",
                ],
                [],
                [
                    "published_20241231_report_01",
                    "published_20241231_report_02",
                ],
            ),
            (
                [
                    "archive_20071021_VOTANTS_01",
                    "archive_20081130_VOTANTS_01",
                    "archive_20090927_VOTANTS_04",
                    "archive_20100926_VOTANTS_04",
                    "archive_20110313_VOTANTS_06",
                ],
                [
                    _series_case(
                        f"archive_{PERIOD_PLACEHOLDER}_VOTANTS_01",
                        ("2007/10/21", "2008/11/30"),
                    ),
                    _series_case(
                        f"archive_{PERIOD_PLACEHOLDER}_VOTANTS_04",
                        ("2009/09/27", "2010/09/26"),
                    ),
                ],
                ["archive_20110313_VOTANTS_06"],
            ),
            (
                ["dim_age_01", "dim_age_02", "dim_age_03"],
                [],
                ["dim_age_01", "dim_age_02", "dim_age_03"],
            ),
        ],
    )
    def test_table_time_series_layouts(
        self,
        tables: list[str],
        expected_series: list[tuple[str, tuple[str, ...], str | None]],
        expected_singles: list[str],
    ):
        """Group realistic table names into deterministic series and singles."""
        series, singles = group_table_time_series(tables)

        assert _table_series_output(series) == expected_series
        assert sorted(singles) == expected_singles


class TestGroupTableTimeSeries:
    """Tests for group_table_time_series (database table grouping)."""

    def test_yearly_tables(self):
        """Group tables with yearly pattern."""
        tables = ["stats_2022", "stats_2023", "stats_2024"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 1
        assert len(singles) == 0
        assert len(series[0].tables) == 3
        assert series[0].tables[0] == ("2022", "stats_2022")
        assert series[0].tables[-1] == ("2024", "stats_2024")
        assert PERIOD_PLACEHOLDER in series[0].normalized_name

    def test_no_temporal_pattern(self):
        """Tables without temporal pattern remain singles."""
        tables = ["users", "orders", "products"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 0
        assert sorted(singles) == sorted(tables)

    def test_single_table_with_year_not_grouped(self):
        """A single table with a year pattern stays as single."""
        tables = ["stats_2024", "users", "orders"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 0
        assert "stats_2024" in singles

    def test_mixed_tables(self):
        """Mix of series and non-series tables."""
        tables = ["stats_2022", "stats_2023", "users", "logs_2023", "logs_2024"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 2
        assert "users" in singles

    def test_year_month_tables(self):
        """Tables with year-month pattern."""
        tables = ["log_2024_01", "log_2024_02", "log_2024_03"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 1
        assert series[0].tables[0][0] == "2024/01"

    def test_sorted_by_period(self):
        """Series tables are sorted chronologically."""
        tables = ["data_2024", "data_2020", "data_2022"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 1
        periods = [p for p, _ in series[0].tables]
        assert periods == ["2020", "2022", "2024"]

    def test_empty_input(self):
        """Empty table list returns empty results."""
        series, singles = group_table_time_series([])
        assert series == []
        assert singles == []

    def test_name_entirely_temporal_not_grouped(self):
        """Tables named 't1', 't2' etc. should not be grouped as time series."""
        tables = ["t1", "t2", "t3", "t4"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 0
        assert sorted(singles) == sorted(tables)

    def test_no_year_anywhere_not_grouped(self):
        """Tables without any 4-digit year must not be grouped as a series.

        Regression: trailing two-digit suffixes like 'MONTH12' / 'MONTH13' were
        previously interpreted as month/day fragments and grouped, producing
        bogus periods such as '0/12' and '0/00/13'.
        """
        tables = ["TABLE_LOOKUP_MONTH12", "TABLE_LOOKUP_MONTH13"]
        series, singles = group_table_time_series(tables)
        assert series == []
        assert sorted(singles) == sorted(tables)

    def test_same_bucket_without_year_tables_become_singles(self):
        """Two no-year table candidates in the same bucket must remain singles."""
        tables = ["stats_11", "stats_12"]

        series, singles = group_table_time_series(tables)

        assert series == []
        assert singles == ["stats_11", "stats_12"]

    def test_constant_prefix_digit_not_treated_as_period(self):
        """Constant digits in prefix must not hide a mixed-granularity split."""
        tables = [
            "PREFIX03_DATA2010",
            "PREFIX03_DATA2014",
            "PREFIX03_DATA201607",
            "PREFIX03_DATA2017",
        ]
        series, singles = group_table_time_series(tables)
        assert len(series) == 1
        assert singles == ["PREFIX03_DATA201607"]
        # '03' is constant across all tables → must NOT be a period placeholder
        assert "PREFIX03_DATA" in series[0].normalized_name
        # Only the varying yearly part should be the period in the grouped series
        assert series[0].normalized_name.count(PERIOD_PLACEHOLDER) == 1
        assert [p for p, _ in series[0].tables] == ["2010", "2014", "2017"]

    def test_position_order_matches_placeholders(self):
        """Period positions list must align with placeholder order in normalized name."""
        tables = [
            "X03A_TABLE2010",
            "X03A_TABLE2014",
            "X03A_TABLE2017",
        ]
        series, singles = group_table_time_series(tables)
        assert len(series) == 1
        # The varying part is the year, not '03'
        periods = [p for p, _ in series[0].tables]
        assert periods == ["2010", "2014", "2017"]

    def test_mixed_yearly_and_quarterly_tables_split_by_granularity(self):
        """Yearly and quarterly tables with the same base name must not merge."""
        tables = ["data_2021", "data_2021Q1", "data_2021Q2", "data_2022"]

        series, singles = group_table_time_series(tables)

        assert len(series) == 2
        assert len(singles) == 0
        assert sorted([period for period, _ in group.tables] for group in series) == [
            ["2021", "2022"],
            ["2021Q1", "2021Q2"],
        ]
        assert sorted(group.id_suffix or "" for group in series) == [
            "quarterly",
            "yearly",
        ]

    def test_compact_dates_tables_group(self):
        """Compact YYYYMMDD table names should form a time series."""
        tables = ["data_20210325", "data_20211214", "data_20240925"]

        series, singles = group_table_time_series(tables)

        assert len(series) == 1
        assert len(singles) == 0
        assert [period for period, _ in series[0].tables] == [
            "2021/03/25",
            "2021/12/14",
            "2024/09/25",
        ]

    def test_refine_subgroup_with_single_table_becomes_single(self):
        """When _refine_group sub-groups and one group has <2 tables, it becomes a single."""
        # Both year positions vary → order violation → sub-group by first year
        # data2020 has 2 tables (stats2022, stats2023) → series
        # data2021 has 1 table (stats2022) → single
        tables = ["data2020_stats2022", "data2020_stats2023", "data2021_stats2022"]
        series, singles = group_table_time_series(tables)
        assert len(series) == 1
        assert series[0].tables[0][0] == "2022"
        assert series[0].tables[1][0] == "2023"
        assert "data2021_stats2022" in singles
