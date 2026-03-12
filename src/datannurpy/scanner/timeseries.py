"""Time series detection and grouping for datasets with temporal patterns."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from ..utils import make_id, sanitize_id

# Patterns ordered by length (longest first to avoid partial matches)
# Each pattern: (regex, length_hint for priority)
_PERIOD_PATTERNS = [
    # Full date: 2024-03-15 or 2024_03_15
    (re.compile(r"((?:19|20)\d{2})[_-](\d{2})[_-](\d{2})"), "date"),
    # Year-month: 2024-03 or 2024_03
    (re.compile(r"((?:19|20)\d{2})[_-](\d{2})(?![_-]?\d)"), "year_month"),
    # Year-month compact: 202403
    (re.compile(r"((?:19|20)\d{2})(0[1-9]|1[0-2])(?!\d)"), "year_month_compact"),
    # Quarter with year: 2024Q1, 2024-Q1, 2024_T2
    (re.compile(r"((?:19|20)\d{2})[_-]?([QqTt])([1-4])"), "quarter"),
    # Year only: 2024
    (re.compile(r"(?<![0-9])((?:19|20)\d{2})(?![0-9])"), "year"),
    # Quarter only (no year): Q1, T2 - for combining with year from other segment
    (re.compile(r"(?<![0-9a-zA-Z])([QqTt])([1-4])(?![0-9])"), "quarter_only"),
    # Month only (01-12): for combining with year from other segment
    (re.compile(r"(?<![0-9])(0[1-9]|1[0-2])(?![0-9])"), "month_only"),
    # Day only (01-31): for combining with month/year from other segment
    (re.compile(r"(?<![0-9])(0[1-9]|[12][0-9]|3[01])(?![0-9])"), "day_only"),
]

# Placeholder used in normalized paths (valid for IDs)
PERIOD_PLACEHOLDER = "---PERIOD---"


@dataclass
class PeriodInfo:
    """Extracted period information from a path."""

    year: int
    sub_period: int  # 0=none, 1-4=quarter, 1-12=month
    day: int  # 0=none
    original: str  # Original matched string

    def to_sort_key(self) -> tuple[int, int, int]:
        """Convert to sortable tuple (year, sub_period, day)."""
        return (self.year, self.sub_period, self.day)

    def to_string(self) -> str:
        """Convert to human-readable string like '2024', '2024Q1', '2024/03', '2024/03/15'."""
        if self.day > 0:
            return f"{self.year}/{self.sub_period:02d}/{self.day:02d}"
        if self.sub_period > 12:
            # Quarter (13-16 maps to Q1-Q4)
            return f"{self.year}Q{self.sub_period - 12}"
        if self.sub_period > 0:
            return f"{self.year}/{self.sub_period:02d}"
        return str(self.year)


def _extract_period_from_segment(segment: str) -> list[tuple[str, PeriodInfo]]:
    """Extract all period matches from a path segment.

    Returns only the most specific match for each segment position.
    """
    matches: list[tuple[str, PeriodInfo]] = []
    matched_positions: set[tuple[int, int]] = (
        set()
    )  # (start, end) positions already matched

    for pattern, pattern_type in _PERIOD_PATTERNS:
        for match in pattern.finditer(segment):
            start, end = match.span()
            # Skip if this position already has a more specific match
            if (start, end) in matched_positions:
                continue

            original = match.group(0)

            if pattern_type == "date":
                year, month, day = match.groups()
                info = PeriodInfo(int(year), int(month), int(day), original)
            elif pattern_type == "year_month":
                year, month = match.groups()
                info = PeriodInfo(int(year), int(month), 0, original)
            elif pattern_type == "year_month_compact":
                year, month = match.groups()
                info = PeriodInfo(int(year), int(month), 0, original)
            elif pattern_type == "quarter":
                year, _, q_num = match.groups()
                # Store quarter as 13-16 to distinguish from months
                info = PeriodInfo(int(year), int(q_num) + 12, 0, original)
            elif pattern_type == "quarter_only":
                # Quarter without year (Q1, T2) - year=0 as placeholder
                _, q_num = match.groups()
                info = PeriodInfo(0, int(q_num) + 12, 0, original)
            elif pattern_type == "month_only":
                # Month without year (01-12) - year=0 as placeholder
                month = match.group(1)
                info = PeriodInfo(0, int(month), 0, original)
            elif pattern_type == "day_only":
                # Day without year/month (01-31) - year=0, sub_period=0 as placeholder
                day = match.group(1)
                info = PeriodInfo(0, 0, int(day), original)
            else:  # year
                year = match.group(1)
                info = PeriodInfo(int(year), 0, 0, original)

            matches.append((original, info))
            matched_positions.add((start, end))

    return matches


def _combine_periods(periods: list[PeriodInfo]) -> PeriodInfo | None:
    """Combine multiple period infos into one (hierarchical: year > quarter/month > day)."""
    if not periods:
        return None

    if len(periods) == 1:
        return periods[0]

    # Sort by specificity (year first, then sub_period, then day)
    # Take year from the first year-only or year component
    year = None
    sub_period = 0
    day = 0
    original_parts: list[str] = []

    for p in periods:
        if year is None and p.year:
            year = p.year
            if p.sub_period == 0 and p.day == 0:
                original_parts.append(str(p.year))
        if p.sub_period > 0 and sub_period == 0:
            sub_period = p.sub_period
            if p.day == 0:
                original_parts.append(p.original)
        if p.day > 0 and day == 0:
            day = p.day
            original_parts.append(p.original)

    if year is None:
        return None

    return PeriodInfo(year, sub_period, day, "-".join(original_parts))


def extract_period(path: Path, root: Path | None = None) -> PeriodInfo | None:
    """Extract the combined period from a file path."""
    rel_path = path.relative_to(root) if root else path
    all_periods: list[PeriodInfo] = []

    # Process each path segment
    for segment in rel_path.parts:
        matches = _extract_period_from_segment(segment)
        for _, info in matches:
            all_periods.append(info)

    return _combine_periods(all_periods)


def normalize_path(path: Path, root: Path | None = None) -> str:
    """Normalize path by replacing temporal patterns with placeholder.

    Returns the path with all period patterns replaced by PERIOD_PLACEHOLDER.
    """
    rel_path = path.relative_to(root) if root else path
    normalized_parts: list[str] = []

    for segment in rel_path.parts:
        normalized_segment = segment
        matches = _extract_period_from_segment(segment)

        # Sort matches by position (reverse) to replace from end to start
        sorted_matches = sorted(matches, key=lambda m: segment.find(m[0]), reverse=True)

        for original, _ in sorted_matches:
            # Replace only the first occurrence of this match
            idx = normalized_segment.find(original)
            if idx >= 0:
                normalized_segment = (
                    normalized_segment[:idx]
                    + PERIOD_PLACEHOLDER
                    + normalized_segment[idx + len(original) :]
                )

        normalized_parts.append(normalized_segment)

    return str(Path(*normalized_parts))


def period_sort_key(period: str | PeriodInfo) -> tuple[int, int, int]:
    """Convert a period string or PeriodInfo to a sortable tuple (year, sub_period, day)."""
    if isinstance(period, PeriodInfo):
        return period.to_sort_key()

    # Parse period string
    period = period.strip()

    # Try full date: 2024/03/15
    if match := re.match(r"(\d{4})/(\d{2})/(\d{2})$", period):
        year, month, day = match.groups()
        return (int(year), int(month), int(day))

    # Try quarter: 2024Q1
    if match := re.match(r"(\d{4})Q([1-4])$", period):
        year, quarter = match.groups()
        return (int(year), int(quarter) + 12, 0)  # 13-16 for quarters

    # Try month: 2024/03
    if match := re.match(r"(\d{4})/(\d{2})$", period):
        year, month = match.groups()
        return (int(year), int(month), 0)

    # Try compact month: 202403
    if match := re.match(r"(\d{4})(0[1-9]|1[0-2])$", period):
        year, month = match.groups()
        return (int(year), int(month), 0)

    # Try year only: 2024
    if match := re.match(r"(\d{4})$", period):
        year = match.group(1)
        return (int(year), 0, 0)

    # Fallback: try to extract year
    if match := re.search(r"((?:19|20)\d{2})", period):
        return (int(match.group(1)), 0, 0)

    return (0, 0, 0)


@dataclass
class TimeSeriesGroup:
    """A group of files forming a time series."""

    normalized_path: str
    files: list[tuple[str, Path]]  # [(period_string, path), ...]
    max_mtime: int


def group_time_series(
    files: list[tuple[Path, int]],  # [(path, mtime), ...]
    root: Path,
) -> tuple[list[TimeSeriesGroup], list[tuple[Path, int]]]:
    """Group files into time series and return singles separately.

    Returns:
        (time_series_groups, single_files)
        - time_series_groups: groups with ≥2 files
        - single_files: files that don't form a series
    """
    # Group by normalized path
    groups: dict[str, list[tuple[str, Path, int]]] = defaultdict(list)

    for path, mtime in files:
        period_info = extract_period(path, root)
        if period_info is None:
            # No period found, treat as single file
            groups["__no_period__" + str(path)].append(("", path, mtime))
            continue

        normalized = normalize_path(path, root)
        period_str = period_info.to_string()
        groups[normalized].append((period_str, path, mtime))

    # Separate series (≥2 files) from singles
    time_series: list[TimeSeriesGroup] = []
    singles: list[tuple[Path, int]] = []

    for normalized, file_list in groups.items():
        if len(file_list) >= 2:
            # Sort by period
            sorted_files = sorted(file_list, key=lambda x: period_sort_key(x[0]))
            max_mtime = max(m for _, _, m in sorted_files)
            time_series.append(
                TimeSeriesGroup(
                    normalized_path=normalized,
                    files=[(period, path) for period, path, _ in sorted_files],
                    max_mtime=max_mtime,
                )
            )
        else:
            for _, path, mtime in file_list:
                singles.append((path, mtime))

    return time_series, singles


def get_series_folder_parts(normalized_path: str) -> list[str]:
    """Get non-temporal parent folder parts from normalized path."""
    parent_parts = Path(normalized_path).parent.parts
    return [p for p in parent_parts if PERIOD_PLACEHOLDER not in p]


def build_series_dataset_id(normalized_path: str, prefix: str) -> str:
    """Build dataset ID from normalized path."""
    parts = [sanitize_id(p) for p in Path(normalized_path).parts]
    return make_id(prefix, *parts)


def build_series_dataset_name(
    normalized_path: str,
    periods: list[str],
) -> str:
    """Build human-readable dataset name from normalized path.

    If period is in filename: enquete_{{PERIOD}}.csv → enquete_[YYYY]
    If period only in folder: {{PERIOD}}/enquete.csv → enquete
    """
    path = Path(normalized_path)
    stem = path.stem

    # Check if period is in the filename
    if PERIOD_PLACEHOLDER in stem:
        # Determine pattern from first period
        if periods:
            first = periods[0]
            if re.match(r"\d{4}/\d{2}/\d{2}$", first):
                pattern = "[YYYY/MM/DD]"
            elif re.match(r"\d{4}Q\d$", first):
                pattern = "[YYYY]Q[N]"
            elif re.match(r"\d{4}/\d{2}$", first):
                pattern = "[YYYY/MM]"
            else:
                pattern = "[YYYY]"
        else:
            pattern = "[YYYY]"

        return stem.replace(PERIOD_PLACEHOLDER, pattern)

    # Period only in folder path
    return stem.replace(PERIOD_PLACEHOLDER, "")


def compute_variable_periods(
    columns_by_period: dict[str, list[str]],
) -> dict[str, tuple[str | None, str | None]]:
    """Compute start_date/end_date for each variable across periods.

    Args:
        columns_by_period: {period: [column_names]}

    Returns:
        {variable_name: (start_date, end_date)}
        - start_date is None if present from first period
        - end_date is None if present until last period
    """
    if not columns_by_period:
        return {}

    # Sort periods chronologically
    sorted_periods = sorted(columns_by_period.keys(), key=period_sort_key)
    first_period = sorted_periods[0]
    last_period = sorted_periods[-1]

    # Track first/last appearance of each variable
    var_first: dict[str, str] = {}
    var_last: dict[str, str] = {}

    for period in sorted_periods:
        for col in columns_by_period[period]:
            if col not in var_first:
                var_first[col] = period
            var_last[col] = period

    # Build result
    result: dict[str, tuple[str | None, str | None]] = {}
    for var_name in var_first:
        start = var_first[var_name] if var_first[var_name] != first_period else None
        end = var_last[var_name] if var_last[var_name] != last_period else None
        result[var_name] = (start, end)

    return result
