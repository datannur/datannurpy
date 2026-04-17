"""Time series detection and grouping for datasets with temporal patterns."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePath, PurePosixPath

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
    matched_ranges: list[tuple[int, int]] = []

    def _overlaps(start: int, end: int) -> bool:
        return any(start < me and end > ms for ms, me in matched_ranges)

    for pattern, pattern_type in _PERIOD_PATTERNS:
        for match in pattern.finditer(segment):
            start, end = match.span()
            # Skip if this position overlaps with a more specific match
            if _overlaps(start, end):
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
            matched_ranges.append((start, end))

    # Sort by string position so matches[i] corresponds to the i-th placeholder
    matches = [m for _, m in sorted(zip(matched_ranges, matches))]
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


def _extract_file_info(
    path: PurePath,
    root: PurePath | None = None,
) -> tuple[str, list[PeriodInfo]]:
    """Extract normalized path and period positions from a file path in one pass."""
    rel_path = path.relative_to(root) if root else path
    normalized_parts: list[str] = []
    positions: list[PeriodInfo] = []

    for segment in rel_path.parts:
        matches = _extract_period_from_segment(segment)

        # Build normalized segment (replace from end to preserve indices)
        normalized_segment = segment
        sorted_matches = sorted(
            matches,
            key=lambda m: segment.find(m[0]),
            reverse=True,
        )
        for original, _ in sorted_matches:
            idx = normalized_segment.find(original)
            assert idx >= 0
            normalized_segment = (
                normalized_segment[:idx]
                + PERIOD_PLACEHOLDER
                + normalized_segment[idx + len(original) :]
            )
        normalized_parts.append(normalized_segment)

        # Collect period positions (in order)
        for _, info in matches:
            positions.append(info)

    return "/".join(normalized_parts), positions


def extract_period(path: PurePath, root: PurePath | None = None) -> PeriodInfo | None:
    """Extract the combined period from a file path."""
    _, positions = _extract_file_info(path, root)
    return _combine_periods(positions)


def normalize_path(path: PurePath, root: PurePath | None = None) -> str:
    """Normalize path by replacing temporal patterns with placeholder."""
    normalized, _ = _extract_file_info(path, root)
    return normalized


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
    files: list[tuple[str, PurePath]]  # [(period_string, path), ...]
    max_mtime: int


def _period_granularity(info: PeriodInfo) -> int:
    """Return temporal granularity: 1=year, 2=month/quarter, 3=day/full-date."""
    if info.day > 0:
        return 3
    if info.sub_period > 0:
        return 2
    if info.year > 0:
        return 1
    return 0


def _refine_normalized_path(
    normalized_path: str,
    positions: list[PeriodInfo],
    is_period: list[bool],
) -> str:
    """Rebuild normalized path, restoring non-period positions to their original value."""
    parts = normalized_path.split(PERIOD_PLACEHOLDER)
    if len(parts) != len(positions) + 1:
        return normalized_path
    result = parts[0]
    for i, info in enumerate(positions):
        result += PERIOD_PLACEHOLDER if is_period[i] else info.original
        result += parts[i + 1]
    return result


def _refine_group(
    normalized_path: str,
    file_list: Sequence[tuple[PurePath, int, list[PeriodInfo]]],
) -> list[tuple[str, list[tuple[str, PurePath, int]]]]:
    """Classify period positions as variable/constant and build refined periods.

    For each PERIOD_PLACEHOLDER position, compare values across all files:
    - Variable (values differ): candidate for the period
    - Constant (same value everywhere): restore to literal in the path

    Among variable positions, checks temporal order (YYYY → MM/Q → DD).
    An order violation triggers sub-grouping.

    Returns list of (refined_normalized_path, [(period_str, path, mtime), ...]).
    """
    all_positions = [positions for _, _, positions in file_list]
    n_positions = len(all_positions[0]) if all_positions else 0

    if n_positions == 0:
        return [(normalized_path, [("", p, m) for p, m, _ in file_list])]

    # --- Classify each position as variable or constant ---
    is_variable: list[bool] = []
    for pos_idx in range(n_positions):
        values = {
            fp[pos_idx].to_sort_key() for fp in all_positions if pos_idx < len(fp)
        }
        is_variable.append(len(values) > 1)

    variable_indices = [i for i, v in enumerate(is_variable) if v]

    # Fallback: if all positions are constant, use all positions
    if not variable_indices:
        variable_indices = list(range(n_positions))
        is_variable = [True] * n_positions

    # --- Check temporal order among variable positions ---
    # Valid: YYYY → MM/Q → DD (increasing granularity)
    # Violation (e.g. YYYY_MM → YYYY): preceding positions become sub-group axes
    period_indices: list[int] = []
    subgroup_indices: list[int] = []

    if len(variable_indices) == 1:
        period_indices = variable_indices[:]
    else:
        prev_gran = 0
        for idx in variable_indices:
            gran = _period_granularity(all_positions[0][idx])
            if prev_gran > 0 and gran <= prev_gran:
                subgroup_indices.extend(period_indices)
                period_indices = [idx]
            else:
                period_indices.append(idx)
            prev_gran = gran

    # --- Include constant year context if period has no year ---
    # Example: 2024/data_01.csv → month varies, year is constant but needed for "2024/01"
    context_indices: list[int] = []
    has_year = any(all_positions[0][i].year > 0 for i in period_indices)
    if not has_year:
        sg_set = set(subgroup_indices)
        for i in range(n_positions):
            if not is_variable[i] and i not in sg_set:
                if (
                    all_positions[0][i].year > 0
                    and _period_granularity(all_positions[0][i]) == 1
                ):
                    context_indices.append(i)
                    break

    all_period_indices = sorted(context_indices + period_indices)
    period_set = set(period_indices)

    def _build_result_files(
        indexed_files: list[tuple[int, PurePath, int]],
    ) -> list[tuple[str, PurePath, int]]:
        result: list[tuple[str, PurePath, int]] = []
        for fi, path, mtime in indexed_files:
            infos = [all_positions[fi][j] for j in all_period_indices]
            period = _combine_periods(infos) if infos else None
            result.append((period.to_string() if period else "", path, mtime))
        return result

    # --- Build results (with or without sub-grouping) ---
    indexed = [(fi, p, m) for fi, (p, m, _) in enumerate(file_list)]
    is_period_marker = [i in period_set for i in range(n_positions)]

    if not subgroup_indices:
        refined = _refine_normalized_path(
            normalized_path, all_positions[0], is_period_marker
        )
        return [(refined, _build_result_files(indexed))]

    # Sub-group by subgroup positions (order violation case)
    subgroups: dict[
        tuple[tuple[int, int, int], ...], list[tuple[int, PurePath, int]]
    ] = defaultdict(list)
    for fi, path, mtime in indexed:
        key = tuple(all_positions[fi][j].to_sort_key() for j in subgroup_indices)
        subgroups[key].append((fi, path, mtime))

    results: list[tuple[str, list[tuple[str, PurePath, int]]]] = []
    for _key, sub_indexed in subgroups.items():
        rep_idx = sub_indexed[0][0]
        refined = _refine_normalized_path(
            normalized_path,
            all_positions[rep_idx],
            is_period_marker,
        )
        results.append((refined, _build_result_files(sub_indexed)))

    return results


def group_time_series(
    files: Sequence[tuple[PurePath, int]],  # [(path, mtime), ...]
    root: PurePath,
) -> tuple[list[TimeSeriesGroup], list[tuple[PurePath, int]]]:
    """Group files into time series and return singles separately.

    Returns:
        (time_series_groups, single_files)
        - time_series_groups: groups with ≥2 files
        - single_files: files that don't form a series
    """
    # Extract info once per file, group by normalized path
    raw_groups: dict[str, list[tuple[PurePath, int, list[PeriodInfo]]]] = defaultdict(
        list
    )
    no_period: list[tuple[PurePath, int]] = []

    for path, mtime in files:
        normalized, positions = _extract_file_info(path, root)
        if not positions:
            no_period.append((path, mtime))
            continue
        raw_groups[normalized].append((path, mtime, positions))

    # Refine groups with group-level period detection
    time_series: list[TimeSeriesGroup] = []
    singles: list[tuple[PurePath, int]] = list(no_period)

    for normalized, file_list in raw_groups.items():
        if len(file_list) < 2:
            for path, mtime, _ in file_list:
                singles.append((path, mtime))
            continue

        # Classify variable/constant positions and potentially sub-group
        for refined_path, result_files in _refine_group(normalized, file_list):
            if len(result_files) >= 2:
                sorted_files = sorted(result_files, key=lambda x: period_sort_key(x[0]))
                max_mtime = max(m for _, _, m in sorted_files)
                time_series.append(
                    TimeSeriesGroup(
                        normalized_path=refined_path,
                        files=[(period, path) for period, path, _ in sorted_files],
                        max_mtime=max_mtime,
                    )
                )
            else:
                for _, path, mtime in result_files:
                    singles.append((path, mtime))

    return time_series, singles


@dataclass
class TableSeriesGroup:
    """A group of database tables forming a time series."""

    normalized_name: str  # e.g., "stats_---PERIOD---"
    tables: list[tuple[str, str]]  # [(period_str, table_name), ...] sorted


def group_table_time_series(
    table_names: list[str],
) -> tuple[list[TableSeriesGroup], list[str]]:
    """Group database table names by temporal pattern.

    Returns (series_groups, single_tables).
    """
    raw_groups: dict[str, list[tuple[str, list[PeriodInfo]]]] = defaultdict(list)
    no_period: list[str] = []

    for name in table_names:
        matches = _extract_period_from_segment(name)
        if not matches:
            no_period.append(name)
            continue

        # Build normalized name (replace temporal parts with placeholder)
        normalized = name
        for original, _ in sorted(matches, key=lambda m: name.find(m[0]), reverse=True):
            idx = normalized.find(original)
            normalized = (
                normalized[:idx]
                + PERIOD_PLACEHOLDER
                + normalized[idx + len(original) :]
            )

        # Skip if entire name is consumed by temporal pattern (e.g. "t1", "t2")
        base = normalized.replace(PERIOD_PLACEHOLDER, "").strip("_- ")
        if not base:
            no_period.append(name)
            continue

        raw_groups[normalized].append((name, [info for _, info in matches]))

    series: list[TableSeriesGroup] = []
    singles: list[str] = list(no_period)

    for normalized, table_list in raw_groups.items():
        if len(table_list) < 2:
            singles.append(table_list[0][0])
            continue

        # Adapt to _refine_group's expected input: (PurePath, mtime, positions)
        file_list = [
            (PurePosixPath(name), 0, positions) for name, positions in table_list
        ]
        for refined_name, result_files in _refine_group(normalized, file_list):
            if len(result_files) < 2:
                for _, path, _ in result_files:
                    singles.append(path.name)
                continue
            sorted_files = sorted(result_files, key=lambda x: period_sort_key(x[0]))
            series.append(
                TableSeriesGroup(
                    normalized_name=refined_name,
                    tables=[(p, path.name) for p, path, _ in sorted_files],
                )
            )

    return series, singles


def get_series_folder_parts(normalized_path: str) -> list[str]:
    """Get non-temporal parent folder parts from normalized path."""
    parent_parts = PurePosixPath(normalized_path).parent.parts
    return [p for p in parent_parts if PERIOD_PLACEHOLDER not in p]


def build_series_dataset_id(normalized_path: str, prefix: str) -> str:
    """Build dataset ID from normalized path."""
    parts = [sanitize_id(p) for p in PurePosixPath(normalized_path).parts]
    return make_id(prefix, *parts)


def build_series_dataset_name(
    normalized_path: str,
    periods: list[str],
) -> str:
    """Build human-readable dataset name from normalized path.

    If period is in filename: enquete_{{PERIOD}}.csv → enquete_[YYYY]
    If period only in folder: {{PERIOD}}/enquete.csv → enquete
    """
    path = PurePosixPath(normalized_path)
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
