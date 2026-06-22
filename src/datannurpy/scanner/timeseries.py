"""Time series detection and grouping for datasets with temporal patterns."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePath, PurePosixPath
from typing import Literal, Union

from ..utils import make_id, sanitize_id

# Patterns ordered by length (longest first to avoid partial matches)
# Each pattern: (regex, length_hint for priority)
_PERIOD_PATTERNS = [
    # Full date: 2024-03-15 or 2024_03_15
    (re.compile(r"((?:19|20)\d{2})[_-](\d{2})[_-](\d{2})"), "date"),
    # Compact full date: 20240315
    (
        re.compile(
            r"(?<!\d)((?:19|20)\d{2})(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])(?!\d)"
        ),
        "date_compact",
    ),
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

_SORT_DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})$")
_SORT_QUARTER_RE = re.compile(r"(\d{4})Q([1-4])$")
_SORT_MONTH_RE = re.compile(r"(\d{4})/(\d{2})$")
_SORT_COMPACT_MONTH_RE = re.compile(r"(\d{4})(0[1-9]|1[0-2])$")
_SORT_YEAR_RE = re.compile(r"(\d{4})$")
_SORT_FALLBACK_YEAR_RE = re.compile(r"((?:19|20)\d{2})")

_DATE_PATTERN_TYPES = {"date", "date_compact"}
_YEAR_MONTH_PATTERN_TYPES = {"year_month", "year_month_compact"}

PeriodFrequency = Literal["daily", "quarterly", "monthly", "yearly"]
SeriesIdSuffix = Union[PeriodFrequency, Literal["series"]]
PeriodSortKey = tuple[int, int, int]
PeriodGroupKey = tuple[PeriodSortKey, ...]
PeriodSignature = tuple[int, ...]

_PERIOD_NAME_PATTERNS: dict[PeriodFrequency, str] = {
    "daily": "[YYYY/MM/DD]",
    "quarterly": "[YYYY]Q[N]",
    "monthly": "[YYYY/MM]",
    "yearly": "[YYYY]",
}

PERIOD_MATCH_PATTERNS = tuple(
    sorted(set(_PERIOD_NAME_PATTERNS.values()), key=len, reverse=True)
)

# Frequency-specific placeholders used to build metadata-first match keys.
# A yearly and a monthly series can share the same filename skeleton
# (e.g. ``base_---PERIOD---.csv``); tagging the placeholder with the frequency
# keeps their match identities distinct so a ``[YYYY/MM]`` `_match_path` cannot
# collapse onto a ``[YYYY]`` group. See `normalize_match_key` in add_metadata.
_PERIOD_MATCH_PLACEHOLDERS: dict[PeriodFrequency, str] = {
    "daily": "---PERIOD-D---",
    "quarterly": "---PERIOD-Q---",
    "monthly": "---PERIOD-M---",
    "yearly": "---PERIOD-Y---",
}

# Reverse lookup keyed by display pattern, for `period_match_placeholder`.
_PERIOD_MATCH_PLACEHOLDER_BY_PATTERN = {
    _PERIOD_NAME_PATTERNS[freq]: placeholder
    for freq, placeholder in _PERIOD_MATCH_PLACEHOLDERS.items()
}


@dataclass
class PeriodInfo:
    """Extracted period information from a path."""

    year: int
    sub_period: int  # 0=none, 1-4=quarter, 1-12=month
    day: int  # 0=none
    original: str  # Original matched string

    def to_sort_key(self) -> PeriodSortKey:
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


PeriodMatch = tuple[tuple[int, int], str, PeriodInfo]
RefineInput = tuple[PurePath, int, list[PeriodInfo]]
IndexedRefineInput = tuple[int, PurePath, int]
RefinedFile = tuple[str, PurePath, int]
RefinedGroup = tuple[str, list[RefinedFile]]


def _extract_period_from_segment(
    segment: str,
) -> list[PeriodMatch]:
    """Extract all period matches from a path segment.

    Returns only the most specific match for each segment position.
    """
    matches: list[PeriodMatch] = []
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

            if pattern_type in _DATE_PATTERN_TYPES:
                year, month, day = match.groups()
                info = PeriodInfo(int(year), int(month), int(day), original)
            elif pattern_type in _YEAR_MONTH_PATTERN_TYPES:
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

            matches.append(((start, end), original, info))
            matched_ranges.append((start, end))

    return sorted(matches, key=lambda match: match[0][0])


def _normalize_segment(segment: str, matches: Sequence[PeriodMatch]) -> str:
    """Replace matched temporal ranges in a segment with the period placeholder."""
    normalized = segment
    for (start, end), _, _ in reversed(matches):
        normalized = normalized[:start] + PERIOD_PLACEHOLDER + normalized[end:]
    return normalized


def _match_positions(matches: Sequence[PeriodMatch]) -> list[PeriodInfo]:
    """Return period info values in match order."""
    return [info for _, _, info in matches]


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
        if not matches:
            normalized_parts.append(segment)
            continue

        normalized_parts.append(_normalize_segment(segment, matches))
        positions.extend(_match_positions(matches))

    return "/".join(normalized_parts), positions


def extract_period(path: PurePath, root: PurePath | None = None) -> PeriodInfo | None:
    """Extract the combined period from a file path."""
    _, positions = _extract_file_info(path, root)
    return _combine_periods(positions)


def normalize_path(path: PurePath, root: PurePath | None = None) -> str:
    """Normalize path by replacing temporal patterns with placeholder."""
    normalized, _ = _extract_file_info(path, root)
    return normalized


def period_sort_key(period: str | PeriodInfo) -> PeriodSortKey:
    """Convert a period string or PeriodInfo to a sortable tuple (year, sub_period, day)."""
    if isinstance(period, PeriodInfo):
        return period.to_sort_key()

    # Parse period string
    period = period.strip()

    # Try full date: 2024/03/15
    if match := _SORT_DATE_RE.match(period):
        year, month, day = match.groups()
        return (int(year), int(month), int(day))

    # Try quarter: 2024Q1
    if match := _SORT_QUARTER_RE.match(period):
        year, quarter = match.groups()
        return (int(year), int(quarter) + 12, 0)  # 13-16 for quarters

    # Try month: 2024/03
    if match := _SORT_MONTH_RE.match(period):
        year, month = match.groups()
        return (int(year), int(month), 0)

    # Try compact month: 202403
    if match := _SORT_COMPACT_MONTH_RE.match(period):
        year, month = match.groups()
        return (int(year), int(month), 0)

    # Try year only: 2024
    if match := _SORT_YEAR_RE.match(period):
        year = match.group(1)
        return (int(year), 0, 0)

    # Fallback: try to extract year
    if match := _SORT_FALLBACK_YEAR_RE.search(period):
        return (int(match.group(1)), 0, 0)

    return (0, 0, 0)


@dataclass
class TimeSeriesGroup:
    """A group of files forming a time series."""

    normalized_path: str
    files: list[tuple[str, PurePath]]  # [(period_string, path), ...]
    max_mtime: int
    id_suffix: PeriodFrequency | None = None


def _period_granularity(info: PeriodInfo) -> int:
    """Return temporal granularity: 1=year, 2=month/quarter, 3=day/full-date."""
    if info.day > 0:
        return 3
    if info.sub_period > 0:
        return 2
    if info.year > 0:
        return 1
    return 0


def _period_granularity_signature(positions: Sequence[PeriodInfo]) -> PeriodSignature:
    """Return a grouping signature that distinguishes year, quarter, month, and date."""
    signature: list[int] = []
    for info in positions:
        if info.day > 0:
            signature.append(4)
        elif info.sub_period > 12:
            signature.append(2)
        elif info.sub_period > 0:
            signature.append(3)
        elif info.year > 0:
            signature.append(1)
        else:
            signature.append(0)
    return tuple(signature)


def _series_id_suffix(periods: Sequence[str]) -> SeriesIdSuffix:
    """Return a stable suffix for series IDs based on period granularity."""
    if not periods:
        return "series"

    return _period_frequency(periods[0])


def _period_frequency(period: str) -> PeriodFrequency:
    """Return the dataset frequency label implied by a period string."""
    if _SORT_DATE_RE.match(period):
        return "daily"
    if _SORT_QUARTER_RE.match(period):
        return "quarterly"
    if _SORT_MONTH_RE.match(period):
        return "monthly"
    return "yearly"


def series_match_normalized_path(normalized_path: str, periods: Sequence[str]) -> str:
    """Tag the generic period placeholder with the series frequency.

    The scan-side normalized path uses a single ``PERIOD_PLACEHOLDER`` for any
    granularity, so yearly and monthly series sharing a skeleton would produce
    identical metadata match keys. Embedding the frequency keeps them distinct;
    mirrors the metadata-side normalisation in `normalize_match_key`.
    """
    placeholder = _PERIOD_MATCH_PLACEHOLDERS[_period_frequency(periods[0])]
    return normalized_path.replace(PERIOD_PLACEHOLDER, placeholder)


def period_match_placeholder(value: str) -> str | None:
    """Return the frequency-specific match placeholder for period patterns in *value*.

    Tests patterns longest-first (PERIOD_MATCH_PATTERNS order), so the finest /
    overlapping pattern wins — e.g. ``[YYYY]Q[N]`` before its ``[YYYY]`` substring.
    Returns None when *value* carries no period placeholder.
    """
    for pattern in PERIOD_MATCH_PATTERNS:
        if pattern in value:
            return _PERIOD_MATCH_PLACEHOLDER_BY_PATTERN[pattern]
    return None


def _sorted_valid_refined_files(
    result_files: Sequence[RefinedFile],
) -> list[RefinedFile] | None:
    """Return sorted refined files when they form a valid final series."""
    if len(result_files) < 2:
        return None

    keyed_files: list[tuple[PeriodSortKey, RefinedFile]] = []
    for file in result_files:
        period = file[0]
        sort_key = period_sort_key(period)
        year, sub_period, day = sort_key
        if year <= 0 or (day > 0 and not 1 <= sub_period <= 12):
            return None
        keyed_files.append((sort_key, file))

    return [file for _, file in sorted(keyed_files, key=lambda item: item[0])]


def _ambiguous_id_suffix(
    is_ambiguous: bool,
    sorted_files: Sequence[RefinedFile],
) -> PeriodFrequency | None:
    """Return the ID suffix needed to disambiguate mixed-frequency series."""
    return _period_frequency(sorted_files[0][0]) if is_ambiguous else None


def _has_year_position(
    file_list: Sequence[RefineInput],
) -> bool:
    """Return True when any extracted position includes a real year."""
    return any(info.year > 0 for _, _, positions in file_list for info in positions)


def _refine_normalized_path(
    normalized_path: str,
    positions: list[PeriodInfo],
    is_period: list[bool],
) -> str:
    """Rebuild normalized path, restoring non-period positions to their original value."""
    parts = normalized_path.split(PERIOD_PLACEHOLDER)
    if len(parts) != len(positions) + 1:
        return normalized_path
    result = [parts[0]]
    for i, info in enumerate(positions):
        result.append(PERIOD_PLACEHOLDER if is_period[i] else info.original)
        result.append(parts[i + 1])
    return "".join(result)


def _refine_group(
    normalized_path: str,
    file_list: Sequence[RefineInput],
) -> list[RefinedGroup]:
    """Classify period positions as variable/constant and build refined periods.

    For each PERIOD_PLACEHOLDER position, compare values across all files:
    - Variable (values differ): candidate for the period
    - Constant (same value everywhere): restore to literal in the path

    Among variable positions, checks temporal order (YYYY → MM/Q → DD).
    An order violation triggers sub-grouping.

    Returns list of (refined_normalized_path, [(period_str, path, mtime), ...]).
    """
    all_positions = [positions for _, _, positions in file_list]
    reference_positions = all_positions[0] if all_positions else []
    n_positions = len(reference_positions)

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
    # Valid: YYYY → MM/Q → DD (increasing granularity).
    # A lower-granularity token with its own year starts a new period candidate;
    # a lower-granularity token without a year is treated as a sub-group suffix.
    period_indices: list[int] = []
    subgroup_indices: list[int] = []

    if len(variable_indices) == 1:
        period_indices = variable_indices[:]
    else:
        prev_gran = 0
        for idx in variable_indices:
            info = reference_positions[idx]
            gran = _period_granularity(info)
            if subgroup_indices and info.year == 0:
                subgroup_indices.append(idx)
            elif prev_gran > 0 and gran <= prev_gran:
                if info.year == 0:
                    subgroup_indices.append(idx)
                else:
                    subgroup_indices.extend(period_indices)
                    period_indices = [idx]
            else:
                period_indices.append(idx)
            prev_gran = gran

    indexed = [(fi, p, m) for fi, (p, m, _) in enumerate(file_list)]

    # If a left-side year varies one-to-one with the chosen right-side period,
    # keeping it as a sub-group would turn an otherwise valid series into only
    # singletons. Preserve its placeholder instead of restoring an arbitrary year.
    neutralized_indices: set[int] = set()
    if subgroup_indices:
        subgroup_counts: dict[PeriodGroupKey, int] = defaultdict(int)
        for fi, _, _ in indexed:
            key = tuple(all_positions[fi][j].to_sort_key() for j in subgroup_indices)
            subgroup_counts[key] += 1

        subgroup_has_only_years = all(
            all_positions[fi][idx].year > 0
            for fi, _, _ in indexed
            for idx in subgroup_indices
        )
        selected_periods = {
            tuple(all_positions[fi][j].to_sort_key() for j in period_indices)
            for fi, _, _ in indexed
        }
        if (
            subgroup_counts
            and all(count == 1 for count in subgroup_counts.values())
            and subgroup_has_only_years
            and len(selected_periods) > 1
        ):
            neutralized_indices.update(subgroup_indices)
            subgroup_indices = []

    # --- Include constant year context if period has no year ---
    # Example: 2024/data_01.csv → month varies, year is constant but needed for "2024/01"
    context_indices: list[int] = []
    subgroup_set = set(subgroup_indices)
    has_year = any(reference_positions[i].year > 0 for i in period_indices)
    if not has_year:
        for i in range(n_positions):
            if not is_variable[i] and i not in subgroup_set:
                info = reference_positions[i]
                if info.year > 0 and _period_granularity(info) == 1:
                    context_indices.append(i)
                    break

    # Example: 2024/01/day15.csv → day varies, constant year/month are needed
    # for "2024/01/15" instead of "2024/00/15".
    has_day = any(reference_positions[i].day > 0 for i in period_indices)
    has_sub_period = any(
        reference_positions[i].sub_period > 0 for i in context_indices + period_indices
    )
    if has_day and not has_sub_period:
        for i in range(n_positions):
            if not is_variable[i] and i not in subgroup_set and i not in period_indices:
                info = reference_positions[i]
                if 0 < info.sub_period <= 12 and info.day == 0:
                    context_indices.append(i)
                    break

    all_period_indices = sorted(context_indices + period_indices)
    period_set = set(period_indices)

    def _build_result_files(
        indexed_files: list[IndexedRefineInput],
    ) -> list[RefinedFile]:
        result: list[RefinedFile] = []
        for fi, path, mtime in indexed_files:
            infos = [all_positions[fi][j] for j in all_period_indices]
            period = _combine_periods(infos) if infos else None
            result.append((period.to_string() if period else "", path, mtime))
        return result

    # --- Build results (with or without sub-grouping) ---
    is_period_marker = [
        i in period_set or i in neutralized_indices for i in range(n_positions)
    ]

    if not subgroup_indices:
        refined = _refine_normalized_path(
            normalized_path, reference_positions, is_period_marker
        )
        return [(refined, _build_result_files(indexed))]

    # Sub-group by subgroup positions (order violation case)
    subgroups: dict[PeriodGroupKey, list[IndexedRefineInput]] = defaultdict(list)
    for fi, path, mtime in indexed:
        key = tuple(all_positions[fi][j].to_sort_key() for j in subgroup_indices)
        subgroups[key].append((fi, path, mtime))

    results: list[RefinedGroup] = []
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
    # Extract info once per file, group by normalized path and period granularity.
    raw_groups: dict[tuple[str, PeriodSignature], list[RefineInput]] = defaultdict(list)
    signatures_by_normalized: dict[str, set[PeriodSignature]] = defaultdict(set)
    no_period: list[tuple[PurePath, int]] = []

    for path, mtime in files:
        normalized, positions = _extract_file_info(path, root)
        if not positions:
            no_period.append((path, mtime))
            continue
        signature = _period_granularity_signature(positions)
        signatures_by_normalized[normalized].add(signature)
        raw_groups[(normalized, signature)].append((path, mtime, positions))

    # Refine groups with group-level period detection
    time_series: list[TimeSeriesGroup] = []
    singles: list[tuple[PurePath, int]] = list(no_period)

    for (normalized, _signature), file_list in raw_groups.items():
        is_ambiguous = len(signatures_by_normalized[normalized]) > 1
        if len(file_list) < 2:
            singles.extend((path, mtime) for path, mtime, _ in file_list)
            continue

        # Require at least one full 4-digit year somewhere in the path/name;
        # otherwise partial fragments (e.g. trailing "12"/"13") would form a
        # bogus series. See group_table_time_series for the same guard.
        if not _has_year_position(file_list):
            singles.extend((path, mtime) for path, mtime, _ in file_list)
            continue

        # Classify variable/constant positions and potentially sub-group
        for refined_path, result_files in _refine_group(normalized, file_list):
            sorted_files = _sorted_valid_refined_files(result_files)
            if sorted_files:
                max_mtime = max(m for _, _, m in sorted_files)
                time_series.append(
                    TimeSeriesGroup(
                        normalized_path=refined_path,
                        files=[(period, path) for period, path, _ in sorted_files],
                        max_mtime=max_mtime,
                        id_suffix=_ambiguous_id_suffix(is_ambiguous, sorted_files),
                    )
                )
            else:
                singles.extend((path, mtime) for _, path, mtime in result_files)

    return time_series, singles


@dataclass
class TableSeriesGroup:
    """A group of database tables forming a time series."""

    normalized_name: str  # e.g., "stats_---PERIOD---"
    tables: list[tuple[str, str]]  # [(period_str, table_name), ...] sorted
    id_suffix: PeriodFrequency | None = None


def group_table_time_series(
    table_names: list[str],
) -> tuple[list[TableSeriesGroup], list[str]]:
    """Group database table names by temporal pattern.

    Returns (series_groups, single_tables).
    """
    raw_groups: dict[
        tuple[str, PeriodSignature], list[tuple[str, list[PeriodInfo]]]
    ] = defaultdict(list)
    signatures_by_normalized: dict[str, set[PeriodSignature]] = defaultdict(set)
    no_period: list[str] = []

    for name in table_names:
        matches = _extract_period_from_segment(name)
        if not matches:
            no_period.append(name)
            continue

        normalized = _normalize_segment(name, matches)

        # Skip if entire name is consumed by temporal pattern (e.g. "t1", "t2")
        base = normalized.replace(PERIOD_PLACEHOLDER, "").strip("_- ")
        if not base:
            no_period.append(name)
            continue

        positions = _match_positions(matches)
        signature = _period_granularity_signature(positions)
        signatures_by_normalized[normalized].add(signature)
        raw_groups[(normalized, signature)].append((name, positions))

    series: list[TableSeriesGroup] = []
    singles: list[str] = list(no_period)

    for (normalized, _signature), table_list in raw_groups.items():
        is_ambiguous = len(signatures_by_normalized[normalized]) > 1
        if len(table_list) < 2:
            singles.append(table_list[0][0])
            continue

        # Adapt to _refine_group's expected input: (PurePath, mtime, positions)
        file_list = [
            (PurePosixPath(name), 0, positions) for name, positions in table_list
        ]
        # Require at least one full 4-digit year; otherwise partial digits
        # like "MONTH12"/"MONTH13" would be falsely grouped as a series.
        if not _has_year_position(file_list):
            singles.extend(path.name for path, _, _ in file_list)
            continue

        for refined_name, result_files in _refine_group(normalized, file_list):
            sorted_files = _sorted_valid_refined_files(result_files)
            if not sorted_files:
                singles.extend(path.name for _, path, _ in result_files)
                continue
            series.append(
                TableSeriesGroup(
                    normalized_name=refined_name,
                    tables=[(p, path.name) for p, path, _ in sorted_files],
                    id_suffix=_ambiguous_id_suffix(is_ambiguous, sorted_files),
                )
            )

    return series, singles


def get_series_folder_parts(normalized_path: str) -> list[str]:
    """Get non-temporal parent folder parts from normalized path."""
    parent_parts = PurePosixPath(normalized_path).parent.parts
    return [p for p in parent_parts if PERIOD_PLACEHOLDER not in p]


def _build_series_dataset_id_with_suffix(
    normalized_path: str,
    prefix: str,
    suffix: str | None = None,
) -> str:
    """Build dataset ID from normalized path with an optional suffix."""
    parts = [sanitize_id(p) for p in PurePosixPath(normalized_path).parts]
    if suffix:
        parts[-1] = f"{parts[-1]}_{suffix}"
    return make_id(prefix, *parts)


def build_series_dataset_id(normalized_path: str, prefix: str) -> str:
    """Build dataset ID from normalized path."""
    return _build_series_dataset_id_with_suffix(normalized_path, prefix)


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
        pattern = (
            _PERIOD_NAME_PATTERNS[_period_frequency(periods[0])]
            if periods
            else "[YYYY]"
        )
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
