"""Auto-tagging of string columns by content type."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import TYPE_CHECKING

import ibis

if TYPE_CHECKING:
    from ..catalog import Catalog

from ..schema import Tag

# --- Tag tree (id, name, parent_id) ---

SCAN_TAG_ID = "scan"
SCAN_TAG_DESCRIPTION = "Tags generated automatically during scanning"
_AUTO_TAG_PARENT = "auto"

# (id, name, description, parent_id)
_AUTO_TREE: list[tuple[str, str, str, str | None]] = [
    (SCAN_TAG_ID, "Scan", SCAN_TAG_DESCRIPTION, None),
    (
        _AUTO_TAG_PARENT,
        "Auto-detected",
        "Tags detected automatically by content analysis",
        SCAN_TAG_ID,
    ),
    ("auto---format", "Format", "Recognized data formats", _AUTO_TAG_PARENT),
    ("auto---email", "Email", "Values matching email address format", "auto---format"),
    ("auto---phone", "Phone", "Values matching phone number format", "auto---format"),
    ("auto---uuid", "UUID", "Values matching UUID format", "auto---format"),
    ("auto---iban", "IBAN", "Values matching IBAN format", "auto---format"),
    (
        "auto---security",
        "Security",
        "Sensitive or security-related content",
        _AUTO_TAG_PARENT,
    ),
    (
        "auto---bcrypt",
        "Bcrypt",
        "Bcrypt password hashes ($2a$, $2b$, $2y$)",
        "auto---security",
    ),
    ("auto---argon2", "Argon2", "Argon2 password hashes", "auto---security"),
    ("auto---jwt", "JWT", "JSON Web Tokens", "auto---security"),
    (
        "auto---secret",
        "Secret",
        "Likely secrets, tokens, or technical hashes",
        "auto---security",
    ),
    ("auto---text", "Text", "Text content classification", _AUTO_TAG_PARENT),
    (
        "auto---structured",
        "Structured",
        "Repeating pattern covers ≥50% of values",
        "auto---text",
    ),
    (
        "auto---semi-structured",
        "Semi-structured",
        "Top 3 patterns cover ≥50% of values",
        "auto---text",
    ),
    ("auto---free-text", "Free text", "No dominant pattern detected", "auto---text"),
    (
        "auto---natural-text",
        "Natural text",
        "Multi-word natural language text",
        "auto---free-text",
    ),
]

_SECURITY_TAGS = frozenset(
    tag_id for tag_id, _, _, parent_id in _AUTO_TREE if parent_id == "auto---security"
)


def ensure_auto_tags(catalog: Catalog) -> None:
    """Create or mark auto-detection tags as seen."""
    for tag_id, name, description, parent_id in _AUTO_TREE:
        if catalog.tag.get(tag_id) is None:
            catalog.tag.add(
                Tag(
                    id=tag_id,
                    name=name,
                    description=description,
                    parent_id=parent_id,
                    _seen=True,
                )
            )
        else:
            catalog.tag.update(tag_id, _seen=True)


# --- Specific detectors ---

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-zA-Z]{2,}$")
_PHONE_INTL_RE = re.compile(r"^\+\d[\d\s\-()]{6,}$")
_PHONE_LOCAL_RE = re.compile(r"^0\d[\d\s\-()]{5,}$")
_IBAN_RE = re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{10,30}$")
_HASH_PREFIX_RE = re.compile(r"^\$[a-zA-Z0-9]+\$")


def _is_jwt(v: str) -> bool:
    parts = v.split(".")
    return len(parts) == 3 and all(parts) and len(v) >= 20


def _is_phone(v: str) -> bool:
    if _PHONE_INTL_RE.match(v):
        return True
    if not _PHONE_LOCAL_RE.match(v) or not any(c in v for c in " -()"):
        return False
    return sum(c.isdigit() for c in v) >= 9


_Detector = Callable[[str], bool]

_SPECIFIC_DETECTORS: list[tuple[str, _Detector, float]] = [
    ("auto---bcrypt", lambda v: v.startswith(("$2a$", "$2b$", "$2y$")), 0.8),
    ("auto---argon2", lambda v: v.startswith("$argon2"), 0.8),
    ("auto---jwt", _is_jwt, 0.8),
    ("auto---uuid", lambda v: bool(_UUID_RE.match(v)), 0.8),
    ("auto---email", lambda v: bool(_EMAIL_RE.match(v)), 0.8),
    ("auto---phone", _is_phone, 0.6),
    ("auto---iban", lambda v: bool(_IBAN_RE.match(v)), 0.8),
    ("auto---secret", lambda v: bool(_HASH_PREFIX_RE.match(v)), 0.8),
]


def _detect_specific(values: list[str]) -> str | None:
    """Return the first specific detector tag that fires, or None."""
    n = len(values)
    for tag_id, test, threshold in _SPECIFIC_DETECTORS:
        if sum(1 for v in values if test(v)) / n >= threshold:
            return tag_id
    return None


# --- Generic detectors ---


def _is_secret(values: list[str]) -> bool:
    """Heuristic: long, mostly-alphanumeric strings without spaces → likely secret."""
    n = len(values)
    avg_len = sum(len(v) for v in values) / n
    space_ratio = sum(" " in v for v in values) / n
    alnum_ratio = (
        sum(sum(c.isalnum() or c == "_" for c in v) / len(v) for v in values if v) / n
    )
    if avg_len < 20 or space_ratio >= 0.1 or alnum_ratio < 0.90:
        return False
    if n < 5:
        return True
    return len(set(values)) / n >= 0.9


def _is_natural_text(values: list[str]) -> bool:
    """Heuristic: letters, spaces, multi-word → likely natural text."""
    if len(values) < 5:
        return False
    avg_words = sum(len(v.split()) for v in values) / len(values)
    space_ratio = sum(" " in v for v in values) / len(values)
    return avg_words >= 3 and space_ratio >= 0.5


def _detect_generic(values: list[str]) -> str | None:
    """Return the first generic detector tag that fires, or None."""
    if _is_secret(values):
        return "auto---secret"
    if _is_natural_text(values):
        return "auto---natural-text"
    return None


# --- Main entry point ---


def compute_auto_tags(
    table: ibis.Table,
    string_cols: list[str],
    *,
    sample_size: int = 100,
) -> dict[str, str]:
    """Return {col_name: tag_id} — one leaf tag per column."""
    if not string_cols:
        return {}

    sample = table.select(*string_cols).limit(sample_size).to_pyarrow()
    result: dict[str, str] = {}

    for col in string_cols:
        col_data = sample.column(col)
        values = [v for v in col_data.to_pylist() if v is not None]
        if not values:
            continue

        tag = _detect_specific(values)
        if not tag:
            tag = _detect_generic(values)
        if tag:
            result[col] = tag

    return result
