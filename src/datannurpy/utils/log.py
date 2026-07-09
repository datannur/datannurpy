"""Logging utilities for scan progress."""

from __future__ import annotations

import io
import re
import sys
import time
import traceback
from pathlib import Path

# Module-level logging configuration
_verbose: bool = False
_log_file_path: Path | None = None

_ICON_SPACING = "  "
_LOADING_SPACING = " "


def _reconfigure_utf8(stream: object) -> None:
    """Force one text stream to UTF-8 so status glyphs never raise on Windows.

    Windows consoles default to a legacy code page (e.g. cp1252) that can't
    encode glyphs like ✓/⏳/✗, so every log line would raise UnicodeEncodeError.
    Reconfiguring the stream keeps the output readable and crash-free.
    """
    reconfigure = getattr(stream, "reconfigure", None)
    if reconfigure is None:  # not a reconfigurable TextIOWrapper (e.g. pytest capture)
        return
    encoding = (getattr(stream, "encoding", "") or "").lower().replace("-", "")
    if encoding == "utf8":
        return
    try:
        reconfigure(encoding="utf-8", errors="replace")
    except (ValueError, OSError):  # detached/closed stream — leave it as-is
        pass


def _ensure_utf8_output() -> None:
    """Make stderr/stdout tolerate the Unicode status glyphs on any platform."""
    _reconfigure_utf8(sys.stderr)
    _reconfigure_utf8(sys.stdout)


def configure_logging(
    *, verbose: bool = False, log_file: str | Path | None = None
) -> None:
    """Set logging verbosity and optional log file (truncated each run)."""
    global _verbose, _log_file_path  # noqa: PLW0603
    _ensure_utf8_output()
    _verbose = verbose
    if log_file is not None:
        _log_file_path = Path(log_file)
        _log_file_path.write_text("", encoding="utf-8")
    else:
        _log_file_path = None


def _write_log(message: str) -> None:
    """Append a message to the log file if configured."""
    if _log_file_path is not None:
        with open(_log_file_path, "a", encoding="utf-8") as f:
            f.write(message + "\n")


def log_start(msg: str, quiet: bool) -> float:
    """Log start of an operation (with ... suffix, no newline). Returns start time."""
    if not quiet:
        print(f"  ⏳{_LOADING_SPACING}{msg}...", end="", flush=True, file=sys.stderr)
    return time.perf_counter()


def log_done(msg: str, quiet: bool, start_time: float | None = None) -> None:
    """Log completion (replaces the 'start' line)."""
    if start_time is not None:
        elapsed = time.perf_counter() - start_time
        text = f"✓{_ICON_SPACING}{msg} in {elapsed:.1f}s"
    else:
        text = f"✓{_ICON_SPACING}{msg}"
    if not quiet:
        print(f"\r  {text}", file=sys.stderr)
    _write_log(f"  {text}")


def log_warn(msg: str, quiet: bool) -> None:
    """Log a warning (replaces the 'start' line)."""
    if not quiet:
        print(f"\r  ⚠{_ICON_SPACING}{msg}", file=sys.stderr)
    _write_log(f"  ⚠{_ICON_SPACING}{msg}")


def log_debug(msg: str, quiet: bool) -> None:
    """Log a debug message (only printed when verbose; always written to log file)."""
    if not quiet and _verbose:
        print(f"\r  ·{_ICON_SPACING}{msg}", file=sys.stderr)
    _write_log(f"  ·{_ICON_SPACING}{msg}")


def log_skip(msg: str, quiet: bool) -> None:
    """Log a skipped item (unchanged, no rescan needed)."""
    if not quiet:
        print(f"  ⏭{_ICON_SPACING}{msg} (unchanged)", file=sys.stderr)
    _write_log(f"  ⏭{_ICON_SPACING}{msg} (unchanged)")


def log_section(method: str, target: str, quiet: bool) -> float:
    """Log a section header with method name. Returns start time for timer."""
    if not quiet:
        print(f"\n[{method}] {target}", file=sys.stderr)
    _write_log(f"\n[{method}] {target}")
    return time.perf_counter()


def log_folder(name: str, quiet: bool) -> None:
    """Log a folder/schema."""
    if not quiet:
        print(f"\n  📁{_ICON_SPACING}{name}", file=sys.stderr)
    _write_log(f"\n  📁{_ICON_SPACING}{name}")


_CRED_RE = re.compile(r"://[^@/]+@")


def _redact(text: str) -> str:
    """Replace credentials in connection URLs with ***."""
    return _CRED_RE.sub("://***@", text)


def _format_traceback(error: BaseException) -> str:
    """Render an error's own traceback, or "" when it has none (e.g. not raised)."""
    if error.__traceback__ is None:
        return ""
    buf = io.StringIO()
    traceback.print_exception(type(error), error, error.__traceback__, file=buf)
    return buf.getvalue()


def log_error(name: str, error: BaseException, quiet: bool) -> None:
    """Log a scan error (replaces the 'start' line)."""
    msg = _redact(str(error).split("\n")[0])
    header = f"\r  ✗{_ICON_SPACING}{name} — {type(error).__name__}: {msg}"
    if not quiet or _verbose:
        print(header, file=sys.stderr)
    tb = _format_traceback(error) if (_verbose or _log_file_path is not None) else ""
    if _verbose and tb:
        print(tb, file=sys.stderr, end="")
    _write_log(
        f"  ✗{_ICON_SPACING}{name} — {type(error).__name__}: {_redact(str(error))}"
    )
    if _log_file_path is not None and tb:
        with open(_log_file_path, "a", encoding="utf-8") as f:
            f.write(_redact(tb))


def _nonzero_parts(*pairs: tuple[int | None, str]) -> list[str]:
    """Render ``(count, label)`` pairs, dropping any whose count is zero/None.

    The shared "show a count only when it happened" rule behind both scan
    summaries — e.g. an all-unchanged run omits ``0 scanned`` rather than
    printing a misleading zero.
    """
    return [f"{n} {label}" for n, label in pairs if n]


def log_summary(
    scanned: int,
    variables: int | None,
    quiet: bool,
    start_time: float,
    errors: int = 0,
    resource_count: int | None = None,
    resource_label: str | None = None,
    unchanged: int = 0,
) -> None:
    """Log final summary with elapsed time.

    ``scanned`` counts datasets newly scanned or updated this run; ``unchanged``
    counts datasets an incremental run left untouched. Each count is shown only
    when non-zero (like ``errors``), so an all-unchanged run reports its real work
    (``7 unchanged``) instead of a misleading ``0 datasets``, and a fresh scan
    stays terse (``2 scanned``) without a noisy ``0 unchanged``.
    """
    elapsed = time.perf_counter() - start_time
    parts: list[str] = []
    if resource_count is not None and resource_label is not None:
        parts.append(f"{resource_count} {resource_label}")
    parts += _nonzero_parts(
        (scanned, "scanned"),
        (unchanged, "unchanged"),
        (variables, "variables"),
        (errors, "errors"),
    )
    text = f"\n  →{_ICON_SPACING}{', '.join(parts)} in {elapsed:.1f}s"
    if not quiet:
        print(text, file=sys.stderr)
    _write_log(text)


def log_run_summary(
    folders: int,
    datasets: int,
    variables: int,
    quiet: bool,
    *,
    scanned: int = 0,
    unchanged: int = 0,
    errors: int = 0,
) -> None:
    """Log a whole-run recap at export time.

    Reports the catalogue's final totals (folders / datasets / variables) plus
    how this run got there — ``scanned`` (new or updated), ``unchanged`` (skipped
    by the incremental check) and ``errors``. Ventilation parts are shown only
    when non-zero, matching ``log_summary``. Emitted once per export, so a run
    spanning many ``add:`` sources ends with a single aggregated bilan instead of
    only per-source lines.
    """
    breakdown = _nonzero_parts(
        (scanned, "scanned"), (unchanged, "unchanged"), (errors, "errors")
    )
    text = f"\n[summary] {folders} folders, {datasets} datasets, {variables} variables"
    if breakdown:
        # Name the subject ("datasets") once so the run clause can't be misread
        # as qualifying the variables total it follows.
        count, label = breakdown[0].split(" ", 1)
        clause = ", ".join([f"{count} datasets {label}", *breakdown[1:]])
        text += f" ({clause} this run)"
    if not quiet:
        print(text, file=sys.stderr)
    _write_log(text)
