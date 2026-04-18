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


def configure_logging(
    *, verbose: bool = False, log_file: str | Path | None = None
) -> None:
    """Set logging verbosity and optional log file (truncated each run)."""
    global _verbose, _log_file_path  # noqa: PLW0603
    _verbose = verbose
    if log_file is not None:
        _log_file_path = Path(log_file)
        _log_file_path.write_text("")
    else:
        _log_file_path = None


def _write_log(message: str) -> None:
    """Append a message to the log file if configured."""
    if _log_file_path is not None:
        with open(_log_file_path, "a") as f:
            f.write(message + "\n")


def log_start(msg: str, quiet: bool) -> float:
    """Log start of an operation (with ... suffix, no newline). Returns start time."""
    if not quiet:
        print(f"  ⏳{msg}...", end="", flush=True, file=sys.stderr)
    return time.perf_counter()


def log_progress(msg: str, quiet: bool) -> None:
    """Update the in-progress line with extra info (overwrites log_start line)."""
    if not quiet:
        print(f"\r  ⏳{msg}...", end="", flush=True, file=sys.stderr)


def log_done(msg: str, quiet: bool, start_time: float | None = None) -> None:
    """Log completion (replaces the 'start' line)."""
    if start_time is not None:
        elapsed = time.perf_counter() - start_time
        text = f"✓ {msg} in {elapsed:.1f}s"
    else:
        text = f"✓ {msg}"
    if not quiet:
        print(f"\r  {text}", file=sys.stderr)
    _write_log(f"  {text}")


def log_warn(msg: str, quiet: bool) -> None:
    """Log a warning (replaces the 'start' line)."""
    if not quiet:
        print(f"\r  ⚠ {msg}", file=sys.stderr)
    _write_log(f"  ⚠ {msg}")


def log_skip(msg: str, quiet: bool) -> None:
    """Log a skipped item (unchanged, no rescan needed)."""
    if not quiet:
        print(f"  ⏭ {msg} (unchanged)", file=sys.stderr)
    _write_log(f"  ⏭ {msg} (unchanged)")


def log_section(method: str, target: str, quiet: bool) -> float:
    """Log a section header with method name. Returns start time for timer."""
    if not quiet:
        print(f"\n[{method}] {target}", file=sys.stderr)
    _write_log(f"\n[{method}] {target}")
    return time.perf_counter()


def log_folder(name: str, quiet: bool) -> None:
    """Log a folder/schema."""
    if not quiet:
        print(f"\n  📁 {name}", file=sys.stderr)
    _write_log(f"\n  📁 {name}")


_CRED_RE = re.compile(r"://[^@/]+@")


def _redact(text: str) -> str:
    """Replace credentials in connection URLs with ***."""
    return _CRED_RE.sub("://***@", text)


def log_error(name: str, error: BaseException, quiet: bool) -> None:
    """Log a scan error (replaces the 'start' line)."""
    msg = _redact(str(error).split("\n")[0])
    header = f"\r  ✗ {name} — {type(error).__name__}: {msg}"
    if not quiet or _verbose:
        print(header, file=sys.stderr)
    if _verbose:
        traceback.print_exc(file=sys.stderr)
    _write_log(f"  ✗ {name} — {type(error).__name__}: {_redact(str(error))}")
    if _log_file_path is not None:
        buf = io.StringIO()
        traceback.print_exc(file=buf)
        with open(_log_file_path, "a") as f:
            f.write(_redact(buf.getvalue()))


def log_summary(
    datasets: int, variables: int, quiet: bool, start_time: float, errors: int = 0
) -> None:
    """Log final summary with elapsed time."""
    elapsed = time.perf_counter() - start_time
    parts = [f"{datasets} datasets", f"{variables} variables"]
    if errors:
        parts.append(f"{errors} errors")
    text = f"\n  → {', '.join(parts)} in {elapsed:.1f}s"
    if not quiet:
        print(text, file=sys.stderr)
    _write_log(text)
