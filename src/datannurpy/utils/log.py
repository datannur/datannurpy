"""Logging utilities for scan progress."""

from __future__ import annotations

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


def log_start(msg: str, quiet: bool) -> None:
    """Log start of an operation (with ... suffix, no newline)."""
    if quiet:
        return
    # Clear line and print without newline
    print(f"  {msg}...", end="", flush=True, file=sys.stderr)


def log_done(msg: str, quiet: bool, start_time: float | None = None) -> None:
    """Log completion (replaces the 'start' line)."""
    if quiet:
        return
    # Carriage return to overwrite the "..." line
    if start_time is not None:
        elapsed = time.perf_counter() - start_time
        print(f"\r  ✓ {msg} in {elapsed:.1f}s", file=sys.stderr)
    else:
        print(f"\r  ✓ {msg}", file=sys.stderr)


def log_warn(msg: str, quiet: bool) -> None:
    """Log a warning (replaces the 'start' line)."""
    if quiet:
        return
    print(f"\r  ⚠ {msg}", file=sys.stderr)


def log_skip(msg: str, quiet: bool) -> None:
    """Log a skipped item (unchanged, no rescan needed)."""
    if quiet:
        return
    print(f"  ⏭ {msg} (unchanged)", file=sys.stderr)


def log_section(method: str, target: str, quiet: bool) -> float:
    """Log a section header with method name. Returns start time for timer."""
    if not quiet:
        print(f"\n[{method}] {target}", file=sys.stderr)
    return time.perf_counter()


def log_folder(name: str, quiet: bool) -> None:
    """Log a folder/schema."""
    if quiet:
        return
    print(f"  📁 {name}", file=sys.stderr)


def log_error(name: str, error: BaseException, quiet: bool) -> None:
    """Log a scan error (replaces the 'start' line)."""
    msg = str(error).split("\n")[0]
    header = f"\r  ✗ {name} — {type(error).__name__}: {msg}"
    if not quiet or _verbose:
        print(header, file=sys.stderr)
    if _verbose:
        traceback.print_exc(file=sys.stderr)
    if _log_file_path is not None:
        with open(_log_file_path, "a") as f:
            f.write(f"✗ {name} — {type(error).__name__}: {error}\n")
            traceback.print_exc(file=f)
            f.write("\n")


def log_summary(
    datasets: int, variables: int, quiet: bool, start_time: float, errors: int = 0
) -> None:
    """Log final summary with elapsed time."""
    if quiet:
        return
    elapsed = time.perf_counter() - start_time
    parts = [f"{datasets} datasets", f"{variables} variables"]
    if errors:
        parts.append(f"{errors} errors")
    print(
        f"  → {', '.join(parts)} in {elapsed:.1f}s",
        file=sys.stderr,
    )
