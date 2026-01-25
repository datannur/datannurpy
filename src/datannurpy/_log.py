"""Logging utilities for scan progress."""

from __future__ import annotations

import sys
import time


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


def log_summary(datasets: int, variables: int, quiet: bool, start_time: float) -> None:
    """Log final summary with elapsed time."""
    if quiet:
        return
    elapsed = time.perf_counter() - start_time
    print(
        f"  → {datasets} datasets, {variables} variables in {elapsed:.1f}s",
        file=sys.stderr,
    )
