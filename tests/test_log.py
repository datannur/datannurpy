"""Tests for logging utilities."""

import time

from datannurpy.utils.log import (
    log_done,
    log_error,
    log_folder,
    log_section,
    log_start,
    log_summary,
    log_warn,
)


def test_quiet_mode_produces_no_output(capsys):
    """All log functions with quiet=True should produce no output."""
    start = time.perf_counter()
    log_start("msg", quiet=True)
    log_done("msg", quiet=True)
    log_done("msg", quiet=True, start_time=start)
    log_warn("msg", quiet=True)
    log_section("method", "target", quiet=True)
    log_folder("name", quiet=True)
    log_summary(1, 1, quiet=True, start_time=start)
    log_error("file.xlsx", ValueError("bad"), quiet=True)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_log_error_output(capsys):
    """log_error should show type, first line only."""
    log_error("f.csv", ValueError("line1\nline2"), quiet=False)
    err = capsys.readouterr().err
    assert "✗ f.csv" in err
    assert "ValueError" in err
    assert "line1" in err
    assert "line2" not in err


def test_log_summary_with_errors(capsys):
    """log_summary should include error count when errors > 0."""
    log_summary(5, 10, quiet=False, start_time=time.perf_counter(), errors=2)
    err = capsys.readouterr().err
    assert "5 datasets" in err and "2 errors" in err


def test_log_summary_without_errors(capsys):
    """log_summary should not show errors when errors=0."""
    log_summary(5, 10, quiet=False, start_time=time.perf_counter())
    assert "errors" not in capsys.readouterr().err
