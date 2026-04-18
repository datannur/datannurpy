"""Tests for logging utilities."""

import time

from datannurpy.utils.log import (
    configure_logging,
    log_done,
    log_error,
    log_folder,
    log_section,
    log_skip,
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


def test_verbose_shows_traceback(capsys):
    """log_error with verbose should print traceback to stderr."""
    configure_logging(verbose=True)
    try:
        try:
            raise ValueError("deep error")
        except Exception as exc:
            log_error("f.csv", exc, quiet=False)
    finally:
        configure_logging(verbose=False)

    err = capsys.readouterr().err
    assert "✗ f.csv" in err
    assert "Traceback" in err
    assert "deep error" in err


def test_verbose_shows_errors_even_when_quiet(capsys):
    """verbose=True + quiet=True should still show errors."""
    configure_logging(verbose=True)
    try:
        try:
            raise RuntimeError("hidden?")
        except Exception as exc:
            log_error("f.csv", exc, quiet=True)
    finally:
        configure_logging(verbose=False)

    err = capsys.readouterr().err
    assert "✗ f.csv" in err
    assert "Traceback" in err


def test_log_file_receives_tracebacks(tmp_path):
    """log_file should capture full tracebacks."""
    log_path = tmp_path / "errors.log"
    configure_logging(log_file=log_path)
    try:
        try:
            raise TypeError("type issue")
        except Exception as exc:
            log_error("data.csv", exc, quiet=True)
    finally:
        configure_logging()

    content = log_path.read_text()
    assert "data.csv" in content
    assert "TypeError" in content
    assert "Traceback" in content
    assert "type issue" in content


def test_log_file_truncated_each_run(tmp_path):
    """configure_logging should truncate log file on each call."""
    log_path = tmp_path / "errors.log"
    log_path.write_text("old content\n")

    configure_logging(log_file=log_path)
    configure_logging()  # reset

    assert log_path.read_text() == ""


def test_log_file_captures_all_levels(tmp_path):
    """log_file should capture output from all log functions."""
    log_path = tmp_path / "full.log"
    configure_logging(log_file=log_path)
    try:
        start = time.perf_counter()
        log_section("add_folder", "/data", quiet=False)
        log_folder("subdir", quiet=False)
        log_start("scanning file.csv", quiet=False)
        log_done("scanning file.csv", quiet=False, start_time=start)
        log_warn("col has nulls", quiet=False)
        log_skip("cached.csv", quiet=False)
        log_summary(2, 10, quiet=False, start_time=start, errors=1)
    finally:
        configure_logging()

    content = log_path.read_text()
    assert "[add_folder] /data" in content
    assert "📁 subdir" in content
    assert "✓ scanning file.csv" in content
    assert "⚠ col has nulls" in content
    assert "⏭ cached.csv (unchanged)" in content
    assert "2 datasets" in content
    assert "1 errors" in content


def test_log_file_captures_when_quiet(tmp_path):
    """log_file should capture output even when quiet=True."""
    log_path = tmp_path / "quiet.log"
    configure_logging(log_file=log_path)
    try:
        log_warn("warning msg", quiet=True)
        log_skip("skipped.csv", quiet=True)
        log_folder("dir", quiet=True)
    finally:
        configure_logging()

    content = log_path.read_text()
    assert "⚠ warning msg" in content
    assert "⏭ skipped.csv" in content
    assert "📁 dir" in content


def test_configure_logging_defaults(capsys):
    """Default configure_logging disables verbose and log_file."""
    configure_logging()
    try:
        raise ValueError("no trace")
    except Exception as exc:
        log_error("f.csv", exc, quiet=False)

    err = capsys.readouterr().err
    assert "✗ f.csv" in err
    assert "Traceback" not in err
