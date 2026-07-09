"""Tests for logging utilities."""

from __future__ import annotations

import time

from datannurpy.utils.log import (
    _reconfigure_utf8,
    configure_logging,
    log_debug,
    log_done,
    log_error,
    log_folder,
    log_run_summary,
    log_section,
    log_skip,
    log_start,
    log_summary,
    log_warn,
)


class _FakeStream:
    """Minimal text-stream stub recording reconfigure() calls."""

    def __init__(
        self, encoding: str | None = "cp1252", *, has_reconfigure=True, raises=None
    ):
        self.encoding = encoding
        self.calls: list[dict] = []
        self._raises = raises
        if not has_reconfigure:
            # Emulate a stream that isn't a reconfigurable TextIOWrapper.
            del self.reconfigure

    def reconfigure(self, **kwargs):
        if self._raises is not None:
            raise self._raises
        self.calls.append(kwargs)


class TestReconfigureUtf8:
    """A legacy-code-page stream is forced to UTF-8 so ✓/✗ never crash."""

    def test_cp1252_stream_is_reconfigured(self):
        stream = _FakeStream(encoding="cp1252")
        _reconfigure_utf8(stream)
        assert stream.calls == [{"encoding": "utf-8", "errors": "replace"}]

    def test_utf8_stream_is_left_alone(self):
        stream = _FakeStream(encoding="UTF-8")
        _reconfigure_utf8(stream)
        assert stream.calls == []

    def test_stream_without_reconfigure_is_ignored(self):
        # Object with no reconfigure attr (e.g. a pytest capture buffer).
        _reconfigure_utf8(object())

    def test_none_encoding_is_reconfigured(self):
        stream = _FakeStream(encoding=None)
        _reconfigure_utf8(stream)
        assert stream.calls == [{"encoding": "utf-8", "errors": "replace"}]

    def test_reconfigure_error_is_swallowed(self):
        stream = _FakeStream(encoding="cp1252", raises=ValueError("detached"))
        _reconfigure_utf8(stream)  # must not raise
        assert stream.calls == []


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
    assert "✗  f.csv" in err
    assert "ValueError" in err
    assert "line1" in err
    assert "line2" not in err


def test_log_summary_with_errors(capsys):
    """log_summary should include error count when errors > 0."""
    log_summary(5, 10, quiet=False, start_time=time.perf_counter(), errors=2)
    err = capsys.readouterr().err
    assert "5 scanned" in err and "2 errors" in err


def test_log_summary_without_errors(capsys):
    """log_summary should not show errors when errors=0."""
    log_summary(5, 10, quiet=False, start_time=time.perf_counter())
    assert "errors" not in capsys.readouterr().err


def test_log_summary_with_resource_count_and_no_variables(capsys):
    """log_summary should show resources first and omit variables when unset."""
    log_summary(
        13,
        None,
        quiet=False,
        start_time=time.perf_counter(),
        resource_count=141,
        resource_label="files",
    )
    err = capsys.readouterr().err
    assert "141 files" in err
    assert "13 scanned" in err
    assert "variables" not in err


def test_log_summary_breakdown_shows_scanned_and_unchanged(capsys):
    """A mixed incremental run shows both scanned and unchanged counts."""
    log_summary(
        3,
        45,
        quiet=False,
        start_time=time.perf_counter(),
        resource_count=12,
        resource_label="files",
        unchanged=9,
    )
    err = capsys.readouterr().err
    assert "3 scanned, 9 unchanged" in err
    assert "12 files" in err
    assert "45 variables" in err


def test_log_summary_omits_zero_counts(capsys):
    """Zero-valued counts are omitted; an all-unchanged run shows only unchanged."""
    log_summary(
        0,
        0,
        quiet=False,
        start_time=time.perf_counter(),
        resource_count=7,
        resource_label="files",
        unchanged=7,
    )
    err = capsys.readouterr().err
    assert "7 unchanged" in err
    assert "scanned" not in err  # 0 scanned is omitted
    assert "variables" not in err  # 0 variables is omitted


def test_log_run_summary_shows_totals_and_breakdown(capsys):
    """The run bilan reports catalogue totals plus this run's ventilation."""
    log_run_summary(3, 12, 45, quiet=False, scanned=9, unchanged=3, errors=2)
    err = capsys.readouterr().err
    assert "[summary] 3 folders, 12 datasets, 45 variables" in err
    assert "(9 datasets scanned, 3 unchanged, 2 errors this run)" in err


def test_log_run_summary_all_unchanged_keeps_real_totals(capsys):
    """An all-unchanged run still shows real totals — the point of the bilan."""
    log_run_summary(2, 5, 20, quiet=False, scanned=0, unchanged=5)
    err = capsys.readouterr().err
    assert "2 folders, 5 datasets, 20 variables" in err
    assert "(5 datasets unchanged this run)" in err
    assert "scanned" not in err  # 0 scanned is omitted


def test_log_run_summary_omits_breakdown_when_all_zero(capsys):
    """With no run activity, only the totals are shown (no empty parentheses)."""
    log_run_summary(1, 4, 10, quiet=False)
    err = capsys.readouterr().err
    assert "1 folders, 4 datasets, 10 variables" in err
    assert "(" not in err


def test_log_run_summary_quiet(capsys):
    """quiet=True suppresses the bilan."""
    log_run_summary(3, 12, 45, quiet=True, scanned=9)
    assert capsys.readouterr().err == ""


def test_log_summary_fresh_scan_omits_zero_unchanged(capsys):
    """A fresh scan (nothing skipped) stays terse: no '0 unchanged' noise."""
    log_summary(
        4,
        8,
        quiet=False,
        start_time=time.perf_counter(),
        resource_count=4,
        resource_label="files",
        unchanged=0,
    )
    err = capsys.readouterr().err
    assert "4 scanned" in err
    assert "8 variables" in err
    assert "unchanged" not in err


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
    assert "✗  f.csv" in err
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
    assert "✗  f.csv" in err
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


def test_log_file_no_nonetype_for_untraced_error(tmp_path):
    """An error logged outside an except block writes a clean line, no NoneType."""
    log_path = tmp_path / "errors.log"
    configure_logging(log_file=log_path)
    try:
        # Constructed but never raised, so it carries no traceback.
        log_error("add_folder", ValueError("Folder not found: /missing"), quiet=True)
    finally:
        configure_logging()

    content = log_path.read_text()
    assert "add_folder — ValueError: Folder not found: /missing" in content
    assert "NoneType: None" not in content
    assert "Traceback" not in content


def test_verbose_no_nonetype_for_untraced_error(capsys):
    """Verbose mode must not print 'NoneType: None' for an untraced error."""
    configure_logging(verbose=True)
    try:
        log_error("add_folder", ValueError("Folder not found: /missing"), quiet=False)
    finally:
        configure_logging(verbose=False)

    err = capsys.readouterr().err
    assert "✗  add_folder" in err
    assert "NoneType: None" not in err


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
    assert "📁  subdir" in content
    assert "✓  scanning file.csv" in content
    assert "⚠  col has nulls" in content
    assert "⏭  cached.csv (unchanged)" in content
    assert "2 scanned" in content
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
    assert "⚠  warning msg" in content
    assert "⏭  skipped.csv" in content
    assert "📁  dir" in content


def test_configure_logging_defaults(capsys):
    """Default configure_logging disables verbose and log_file."""
    configure_logging()
    try:
        raise ValueError("no trace")
    except Exception as exc:
        log_error("f.csv", exc, quiet=False)

    err = capsys.readouterr().err
    assert "✗  f.csv" in err
    assert "Traceback" not in err


def test_log_debug_silent_without_verbose(capsys, tmp_path):
    """log_debug is silent on stderr without verbose, but still written to log file."""
    log_path = tmp_path / "debug.log"
    configure_logging(verbose=False, log_file=log_path)
    try:
        log_debug("hidden detail", quiet=False)
    finally:
        configure_logging()

    assert capsys.readouterr().err == ""
    assert "·  hidden detail" in log_path.read_text()


def test_log_debug_visible_with_verbose(capsys):
    """log_debug prints to stderr when verbose is enabled."""
    configure_logging(verbose=True)
    try:
        log_debug("shown detail", quiet=False)
    finally:
        configure_logging(verbose=False)

    assert "·  shown detail" in capsys.readouterr().err


def test_log_icon_spacing_exact(capsys, tmp_path):
    """Leading log icons use one extra space after the prefix."""
    log_path = tmp_path / "spacing.log"
    configure_logging(verbose=True, log_file=log_path)
    try:
        start = log_start("loading.csv", quiet=False)
        log_done("done.csv", quiet=False, start_time=start)
        log_warn("warn.csv", quiet=False)
        log_skip("skip.csv", quiet=False)
        log_folder("folder", quiet=False)
        log_debug("debug.csv", quiet=False)
        log_summary(
            1,
            2,
            quiet=False,
            start_time=start,
            resource_count=3,
            resource_label="files",
        )
        try:
            raise ValueError("boom")
        except Exception as exc:
            log_error("error.csv", exc, quiet=False)
    finally:
        configure_logging()

    err = capsys.readouterr().err
    assert "  ⏳ loading.csv..." in err
    assert "\r  ✓  done.csv in " in err
    assert "\r  ⚠  warn.csv" in err
    assert "  ⏭  skip.csv (unchanged)" in err
    assert "\n  📁  folder" in err
    assert "\r  ·  debug.csv" in err
    assert "\n  →  3 files, 1 scanned, 2 variables in " in err
    assert "\r  ✗  error.csv — ValueError: boom" in err

    content = log_path.read_text()
    assert "  ✓  done.csv in " in content
    assert "  ⚠  warn.csv" in content
    assert "  ⏭  skip.csv (unchanged)" in content
    assert "\n  📁  folder" in content
    assert "  ·  debug.csv" in content
    assert "\n  →  3 files, 1 scanned, 2 variables in " in content
    assert "  ✗  error.csv — ValueError: boom" in content
