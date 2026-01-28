"""Tests for logging utilities."""

import time

from datannurpy.utils.log import (
    log_done,
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

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
