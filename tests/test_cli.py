"""Tests for CLI entry point."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from datannurpy.__main__ import main


def test_main_no_args() -> None:
    """main() without args shows usage and exits."""
    with patch.object(sys, "argv", ["datannurpy"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


@pytest.mark.parametrize("flag", ["-h", "--help"])
def test_main_help(flag: str, capsys) -> None:
    """main() with --help shows usage and exits 0."""
    with patch.object(sys, "argv", ["datannurpy", flag]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    assert "Usage:" in capsys.readouterr().out


@pytest.mark.parametrize("flag", ["-V", "--version"])
def test_main_version(flag: str, capsys) -> None:
    """main() with --version shows version and exits 0."""
    with patch("datannurpy.__main__.version", return_value="1.2.3"):
        with patch.object(sys, "argv", ["datannurpy", flag]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
    assert capsys.readouterr().out == "datannurpy 1.2.3\n"


def test_public_imports() -> None:
    """Package root exposes the documented public API."""
    from datannurpy import Catalog, ConfigError, Folder, __version__, run_config

    assert Catalog.__name__ == "Catalog"
    assert ConfigError.__name__ == "ConfigError"
    assert Folder.__name__ == "Folder"
    assert callable(run_config)
    assert __version__


def test_public_import_unknown_attribute() -> None:
    """Package root reports unknown lazy attributes normally."""
    import datannurpy

    with pytest.raises(AttributeError):
        datannurpy.__getattr__("missing")


def test_main_run_config(tmp_path: Path) -> None:
    """main() runs config file."""
    config = tmp_path / "test.yml"
    output = tmp_path / "output"
    data_path = Path(__file__).parent.parent / "data" / "csv"
    config.write_text(f"""
app_path: "{output}"
add:
  - type: folder
    path: "{data_path}"
""")
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        main()
    assert (output / "data" / "db" / "dataset.json").exists()


def test_main_invalid_config_type(tmp_path: Path, capsys) -> None:
    """main() shows clean error for invalid config type."""
    config = tmp_path / "test.yml"
    config.write_text("""
add:
  - type: invalid_type
    path: /some/path
""")
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    captured = capsys.readouterr()
    assert "Unknown type 'invalid_type'" in captured.err
    assert "Traceback" not in captured.err


def test_main_file_not_found(tmp_path: Path, capsys) -> None:
    """main() shows clean error for missing config file."""
    config = tmp_path / "nonexistent.yml"
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    captured = capsys.readouterr()
    assert "Error:" in captured.err
