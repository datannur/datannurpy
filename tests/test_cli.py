"""Tests for CLI entry point."""

from __future__ import annotations

import json
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
    # as_posix() so a Windows path's backslashes don't become YAML escapes.
    config.write_text(f"""
app_path: "{output.as_posix()}"
add:
  - type: folder
    path: "{data_path.as_posix()}"
""")
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        main()
    assert (output / "data" / "db" / "dataset.json").exists()


def _write_partial_scan_config(
    tmp_path: Path, *, on_scan_error: str | None = None
) -> tuple[Path, Path]:
    """Config scanning one good CSV + one corrupted parquet (a real scan error)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "good.csv").write_text("a,b\n1,2\n")
    (data_dir / "bad.parquet").write_bytes(b"not a real parquet file")
    output = tmp_path / "output"

    # as_posix() so a Windows path's backslashes don't become YAML escapes.
    lines = [f'output_dir: "{output.as_posix()}"']
    if on_scan_error is not None:
        lines.append(f'on_scan_error: "{on_scan_error}"')
    lines += ["add:", "  - type: folder", f'    path: "{data_dir.as_posix()}"']
    config = tmp_path / "test.yml"
    config.write_text("\n".join(lines) + "\n")
    return config, output


def test_main_partial_scan_default_tolerant(tmp_path: Path) -> None:
    """Default (on_scan_error='warn'): a partial scan still exits 0 with a valid catalogue."""
    config, output = _write_partial_scan_config(tmp_path)
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        main()  # no SystemExit — tolerant by default
    # The valid file is exported (continue-on-error), only the corrupted one dropped.
    datasets = json.loads((output / "dataset.json").read_text())
    assert len(datasets) == 1


def test_main_partial_scan_fail_exits_2(tmp_path: Path, capsys) -> None:
    """on_scan_error='fail': a partial scan exits 2 but still exports the valid catalogue."""
    config, output = _write_partial_scan_config(tmp_path, on_scan_error="fail")
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code == 2
    assert "failed to scan" in capsys.readouterr().err
    # Continue-on-error is preserved: the valid dataset is still exported.
    datasets = json.loads((output / "dataset.json").read_text())
    assert len(datasets) == 1


def test_main_invalid_on_scan_error(tmp_path: Path, capsys) -> None:
    """An invalid on_scan_error value is a ConfigError (exit 1)."""
    config, _ = _write_partial_scan_config(tmp_path, on_scan_error="bogus")
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    assert "on_scan_error must be 'warn' or 'fail'" in capsys.readouterr().err


def _write_metadata_config(
    tmp_path: Path, *, on_metadata_error: str | None = None
) -> tuple[Path, Path]:
    """Config with a valid tag.csv and a broken dataset.csv (missing id column)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "good.csv").write_text("a,b\n1,2\n")

    meta_dir = tmp_path / "metadata"
    meta_dir.mkdir()
    (meta_dir / "tag.csv").write_text("id,name\nt1,Tag One\n")
    # Missing the required 'id' column -> validation error for this table only.
    (meta_dir / "dataset.csv").write_text("name,description\nDS,No id here\n")

    output = tmp_path / "output"
    # as_posix() so a Windows path's backslashes don't become YAML escapes.
    lines = [
        f'app_path: "{output.as_posix()}"',
        f'metadata_path: "{meta_dir.as_posix()}"',
    ]
    if on_metadata_error is not None:
        lines.append(f'on_metadata_error: "{on_metadata_error}"')
    lines += ["add:", "  - type: folder", f'    path: "{data_dir.as_posix()}"']
    config = tmp_path / "test.yml"
    config.write_text("\n".join(lines) + "\n")
    return config, output


def test_main_invalid_metadata_default_tolerant(tmp_path: Path) -> None:
    """Default (on_metadata_error='warn'): a broken metadata table is skipped,
    valid tables still apply, and the run exits 0."""
    config, output = _write_metadata_config(tmp_path)
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        main()  # no SystemExit — tolerant by default
    tags = json.loads((output / "data" / "db" / "tag.json").read_text())
    assert any(t.get("id") == "t1" for t in tags)


def test_main_invalid_metadata_fail_exits_3(tmp_path: Path, capsys) -> None:
    """on_metadata_error='fail': a broken metadata table exits 3, but valid
    tables are still applied (continue-on-error preserved)."""
    config, output = _write_metadata_config(tmp_path, on_metadata_error="fail")
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        with pytest.raises(SystemExit) as exc_info:
            main()
    assert exc_info.value.code == 3
    assert "failed validation" in capsys.readouterr().err
    tags = json.loads((output / "data" / "db" / "tag.json").read_text())
    assert any(t.get("id") == "t1" for t in tags)


def test_main_invalid_on_metadata_error(tmp_path: Path, capsys) -> None:
    """An invalid on_metadata_error value is a ConfigError (exit 1)."""
    config, _ = _write_metadata_config(tmp_path, on_metadata_error="bogus")
    with patch.object(sys, "argv", ["datannurpy", str(config)]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1
    assert "on_metadata_error must be 'warn' or 'fail'" in capsys.readouterr().err


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
