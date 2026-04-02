"""Tests for Excel dataset validation and scanning."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from datannurpy import Catalog
from datannurpy.scanner.excel import is_valid_excel_dataset


def _write_xlsx(path: Path, rows: list[list[object]]) -> None:
    """Write rows to an xlsx file using openpyxl."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    assert ws is not None
    for row in rows:
        ws.append(row)
    wb.save(path)


class TestIsValidExcelDataset:
    """Tests for is_valid_excel_dataset()."""

    def test_valid_dataset(self):
        rows = [("id", "name", "age"), (1, "Alice", 30), (2, "Bob", 25)]
        valid, reason = is_valid_excel_dataset(rows)
        assert valid
        assert reason == ""

    def test_empty_rows(self):
        valid, reason = is_valid_excel_dataset([])
        assert not valid
        assert reason == "empty sheet"

    def test_empty_header(self):
        valid, reason = is_valid_excel_dataset([()])
        assert not valid
        assert reason == "empty header row"

    def test_does_not_start_at_a1(self):
        rows = [(None, None, "Code", "Name")]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid
        assert reason == "header does not start at column A"

    def test_none_gap_in_header(self):
        rows = [("Code", None, "Name", "Salary")]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid
        assert reason == "empty cells in header row"

    def test_duplicate_column_names(self):
        rows = [("Total", "Count", "Total")]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid
        assert reason == "duplicate column names"

    def test_non_text_header_numbers(self):
        rows = [(2023, 2024, 2025), (100, 200, 150)]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid
        assert reason == "non-text values in header row"

    def test_non_text_header_mixed(self):
        rows = [("Code", 42, "Name")]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid
        assert reason == "non-text values in header row"

    def test_data_wider_than_header(self):
        rows = [
            ("Rapport",),
            ("Code", "Nom", "Dept", "Salaire"),
        ]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid
        assert reason == "data wider than header row"

    def test_data_same_width_ok(self):
        rows = [
            ("id", "name", "salary"),
            (1, "Alice", 5000),
            (2, "Bob", 4500),
        ]
        valid, reason = is_valid_excel_dataset(rows)
        assert valid

    def test_data_narrower_ok(self):
        rows = [
            ("id", "name", "salary"),
            (1, "Alice", None),
            (2, None, None),
        ]
        valid, reason = is_valid_excel_dataset(rows)
        assert valid

    def test_data_row_all_none_ignored(self):
        rows = [
            ("id", "name"),
            (None, None),
            (1, "Alice"),
        ]
        valid, reason = is_valid_excel_dataset(rows)
        assert valid

    def test_empty_tuple_row_ignored(self):
        rows = [
            ("id", "name"),
            (),
            (1, "Alice"),
        ]
        valid, reason = is_valid_excel_dataset(rows)
        assert valid

    def test_title_then_wider_data(self):
        """Title in row 1, real header wider → detected by criterion 5."""
        rows = [
            ("Rapport 2024", None, None),
            ("Code", "Nom", "Salaire"),
            (101, "Ventes", 5000),
        ]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid
        assert reason == "data wider than header row"

    def test_merged_cells_produce_none(self):
        """Merged cells in openpyxl read-only produce None → detected."""
        rows = [("Total", None, "Total", None)]
        valid, reason = is_valid_excel_dataset(rows)
        assert not valid

    def test_single_column_dataset(self):
        rows = [("id",), (1,), (2,)]
        valid, reason = is_valid_excel_dataset(rows)
        assert valid

    def test_header_only_no_data(self):
        rows = [("id", "name", "salary")]
        valid, reason = is_valid_excel_dataset(rows)
        assert valid


class TestScanExcelValidation:
    """Integration tests: invalid Excel files are skipped in scan."""

    def test_xlsx_with_title_row_skipped(self, tmp_path: Path, capsys):
        """xlsx with a title row should be skipped."""
        _write_xlsx(
            tmp_path / "report.xlsx",
            [
                ["Rapport annuel 2024", None, None],
                ["Code", "Nom", "Salaire"],
                [101, "Alice", 5000],
            ],
        )

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "skipped (not a raw dataset" in captured.err
        assert len(catalog.variable.all()) == 0

    def test_xlsx_with_numeric_header_skipped(self, tmp_path: Path, capsys):
        """xlsx with numbers in header should be skipped."""
        _write_xlsx(
            tmp_path / "pivot.xlsx",
            [
                [2023, 2024, 2025],
                [100, 200, 150],
            ],
        )

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "skipped (not a raw dataset" in captured.err
        assert len(catalog.variable.all()) == 0

    def test_xlsx_with_duplicate_headers_skipped(self, tmp_path: Path, capsys):
        """xlsx with duplicate column names should be skipped."""
        _write_xlsx(
            tmp_path / "dupes.xlsx",
            [
                ["Total", "Count", "Total"],
                [100, 5, 200],
            ],
        )

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "skipped (not a raw dataset" in captured.err
        assert len(catalog.variable.all()) == 0

    def test_xlsx_valid_dataset_scanned(self, tmp_path: Path):
        """Valid xlsx should be scanned normally."""
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]})
        df.to_excel(tmp_path / "valid.xlsx", index=False)

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.dataset.all()) == 1
        assert len(catalog.variable.all()) == 3

    def test_xlsx_data_wider_than_header_skipped(self, tmp_path: Path, capsys):
        """xlsx where data extends beyond header width should be skipped."""
        _write_xlsx(
            tmp_path / "wider.xlsx",
            [
                ["Titre"],
                ["Code", "Nom", "Salaire"],
                [101, "Alice", 5000],
            ],
        )

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "skipped (not a raw dataset" in captured.err
        assert len(catalog.variable.all()) == 0

    def test_schema_mode_xlsx_invalid_skipped(self, tmp_path: Path):
        """Invalid xlsx should return no variables in schema mode."""
        _write_xlsx(
            tmp_path / "pivot.xlsx",
            [
                [2023, 2024, 2025],
                [100, 200, 150],
            ],
        )

        catalog = Catalog(depth="schema")
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.variable.all()) == 0

    def test_schema_mode_xlsx_valid_scanned(self, tmp_path: Path):
        """Valid xlsx should return variables in schema mode."""
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        df.to_excel(tmp_path / "valid.xlsx", index=False)

        catalog = Catalog(depth="schema")
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.variable.all()) == 2

    def test_xls_empty_file(self, tmp_path: Path):
        """Empty .xls file should produce no variables."""
        (tmp_path / "empty.xls").write_bytes(b"")

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.variable.all()) == 0

    def test_xls_corrupted_file(self, tmp_path: Path, capsys):
        """Corrupted .xls file should warn and produce no variables."""
        (tmp_path / "bad.xls").write_bytes(b"not a real xls file")

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "bad.xls" in captured.err
        assert len(catalog.variable.all()) == 0

    def test_xls_invalid_header_skipped(self, tmp_path: Path, monkeypatch):
        """xls with numeric columns should be skipped via post-read validation."""
        from datannurpy.scanner import excel as excel_mod

        xls_path = tmp_path / "pivot.xls"
        xls_path.write_bytes(b"dummy")

        numeric_df = pd.DataFrame({2023: [100], 2024: [200]})
        monkeypatch.setattr(excel_mod, "read_excel", lambda *_a, **_kw: numeric_df)

        vars_, count, freq = excel_mod.scan_excel(
            xls_path, dataset_id="test---pivot_xls"
        )
        assert vars_ == []
        assert count == 0
        assert freq is None

    def test_xls_empty_sheet(self, tmp_path: Path, monkeypatch):
        """xls with empty sheet should return None from read_excel."""
        from datannurpy.scanner.excel import read_excel

        xls_path = tmp_path / "empty_sheet.xls"
        xls_path.write_bytes(b"x" * 10)

        monkeypatch.setattr(pd, "read_excel", lambda *_a, **_kw: pd.DataFrame())

        result = read_excel(xls_path)
        assert result is None

    def test_schema_mode_xls_invalid_skipped(self, tmp_path: Path, monkeypatch):
        """Schema-only scan of .xls with invalid header returns no variables."""
        xls_path = tmp_path / "pivot.xls"
        xls_path.write_bytes(b"dummy")

        numeric_df = pd.DataFrame({2023: [100], 2024: [200]})
        monkeypatch.setattr(pd, "read_excel", lambda *_a, **_kw: numeric_df)

        catalog = Catalog(depth="schema")
        catalog.add_folder(tmp_path, quiet=True)

        assert len(catalog.variable.all()) == 0


class TestScanExcelSchemaRemote:
    """Test remote schema-only Excel validation."""

    def test_remote_xlsx_invalid_skipped(self, tmp_path: Path):
        """Remote invalid xlsx should return no variables in schema mode."""
        from io import BytesIO
        from unittest.mock import MagicMock

        import openpyxl

        from datannurpy.scanner.scan import scan_file

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append([2023, 2024, 2025])
        ws.append([100, 200, 150])
        buf = BytesIO()
        wb.save(buf)
        buf.seek(0)

        mock_fs = MagicMock()
        mock_fs.is_local = False
        mock_fs.open.return_value.__enter__ = MagicMock(return_value=buf)
        mock_fs.open.return_value.__exit__ = MagicMock(return_value=None)

        result = scan_file(
            Path("/remote/pivot.xlsx"),
            "excel",
            dataset_id="test---pivot_xlsx",
            schema_only=True,
            fs=mock_fs,
        )

        assert len(result.variables) == 0
        assert result.nb_row is None
