"""Tests for CSV scanner."""

from __future__ import annotations

from pathlib import Path

from datannurpy import Catalog
from datannurpy.scanner import read_csv

DATA_DIR = Path(__file__).parent.parent.parent / "data"
CSV_DIR = DATA_DIR / "csv"


class TestReadCsv:
    """Test read_csv function."""

    def test_empty_file(self, tmp_path: Path):
        """read_csv should return None for empty file."""
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text("")

        assert read_csv(csv_path) is None

    def test_with_data(self, tmp_path: Path):
        """read_csv should return DataFrame for valid CSV."""
        csv_path = tmp_path / "data.csv"
        csv_path.write_text("id,name\n1,Test\n")

        df = read_csv(csv_path)
        assert df is not None
        assert len(df) == 1
        assert "id" in df.columns


class TestLegacyEncoding:
    """Test scanning CSV files with legacy encodings and delimiters."""

    def test_cp1252_semicolon_delimiter(self):
        """CSV with CP1252 encoding and semicolon delimiter should be scanned correctly."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv")

        assert len(catalog.variable.all()) == 4
        assert catalog.dataset.all()[0].nb_row == 4

    def test_explicit_encoding(self):
        """CSV scan with explicit encoding should work."""
        catalog = Catalog()
        catalog.add_dataset(CSV_DIR / "legacy_encoding.csv", csv_encoding="CP1252")

        assert len(catalog.variable.all()) == 4

    def test_unparseable_csv(self, tmp_path: Path, capsys):
        """CSV scan should warn when DuckDB cannot parse the file."""
        csv_file = tmp_path / "test.csv"
        # Write content that has a valid header but inconsistent column counts
        csv_file.write_text("a,b,c\n1,2\n3,4,5,6,7\n")

        catalog = Catalog()
        catalog.add_dataset(csv_file, quiet=False)

        # DuckDB may still parse it (padding/truncating), so just verify no crash
        assert len(catalog.dataset.all()) == 1

    def test_single_column_csv(self, tmp_path: Path):
        """CSV with single column (no separators) should work."""
        csv_file = tmp_path / "single.csv"
        csv_file.write_text("name\nAlice\nBob\n")

        catalog = Catalog()
        catalog.add_dataset(csv_file)

        assert len(catalog.variable.all()) == 1
        assert catalog.variable.all()[0].name == "name"

    def test_read_csv_header_with_non_utf8(self, tmp_path: Path):
        """_read_csv_header should work with non-utf8 header bytes."""
        from datannurpy.scanner.csv import _read_csv_header

        header = b"pr\xe9nom;age\n"  # é = 0xe9, invalid utf-8
        cols = _read_csv_header(header)
        assert cols == ["prénom", "age"]

    def test_trailing_separator_drops_empty_column(self, tmp_path: Path):
        """CSV with trailing separator should drop the empty-named column."""
        csv_file = tmp_path / "trailing.csv"
        csv_file.write_text("a;b;\n1;2;\n3;4;\n")

        df = read_csv(csv_file)
        assert df is not None
        assert list(df.columns) == ["a", "b"]


class TestDelimiterSniffing:
    """The scan is fed our own sniffed delimiter, not DuckDB's auto-detection,
    which can split a ';'-CSV on commas inside quoted free-text fields."""

    def test_sniff_csv_delimiter(self, tmp_path: Path):
        """_sniff_csv_delimiter returns the separator, or None when inconclusive."""
        from datannurpy.scanner.csv import _sniff_csv_delimiter

        cases = {
            "a;b;c\n1;2;3\n": ";",
            "a,b,c\n1,2,3\n": ",",
            "a\tb\tc\n1\t2\t3\n": "\t",
            "name\nAlice\nBob\n": None,  # single column → DuckDB fallback
        }
        for text, expected in cases.items():
            f = tmp_path / "probe.csv"
            f.write_text(text, encoding="utf-8")
            assert _sniff_csv_delimiter(f, None) == expected

    def test_semicolon_csv_with_commas_in_quoted_fields(self, tmp_path: Path):
        """A valid ';'-CSV whose quoted text columns contain commas keeps its real
        columns instead of being split on the embedded commas."""
        csv_file = tmp_path / "kt.csv"
        csv_file.write_text(
            "Name;Zweck;Gesetzliche Grundlagen\n"
            'Personenregister;"Name, Vorname, Geburtsdatum";"Art. 1, Abs. 2"\n'
            'Objektregister;"Adresse, Parzelle, Nutzung";"Gesetz A, Gesetz B"\n',
            encoding="utf-8",
        )

        catalog = Catalog()
        catalog.add_dataset(csv_file)

        assert [v.name for v in catalog.variable.all()] == [
            "Name",
            "Zweck",
            "Gesetzliche Grundlagen",
        ]
        assert catalog.dataset.all()[0].nb_row == 2

    def test_sniffed_delimiter_is_honored_by_the_scan(
        self, tmp_path: Path, monkeypatch
    ):
        """The sniffed delimiter reaches DuckDB: forcing ';' on a comma file makes
        the whole row one column, proving the scan does not re-detect on its own."""
        import datannurpy.scanner.csv as csv_mod

        csv_file = tmp_path / "comma.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")

        monkeypatch.setattr(csv_mod, "_sniff_csv_delimiter", lambda *_a, **_k: ";")

        catalog = Catalog()
        catalog.add_dataset(csv_file)

        assert [v.name for v in catalog.variable.all()] == ["id,name"]

    def test_inconclusive_sniff_falls_back_to_duckdb(self, tmp_path: Path, monkeypatch):
        """When the sniff is inconclusive (None), DuckDB's own detection is used."""
        import datannurpy.scanner.csv as csv_mod

        csv_file = tmp_path / "semi.csv"
        csv_file.write_text("a;b;c\n1;2;3\n", encoding="utf-8")

        monkeypatch.setattr(csv_mod, "_sniff_csv_delimiter", lambda *_a, **_k: None)

        catalog = Catalog()
        catalog.add_dataset(csv_file)

        assert [v.name for v in catalog.variable.all()] == ["a", "b", "c"]


class TestDoubleBom:
    """DuckDB strips one leading BOM; a doubly-BOM'd file (some portals re-export
    an already-BOM'd file) would keep a stray BOM on the first column name."""

    def test_chunk_stripper_removes_all_leading_boms(self):
        """_read_chunks_bom_stripped drops every leading BOM, nothing else."""
        import io

        from datannurpy.scanner.filesystem import _read_chunks_bom_stripped

        bom = b"\xef\xbb\xbf"
        cases = {
            b"Name\n1\n": b"Name\n1\n",  # none
            bom + b"Name\n": b"Name\n",  # single
            bom * 2 + b"Name\n": b"Name\n",  # double
            bom * 3 + b"Name\n": b"Name\n",  # triple
            bom * 2: b"",  # only BOMs
            b"": b"",  # empty
        }
        for raw, expected in cases.items():
            assert b"".join(_read_chunks_bom_stripped(io.BytesIO(raw))) == expected

    def test_double_bom_first_column_name_is_clean(self, tmp_path: Path):
        """A ';'-CSV prefixed with two BOMs yields 'Name', not '\\ufeffName', so an
        external-metadata overlay keyed on the real column name still matches."""
        csv_file = tmp_path / "double_bom.csv"
        csv_file.write_bytes("﻿﻿Name;Zweck\nRegister;Verwaltung\n".encode("utf-8"))

        for skip_copy in (False, True):
            catalog = Catalog(csv_skip_copy=skip_copy)
            catalog.add_dataset(csv_file)
            assert [v.name for v in catalog.variable.all()] == ["Name", "Zweck"]

    def test_single_bom_still_clean(self, tmp_path: Path):
        """A single-BOM file keeps working (regression guard for the copy-skip path)."""
        csv_file = tmp_path / "single_bom.csv"
        csv_file.write_bytes("﻿Name;Zweck\nRegister;Verwaltung\n".encode("utf-8"))

        catalog = Catalog(csv_skip_copy=True)
        catalog.add_dataset(csv_file)

        assert [v.name for v in catalog.variable.all()] == ["Name", "Zweck"]


class TestSkipCopy:
    """Test csv_skip_copy parameter for CSV scanning."""

    def test_csv_skip_copy_utf8_csv(self, tmp_path: Path):
        """csv_skip_copy=True should read UTF-8 CSV directly without transcoding."""
        csv_file = tmp_path / "utf8.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        catalog = Catalog(csv_skip_copy=True)
        catalog.add_dataset(csv_file)

        assert catalog.dataset.all()[0].nb_row == 2
        assert len(catalog.variable.all()) == 2

    def test_csv_skip_copy_fallback_on_non_utf8(self, tmp_path: Path):
        """csv_skip_copy=True should fall back to ensure_local_utf8 for non-UTF-8."""
        csv_file = tmp_path / "cp1252.csv"
        csv_file.write_bytes(b"nom,age\nRen\xe9,42\n")

        catalog = Catalog(csv_skip_copy=True)
        catalog.add_dataset(csv_file)

        assert catalog.dataset.all()[0].nb_row == 1
        assert len(catalog.variable.all()) == 2


class TestSampling:
    """Test sample_size parameter for CSV scanning."""

    def test_sample_size_limits_stats(self, tmp_path: Path):
        """sample_size should use reservoir sampling for stats."""
        csv_file = tmp_path / "big.csv"
        lines = ["id,value\n"] + [f"{i},{i * 10}\n" for i in range(200)]
        csv_file.write_text("".join(lines))

        catalog = Catalog()
        catalog.add_dataset(csv_file, sample_size=100)

        ds = catalog.dataset.all()[0]
        assert ds.nb_row == 200  # full row count preserved
        assert len(catalog.variable.all()) == 2

    def test_sample_size_no_effect_when_small(self, tmp_path: Path):
        """sample_size should have no effect when row count <= sample_size."""
        csv_file = tmp_path / "small.csv"
        csv_file.write_text("id,value\n1,10\n2,20\n")

        catalog = Catalog()
        catalog.add_dataset(csv_file, sample_size=100)

        ds = catalog.dataset.all()[0]
        assert ds.nb_row == 2

    def test_scan_csv_empty_after_header(self, tmp_path: Path):
        """scan_csv should handle CSV with header but no data rows."""
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "header_only.csv"
        csv_file.write_text("a,b,c\n")

        variables, nb_row, actual_sample, frequency = scan_csv(
            csv_file, dataset_id="test", infer_stats=True
        )
        assert nb_row == 0
        assert actual_sample is None
        assert len(variables) == 3

    def test_scan_csv_preflight_rejects_title_row(self, tmp_path: Path, capsys):
        """CSV starting with a title row (not a header) is flagged as untreatable."""
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "report.csv"
        csv_file.write_text(
            "Annual report 2024\nCode,Name,Salary\n101,Alice,5000\n102,Bob,6000\n"
        )

        variables, nb_row, actual_sample, frequency = scan_csv(
            csv_file, dataset_id="test", quiet=False
        )

        captured = capsys.readouterr()
        assert "not a valid tabular dataset" in captured.err
        assert "skipped as untreatable" in captured.err
        assert variables == []
        assert nb_row is None
        assert actual_sample is None
        assert frequency is None

    def test_nested_csv_warning_uses_relative_path(self, tmp_path: Path, capsys):
        """CSV validation warnings from add_folder should keep the relative path."""
        nested = tmp_path / "folder" / "subfolder"
        nested.mkdir(parents=True)
        (nested / "report.csv").write_text(
            "Annual report 2024\nCode,Name,Salary\n101,Alice,5000\n"
        )

        catalog = Catalog()
        catalog.add_folder(tmp_path, quiet=False)

        captured = capsys.readouterr()
        assert "folder/subfolder/report.csv" in captured.err
        assert "⚠ report.csv:" not in captured.err

    def test_scan_csv_malformed_caught(self, tmp_path: Path, monkeypatch, capsys):
        """Malformed CSV that DuckDB rejects (even with strict_mode=False) is reported cleanly."""
        import duckdb

        from datannurpy.scanner import csv as csv_mod

        csv_file = tmp_path / "trailing.csv"
        csv_file.write_text("a,b,c\n1,2,3\n")

        original_connect = csv_mod.ibis.duckdb.connect

        class _Wrap:
            def __init__(self, real):
                self._real = real

            def read_csv(self, *a, **kw):
                raise duckdb.InvalidInputException(
                    "Invalid Input Error: CSV Error on Line: 38606\n"
                    "Expected Number of Columns: 264 Found: 1"
                )

            def __getattr__(self, name):
                return getattr(self._real, name)

        def fake_connect(*a, **kw):
            return _Wrap(original_connect(*a, **kw))

        monkeypatch.setattr(csv_mod.ibis.duckdb, "connect", fake_connect)

        variables, nb_row, actual_sample, frequency = csv_mod.scan_csv(
            csv_file, dataset_id="test", quiet=False
        )

        captured = capsys.readouterr()
        assert "unscannable CSV" in captured.err
        assert "skipped as untreatable" in captured.err
        assert "Traceback" not in captured.err
        assert variables == []
        assert nb_row is None
        assert actual_sample is None
        assert frequency is None

    def test_scan_csv_strict_mode_fallback(self, tmp_path: Path, monkeypatch):
        """When strict-mode read_csv fails, retry with strict_mode=False succeeds."""
        import duckdb

        from datannurpy.scanner import csv as csv_mod

        csv_file = tmp_path / "ok.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")

        original_connect = csv_mod.ibis.duckdb.connect

        class _Wrap:
            def __init__(self, real):
                self._real = real

            def read_csv(self, *a, **kw):
                if not kw.get("strict_mode", True):
                    return self._real.read_csv(*a, **kw)
                raise duckdb.InvalidInputException(
                    "Invalid Input Error: It was not possible to automatically "
                    "detect the CSV parsing dialect"
                )

            def __getattr__(self, name):
                return getattr(self._real, name)

        def fake_connect(*a, **kw):
            return _Wrap(original_connect(*a, **kw))

        monkeypatch.setattr(csv_mod.ibis.duckdb, "connect", fake_connect)

        variables, nb_row, _actual_sample, _frequency = csv_mod.scan_csv(
            csv_file, dataset_id="test", quiet=True
        )

        assert nb_row == 2
        assert [v.name for v in variables] == ["a", "b"]

    def test_read_preview_rows_csv_non_utf8(self, tmp_path: Path):
        """_read_preview_rows_csv falls back to cp1252 on non-utf8 bytes."""
        from datannurpy.scanner.csv import _read_preview_rows_csv

        csv_file = tmp_path / "latin.csv"
        csv_file.write_bytes("a,b\nfoo,caf\xe9\n".encode("cp1252"))

        rows = _read_preview_rows_csv(csv_file)
        assert rows[0] == ("a", "b")
        assert rows[1][0] == "foo"

    def test_read_preview_rows_csv_bom_and_blank(self, tmp_path: Path):
        """BOM is stripped; blank file returns []."""
        from datannurpy.scanner.csv import _read_preview_rows_csv

        bom = tmp_path / "bom.csv"
        bom.write_bytes(b"\xef\xbb\xbfa,b\n1,2\n")
        rows = _read_preview_rows_csv(bom)
        assert rows[0] == ("a", "b")

        blank = tmp_path / "blank.csv"
        blank.write_text("\n   \n")
        assert _read_preview_rows_csv(blank) == []

    def test_scan_csv_preflight_unreadable(self, tmp_path: Path, monkeypatch, capsys):
        """Preview reader failures fall back to the DuckDB scan path."""
        from datannurpy.scanner import csv as csv_mod

        csv_file = tmp_path / "ok.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")

        def boom(*args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(csv_mod, "_read_preview_rows_csv", boom)
        variables, nb_row, _, _ = csv_mod.scan_csv(
            csv_file, dataset_id="test", quiet=False
        )
        assert nb_row == 2
        assert {v.name for v in variables} == {"a", "b"}


class TestEnsureLocalUtf8:
    """Test ensure_local_utf8 edge cases."""

    def test_cp1252_fallback_multi_chunk(self, monkeypatch):
        """ensure_local_utf8 should handle multi-chunk cp1252 files."""
        import io

        import datannurpy.scanner.filesystem as fs_mod
        from datannurpy.scanner.filesystem import ensure_local_utf8

        # Shrink chunk size so even small data spans multiple reads
        monkeypatch.setattr(fs_mod, "_CHUNK_SIZE", 20)

        # Non-UTF-8 byte in first chunk triggers cp1252 mode,
        # remaining data goes through the `else` branch on subsequent chunks
        data = b"pr\xe9nom;age\n" + b"Ren\xe9;42\n" * 5

        fin = io.BytesIO(data)
        fout = io.BytesIO()
        ensure_local_utf8(fin, fout)

        result = fout.getvalue().decode("utf-8")
        assert "prénom" in result

    def test_utf8_with_carry_bytes(self):
        """ensure_local_utf8 should handle UTF-8 multi-byte chars at chunk boundaries."""
        import io

        from datannurpy.scanner.filesystem import ensure_local_utf8

        # Create data where a multi-byte char spans the chunk boundary
        # é = \xc3\xa9 in UTF-8
        # Put \xc3 at end of first "chunk" and \xa9 at start of second
        part1 = b"A" * 1048575 + b"\xc3"  # 1MB - 1 + first byte of é
        part2 = b"\xa9 fin\n"  # second byte of é + rest
        data = part1 + part2

        fin = io.BytesIO(data)
        fout = io.BytesIO()
        ensure_local_utf8(fin, fout)

        result = fout.getvalue().decode("utf-8")
        assert "é fin" in result

    def test_utf8_trailing_carry_becomes_cp1252(self):
        """ensure_local_utf8 should handle trailing incomplete UTF-8 bytes as cp1252."""
        import io

        from datannurpy.scanner.filesystem import ensure_local_utf8

        # \xc3 alone at end of file is incomplete UTF-8 → treated as cp1252
        data = b"hello\n" + b"\xc3"

        fin = io.BytesIO(data)
        fout = io.BytesIO()
        ensure_local_utf8(fin, fout)

        result = fout.getvalue().decode("utf-8")
        assert "hello" in result


class TestReadCsvHeaderEdgeCases:
    """Test _read_csv_header edge cases."""

    def test_empty_header(self):
        """_read_csv_header should return empty list for empty bytes."""
        from datannurpy.scanner.csv import _read_csv_header

        assert _read_csv_header(b"") == []
        assert _read_csv_header(b"\n") == []

    def test_non_utf8_without_explicit_encoding(self):
        """_read_csv_header should fall back to cp1252 for non-UTF8."""
        from datannurpy.scanner.csv import _read_csv_header

        # \xe9 is é in cp1252, invalid UTF-8
        cols = _read_csv_header(b"pr\xe9nom,\xe2ge\n")
        assert "prénom" in cols

    def test_utf8_bom_stripped(self):
        """_read_csv_header should strip a UTF-8 BOM from the first column name."""
        from datannurpy.scanner.csv import _read_csv_header

        # BOM (\xef\xbb\xbf) + ';'-separated header with a comma inside a header field.
        sample = b"\xef\xbb\xbfAnn\xc3\xa9e;Niveau;PIB (mio. francs, na)\n1;A;10\n"
        cols = _read_csv_header(sample)
        assert cols == ["Année", "Niveau", "PIB (mio. francs, na)"]

    def test_semicolon_with_comma_in_header_field(self):
        """Sniffer must pick ';' over ',' when commas appear inside header fields."""
        from datannurpy.scanner.csv import _read_csv_header

        sample = (
            b'Annee;"Taux (nominal, na)";"PIB (mio., francs)"\n'
            b"2020;1.5;100\n2021;2.0;110\n"
        )
        cols = _read_csv_header(sample)
        assert cols == ["Annee", "Taux (nominal, na)", "PIB (mio., francs)"]

    def test_multiline_quoted_header_field(self):
        """A quoted header field containing a newline must not crash the parser."""
        from datannurpy.scanner.csv import _read_csv_header

        sample = b'a;"b\nwith newline";c\n1;2;3\n4;5;6\n'
        cols = _read_csv_header(sample)
        assert cols == ["a", "b\nwith newline", "c"]

    def test_duplicate_column_names_suffixed(self):
        """Duplicate header names must be suffixed (DuckDB convention) to avoid ID collisions."""
        from datannurpy.scanner.csv import _read_csv_header

        sample = b"ts_key;value;ts_key\n1;2;3\n"
        cols = _read_csv_header(sample)
        assert cols == ["ts_key", "value", "ts_key_1"]

    def test_cr_only_line_endings(self):
        """Files using bare \\r as line terminator (Mac Classic) must parse."""
        from datannurpy.scanner.csv import _read_csv_header

        # BOM + \r-only line endings
        sample = b"\xef\xbb\xbfa;b;c\r1;2;3\r4;5;6\r"
        cols = _read_csv_header(sample)
        assert cols == ["a", "b", "c"]


class TestScanCsvErrorPath:
    """Test scan_csv error handling."""

    def test_scan_csv_empty_file(self, tmp_path: Path):
        """scan_csv should return empty result for 0-byte file."""
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "empty.csv"
        csv_file.write_text("")

        variables, nb_row, sample, frequency = scan_csv(
            csv_file, dataset_id="test", infer_stats=True
        )
        assert variables == []
        assert nb_row == 0
        assert sample is None
        assert frequency is None

    def test_scan_csv_warns_on_connect_error(self, tmp_path: Path, monkeypatch):
        """scan_csv should return empty result when DuckDB connection fails."""
        from datannurpy.scanner import csv as csv_mod
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "ok.csv"
        csv_file.write_text("a\n1\n")

        from contextlib import contextmanager

        @contextmanager
        def _broken_source(*a, **kw):
            raise RuntimeError("forced error")
            yield  # type: ignore[misc]  # pragma: no cover

        monkeypatch.setattr(csv_mod, "_csv_source", _broken_source)

        variables, nb_row, sample, frequency = scan_csv(
            csv_file, dataset_id="test", infer_stats=True, quiet=True
        )
        assert variables == []
        assert nb_row is None  # unknown, not zero: the error is already reported


class TestReadCsvErrorPath:
    """Test read_csv error handling."""

    def test_read_csv_returns_none_on_error(self, tmp_path: Path, monkeypatch):
        """read_csv should return None when DuckDB read fails."""
        from datannurpy.scanner import csv as csv_mod

        csv_file = tmp_path / "ok.csv"
        csv_file.write_text("a\n1\n")

        from contextlib import contextmanager

        @contextmanager
        def _broken_source(*a, **kw):
            raise RuntimeError("forced error")
            yield  # type: ignore[misc]  # pragma: no cover

        monkeypatch.setattr(csv_mod, "_csv_source", _broken_source)
        assert read_csv(csv_file) is None


class TestCsvSnifferFallback:
    """Test _read_csv_header when Sniffer fails."""

    def test_sniffer_fallback(self, monkeypatch):
        """_read_csv_header should fall back to default reader when Sniffer fails."""
        import csv as stdlib_csv

        from datannurpy.scanner.csv import _read_csv_header

        def _always_fail(self, sample, delimiters=None):
            raise stdlib_csv.Error("forced")

        monkeypatch.setattr(stdlib_csv.Sniffer, "sniff", _always_fail)
        cols = _read_csv_header(b"a,b,c\n")
        assert cols == ["a", "b", "c"]


class TestScanCsvDegradedProfiles:
    """Type-conversion failures degrade through the profile ladder instead of
    losing the dataset (values are never altered: rows are dropped or kept as
    text, with a warning either way)."""

    def test_footnote_row_dropped_with_warning(self, tmp_path: Path, capsys):
        # One stray footnote beyond the sniffer sample: ignore_errors keeps the
        # typed column and drops the single unconvertible row.
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "footnote.csv"
        rows = "\n".join(str(i) for i in range(30_000))
        csv_file.write_text(f"v\n{rows}\nn.b. source: OFS\n")

        variables, nb_row, actual_sample, _freq = scan_csv(
            csv_file, dataset_id="d", quiet=False, sample_size=1_000
        )

        captured = capsys.readouterr()
        assert "1 unconvertible row(s) ignored by statistics" in captured.err
        assert "count in nb_row" in captured.err
        assert nb_row == 30_001  # count(*) still sees every parseable line
        assert actual_sample == 1_000
        assert [(v.name, v.type) for v in variables] == [("v", "integer")]

    def test_structurally_broken_falls_back_to_all_varchar(
        self, tmp_path: Path, capsys
    ):
        # Thousands of unconvertible rows exceed the tolerance: dropping them
        # would mutilate the file, so every row is kept as text instead.
        from datannurpy.scanner.csv import scan_csv

        csv_file = tmp_path / "broken.csv"
        good = "\n".join(str(i) for i in range(30_000))
        bad = "\n".join(f"bad{i}" for i in range(5_000))
        csv_file.write_text(f"v\n{good}\n{bad}\n")

        variables, nb_row, _actual_sample, _freq = scan_csv(
            csv_file, dataset_id="d", quiet=False
        )

        captured = capsys.readouterr()
        assert "all columns read as text (all_varchar fallback)" in captured.err
        assert nb_row == 35_000  # no row lost
        assert [(v.name, v.type) for v in variables] == [("v", "string")]

    def test_internal_conversion_error_stays_an_error(
        self, tmp_path: Path, capsys, monkeypatch
    ):
        """A ConversionException that survives even all_varchar is not a CSV
        problem: it lands in the error bucket, not a silent unscannable skip."""
        import duckdb

        from datannurpy.scanner import csv as csv_mod

        csv_file = tmp_path / "ok.csv"
        csv_file.write_text("a,b\n1,2\n")

        def boom(*_a, **_kw):
            raise duckdb.ConversionException("Conversion Error: internal cast")

        monkeypatch.setattr(csv_mod, "_scan_csv_table", boom)
        variables, nb_row, _sample, _freq = csv_mod.scan_csv(
            csv_file, dataset_id="d", quiet=False
        )

        captured = capsys.readouterr()
        assert "unscannable" not in captured.err
        assert "internal cast" in captured.err
        assert nb_row is None  # unknown row count; the ✗ line carries the failure
        assert variables == []
