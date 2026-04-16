"""Tests for scanner/autotag.py."""

from __future__ import annotations

import ibis
import pyarrow as pa

from datannurpy.scanner.autotag import (
    _detect_generic,
    _detect_specific,
    _is_natural_text,
    _is_secret,
    compute_auto_tags,
    ensure_auto_tags,
)


class TestDetectSpecific:
    def test_bcrypt(self):
        values = ["$2a$10$N9qo8uLOickgx2ZMRZoMye"] * 10
        assert _detect_specific(values) == "auto---bcrypt"

    def test_bcrypt_2b(self):
        values = ["$2b$12$LJ3m4ys2Xq2sN.GBMCvRuO"] * 10
        assert _detect_specific(values) == "auto---bcrypt"

    def test_argon2(self):
        values = ["$argon2id$v=19$m=65536,t=3,p=4$c29tZXNhbHQ"] * 10
        assert _detect_specific(values) == "auto---argon2"

    def test_jwt(self):
        values = ["eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.Abc123Xyz789"] * 10
        assert _detect_specific(values) == "auto---jwt"

    def test_jwt_short_rejected(self):
        values = ["a.b.c"] * 10
        assert _detect_specific(values) != "auto---jwt"

    def test_uuid(self):
        values = ["550e8400-e29b-41d4-a716-446655440000"] * 10
        assert _detect_specific(values) == "auto---uuid"

    def test_email(self):
        values = ["user@example.com"] * 10
        assert _detect_specific(values) == "auto---email"

    def test_phone_international(self):
        values = ["+41 79 123 45 67"] * 10
        assert _detect_specific(values) == "auto---phone"

    def test_phone_local_with_separator(self):
        values = ["079 123 45 67"] * 10
        assert _detect_specific(values) == "auto---phone"

    def test_phone_no_separator_rejected(self):
        """Bare digits without + or separators should not match phone."""
        values = ["0791234567"] * 10
        assert _detect_specific(values) != "auto---phone"

    def test_iban(self):
        values = ["CH9300762011623852957"] * 10
        assert _detect_specific(values) == "auto---iban"

    def test_below_threshold(self):
        values = ["user@example.com"] * 7 + ["not-email"] * 3
        assert _detect_specific(values) is None

    def test_phone_lower_threshold(self):
        values = ["+41 79 123 45 67"] * 6 + ["not-phone"] * 4
        assert _detect_specific(values) == "auto---phone"

    def test_no_match(self):
        values = ["hello", "world", "foo", "bar", "baz"]
        assert _detect_specific(values) is None


class TestDetectGeneric:
    def test_secret(self):
        values = [f"a1b2c3d4e5f6g7h8i9j0k1l2m3_{i}" for i in range(20)]
        assert _detect_generic(values) == "auto---secret"

    def test_natural_text(self):
        values = ["This is a full sentence with many words"] * 10
        assert _detect_generic(values) == "auto---natural-text"

    def test_no_match(self):
        values = ["ab", "cd", "ef", "gh", "ij"]
        assert _detect_generic(values) is None


class TestIsSecret:
    def test_few_values_long_no_spaces(self):
        assert _is_secret(["abc123def456ghi789jkl0"]) is True

    def test_few_values_short(self):
        assert _is_secret(["short"]) is False

    def test_md5_hash(self):
        assert _is_secret(["d41d8cd98f00b204e9800998ecf8427e"]) is True

    def test_sha256_hash(self):
        hashes = [f"e3b0c44298fc1c149afbf4c8996fb924{i:032d}" for i in range(10)]
        assert _is_secret(hashes) is True

    def test_api_key(self):
        assert _is_secret(["AKIAIOSFODNN7EXAMPLEKEY"]) is True

    def test_unix_path_rejected(self):
        assert _is_secret(["/usr/local/share/applications/foo.desktop"]) is False

    def test_url_rejected(self):
        assert (
            _is_secret(["https://cdn.example.com/assets/images/header-bg.png"]) is False
        )

    def test_short_strings(self):
        values = [f"short_{i}" for i in range(20)]
        assert _is_secret(values) is False

    def test_with_spaces(self):
        values = [f"this has spaces and is long enough {i}" for i in range(20)]
        assert _is_secret(values) is False

    def test_low_uniqueness(self):
        values = ["abc123def456ghi789jkl0"] * 20
        assert _is_secret(values) is False


class TestIsNaturalText:
    def test_too_few_values(self):
        assert _is_natural_text(["hello world foo bar"]) is False

    def test_single_words(self):
        values = ["hello", "world", "foo", "bar", "baz"]
        assert _is_natural_text(values) is False

    def test_no_spaces(self):
        values = ["helloworld", "foobarBaz", "abcdefghi", "xyz12345", "testvalue"]
        assert _is_natural_text(values) is False


class TestComputeAutoTags:
    def test_email_column(self):
        table = ibis.memtable({"email": ["a@b.com", "c@d.org"] * 50})
        result = compute_auto_tags(table, ["email"])
        assert result == {"email": "auto---email"}

    def test_empty_string_cols(self):
        table = ibis.memtable({"num": [1, 2, 3]})
        result = compute_auto_tags(table, [])
        assert result == {}

    def test_all_null_column(self):
        arrow = pa.table({"col": pa.array([None, None, None], type=pa.string())})
        table = ibis.memtable(arrow)
        result = compute_auto_tags(table, ["col"])
        assert result == {}

    def test_no_match_no_tag(self):
        values = [f"x{i}" for i in range(100)]
        table = ibis.memtable({"col": values})
        result = compute_auto_tags(table, ["col"])
        assert result == {}

    def test_multiple_columns(self):
        table = ibis.memtable(
            {
                "email": ["a@b.com"] * 100,
                "name": ["hello world this is text"] * 100,
            }
        )
        result = compute_auto_tags(table, ["email", "name"])
        assert result["email"] == "auto---email"
        assert result["name"] == "auto---natural-text"


class TestEnsureAutoTags:
    def test_creates_all_tags(self):
        from datannurpy import Catalog

        catalog = Catalog()
        ensure_auto_tags(catalog)
        assert catalog.tag.get("auto") is not None
        assert catalog.tag.get("auto---format") is not None
        assert catalog.tag.get("auto---security") is not None
        assert catalog.tag.get("auto---text") is not None
        assert catalog.tag.get("auto---email") is not None
        assert catalog.tag.get("auto---bcrypt") is not None
        assert catalog.tag.get("auto---structured") is not None

    def test_hierarchy(self):
        from datannurpy import Catalog

        catalog = Catalog()
        ensure_auto_tags(catalog)
        tag_auto = catalog.tag.get("auto")
        tag_format = catalog.tag.get("auto---format")
        tag_email = catalog.tag.get("auto---email")
        tag_bcrypt = catalog.tag.get("auto---bcrypt")
        tag_structured = catalog.tag.get("auto---structured")
        assert tag_auto is not None and tag_auto.parent_id is None
        assert tag_format is not None and tag_format.parent_id == "auto"
        assert tag_email is not None and tag_email.parent_id == "auto---format"
        assert tag_bcrypt is not None and tag_bcrypt.parent_id == "auto---security"
        assert tag_structured is not None and tag_structured.parent_id == "auto---text"

    def test_idempotent(self):
        from datannurpy import Catalog

        catalog = Catalog()
        ensure_auto_tags(catalog)
        ensure_auto_tags(catalog)
        assert catalog.tag.get("auto") is not None
