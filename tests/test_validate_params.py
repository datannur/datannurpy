"""Tests for parameter validation with suggestions."""

from __future__ import annotations

import pytest

from datannurpy.catalog import Catalog
from datannurpy.utils.params import validate_params


class TestValidateParamsDecorator:
    """Test the validate_params decorator directly."""

    def test_unknown_param_raises_type_error(self) -> None:
        @validate_params
        def func(*, name: str = "a") -> str:
            return name

        with pytest.raises(TypeError, match="unknown parameter 'nme'"):
            func(nme="b")  # type: ignore[call-arg]

    def test_close_match_suggests(self) -> None:
        @validate_params
        def func(*, exclude: str = "") -> str:
            return exclude

        with pytest.raises(TypeError, match="Did you mean 'exclude'"):
            func(exclude_patterns="x")  # type: ignore[call-arg]

    def test_no_suggestion_for_unrelated_param(self) -> None:
        @validate_params
        def func(*, name: str = "a") -> str:
            return name

        with pytest.raises(TypeError, match="unknown parameter 'zzz'") as exc_info:
            func(zzz="b")  # type: ignore[call-arg]
        assert "Did you mean" not in str(exc_info.value)

    def test_valid_params_pass_through(self) -> None:
        @validate_params
        def func(*, name: str = "a", value: int = 0) -> str:
            return f"{name}:{value}"

        assert func(name="b", value=1) == "b:1"

    def test_positional_args_unaffected(self) -> None:
        @validate_params
        def func(x: int, y: int, *, z: int = 0) -> int:
            return x + y + z

        assert func(1, 2, z=3) == 6


class TestCatalogParamValidation:
    """Test that public API methods raise helpful errors on unknown params."""

    def test_catalog_init_unknown_param(self) -> None:
        with pytest.raises(TypeError, match="unknown parameter 'queit'"):
            Catalog(queit=True)  # type: ignore[call-arg]

    def test_catalog_init_suggests_close_match(self) -> None:
        with pytest.raises(TypeError, match="Did you mean 'quiet'"):
            Catalog(queit=True)  # type: ignore[call-arg]

    def test_add_folder_unknown_param(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="unknown parameter 'exclude_patterns'"):
            c.add_folder(".", exclude_patterns=["*.tmp"])  # type: ignore[call-arg]

    def test_add_folder_suggests_exclude(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="Did you mean 'exclude'"):
            c.add_folder(".", exclude_patterns=["*.tmp"])  # type: ignore[call-arg]

    def test_add_dataset_unknown_param(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="unknown parameter 'refreshh'"):
            c.add_dataset("x.csv", refreshh=True)  # type: ignore[call-arg]

    def test_add_database_unknown_param(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="unknown parameter 'schemas'"):
            c.add_database("sqlite:///x.db", schemas="main")  # type: ignore[call-arg]

    def test_export_app_unknown_param(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="unknown parameter 'open_broser'"):
            c.export_app(open_broser=True)  # type: ignore[call-arg]

    def test_export_app_suggests_open_browser(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="Did you mean 'open_browser'"):
            c.export_app(open_broser=True)  # type: ignore[call-arg]

    def test_export_db_unknown_param(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="unknown parameter 'track_evol'"):
            c.export_db(track_evol=True)  # type: ignore[call-arg]

    def test_add_metadata_unknown_param(self) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="unknown parameter 'depht'"):
            c.add_metadata(".", depht="full")  # type: ignore[call-arg]
