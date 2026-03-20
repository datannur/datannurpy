"""Tests for parameter validation with suggestions."""

from __future__ import annotations

import pytest

from datannurpy.catalog import Catalog
from datannurpy.utils.params import validate_params


class TestValidateParamsDecorator:
    """Test the validate_params decorator directly."""

    def test_unknown_param_with_suggestion(self) -> None:
        @validate_params
        def func(*, exclude: str = "") -> str:
            return exclude

        with pytest.raises(TypeError, match="Did you mean 'exclude'"):
            func(exclude_patterns="x")  # type: ignore[call-arg]

    def test_unknown_param_without_suggestion(self) -> None:
        @validate_params
        def func(*, name: str = "a") -> str:
            return name

        with pytest.raises(TypeError, match="unknown parameter 'zzz'") as exc_info:
            func(zzz="b")  # type: ignore[call-arg]
        assert "Did you mean" not in str(exc_info.value)

    def test_valid_params_pass_through(self) -> None:
        @validate_params
        def func(x: int, *, name: str = "a") -> str:
            return f"{x}:{name}"

        assert func(1, name="b") == "1:b"


class TestCatalogParamValidation:
    """Test that public API methods raise helpful errors on unknown params."""

    def test_catalog_init(self) -> None:
        with pytest.raises(TypeError, match="Did you mean 'quiet'"):
            Catalog(queit=True)  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "method,args,kwargs",
        [
            ("add_folder", (".",), {"exclude_patterns": ["*.tmp"]}),
            ("add_dataset", ("x.csv",), {"refreshh": True}),
            ("add_database", ("sqlite:///x.db",), {"schemas": "main"}),
            ("add_metadata", (".",), {"depht": "full"}),
            ("export_app", (), {"open_broser": True}),
            ("export_db", (), {"track_evol": True}),
        ],
    )
    def test_methods_reject_unknown_params(
        self, method: str, args: tuple[str, ...], kwargs: dict[str, object]
    ) -> None:
        c = Catalog(quiet=True)
        with pytest.raises(TypeError, match="unknown parameter"):
            getattr(c, method)(*args, **kwargs)
