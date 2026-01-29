"""Tests for Catalog class."""

from __future__ import annotations

from datannurpy import Catalog


class TestCatalogRepr:
    """Test Catalog __repr__ method."""

    def test_repr(self):
        """Catalog repr should show counts."""
        catalog = Catalog()
        result = repr(catalog)
        assert "Catalog(" in result
        assert "folders=0" in result
        assert "datasets=0" in result
        assert "variables=0" in result
        assert "modalities=0" in result
        assert "values=0" in result
        assert "institutions=0" in result
        assert "tags=0" in result
        assert "docs=0" in result
