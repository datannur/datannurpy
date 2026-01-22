"""Tests for prefix-based grouping."""

from __future__ import annotations

from datannurpy._prefix import get_prefix_folders, get_table_prefix


class TestGetPrefixFolders:
    """Tests for get_prefix_folders function."""

    def test_empty_list(self) -> None:
        """Should return empty list for empty input."""
        assert get_prefix_folders([]) == []

    def test_no_common_prefix(self) -> None:
        """Tables without common prefixes should not create folders."""
        tables = ["users", "orders", "products"]
        assert get_prefix_folders(tables) == []

    def test_single_prefix_group(self) -> None:
        """Tables sharing a prefix should be grouped."""
        tables = ["hr_employees", "hr_departments", "sales_orders"]
        result = get_prefix_folders(tables)
        assert len(result) == 1
        assert result[0].prefix == "hr"
        assert result[0].parent_prefix is None

    def test_multiple_prefix_groups(self) -> None:
        """Multiple prefix groups should be detected."""
        tables = [
            "hr_employees",
            "hr_departments",
            "sales_orders",
            "sales_customers",
            "products",
        ]
        result = get_prefix_folders(tables)
        prefixes = {pf.prefix for pf in result}
        assert prefixes == {"hr", "sales"}

    def test_nested_prefixes(self) -> None:
        """Nested prefixes should have correct parent."""
        tables = [
            "dim_product_category",
            "dim_product_brand",
            "dim_time_day",
            "dim_time_month",
            "fact_sales",
        ]
        result = get_prefix_folders(tables)
        prefixes = {pf.prefix for pf in result}
        # dim has 4 tables, dim_product has 2, dim_time has 2
        assert "dim" in prefixes
        assert "dim_product" in prefixes
        assert "dim_time" in prefixes

        # Check parent relationships
        prefix_map = {pf.prefix: pf for pf in result}
        assert prefix_map["dim"].parent_prefix is None
        assert prefix_map["dim_product"].parent_prefix == "dim"
        assert prefix_map["dim_time"].parent_prefix == "dim"

    def test_universal_prefix_excluded(self) -> None:
        """Prefix common to ALL tables should be excluded."""
        tables = ["app_users", "app_orders", "app_products"]
        result = get_prefix_folders(tables)
        # "app" is common to all 3, so it's excluded
        assert result == []

    def test_min_count_threshold(self) -> None:
        """Only prefixes with >= min_count tables should be included."""
        tables = ["hr_employees", "hr_departments", "sales_orders"]
        # Default min_count=2, so "sales" (1 table) is excluded
        result = get_prefix_folders(tables)
        assert len(result) == 1
        assert result[0].prefix == "hr"

    def test_custom_min_count(self) -> None:
        """Custom min_count should be respected."""
        tables = ["hr_emp", "hr_dept", "hr_payroll", "sales_orders", "sales_cust"]
        result = get_prefix_folders(tables, min_count=3)
        # Only "hr" has 3+ tables
        assert len(result) == 1
        assert result[0].prefix == "hr"

    def test_custom_separator(self) -> None:
        """Custom separator should be used."""
        tables = ["hr-employees", "hr-departments", "sales-orders"]
        result = get_prefix_folders(tables, sep="-")
        assert len(result) == 1
        assert result[0].prefix == "hr"


class TestGetTablePrefix:
    """Tests for get_table_prefix function."""

    def test_table_with_prefix(self) -> None:
        """Should return matching prefix for table."""
        valid = {"hr", "sales"}
        assert get_table_prefix("hr_employees", valid) == "hr"
        assert get_table_prefix("sales_orders", valid) == "sales"

    def test_table_without_prefix(self) -> None:
        """Should return None for table without matching prefix."""
        valid = {"hr", "sales"}
        assert get_table_prefix("products", valid) is None

    def test_nested_prefix_returns_most_specific(self) -> None:
        """Should return the most specific (longest) matching prefix."""
        valid = {"dim", "dim_product", "dim_time"}
        assert get_table_prefix("dim_product_category", valid) == "dim_product"
        assert get_table_prefix("dim_time_day", valid) == "dim_time"

    def test_empty_valid_prefixes(self) -> None:
        """Should return None when no valid prefixes."""
        assert get_table_prefix("hr_employees", set()) is None
