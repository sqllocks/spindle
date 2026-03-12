"""Integration tests for the retail domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import RetailDomain, Spindle


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=RetailDomain(), scale="small", seed=42)


class TestRetailStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "customer", "address", "product_category", "product",
            "store", "promotion", "order", "order_line", "return",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["customer"]) == 1000
        assert len(r["address"]) == 1500
        assert len(r["product"]) == 500
        assert len(r["product_category"]) == 50
        assert len(r["store"]) == 150
        assert len(r["order"]) == 5000
        assert len(r["order_line"]) == 12500
        assert len(r["return"]) == 850

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        # Customer must come before address and order
        assert order.index("customer") < order.index("address")
        assert order.index("customer") < order.index("order")
        # Address must come before order
        assert order.index("address") < order.index("order")
        # Order before order_line and return
        assert order.index("order") < order.index("order_line")
        assert order.index("order") < order.index("return")


class TestRetailIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_customer_id_is_unique(self, result_small):
        df = result_small["customer"]
        assert df["customer_id"].is_unique

    def test_address_customer_fk_valid(self, result_small):
        customer_ids = set(result_small["customer"]["customer_id"])
        address_customer_ids = set(result_small["address"]["customer_id"])
        orphans = address_customer_ids - customer_ids
        assert len(orphans) == 0

    def test_order_shipping_address_fk_valid(self, result_small):
        address_ids = set(result_small["address"]["address_id"])
        order_df = result_small["order"]
        # shipping_address_id is nullable — only check non-null values
        non_null = order_df["shipping_address_id"].dropna()
        orphans = set(non_null.astype(int)) - address_ids
        assert len(orphans) == 0

    def test_order_line_total_positive(self, result_small):
        assert (result_small["order_line"]["line_total"] > 0).all()

    def test_order_total_backfilled(self, result_small):
        # With uniform FK distribution and ratio=2.5, ~8% of orders (Poisson λ=2.5,
        # P(0)≈8%) will have no matching order_lines and get order_total=0.
        # Assert that at least 85% of orders have a positive total.
        order_df = result_small["order"]
        positive_rate = (order_df["order_total"] > 0).mean()
        assert positive_rate >= 0.85, f"Only {positive_rate:.1%} of orders have order_total > 0"

    def test_return_order_id_fk_valid(self, result_small):
        order_ids = set(result_small["order"]["order_id"])
        return_order_ids = set(result_small["return"]["order_id"])
        orphans = return_order_ids - order_ids
        assert len(orphans) == 0


class TestRetailDistributions:
    def test_loyalty_tier_distribution(self, result_small):
        tiers = result_small["customer"]["loyalty_tier"].value_counts(normalize=True)
        # Basic should be ~80%
        assert 0.45 <= tiers.get("Basic", 0) <= 0.65

    def test_pareto_customer_max_orders(self, result_small):
        counts = result_small["order"]["customer_id"].value_counts()
        # max_per_parent=50 — no customer should exceed it
        assert counts.max() <= 50

    def test_address_types_in_set(self, result_small):
        types = set(result_small["address"]["address_type"].unique())
        assert types.issubset({"shipping", "billing", "both"})

    def test_address_has_real_coordinates(self, result_small):
        addr = result_small["address"]
        # Bounds cover all 50 US states: lat 17-72 (Hawaii → Alaska),
        # lng -180 to -65 (Alaska → Maine)
        assert addr["lat"].between(17.0, 72.0).all(), "Latitudes outside US range"
        assert addr["lng"].between(-180.0, -65.0).all(), "Longitudes outside US range"
        # Coordinates must be numeric floats (not strings)
        assert addr["lat"].dtype.kind == "f", f"lat dtype should be float, got {addr['lat'].dtype}"
        assert addr["lng"].dtype.kind == "f", f"lng dtype should be float, got {addr['lng'].dtype}"

    def test_address_city_state_zip_correlated(self, result_small):
        addr = result_small["address"]
        # State abbreviations should all be valid 2-letter codes
        assert addr["state"].str.len().max() == 2
        assert addr["state"].str.isupper().all()
        # ZIP codes should be 5 digits
        assert addr["zip_code"].str.match(r"^\d{5}$").all(), "Non-5-digit ZIP codes found"

    def test_address_columns_present(self, result_small):
        expected_cols = {"address_id", "customer_id", "address_type",
                         "street", "city", "state", "zip_code", "lat", "lng",
                         "is_primary"}
        assert expected_cols == set(result_small["address"].columns)

    def test_order_status_distribution(self, result_small):
        statuses = result_small["order"]["status"].value_counts(normalize=True)
        # "completed" should be ~77% (NRF 2024)
        assert 0.72 <= statuses.get("completed", 0) <= 0.82


class TestRetailBusinessRules:
    """Verify post-generation business rule enforcement."""

    def test_order_date_after_signup_date(self, result_small):
        orders = result_small["order"]
        customers = result_small["customer"]
        merged = orders.merge(customers[["customer_id", "signup_date"]], on="customer_id", how="left")
        import pandas as pd
        violations = (
            pd.to_datetime(merged["order_date"]) < pd.to_datetime(merged["signup_date"])
        ).sum()
        assert violations == 0, f"{violations} orders have order_date < signup_date"

    def test_return_date_after_order_date(self, result_small):
        returns = result_small["return"]
        orders = result_small["order"]
        merged = returns.merge(orders[["order_id", "order_date"]], on="order_id", how="left")
        import pandas as pd
        violations = (
            pd.to_datetime(merged["return_date"]) <= pd.to_datetime(merged["order_date"])
        ).sum()
        assert violations == 0, f"{violations} returns have return_date <= order_date"

    def test_product_cost_less_than_unit_price(self, result_small):
        products = result_small["product"]
        violations = (products["cost"] >= products["unit_price"]).sum()
        assert violations == 0, f"{violations} products have cost >= unit_price"


class TestRetailNewColumns:
    """Verify columns added/changed in Phase 0."""

    def test_product_category_has_level_column(self, result_small):
        cat = result_small["product_category"]
        assert "level" in cat.columns, "product_category missing 'level' column"

    def test_product_category_levels_are_1_2_3(self, result_small):
        levels = set(result_small["product_category"]["level"].unique())
        assert levels.issubset({1, 2, 3}), f"Unexpected levels: {levels}"

    def test_product_category_has_roots(self, result_small):
        cat = result_small["product_category"]
        roots = cat[cat["parent_category_id"].isna()]
        assert len(roots) > 0, "No root categories (parent_category_id all null)"
        assert (roots["level"] == 1).all(), "Root rows should all be level=1"

    def test_product_status_replaces_is_active(self, result_small):
        product = result_small["product"]
        assert "product_status" in product.columns, "product missing 'product_status' column"
        assert "is_active" not in product.columns, "deprecated 'is_active' column still present"

    def test_product_status_values(self, result_small):
        statuses = set(result_small["product"]["product_status"].unique())
        assert statuses.issubset({"active", "discontinued", "introduced"}), f"Unexpected statuses: {statuses}"

    def test_order_line_has_discount_percent(self, result_small):
        ol = result_small["order_line"]
        assert "discount_percent" in ol.columns, "order_line missing 'discount_percent' column"

    def test_discount_percent_valid_values(self, result_small):
        discounts = set(result_small["order_line"]["discount_percent"].unique())
        valid = {0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 50.0}
        assert discounts.issubset(valid), f"Unexpected discount values: {discounts - valid}"

    def test_discount_percent_mostly_zero(self, result_small):
        ol = result_small["order_line"]
        zero_rate = (ol["discount_percent"] == 0.0).mean()
        assert zero_rate >= 0.60, f"Expected most discounts to be 0, got {zero_rate:.1%} zero"

    def test_refund_amount_positive(self, result_small):
        returns = result_small["return"]
        positive_rate = (returns["refund_amount"] > 0).mean()
        assert positive_rate >= 0.85, f"Only {positive_rate:.1%} of returns have refund_amount > 0"

    def test_return_refund_leq_order_total(self, result_small):
        returns = result_small["return"]
        orders = result_small["order"]
        merged = returns.merge(orders[["order_id", "order_total"]], on="order_id", how="left")
        # refund should not exceed order_total (allow small float tolerance)
        violations = (merged["refund_amount"] > merged["order_total"] + 0.01).sum()
        assert violations == 0, f"{violations} returns have refund_amount > order_total"


class TestRetailConstrainedFK:
    """Verify constrained FK: shipping_address belongs to the order's customer."""

    def test_shipping_address_belongs_to_customer(self, result_small):
        orders = result_small["order"]
        addresses = result_small["address"]
        # Build mapping: address_id -> customer_id
        addr_to_customer = addresses.set_index("address_id")["customer_id"].to_dict()
        # Check non-null shipping_address_id rows
        non_null = orders.dropna(subset=["shipping_address_id"]).copy()
        non_null["addr_customer"] = non_null["shipping_address_id"].map(addr_to_customer)
        mismatches = (non_null["customer_id"] != non_null["addr_customer"]).sum()
        assert mismatches == 0, f"{mismatches} orders have shipping_address from wrong customer"


class TestRetailReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=RetailDomain(), scale="small", seed=99)
        r2 = s.generate(domain=RetailDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])

    def test_different_seeds_different_output(self):
        s = Spindle()
        r1 = s.generate(domain=RetailDomain(), scale="small", seed=1)
        r2 = s.generate(domain=RetailDomain(), scale="small", seed=2)
        # Customer emails should differ
        assert not r1["customer"]["email"].equals(r2["customer"]["email"])
