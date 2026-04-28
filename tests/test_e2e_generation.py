"""End-to-end generation tests for the Smart Schema Inference Engine.

These tests exercise the FULL pipeline: DDL → inference → data generation →
integrity verification. They validate that Spindle can parse arbitrary DDL,
infer smart strategies, and produce valid synthetic data — not just schemas.

INTERNAL TEST — not for documentation or publication.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.schema.ddl_parser import DdlParser
from sqllocks_spindle.schema.inference import SchemaInferenceEngine
from sqllocks_spindle.engine.generator import Spindle


FIXTURE = Path(__file__).parent / "fixtures" / "adventureworks_sample.sql"


@pytest.fixture(scope="module")
def aw_result():
    """Parse AdventureWorks DDL, infer strategies, and generate data."""
    raw = DdlParser().parse_file(FIXTURE)
    schema = SchemaInferenceEngine().infer(raw)
    spindle = Spindle()
    return spindle.generate(schema=schema, scale="small", seed=42)


# ---------------------------------------------------------------------------
# Core generation
# ---------------------------------------------------------------------------

class TestGenerationBasics:
    def test_all_tables_generated(self, aw_result):
        expected = {
            "persons", "addresses", "person_addresses",
            "product_categories", "products", "customers",
            "sales_orders", "order_details", "product_reviews",
            "inventory_log",
        }
        assert set(aw_result.tables.keys()) == expected

    def test_no_empty_tables(self, aw_result):
        for name, df in aw_result.tables.items():
            assert len(df) > 0, f"Table {name} is empty"

    def test_total_row_count(self, aw_result):
        total = sum(len(df) for df in aw_result.tables.values())
        assert total > 10_000, f"Expected >10K rows, got {total}"

    def test_generation_deterministic(self, aw_result):
        """Same seed produces same data."""
        raw = DdlParser().parse_file(FIXTURE)
        schema = SchemaInferenceEngine().infer(raw)
        result2 = Spindle().generate(schema=schema, scale="small", seed=42)
        for name in aw_result.tables:
            pd.testing.assert_frame_equal(aw_result.tables[name], result2.tables[name])


# ---------------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------------

class TestReferentialIntegrity:
    def test_no_integrity_errors(self, aw_result):
        errors = aw_result.verify_integrity()
        assert errors == [], f"Integrity errors: {errors}"

    def test_order_details_fk_orders(self, aw_result):
        parent_ids = set(aw_result["sales_orders"]["order_id"])
        child_ids = set(aw_result["order_details"]["order_id"])
        assert child_ids.issubset(parent_ids)

    def test_order_details_fk_products(self, aw_result):
        parent_ids = set(aw_result["products"]["product_id"])
        child_ids = set(aw_result["order_details"]["product_id"])
        assert child_ids.issubset(parent_ids)

    def test_customers_fk_persons(self, aw_result):
        parent_ids = set(aw_result["persons"]["person_id"])
        child_ids = set(aw_result["customers"]["person_id"])
        assert child_ids.issubset(parent_ids)

    def test_sales_orders_fk_customers(self, aw_result):
        parent_ids = set(aw_result["customers"]["customer_id"])
        child_ids = set(aw_result["sales_orders"]["customer_id"])
        assert child_ids.issubset(parent_ids)

    def test_person_addresses_fk_persons(self, aw_result):
        parent_ids = set(aw_result["persons"]["person_id"])
        child_ids = set(aw_result["person_addresses"]["person_id"])
        assert child_ids.issubset(parent_ids)

    def test_person_addresses_fk_addresses(self, aw_result):
        parent_ids = set(aw_result["addresses"]["address_id"])
        child_ids = set(aw_result["person_addresses"]["address_id"])
        assert child_ids.issubset(parent_ids)


# ---------------------------------------------------------------------------
# Column-level data quality
# ---------------------------------------------------------------------------

class TestColumnDataQuality:
    def test_pk_columns_unique(self, aw_result):
        """Single-column PKs must be unique."""
        single_pk_tables = [
            ("persons", "person_id"),
            ("addresses", "address_id"),
            ("products", "product_id"),
            ("customers", "customer_id"),
            ("sales_orders", "order_id"),
            ("order_details", "detail_id"),
            ("product_reviews", "review_id"),
            ("inventory_log", "log_id"),
            ("product_categories", "category_id"),
        ]
        for table, pk in single_pk_tables:
            df = aw_result[table]
            assert df[pk].is_unique, f"{table}.{pk} has duplicates"

    def test_composite_pk_columns_present(self, aw_result):
        """person_addresses has both FK columns from its composite PK."""
        df = aw_result["person_addresses"]
        assert "person_id" in df.columns
        assert "address_id" in df.columns
        assert len(df) > 0

    def test_not_null_columns(self, aw_result):
        """Columns declared NOT NULL should have no nulls."""
        checks = [
            ("persons", "first_name"),
            ("persons", "last_name"),
            ("products", "name"),
            ("products", "unit_price"),
            ("sales_orders", "customer_id"),
            ("sales_orders", "order_date"),
            ("order_details", "order_id"),
            ("order_details", "quantity"),
        ]
        for table, col in checks:
            df = aw_result[table]
            nulls = df[col].isna().sum()
            assert nulls == 0, f"{table}.{col} has {nulls} nulls"

    def test_monetary_columns_positive(self, aw_result):
        """Monetary columns should be non-negative."""
        checks = [
            ("products", "unit_price"),
            ("products", "unit_cost"),
            ("sales_orders", "subtotal"),
            ("sales_orders", "total_amount"),
        ]
        for table, col in checks:
            df = aw_result[table]
            assert (df[col] >= 0).all(), f"{table}.{col} has negative values"

    def test_rating_in_range(self, aw_result):
        df = aw_result["product_reviews"]
        assert (df["rating"] >= 1).all(), "rating below 1"
        assert (df["rating"] <= 5).all(), "rating above 5"


# ---------------------------------------------------------------------------
# Inference-driven strategy validation
# ---------------------------------------------------------------------------

class TestInferredStrategies:
    def test_order_status_has_multiple_values(self, aw_result):
        """Inferred weighted_enum should produce multiple distinct statuses."""
        statuses = aw_result["sales_orders"]["status"].unique()
        assert len(statuses) >= 3, f"Only {len(statuses)} status values"

    def test_payment_method_realistic(self, aw_result):
        """payment_method should have realistic values, not generic type_a/type_b."""
        values = set(aw_result["sales_orders"]["payment_method"].dropna().unique())
        # Should NOT have generic type_a/type_b placeholders
        assert "type_a" not in values
        assert "type_b" not in values

    def test_order_date_has_seasonal_variation(self, aw_result):
        """Seasonal inference should produce non-uniform month distribution."""
        dates = pd.to_datetime(aw_result["sales_orders"]["order_date"])
        months = dates.dt.month.value_counts(normalize=True)
        # December should be higher than average (1/12 ≈ 0.083)
        assert months.get(12, 0) > 0.085, "No December seasonal bias detected"

    def test_birth_dates_age_appropriate(self, aw_result):
        """Birth dates should produce adults (18-65 years old)."""
        dates = pd.to_datetime(aw_result["persons"]["birth_date"].dropna())
        years = dates.dt.year
        assert years.min() >= 1955, f"Birth year too old: {years.min()}"
        assert years.max() <= 2010, f"Birth year too young: {years.max()}"

    def test_fk_distribution_not_uniform(self, aw_result):
        """FK columns with pareto distribution should show skew."""
        customer_ids = aw_result["sales_orders"]["customer_id"]
        counts = customer_ids.value_counts()
        # Top customer should have more orders than bottom — Pareto skew
        assert counts.iloc[0] > counts.iloc[-1]

    def test_quantity_reasonable(self, aw_result):
        """Order quantity should be reasonable (not 0 or extremely large)."""
        qty = aw_result["order_details"]["quantity"]
        assert qty.min() >= 1, f"quantity min={qty.min()}"
        assert qty.max() <= 1000, f"quantity max={qty.max()}"


# ---------------------------------------------------------------------------
# Output format tests
# ---------------------------------------------------------------------------

class TestOutputFormats:
    def test_to_csv(self, aw_result, tmp_path):
        paths = aw_result.to_csv(tmp_path / "csv")
        assert len(paths) == 10
        for p in paths:
            assert p.exists()
            df = pd.read_csv(p)
            assert len(df) > 0

    def test_to_parquet(self, aw_result, tmp_path):
        paths = aw_result.to_parquet(tmp_path / "parquet")
        assert len(paths) == 10
        for p in paths:
            assert p.exists()
            df = pd.read_parquet(p)
            assert len(df) > 0

    def test_roundtrip_csv_integrity(self, aw_result, tmp_path):
        """CSV roundtrip should preserve row counts."""
        aw_result.to_csv(tmp_path / "rt")
        for name, df in aw_result.tables.items():
            loaded = pd.read_csv(tmp_path / "rt" / f"{name}.csv")
            assert len(loaded) == len(df), f"{name}: {len(loaded)} != {len(df)}"


# ---------------------------------------------------------------------------
# Phase 3B: enforce_correlations and fidelity_profile kwargs
# ---------------------------------------------------------------------------

class TestGeneratePhase3BIntegration:
    """Phase 3B: enforce_correlations and fidelity_profile kwargs."""

    def _simple_schema(self) -> dict:
        return {
            "model": {"name": "test", "domain": "test"},
            "tables": {
                "t": {
                    "columns": {
                        "id": {"type": "integer", "generator": {"strategy": "sequence", "start": 1}},
                        "val": {"type": "decimal", "generator": {"strategy": "distribution", "type": "normal", "params": {"loc": 0.0, "scale": 1.0}}},
                    },
                    "primary_key": ["id"],
                }
            },
            "generation": {"scale": "small", "scales": {"small": {"t": 100}}},
        }

    def test_generate_enforce_correlations_false_still_works(self):
        from sqllocks_spindle import Spindle
        s = Spindle()
        result = s.generate(schema=self._simple_schema(), enforce_correlations=False)
        assert "t" in result.tables
        assert len(result.tables["t"]) == 100

    def test_generate_fidelity_profile_none_returns_result(self):
        from sqllocks_spindle import Spindle
        from sqllocks_spindle.engine.generator import GenerationResult
        s = Spindle()
        result = s.generate(schema=self._simple_schema(), fidelity_profile=None)
        assert isinstance(result, GenerationResult)

    def test_generate_with_fidelity_profile_returns_tuple(self):
        import numpy as np
        from sqllocks_spindle import Spindle
        from sqllocks_spindle.inference.comparator import FidelityReport
        from sqllocks_spindle.inference.profiler import (
            ColumnProfile, TableProfile, DatasetProfile
        )
        # Build a minimal DatasetProfile for table "t"
        col_id = ColumnProfile(
            name="id", dtype="integer",
            null_count=0, null_rate=0.0, cardinality=100, cardinality_ratio=1.0,
            is_unique=True, is_enum=False, enum_values=None,
            min_value=1, max_value=100, mean=50.0, std=28.9,
            distribution=None, distribution_params=None, pattern=None,
            is_primary_key=True, is_foreign_key=False, fk_ref_table=None,
        )
        col_val = ColumnProfile(
            name="val", dtype="float",
            null_count=0, null_rate=0.0, cardinality=100, cardinality_ratio=1.0,
            is_unique=False, is_enum=False, enum_values=None,
            min_value=-3.0, max_value=3.0, mean=0.0, std=1.0,
            distribution="normal", distribution_params={"loc": 0.0, "scale": 1.0},
            pattern=None, is_primary_key=False, is_foreign_key=False, fk_ref_table=None,
        )
        table = TableProfile(name="t", row_count=100,
                             columns={"id": col_id, "val": col_val},
                             primary_key=["id"], detected_fks={})
        fidelity_profile = DatasetProfile(tables={"t": table})

        s = Spindle()
        result = s.generate(schema=self._simple_schema(), fidelity_profile=fidelity_profile)
        assert isinstance(result, tuple)
        generation_result, report = result
        assert isinstance(report, FidelityReport)
        assert "t" in report.tables
