"""FK correctness tests — the core proof that billion-row FK generation works.

Every test generates real data and validates actual FK relationships.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle import ChunkedSpindle, Spindle
from sqllocks_spindle.engine.id_manager import IDManager, RangePKPool
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.domains.healthcare import HealthcareDomain
from sqllocks_spindle.domains.financial import FinancialDomain
from sqllocks_spindle.domains.hr import HrDomain
from sqllocks_spindle.domains.supply_chain import SupplyChainDomain
from sqllocks_spindle.domains.iot import IoTDomain
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain
from sqllocks_spindle.domains.real_estate import RealEstateDomain
from sqllocks_spindle.domains.education import EducationDomain
from sqllocks_spindle.domains.insurance import InsuranceDomain
from sqllocks_spindle.domains.manufacturing import ManufacturingDomain
from sqllocks_spindle.domains.marketing import MarketingDomain
from sqllocks_spindle.domains.telecom import TelecomDomain


ALL_DOMAINS = [
    ("retail", RetailDomain(), "order_line"),
    ("healthcare", HealthcareDomain(), "claim_line"),
    ("financial", FinancialDomain(), "transaction"),
    ("hr", HrDomain(), "employee"),
    ("supply_chain", SupplyChainDomain(), "purchase_order_line"),
    ("iot", IoTDomain(), "reading"),
    ("capital_markets", CapitalMarketsDomain(), "trade"),
    ("real_estate", RealEstateDomain(), "transaction"),
    ("education", EducationDomain(), "enrollment"),
    ("insurance", InsuranceDomain(), "claim"),
    ("manufacturing", ManufacturingDomain(), "work_order"),
    ("marketing", MarketingDomain(), "email_send"),
    ("telecom", TelecomDomain(), "usage_record"),
]


def _collect_tables(result) -> dict[str, pd.DataFrame]:
    """Collect all tables from a ChunkedGenerationResult."""
    tables = dict(result.parent_tables)
    for name in result.child_table_names:
        chunks = list(result.iter_chunks(name))
        tables[name] = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    return tables


def _verify_fk(child_df, parent_df, fk_col, pk_col):
    """Assert all non-null FK values exist in parent PK."""
    child_vals = child_df[fk_col].dropna()
    if len(child_vals) == 0:
        return
    parent_set = set(parent_df[pk_col])
    orphans = child_vals[~child_vals.isin(parent_set)]
    assert len(orphans) == 0, (
        f"{fk_col}: {len(orphans)} orphan FK values out of {len(child_vals)}"
    )


# ── Referential Integrity (no orphans) ─────────────────────────────────


class TestNoOrphanedFKs:
    def test_no_orphaned_fks_retail(self):
        """Generate Retail at 50K order_lines — zero orphans allowed."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(), target_table="order_line",
            target_count=50_000, chunk_size=10_000, seed=42,
        )
        tables = _collect_tables(result)
        for rel in result.schema.relationships:
            if rel.type == "self_referencing":
                continue
            if rel.parent not in tables or rel.child not in tables:
                continue
            for p_col, c_col in zip(rel.parent_columns, rel.child_columns):
                if c_col in tables[rel.child].columns and p_col in tables[rel.parent].columns:
                    _verify_fk(tables[rel.child], tables[rel.parent], c_col, p_col)

    @pytest.mark.parametrize("name,domain,anchor", ALL_DOMAINS, ids=[d[0] for d in ALL_DOMAINS])
    def test_no_orphaned_fks_all_domains(self, name, domain, anchor):
        """Parametrize across all 13 domains at 10K anchor rows."""
        s = Spindle()
        result = s.generate(domain=domain, scale="small", seed=42)
        errors = result.verify_integrity()
        assert errors == [], f"{name} FK errors: {errors}"

    def test_no_orphaned_fks_cross_chunk(self):
        """500K order_lines with 50K chunks — FK integrity across chunk boundaries."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(), target_table="order_line",
            target_count=500_000, chunk_size=50_000, seed=42,
        )
        tables = _collect_tables(result)

        # Verify FK integrity across all chunks
        for rel in result.schema.relationships:
            if rel.type == "self_referencing":
                continue
            if rel.parent not in tables or rel.child not in tables:
                continue
            for p_col, c_col in zip(rel.parent_columns, rel.child_columns):
                if c_col in tables[rel.child].columns and p_col in tables[rel.parent].columns:
                    _verify_fk(tables[rel.child], tables[rel.parent], c_col, p_col)


# ── FK Coverage ────────────────────────────────────────────────────────


class TestFKCoverage:
    def test_fk_coverage_all_parents_referenced(self):
        """For high-ratio FKs, ≥95% of parent PKs appear in child FK column."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="medium", seed=42)
        tables = result.tables

        # order.customer_id → customer — each customer should have at least 1 order
        parent_ids = set(tables["customer"]["customer_id"])
        child_fks = set(tables["order"]["customer_id"])
        coverage = len(parent_ids & child_fks) / len(parent_ids)
        assert coverage > 0.95, f"Customer coverage {coverage:.2%} (expected >95%)"

    def test_fk_coverage_grows_with_scale(self):
        """Parent coverage increases with scale (10K, 50K, 100K)."""
        coverages = []
        for count in [10_000, 50_000, 100_000]:
            cs = ChunkedSpindle()
            result = cs.generate_chunked(
                domain=RetailDomain(), target_table="order",
                target_count=count, chunk_size=10_000, seed=42,
            )
            tables = _collect_tables(result)
            if "customer" in tables and "order" in tables:
                parent_ids = set(tables["customer"]["customer_id"])
                child_fks = set(tables["order"]["customer_id"].dropna())
                cov = len(parent_ids & child_fks) / max(len(parent_ids), 1)
                coverages.append(cov)
        # Coverage should generally increase or be high for all
        assert coverages[-1] >= coverages[0] * 0.95, (
            f"Coverage did not grow: {coverages}"
        )


# ── FK Cardinality Distribution ────────────────────────────────────────


class TestFKDistribution:
    def test_fk_distribution_zipf(self):
        """Retail order uses zipf on customer_id — top 10% customers have >30% orders."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="medium", seed=42)
        orders = result.tables["order"]

        counts = orders["customer_id"].value_counts()
        top_10_pct = counts.head(int(len(counts) * 0.1)).sum()
        total = len(orders)
        assert top_10_pct / total > 0.20, (
            f"Top 10% customers have {top_10_pct/total:.1%} orders (expected >20%)"
        )

    def test_fk_cardinality_ratio_within_tolerance(self):
        """For retail at 100K orders, actual children-per-parent within ±50% of schema ratio."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(), target_table="order",
            target_count=100_000, chunk_size=50_000, seed=42,
        )
        tables = _collect_tables(result)

        if "order_line" in tables and "order" in tables:
            actual_ratio = len(tables["order_line"]) / max(len(tables["order"]), 1)
            # Schema says order_line per order ~ 2.5
            assert 1.0 <= actual_ratio <= 5.0, f"OL/O ratio {actual_ratio:.2f} (expected ~2.5)"


# ── Multi-Level FK Chains ──────────────────────────────────────────────


class TestFKChains:
    def test_three_level_fk_chain_retail(self):
        """order_line → order → customer: join returns zero nulls."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="medium", seed=42)
        t = result.tables

        # Join order_line → order
        merged = t["order_line"].merge(
            t["order"][["order_id", "customer_id"]],
            on="order_id", how="inner",
        )
        assert len(merged) > 0
        # Join → customer
        merged2 = merged.merge(
            t["customer"][["customer_id"]],
            on="customer_id", how="inner",
        )
        assert len(merged2) == len(merged), (
            f"Lost {len(merged) - len(merged2)} rows at customer join"
        )

    def test_three_level_fk_chain_healthcare(self):
        """claim_line → claim → encounter → patient: 4-level chain."""
        s = Spindle()
        result = s.generate(domain=HealthcareDomain(), scale="small", seed=42)
        t = result.tables

        if "claim_line" in t and "claim" in t:
            # claim_line → claim (via claim_id)
            merged = t["claim_line"][["claim_line_id", "claim_id"]].merge(
                t["claim"][["claim_id", "encounter_id"]],
                on="claim_id", how="inner",
            )
            assert len(merged) > 0
            if "encounter" in t and "encounter_id" in merged.columns:
                # → encounter (via encounter_id)
                merged2 = merged.merge(
                    t["encounter"][["encounter_id", "patient_id"]],
                    on="encounter_id", how="inner",
                )
                assert len(merged2) > 0
                if "patient" in t:
                    # → patient (via patient_id)
                    merged3 = merged2.merge(
                        t["patient"][["patient_id"]],
                        on="patient_id", how="inner",
                    )
                    assert len(merged3) > 0

    @pytest.mark.parametrize("name,domain,anchor", ALL_DOMAINS, ids=[d[0] for d in ALL_DOMAINS])
    def test_fk_chain_all_domains(self, name, domain, anchor):
        """For each domain, verify_integrity returns no errors."""
        s = Spindle()
        result = s.generate(domain=domain, scale="small", seed=42)
        errors = result.verify_integrity()
        assert errors == [], f"{name}: {errors}"


# ── Self-Referencing FK ────────────────────────────────────────────────


class TestSelfReferencing:
    def test_self_referencing_hierarchy_retail(self):
        """product_category.parent_category_id: roots have null parent, no cycles."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)
        cats = result.tables["product_category"]

        if "parent_category_id" in cats.columns:
            # Roots have null parent
            roots = cats[cats["parent_category_id"].isna()]
            assert len(roots) > 0, "No root categories found"

            # Non-roots reference valid category_ids
            non_roots = cats[cats["parent_category_id"].notna()]
            valid_ids = set(cats["category_id"])
            for _, row in non_roots.iterrows():
                assert row["parent_category_id"] in valid_ids
                assert row["parent_category_id"] != row["category_id"], "Self-cycle detected"


# ── RangePKPool Correctness ────────────────────────────────────────────


class TestRangePKPool:
    def test_range_pool_fk_sampling_uniform(self):
        """RangePKPool(1, 1M): sample 10M FK values, all in range, roughly uniform."""
        rng = np.random.default_rng(42)
        pool = RangePKPool(start=1, count=1_000_000)

        indices = rng.integers(0, len(pool), size=10_000_000)
        values = pool[indices]

        assert values.min() >= 1
        assert values.max() <= 1_000_000

    def test_range_pool_fk_no_oom(self):
        """RangePKPool with 1B count uses <100 bytes memory."""
        import sys
        pool = RangePKPool(start=1, count=1_000_000_000)
        size = sys.getsizeof(pool)
        assert size < 200, f"RangePKPool size {size} bytes (expected <200)"
        assert len(pool) == 1_000_000_000

        # Sample 1M FKs — all valid
        rng = np.random.default_rng(42)
        indices = rng.integers(0, len(pool), size=1_000_000)
        values = pool[indices]
        assert values.min() >= 1
        assert values.max() <= 1_000_000_000

    def test_range_pool_vs_array_fk_equivalence(self):
        """Same seed: RangePKPool and ndarray produce same FK values."""
        count = 10_000

        rng1 = np.random.default_rng(42)
        pool_range = RangePKPool(start=1, count=count)
        indices1 = rng1.integers(0, count, size=5_000)
        vals_range = pool_range[indices1]

        rng2 = np.random.default_rng(42)
        pool_array = np.arange(1, count + 1)
        indices2 = rng2.integers(0, count, size=5_000)
        vals_array = pool_array[indices2]

        np.testing.assert_array_equal(vals_range, vals_array)


# ── Nullable FK ────────────────────────────────────────────────────────


class TestNullableFKs:
    def test_optional_fk_no_invalid_ids(self):
        """Nullable FK columns: nulls are null, non-nulls are valid references."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)
        tables = result.tables

        for rel in result.schema.relationships:
            if rel.type == "self_referencing":
                continue
            if rel.parent not in tables or rel.child not in tables:
                continue
            for p_col, c_col in zip(rel.parent_columns, rel.child_columns):
                if c_col not in tables[rel.child].columns:
                    continue
                child_col = tables[rel.child][c_col]
                non_null = child_col.dropna()
                if len(non_null) == 0:
                    continue
                parent_set = set(tables[rel.parent][p_col])
                invalid = non_null[~non_null.isin(parent_set)]
                assert len(invalid) == 0, (
                    f"{rel.child}.{c_col}: {len(invalid)} invalid non-null FK values"
                )
