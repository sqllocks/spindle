"""Unit tests for the anchor API in ChunkedSpindle."""

from __future__ import annotations

import pytest

from sqllocks_spindle import ChunkedSpindle
from sqllocks_spindle.engine.id_manager import RangePKPool
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


class TestDeriveTableCounts:
    def test_derive_counts_retail(self):
        """Anchor order_line at 1B — customer/order/product counts proportional to reference."""
        cs = ChunkedSpindle()
        domain = RetailDomain()
        result = cs.generate_chunked(
            domain=domain,
            target_table="order_line",
            target_count=1_000_000_000,
            chunk_size=1_000_000,
            seed=42,
        )
        counts = result.row_counts

        assert counts["order_line"] == 1_000_000_000
        # order should be ~400M (order_line / 2.5 ratio)
        assert counts["order"] > 100_000_000
        # customer should scale proportionally
        assert counts["customer"] > 1_000_000
        # All counts should be positive
        assert all(v > 0 for v in counts.values())

    @pytest.mark.parametrize("name,domain,anchor", ALL_DOMAINS, ids=[d[0] for d in ALL_DOMAINS])
    def test_derive_counts_all_13_domains(self, name, domain, anchor):
        """Every domain's anchor table derivation produces nonzero counts for all tables."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=domain,
            target_table=anchor,
            target_count=10_000,
            chunk_size=5_000,
            seed=42,
        )
        for table_name, count in result.row_counts.items():
            assert count > 0, f"{name}.{table_name} has count=0"

    def test_derive_preserves_root_tables(self):
        """Root tables (no FK parents) scale up correctly."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(),
            target_table="order_line",
            target_count=100_000,
            chunk_size=50_000,
            seed=42,
        )
        # customer is a root table — should have a positive count
        assert result.row_counts["customer"] > 0

    def test_derive_single_table_domain(self):
        """Domains with a single large anchor table set count directly."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(),
            target_table="customer",
            target_count=50_000,
            chunk_size=10_000,
            seed=42,
        )
        assert result.row_counts["customer"] == 50_000


class TestAnchorModeRangePKPool:
    def test_generate_chunked_uses_range_pool(self):
        """When target_table provided, IDManager uses RangePKPool for all tables."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(),
            target_table="order_line",
            target_count=10_000,
            chunk_size=5_000,
            seed=42,
        )
        # Access internal ID manager
        id_manager = cs._id_manager
        for table_name in result.generation_order:
            if table_name in result.schema.tables:
                table_def = result.schema.tables[table_name]
                if table_def.primary_key:
                    pool = id_manager._pk_pools.get(table_name)
                    assert isinstance(pool, RangePKPool), (
                        f"Expected RangePKPool for {table_name}, got {type(pool)}"
                    )


class TestAnchorAPIErrors:
    def test_target_table_not_in_schema(self):
        """Raises ValueError with message naming the missing table."""
        cs = ChunkedSpindle()
        with pytest.raises(ValueError, match="nonexistent_table"):
            cs.generate_chunked(
                domain=RetailDomain(),
                target_table="nonexistent_table",
                target_count=1000,
                chunk_size=500,
                seed=42,
            )

    def test_target_count_required(self):
        """Raises ValueError when target_table provided without target_count."""
        cs = ChunkedSpindle()
        with pytest.raises(ValueError, match="target_count is required"):
            cs.generate_chunked(
                domain=RetailDomain(),
                target_table="order_line",
                chunk_size=500,
                seed=42,
            )
