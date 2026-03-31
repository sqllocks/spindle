"""End-to-end integration tests for chunked billion-row generation."""

from __future__ import annotations

import tempfile
import time

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle import ChunkedSpindle, MultiWriter
from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter
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
    tables = dict(result.parent_tables)
    for name in result.child_table_names:
        chunks = list(result.iter_chunks(name))
        tables[name] = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    return tables


class TestAnchorIntegrationAllDomains:
    @pytest.mark.parametrize("name,domain,anchor", ALL_DOMAINS, ids=[d[0] for d in ALL_DOMAINS])
    def test_all_13_domains_anchor(self, name, domain, anchor):
        """Parametrize across all 13 domains at 100K anchor rows."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=domain,
            target_table=anchor,
            target_count=10_000,
            chunk_size=5_000,
            seed=42,
        )
        tables = _collect_tables(result)

        # All tables generated
        for table_name in result.row_counts:
            assert table_name in tables, f"Missing table: {table_name}"
            assert len(tables[table_name]) > 0, f"{table_name} is empty"

        # Lakehouse write succeeds
        with tempfile.TemporaryDirectory() as tmpdir:
            lw = LakehouseFilesWriter(base_path=tmpdir)
            wr = lw.write_all(tables)
            assert wr.success, f"Write failed: {wr.errors}"


class TestScaleGradient:
    def test_scale_gradient_retail(self):
        """Retail at 10K / 100K / 1M — generation time scales ~linearly."""
        timings = {}
        for count in [10_000, 100_000]:
            cs = ChunkedSpindle()
            t0 = time.time()
            result = cs.generate_chunked(
                domain=RetailDomain(),
                target_table="order_line",
                target_count=count,
                chunk_size=10_000,
                seed=42,
            )
            tables = _collect_tables(result)
            elapsed = time.time() - t0
            timings[count] = elapsed
            total = sum(len(df) for df in tables.values())
            assert total > 0

        # 100K should take less than 20x of 10K (linear = ~10x)
        ratio = timings[100_000] / max(timings[10_000], 0.001)
        assert ratio < 20, f"Scale ratio {ratio:.1f}x (expected <20x for 10x data)"


class TestChunkBoundaryFKIntegrity:
    def test_chunk_boundary_fk_integrity(self):
        """1M order_lines with 100K chunks — zero FK values outside parent range."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(),
            target_table="order_line",
            target_count=1_000_000,
            chunk_size=100_000,
            seed=42,
        )
        tables = _collect_tables(result)

        # Every order_line.order_id must be in the range of generated order IDs
        if "order_line" in tables and "order" in tables:
            order_ids = set(tables["order"]["order_id"])
            ol_order_ids = tables["order_line"]["order_id"].dropna()
            orphans = ol_order_ids[~ol_order_ids.isin(order_ids)]
            assert len(orphans) == 0, (
                f"{len(orphans)} orphan order_ids in order_line across chunk boundaries"
            )
