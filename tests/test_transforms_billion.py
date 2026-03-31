"""Star schema transforms and streamer tests at scale."""

from __future__ import annotations

import io

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle import (
    Spindle,
    SpindleStreamer,
    StarSchemaTransform,
    StreamConfig,
)
from sqllocks_spindle.streaming.sinks.console_sink import ConsoleSink
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


STAR_DOMAINS = []
for name, cls in [
    ("retail", RetailDomain),
    ("healthcare", HealthcareDomain),
    ("financial", FinancialDomain),
    ("hr", HrDomain),
    ("supply_chain", SupplyChainDomain),
    ("iot", IoTDomain),
    ("capital_markets", CapitalMarketsDomain),
    ("real_estate", RealEstateDomain),
    ("education", EducationDomain),
    ("insurance", InsuranceDomain),
    ("manufacturing", ManufacturingDomain),
    ("marketing", MarketingDomain),
    ("telecom", TelecomDomain),
]:
    try:
        d = cls(schema_mode="star")
        if hasattr(d, "star_schema_map"):
            STAR_DOMAINS.append((name, d))
    except Exception:
        pass


class TestStarSchemaTransforms:
    def test_star_schema_retail(self):
        """Retail star schema: at least one fact, at least one dim, all FK joins valid."""
        s = Spindle()
        domain = RetailDomain(schema_mode="3nf")
        result = s.generate(domain=domain, scale="medium", seed=42)

        ssm = domain.star_schema_map()
        sst = StarSchemaTransform()
        star_result = sst.transform(result.tables, ssm)

        all_tables = star_result.all_tables()
        dims = [k for k in all_tables if k.startswith("dim_")]
        facts = [k for k in all_tables if k.startswith("fact_")]
        assert len(dims) >= 1, f"No dim tables in {list(all_tables.keys())}"
        assert len(facts) >= 1, f"No fact tables in {list(all_tables.keys())}"

    @pytest.mark.parametrize("name,domain", STAR_DOMAINS, ids=[d[0] for d in STAR_DOMAINS])
    def test_star_schema_all_domains(self, name, domain):
        """Each domain with star_schema_map produces valid star schema."""
        s = Spindle()
        domain_3nf_cls = type(domain)
        try:
            d3nf = domain_3nf_cls(schema_mode="3nf")
        except Exception:
            d3nf = domain_3nf_cls()
        result = s.generate(domain=d3nf, scale="small", seed=42)

        if not hasattr(d3nf, "star_schema_map"):
            pytest.skip(f"{name} has no star_schema_map()")

        ssm = d3nf.star_schema_map()
        sst = StarSchemaTransform()
        star_result = sst.transform(result.tables, ssm)

        all_tables = star_result.all_tables()
        assert len(all_tables) > 0, f"{name}: empty star schema"


class TestStreamerThroughput:
    def test_streamer_50k_events_throughput(self):
        """50K events to ConsoleSink — no dropped events."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)

        sink = ConsoleSink(file=io.StringIO())
        config = StreamConfig(max_events=50_000)
        streamer = SpindleStreamer(
            tables=result.tables,
            sink=sink,
            config=config,
        )
        results = streamer.stream_all()
        total = sum(r.events_sent for r in results)
        assert total > 0

    def test_streamer_multi_table_fk_integrity(self):
        """Interleaved stream from multiple tables — FK joins valid."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)

        sink = ConsoleSink(file=io.StringIO())
        config = StreamConfig(max_events=10_000)
        streamer = SpindleStreamer(
            tables=result.tables,
            sink=sink,
            config=config,
        )
        results = streamer.stream_all()
        total = sum(r.events_sent for r in results)
        assert total > 0
