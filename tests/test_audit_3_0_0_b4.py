"""Audit 3.0.0 - B4 omitted findings."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def test_estimator_duration_is_sum_not_max():
    from sqllocks_spindle.demo.estimator import CostEstimator
    est = CostEstimator()
    one = est.estimate(10_000_000, ["warehouse"])
    two = est.estimate(10_000_000, ["warehouse", "lakehouse"])
    three = est.estimate(10_000_000, ["warehouse", "lakehouse", "sql_db"])
    assert two.estimated_duration_seconds > one.estimated_duration_seconds
    assert three.estimated_duration_seconds > two.estimated_duration_seconds


def test_healthcare_fact_claim_date_cols_use_filing_date():
    from sqllocks_spindle import HealthcareDomain
    sm = HealthcareDomain().star_schema_map()
    assert sm.facts["fact_claim"].date_cols == ["filing_date"]


def test_iot_dim_device_enrich_uses_device_type_id():
    from sqllocks_spindle import IoTDomain
    sm = IoTDomain().star_schema_map()
    enrich = sm.dims["dim_device"].enrich
    assert enrich and enrich[0]["left_on"] == "device_type_id"


def test_marketing_dim_campaign_enrich_uses_campaign_type_id():
    from sqllocks_spindle import MarketingDomain
    sm = MarketingDomain().star_schema_map()
    enrich = sm.dims["dim_campaign"].enrich
    assert enrich and enrich[0]["left_on"] == "campaign_type_id"


def test_education_financial_aid_has_award_date_column():
    from sqllocks_spindle import EducationDomain
    schema = EducationDomain().get_schema()
    assert "award_date" in schema.tables["financial_aid"].columns


def test_capital_markets_declares_exchange_company_relationship():
    from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain
    schema = CapitalMarketsDomain().get_schema()
    names = [r.name for r in schema.relationships]
    assert "exchange_companies" in names


def test_cleanup_drop_table_is_schema_qualified():
    import inspect
    from sqllocks_spindle.demo.cleanup import CleanupEngine
    src = inspect.getsource(CleanupEngine._cleanup_sql_table)
    assert "dbo." in src


def test_streaming_autocleanup_invokes_engine():
    import inspect
    from sqllocks_spindle.demo.modes import streaming as st
    src = inspect.getsource(st)
    assert "CleanupEngine(self._conn).cleanup" in src


def test_composite_bridge_inherits_parent_pk_type():
    from sqllocks_spindle.domains.composite import CompositeDomain
    from sqllocks_spindle.schema.parser import ColumnDef, TableDef, RelationshipDef

    parent = TableDef(
        name="ticker_master",
        primary_key=["ticker"],
        columns={"ticker": ColumnDef(name="ticker", type="string", generator={"strategy": "sequence"})},
    )
    child = TableDef(
        name="quote",
        primary_key=["quote_id"],
        columns={"quote_id": ColumnDef(name="quote_id", type="integer", generator={"strategy": "sequence"})},
    )
    rel = RelationshipDef(
        name="ticker_quotes", parent="ticker_master", child="quote",
        parent_columns=["ticker"], child_columns=["ticker"],
        type="one_to_many",
    )
    tables = {"ticker_master": parent, "quote": child}
    cd = CompositeDomain.__new__(CompositeDomain)
    cd._ensure_bridge_columns(tables, [rel])
    assert tables["quote"].columns["ticker"].type == "string"
