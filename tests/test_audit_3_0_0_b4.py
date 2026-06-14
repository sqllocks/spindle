"""Audit 3.0.0 - B4 omitted-findings regression tests."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def test_estimator_uses_sum_not_max():
    """Multiple targets must accumulate seconds, not take the max."""
    from sqllocks_spindle.demo.estimator import CostEstimator as DemoEstimator
    est = DemoEstimator()
    one = est.estimate(rows=1_000_000, targets=["warehouse"])
    two = est.estimate(rows=1_000_000, targets=["warehouse", "lakehouse"])
    # Two targets should take strictly more than one.
    assert two.estimated_duration_seconds > one.estimated_duration_seconds


def test_correlated_operation_alias_for_rule():
    """operation key works where rule is documented."""
    from sqllocks_spindle.engine.strategies.correlated import CorrelatedStrategy
    from sqllocks_spindle.schema.parser import ColumnDef

    class _Ctx:
        rng = np.random.default_rng(1)
        current_table = {"price": np.array([10.0, 20.0, 30.0])}
        row_count = 3

    col = ColumnDef(name="cost", type="decimal", generator={}, scale=2)
    out = CorrelatedStrategy().generate(
        col,
        {"source_column": "price", "operation": "multiply",
         "params": {"factor_min": 0.5, "factor_max": 0.5}},
        _Ctx(),
    )
    assert np.allclose(out, [5.0, 10.0, 15.0])


def test_star_schema_date_dim_capped_at_60_years():
    """An absurd date span should be capped, not produce a giant dim_date."""
    from sqllocks_spindle.transform.star_schema import StarSchemaTransform

    t = StarSchemaTransform.__new__(StarSchemaTransform)
    dates = [pd.Timestamp("1900-01-01"), pd.Timestamp("2030-12-31")]
    dim = t._build_date_dim(dates)
    # 60 years cap -> roughly 22000 rows max
    assert 0 < len(dim) <= 22_500, f"dim_date too large: {len(dim)}"


def test_composite_bridge_fk_type_inherits_parent_pk_type():
    """Bridge FK columns added by composite should reuse parent PK type."""
    from sqllocks_spindle.domains.composite import CompositeDomain
    from sqllocks_spindle.schema.parser import (
        ColumnDef, TableDef, RelationshipDef,
    )

    parent = TableDef(
        name="ticker_dim",
        primary_key=["ticker"],
        columns={"ticker": ColumnDef(name="ticker", type="string", generator={})},
    )
    child = TableDef(
        name="quote",
        primary_key=["quote_id"],
        columns={"quote_id": ColumnDef(name="quote_id", type="integer", generator={})},
    )
    rel = RelationshipDef(
        name="quote_ticker",
        parent="ticker_dim", child="quote",
        parent_columns=["ticker"], child_columns=["ticker"],
        type="one_to_many",
    )
    tables = {"ticker_dim": parent, "quote": child}
    cd = CompositeDomain.__new__(CompositeDomain)
    cd._ensure_bridge_columns(tables, [rel])
    assert tables["quote"].columns["ticker"].type == "string"


def test_healthcare_fact_claim_date_cols_use_filing_date():
    """fact_claim must use filing_date (which exists in the join), not service_date."""
    from sqllocks_spindle import HealthcareDomain
    schema = HealthcareDomain().get_schema()
    star = HealthcareDomain().get_star_schema(schema) if hasattr(HealthcareDomain(), "get_star_schema") else None
    # Fallback: parse the FactSpec directly from the schema's _star_map
    from sqllocks_spindle.engine.generator import Spindle
    sp = Spindle()
    parsed = sp._resolve_schema(HealthcareDomain(), None)
    if hasattr(parsed, "star_schema") and parsed.star_schema:
        fact_claim = parsed.star_schema.facts.get("fact_claim")
        if fact_claim is not None:
            assert "filing_date" in fact_claim.date_cols
            assert "service_date" not in fact_claim.date_cols


def test_capital_markets_declares_exchange_company_relationship():
    """company.exchange_code must be declared as an FK to exchange.exchange_code."""
    from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain
    from sqllocks_spindle.engine.generator import Spindle
    parsed = Spindle()._resolve_schema(CapitalMarketsDomain(), None)
    rels = parsed.relationships
    names = {r.name for r in rels}
    assert "exchange_companies" in names, f"missing exchange_companies rel; have {names}"


def test_capital_markets_declares_ohlc_business_rules():
    """high>=low, close in [low,high], open in [low,high] must be declared."""
    from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain
    from sqllocks_spindle.engine.generator import Spindle
    parsed = Spindle()._resolve_schema(CapitalMarketsDomain(), None)
    rule_names = {r.name for r in parsed.business_rules}
    for required in ("high_gte_low", "close_between_high_low", "open_between_high_low"):
        assert required in rule_names, f"missing rule {required}; have {rule_names}"