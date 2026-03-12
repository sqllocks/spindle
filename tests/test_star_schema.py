"""Tests for the star schema transform (Phase 6)."""

from __future__ import annotations

import pandas as pd
import pytest

from sqllocks_spindle import RetailDomain, Spindle
from sqllocks_spindle.transform import (
    DimSpec,
    FactSpec,
    StarSchemaMap,
    StarSchemaTransform,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def retail_result():
    spindle = Spindle()
    return spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=42)


@pytest.fixture(scope="module")
def retail_star(retail_result):
    domain = RetailDomain()
    transform = StarSchemaTransform()
    return transform.transform(retail_result.tables, domain.star_schema_map())


# ---------------------------------------------------------------------------
# StarSchemaMap / DimSpec / FactSpec construction
# ---------------------------------------------------------------------------

def test_dim_spec_defaults():
    spec = DimSpec(source="customer", sk="sk_customer", nk="customer_id")
    assert spec.enrich is None
    assert spec.include is None


def test_fact_spec_defaults():
    spec = FactSpec(primary="order_line")
    assert spec.joins is None
    assert spec.fk_map is None
    assert spec.date_cols is None


def test_star_schema_map_construction():
    schema_map = StarSchemaMap(
        dims={"dim_x": DimSpec(source="x", sk="sk_x", nk="x_id")},
        facts={"fact_y": FactSpec(primary="y")},
    )
    assert "dim_x" in schema_map.dims
    assert "fact_y" in schema_map.facts
    assert schema_map.generate_date_dim is True
    assert schema_map.fiscal_year_start == 1


# ---------------------------------------------------------------------------
# Retail star schema — dimension tables
# ---------------------------------------------------------------------------

def test_retail_star_has_expected_dims(retail_star):
    assert "dim_customer" in retail_star.dimensions
    assert "dim_product" in retail_star.dimensions
    assert "dim_store" in retail_star.dimensions
    assert "dim_promotion" in retail_star.dimensions


def test_dim_customer_has_sk(retail_star):
    df = retail_star.dimensions["dim_customer"]
    assert "sk_customer" in df.columns
    assert df["sk_customer"].iloc[0] == 1
    assert df["sk_customer"].is_monotonic_increasing


def test_dim_customer_sk_unique(retail_star):
    df = retail_star.dimensions["dim_customer"]
    assert df["sk_customer"].nunique() == len(df)


def test_dim_product_enriched_with_category(retail_star):
    df = retail_star.dimensions["dim_product"]
    # Should have category columns from product_category enrichment
    cat_cols = [c for c in df.columns if c.startswith("cat_")]
    assert len(cat_cols) > 0, "Expected category columns prefixed with 'cat_'"


def test_dim_product_has_sk(retail_star):
    df = retail_star.dimensions["dim_product"]
    assert "sk_product" in df.columns
    assert df["sk_product"].iloc[0] == 1


def test_dim_store_has_sk(retail_star):
    df = retail_star.dimensions["dim_store"]
    assert "sk_store" in df.columns


def test_dim_promotion_has_sk(retail_star):
    df = retail_star.dimensions["dim_promotion"]
    assert "sk_promotion" in df.columns


# ---------------------------------------------------------------------------
# Date dimension
# ---------------------------------------------------------------------------

def test_date_dim_generated(retail_star):
    assert retail_star.date_dim is not None
    assert len(retail_star.date_dim) > 0


def test_date_dim_columns(retail_star):
    expected = {
        "sk_date", "date", "year", "quarter", "month", "month_name",
        "week_of_year", "day_of_month", "day_of_week", "day_of_week_name",
        "is_weekend", "is_weekday", "fiscal_year", "fiscal_quarter",
    }
    assert expected.issubset(set(retail_star.date_dim.columns))


def test_date_dim_sk_is_yyyymmdd(retail_star):
    df = retail_star.date_dim
    # sk_date should equal year*10000 + month*100 + day
    sample = df.iloc[0]
    expected_sk = sample["year"] * 10000 + sample["month"] * 100 + int(sample["day_of_month"])
    assert sample["sk_date"] == expected_sk


def test_date_dim_no_gaps(retail_star):
    df = retail_star.date_dim
    dates = pd.to_datetime(df["date"])
    assert (dates.diff().dropna() == pd.Timedelta("1D")).all()


def test_date_dim_weekend_flags(retail_star):
    df = retail_star.date_dim
    weekends = df[df["is_weekend"]]
    weekdays = df[df["is_weekday"]]
    assert len(weekends) > 0
    assert len(weekdays) > 0
    assert len(weekends) + len(weekdays) == len(df)


def test_date_dim_fiscal_year(retail_star):
    df = retail_star.date_dim
    assert df["fiscal_year"].notna().all()
    assert df["fiscal_quarter"].between(1, 4).all()


# ---------------------------------------------------------------------------
# Retail star schema — fact tables
# ---------------------------------------------------------------------------

def test_retail_star_has_expected_facts(retail_star):
    assert "fact_sale" in retail_star.facts
    assert "fact_return" in retail_star.facts


def test_fact_sale_has_sk_columns(retail_star):
    df = retail_star.facts["fact_sale"]
    assert "sk_customer" in df.columns
    assert "sk_product" in df.columns
    assert "sk_store" in df.columns


def test_fact_sale_has_nk_columns(retail_star):
    df = retail_star.facts["fact_sale"]
    # Natural keys should be preserved as nk_ columns
    assert "nk_customer_id" in df.columns
    assert "nk_product_id" in df.columns


def test_fact_sale_has_sk_date(retail_star):
    df = retail_star.facts["fact_sale"]
    assert "sk_date" in df.columns
    # sk_date should be 8-digit YYYYMMDD integers
    non_null = df["sk_date"].dropna()
    assert (non_null >= 20000101).all()
    assert (non_null <= 20991231).all()


def test_fact_sale_sk_references_valid_customer(retail_star):
    fact_df = retail_star.facts["fact_sale"]
    dim_df = retail_star.dimensions["dim_customer"]
    valid_sks = set(dim_df["sk_customer"])
    fact_sks = fact_df["sk_customer"].dropna()
    assert fact_sks.isin(valid_sks).all()


def test_fact_sale_sk_references_valid_product(retail_star):
    fact_df = retail_star.facts["fact_sale"]
    dim_df = retail_star.dimensions["dim_product"]
    valid_sks = set(dim_df["sk_product"])
    fact_sks = fact_df["sk_product"].dropna()
    assert fact_sks.isin(valid_sks).all()


def test_fact_return_has_sk_date(retail_star):
    df = retail_star.facts["fact_return"]
    assert "sk_date" in df.columns


# ---------------------------------------------------------------------------
# StarSchemaResult helpers
# ---------------------------------------------------------------------------

def test_star_schema_result_repr(retail_star):
    r = repr(retail_star)
    assert "StarSchemaResult" in r
    assert "dimensions" in r


def test_star_schema_result_summary(retail_star):
    summary = retail_star.summary()
    assert "DIMENSIONS:" in summary
    assert "FACTS:" in summary
    assert "dim_customer" in summary
    assert "fact_sale" in summary


def test_all_tables_includes_everything(retail_star):
    all_t = retail_star.all_tables()
    assert "dim_customer" in all_t
    assert "dim_product" in all_t
    assert "dim_date" in all_t
    assert "fact_sale" in all_t
    assert "fact_return" in all_t


# ---------------------------------------------------------------------------
# scale presets
# ---------------------------------------------------------------------------

def test_fabric_demo_scale():
    spindle = Spindle()
    result = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=1)
    assert result.row_counts["customer"] == 200
    assert result.row_counts["product"] == 100


def test_warehouse_scale_larger_than_large():
    domain = RetailDomain()
    schema = domain.get_schema()
    scales = schema.generation.scales
    # warehouse is a practical Fabric DW scale — larger than large, may be smaller than xlarge (the extreme preset)
    assert scales["warehouse"]["customer"] > scales["large"]["customer"]


# ---------------------------------------------------------------------------
# Healthcare star schema (smoke test)
# ---------------------------------------------------------------------------

def test_healthcare_star_schema_runs():
    from sqllocks_spindle import HealthcareDomain

    spindle = Spindle()
    result = spindle.generate(domain=HealthcareDomain(), scale="fabric_demo", seed=42)
    domain = HealthcareDomain()
    transform = StarSchemaTransform()
    star = transform.transform(result.tables, domain.star_schema_map())

    assert "dim_patient" in star.dimensions
    assert "dim_provider" in star.dimensions
    assert "fact_encounter" in star.facts
    assert star.date_dim is not None
