"""Integration tests for the capital markets domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=CapitalMarketsDomain(), scale="small", seed=42)


class TestCapitalMarketsStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "exchange", "sector", "industry", "company",
            "daily_price", "dividend", "split", "earnings",
            "insider_transaction", "trade",
        }
        assert expected == set(result_small.tables.keys())

    def test_company_count(self, result_small):
        assert len(result_small["company"]) == 100

    def test_exchange_count(self, result_small):
        assert len(result_small["exchange"]) >= 1

    def test_sector_count(self, result_small):
        assert len(result_small["sector"]) >= 1

    def test_industry_count(self, result_small):
        assert len(result_small["industry"]) >= 1

    def test_daily_price_has_rows(self, result_small):
        assert len(result_small["daily_price"]) > 0

    def test_trade_has_rows(self, result_small):
        assert len(result_small["trade"]) > 0

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        # sector → industry (FK), company → child tables (FK)
        # exchange has no FK to company (linked via string exchange_code)
        assert order.index("sector") < order.index("industry")
        assert order.index("company") < order.index("daily_price")
        assert order.index("company") < order.index("trade")


class TestCapitalMarketsIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_company_ticker_is_unique(self, result_small):
        df = result_small["company"]
        pk_col = [c for c in df.columns if c.endswith("_id") or c == "ticker"][0]
        assert df[pk_col].is_unique

    def test_daily_price_company_fk_valid(self, result_small):
        company_df = result_small["company"]
        price_df = result_small["daily_price"]
        pk_col = [c for c in company_df.columns if c.endswith("_id") or c == "ticker"][0]
        fk_col = [c for c in price_df.columns if "company" in c or "ticker" in c][0]
        company_ids = set(company_df[pk_col])
        price_refs = set(price_df[fk_col].dropna())
        orphans = price_refs - company_ids
        assert len(orphans) == 0, f"Orphan company refs in daily_price: {orphans}"

    def test_trade_has_price_columns(self, result_small):
        df = result_small["trade"]
        # Trade should have some price/quantity related columns
        numeric_cols = df.select_dtypes(include=["number"]).columns
        assert len(numeric_cols) >= 1

    def test_dividend_has_rows(self, result_small):
        assert len(result_small["dividend"]) > 0

    def test_earnings_has_rows(self, result_small):
        assert len(result_small["earnings"]) > 0


class TestCapitalMarketsDistributions:
    def test_daily_price_ohlc_positive(self, result_small):
        df = result_small["daily_price"]
        ohlc_cols = [c for c in df.columns if c in ("open", "high", "low", "close")]
        for col in ohlc_cols:
            assert (df[col].dropna() > 0).all(), f"{col} has non-positive values"

    def test_daily_price_high_gte_low(self, result_small):
        df = result_small["daily_price"]
        if "high" in df.columns and "low" in df.columns:
            assert (df["high"] >= df["low"]).all(), "high < low in daily_price"

    def test_volume_non_negative(self, result_small):
        df = result_small["daily_price"]
        if "volume" in df.columns:
            assert (df["volume"].dropna() >= 0).all()


class TestCapitalMarketsReproducibility:
    def test_same_seed_same_result(self):
        s = Spindle()
        r1 = s.generate(domain=CapitalMarketsDomain(), scale="small", seed=99)
        r2 = s.generate(domain=CapitalMarketsDomain(), scale="small", seed=99)
        for table_name in r1.tables:
            assert r1[table_name].equals(r2[table_name]), f"{table_name} not reproducible"
