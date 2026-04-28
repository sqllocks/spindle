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


class TestCapitalMarketsValueCorrectness:
    """Regression tests for field-level value correctness (no NaN, no scrambled codes)."""

    def test_sector_name_not_nan(self, result_small):
        """sector_name must never be NaN — guards against reference_data field extraction bug."""
        df = result_small["sector"]
        assert df["sector_name"].notna().all(), (
            f"sector_name has NaN values: {df[df['sector_name'].isna()]}"
        )

    def test_sector_code_not_nan(self, result_small):
        """sector_code must never be NaN."""
        df = result_small["sector"]
        assert df["sector_code"].notna().all(), (
            f"sector_code has NaN values: {df[df['sector_code'].isna()]}"
        )

    def test_exchange_code_is_short_code(self, result_small):
        """exchange_code must be a short ticker-style code, not the full exchange name."""
        df = result_small["exchange"]
        for code in df["exchange_code"]:
            assert len(str(code)) <= 10, (
                f"exchange_code '{code}' is too long — looks like a full name, not a code"
            )

    def test_exchange_code_overlaps_with_company(self, result_small):
        """At least some exchange codes in the exchange table must appear in company.exchange_code.

        Guards against the field mapping bug where exchange_code stored the full name
        ('NASDAQ Stock Market') while company stored short codes ('NASDAQ').
        """
        exchange_codes = set(result_small["exchange"]["exchange_code"].dropna())
        company_codes = set(result_small["company"]["exchange_code"].dropna())
        overlap = exchange_codes & company_codes
        assert len(overlap) > 0, (
            f"No exchange codes overlap between exchange table {exchange_codes} "
            f"and company table sample {list(company_codes)[:10]}"
        )
