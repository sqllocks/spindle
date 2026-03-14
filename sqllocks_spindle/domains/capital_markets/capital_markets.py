"""Capital Markets domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class CapitalMarketsDomain(Domain):
    """Capital Markets domain — US equities, pricing, corporate actions.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - company: Public company reference data (real S&P 500 tickers)
        - exchange: Stock exchanges (NYSE, NASDAQ, AMEX)
        - sector: GICS sectors (11)
        - industry: GICS industries (61) with sector FK
        - daily_price: OHLCV daily bars via Geometric Brownian Motion
        - dividend: Dividend payments
        - split: Stock splits
        - earnings: Quarterly earnings with EPS surprise
        - insider_transaction: Insider trades (SEC Form 4)
        - trade: Tick-level trades (for streaming / RTI)

    DISCLAIMER: All generated data is synthetic. Price data does not represent
    actual market performance. Not suitable for investment decisions.
    """

    @property
    def name(self) -> str:
        return "capital_markets"

    @property
    def description(self) -> str:
        return (
            "Capital Markets domain with companies, daily prices, "
            "dividends, earnings, insider trades, and tick-level trades"
        )

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build capital markets 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"capital_markets_{self._schema_mode}",
                "description": f"Capital Markets domain — {self._schema_mode} schema",
                "domain": "capital_markets",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── Reference tables ───────────────────────────────
                "exchange": {
                    "description": "Stock exchanges (NYSE, NASDAQ, AMEX)",
                    "primary_key": ["exchange_id"],
                    "columns": {
                        "exchange_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "exchange_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "exchanges",
                                "field": "name",
                            },
                        },
                        "exchange_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "exchanges",
                                "field": "code",
                            },
                        },
                        "mic": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "exchanges",
                                "field": "mic",
                            },
                        },
                        "timezone": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "exchanges",
                                "field": "timezone",
                            },
                        },
                        "trading_hours_start": {
                            "type": "string",
                            "max_length": 5,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "exchanges",
                                "field": "trading_hours_start",
                            },
                        },
                        "trading_hours_end": {
                            "type": "string",
                            "max_length": 5,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "exchanges",
                                "field": "trading_hours_end",
                            },
                        },
                    },
                },
                "sector": {
                    "description": "GICS sectors (11 standard sectors)",
                    "primary_key": ["sector_id"],
                    "columns": {
                        "sector_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "sector_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "gics_sectors",
                                "field": "sector_name",
                            },
                        },
                        "sector_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "gics_sectors",
                                "field": "sector_code",
                            },
                        },
                    },
                },
                "industry": {
                    "description": "GICS industries with sector FK",
                    "primary_key": ["industry_id"],
                    "columns": {
                        "industry_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "sector_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "sector.sector_id",
                            },
                        },
                        "industry_name": {
                            "type": "string",
                            "max_length": 150,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "gics_sectors",
                                "field": "industries.industry_name",
                            },
                        },
                    },
                },
                # ── Company (core entity) ──────────────────────────
                "company": {
                    "description": "Public company reference data from SEC EDGAR",
                    "primary_key": ["ticker"],
                    "columns": {
                        "ticker": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "sp500_constituents",
                                "field": "ticker",
                            },
                        },
                        "company_name": {
                            "type": "string",
                            "max_length": 255,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "name",
                            },
                        },
                        "cik": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "cik",
                            },
                        },
                        "exchange_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "exchange",
                            },
                        },
                        "sector_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "sector",
                            },
                        },
                        "industry_name": {
                            "type": "string",
                            "max_length": 150,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "industry",
                            },
                        },
                        "market_cap_tier": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "market_cap_tier",
                            },
                        },
                        "founding_year": {
                            "type": "integer",
                            "nullable": True,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "founding_year",
                            },
                        },
                        "hq_state": {
                            "type": "string",
                            "max_length": 2,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "hq_state",
                            },
                        },
                        "website": {
                            "type": "string",
                            "max_length": 255,
                            "nullable": True,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "sp500_constituents",
                                "field": "website",
                            },
                        },
                    },
                },
                # ── Market data tables ─────────────────────────────
                "daily_price": {
                    "description": "Daily OHLCV bars — generated via Geometric Brownian Motion",
                    "primary_key": ["price_id"],
                    "columns": {
                        "price_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "ticker": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "company.ticker",
                            },
                        },
                        "trade_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "trading_days",
                                "range_ref": "model.date_range",
                            },
                        },
                        "open": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 4.0,
                                    "sigma": 1.2,
                                    "min": 0.01,
                                    "max": 10000.00,
                                },
                            },
                        },
                        "high": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 4.0,
                                    "sigma": 1.2,
                                    "min": 0.01,
                                    "max": 10000.00,
                                },
                            },
                        },
                        "low": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 4.0,
                                    "sigma": 1.2,
                                    "min": 0.01,
                                    "max": 10000.00,
                                },
                            },
                        },
                        "close": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 4.0,
                                    "sigma": 1.2,
                                    "min": 0.01,
                                    "max": 10000.00,
                                },
                            },
                        },
                        "adj_close": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "close",
                                "rule": "multiply",
                                "params": {"factor_min": 0.95, "factor_max": 1.00},
                            },
                        },
                        "volume": {
                            "type": "bigint",
                            "generator": (lambda d: {
                                "strategy": "distribution",
                                "distribution": d.get("distribution", "log_normal"),
                                "params": {
                                    k: v for k, v in d.items() if k != "distribution"
                                },
                            })(self._dist("daily_price.volume", {
                                "distribution": "log_normal",
                                "mean": 15,
                                "sigma": 0.8,
                                "min": 100,
                                "max": 1000000000,
                            })),
                        },
                    },
                },
                # ── Corporate actions ──────────────────────────────
                "dividend": {
                    "description": "Dividend payments",
                    "primary_key": ["dividend_id"],
                    "columns": {
                        "dividend_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "ticker": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "company.ticker",
                            },
                        },
                        "ex_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "pay_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "ex_date",
                                "rule": "add_days",
                                "params": {"distribution": "uniform", "min": 14, "max": 45},
                            },
                        },
                        "amount": {
                            "type": "decimal",
                            "precision": 8,
                            "scale": 4,
                            "generator": (lambda d: {
                                "strategy": "distribution",
                                "distribution": d.get("distribution", "log_normal"),
                                "params": {
                                    k: v for k, v in d.items() if k != "distribution"
                                },
                            })(self._dist("dividend.amount", {
                                "distribution": "log_normal",
                                "mean": -0.5,
                                "sigma": 0.8,
                                "min": 0.01,
                                "max": 25.00,
                            })),
                        },
                        "frequency": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("dividend.frequency", {
                                    "quarterly": 0.75,
                                    "annual": 0.20,
                                    "special": 0.05,
                                }),
                            },
                        },
                    },
                },
                "split": {
                    "description": "Stock splits",
                    "primary_key": ["split_id"],
                    "columns": {
                        "split_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "ticker": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "company.ticker",
                            },
                        },
                        "split_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "ratio_from": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"2": 0.55, "3": 0.20, "4": 0.10, "5": 0.05, "10": 0.05, "1": 0.05},
                            },
                        },
                        "ratio_to": {
                            "type": "integer",
                            "generator": {"fixed": 1},
                        },
                    },
                },
                "earnings": {
                    "description": "Quarterly earnings reports with EPS surprise",
                    "primary_key": ["earnings_id"],
                    "columns": {
                        "earnings_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "ticker": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "company.ticker",
                            },
                        },
                        "report_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "fiscal_quarter": {
                            "type": "string",
                            "max_length": 6,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"Q1": 0.25, "Q2": 0.25, "Q3": 0.25, "Q4": 0.25},
                            },
                        },
                        "eps_estimate": {
                            "type": "decimal",
                            "precision": 8,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "normal",
                                "params": {
                                    "mean": 2.50,
                                    "std": 1.50,
                                    "min": -5.00,
                                    "max": 20.00,
                                },
                            },
                        },
                        "eps_actual": {
                            "type": "decimal",
                            "precision": 8,
                            "scale": 2,
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "eps_estimate",
                                "rule": "multiply",
                                "params": {"factor_min": 0.85, "factor_max": 1.20},
                            },
                        },
                        "surprise_pct": {
                            "type": "decimal",
                            "precision": 6,
                            "scale": 2,
                            "generator": (lambda d: {
                                "strategy": "distribution",
                                "distribution": d.get("distribution", "normal"),
                                "params": {
                                    k: v for k, v in d.items() if k != "distribution"
                                },
                            })(self._dist("earnings.surprise_pct", {
                                "distribution": "normal",
                                "mean": 0.0,
                                "sigma": 2.5,
                                "min": -25,
                                "max": 25,
                            })),
                        },
                    },
                },
                "insider_transaction": {
                    "description": "Insider trades from SEC Form 4 filings",
                    "primary_key": ["txn_id"],
                    "columns": {
                        "txn_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "ticker": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "company.ticker",
                            },
                        },
                        "txn_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "insider_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                        "insider_title": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("insider_transaction.insider_title", {
                                    "CEO": 0.15, "CFO": 0.12, "COO": 0.08, "CTO": 0.05,
                                    "Director": 0.25, "VP": 0.15, "SVP": 0.10, "EVP": 0.10,
                                }),
                            },
                        },
                        "txn_type": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("insider_transaction.txn_type", {
                                    "BUY": 0.45,
                                    "SELL": 0.50,
                                    "GRANT": 0.05,
                                }),
                            },
                        },
                        "shares": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 8.0,
                                    "sigma": 1.5,
                                    "min": 100,
                                    "max": 5000000,
                                },
                            },
                        },
                        "price_per_share": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 4.0,
                                    "sigma": 1.0,
                                    "min": 1.00,
                                    "max": 5000.00,
                                },
                            },
                        },
                    },
                },
                "trade": {
                    "description": "Tick-level trades for streaming / RTI demos",
                    "primary_key": ["trade_id"],
                    "columns": {
                        "trade_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "ticker": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "company.ticker",
                                "distribution": "zipf",
                                "params": {"alpha": 1.5},
                            },
                        },
                        "timestamp": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "price": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 4.0,
                                    "sigma": 1.2,
                                    "min": 0.01,
                                    "max": 10000.00,
                                },
                            },
                        },
                        "volume": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 4.5,
                                    "sigma": 1.0,
                                    "min": 1,
                                    "max": 100000,
                                },
                            },
                        },
                        "side": {
                            "type": "string",
                            "max_length": 4,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("trade.side", {
                                    "BUY": 0.50,
                                    "SELL": 0.50,
                                }),
                            },
                        },
                    },
                },
            },
            # ── Relationships ──────────────────────────────────
            "relationships": [
                {
                    "name": "sector_industry",
                    "parent": "sector",
                    "child": "industry",
                    "parent_columns": ["sector_id"],
                    "child_columns": ["sector_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "company_daily_prices",
                    "parent": "company",
                    "child": "daily_price",
                    "parent_columns": ["ticker"],
                    "child_columns": ["ticker"],
                    "type": "one_to_many",
                },
                {
                    "name": "company_dividends",
                    "parent": "company",
                    "child": "dividend",
                    "parent_columns": ["ticker"],
                    "child_columns": ["ticker"],
                    "type": "one_to_many",
                },
                {
                    "name": "company_splits",
                    "parent": "company",
                    "child": "split",
                    "parent_columns": ["ticker"],
                    "child_columns": ["ticker"],
                    "type": "one_to_many",
                },
                {
                    "name": "company_earnings",
                    "parent": "company",
                    "child": "earnings",
                    "parent_columns": ["ticker"],
                    "child_columns": ["ticker"],
                    "type": "one_to_many",
                },
                {
                    "name": "company_insider_txns",
                    "parent": "company",
                    "child": "insider_transaction",
                    "parent_columns": ["ticker"],
                    "child_columns": ["ticker"],
                    "type": "one_to_many",
                },
                {
                    "name": "company_trades",
                    "parent": "company",
                    "child": "trade",
                    "parent_columns": ["ticker"],
                    "child_columns": ["ticker"],
                    "type": "one_to_many",
                },
            ],
            # ── Business rules ─────────────────────────────────
            "business_rules": [
                {
                    "name": "high_gte_low",
                    "type": "cross_column",
                    "table": "daily_price",
                    "rule": "high >= low",
                },
                {
                    "name": "close_between_high_low",
                    "type": "cross_column",
                    "table": "daily_price",
                    "rule": "close >= low AND close <= high",
                },
                {
                    "name": "open_between_high_low",
                    "type": "cross_column",
                    "table": "daily_price",
                    "rule": "open >= low AND open <= high",
                },
                {
                    "name": "volume_positive",
                    "type": "constraint",
                    "table": "daily_price",
                    "rule": "volume > 0",
                },
                {
                    "name": "dividend_amount_positive",
                    "type": "constraint",
                    "table": "dividend",
                    "rule": "amount > 0",
                },
                {
                    "name": "pay_date_after_ex_date",
                    "type": "cross_column",
                    "table": "dividend",
                    "rule": "pay_date > ex_date",
                },
                {
                    "name": "shares_positive",
                    "type": "constraint",
                    "table": "insider_transaction",
                    "rule": "shares > 0",
                },
                {
                    "name": "trade_volume_positive",
                    "type": "constraint",
                    "table": "trade",
                    "rule": "volume > 0",
                },
            ],
            # ── Generation config ──────────────────────────────
            "generation": {
                "scale": "small",
                "scales": {
                    "demo": {"company": 30, "daily_price_years": 1},
                    "small": {"company": 100, "daily_price_years": 3},
                    "medium": {"company": 500, "daily_price_years": 5},
                    "large": {"company": 1000, "daily_price_years": 10},
                    "xlarge": {"company": 4000, "daily_price_years": 20},
                },
                "derived_counts": {
                    "exchange": {"fixed": 3},
                    "sector": {"fixed": 11},
                    "industry": {"fixed": 61},
                    "daily_price": {
                        "per_parent": "company",
                        "ratio": self._ratio("daily_price_per_company_per_year", 252),
                    },
                    "dividend": {
                        "per_parent": "company",
                        "ratio": self._ratio("dividend_per_company_per_year", 1.5),
                    },
                    "split": {
                        "per_parent": "company",
                        "ratio": self._ratio("split_per_company_per_year", 0.05),
                    },
                    "earnings": {
                        "per_parent": "company",
                        "ratio": self._ratio("earnings_per_company_per_year", 4.0),
                    },
                    "insider_transaction": {
                        "per_parent": "company",
                        "ratio": self._ratio("insider_txn_per_company_per_year", 2.0),
                    },
                    "trade": {
                        "per_parent": "company",
                        "ratio": self._ratio("trade_per_company_per_year", 1000),
                    },
                },
                "output": {"format": "dataframe"},
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return star schema mapping for the Capital Markets domain.

        Produces:
          - dim_company  (from company)
          - dim_exchange (from exchange)
          - dim_sector   (from sector, enriched with industry)
          - dim_date     (generated from trade_date / report_date / ex_date)
          - fact_daily_price (from daily_price)
          - fact_dividend    (from dividend)
          - fact_earnings    (from earnings)
          - fact_insider_txn (from insider_transaction)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_company": DimSpec(
                    source="company",
                    sk="sk_company",
                    nk="ticker",
                ),
                "dim_exchange": DimSpec(
                    source="exchange",
                    sk="sk_exchange",
                    nk="exchange_id",
                ),
                "dim_sector": DimSpec(
                    source="sector",
                    sk="sk_sector",
                    nk="sector_id",
                    enrich=[{
                        "table": "industry",
                        "left_on": "sector_id",
                        "right_on": "sector_id",
                        "prefix": "ind_",
                    }],
                ),
            },
            facts={
                "fact_daily_price": FactSpec(
                    primary="daily_price",
                    joins=[{
                        "table": "company",
                        "left_on": "ticker",
                        "right_on": "ticker",
                    }],
                    fk_map={
                        "ticker": "dim_company",
                    },
                    date_cols=["trade_date"],
                ),
                "fact_dividend": FactSpec(
                    primary="dividend",
                    joins=[{
                        "table": "company",
                        "left_on": "ticker",
                        "right_on": "ticker",
                    }],
                    fk_map={
                        "ticker": "dim_company",
                    },
                    date_cols=["ex_date"],
                ),
                "fact_earnings": FactSpec(
                    primary="earnings",
                    joins=[{
                        "table": "company",
                        "left_on": "ticker",
                        "right_on": "ticker",
                    }],
                    fk_map={
                        "ticker": "dim_company",
                    },
                    date_cols=["report_date"],
                ),
                "fact_insider_txn": FactSpec(
                    primary="insider_transaction",
                    joins=[{
                        "table": "company",
                        "left_on": "ticker",
                        "right_on": "ticker",
                    }],
                    fk_map={
                        "ticker": "dim_company",
                    },
                    date_cols=["txn_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Capital Markets domain."""
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "company": "Account",
            "exchange": "Organization",
            "sector": "Category",
            "industry": "Category",
            "daily_price": "Observation",
            "dividend": "Transaction",
            "split": "Transaction",
            "earnings": "Observation",
            "insider_transaction": "Transaction",
            "trade": "Transaction",
        })
