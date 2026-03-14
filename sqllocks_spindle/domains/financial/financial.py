"""Financial services domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class FinancialDomain(Domain):
    """Banking and financial services domain.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - branch: Bank branches with locations
        - customer: Bank customers with credit tiers
        - account: Checking, savings, investment accounts
        - transaction: Deposits, withdrawals, transfers, payments
        - transaction_category: Category hierarchy
        - loan: Mortgages, auto, personal, student loans
        - loan_payment: Monthly loan payments
        - card: Credit/debit cards linked to accounts
        - fraud_flag: Suspicious transaction flags
        - statement: Monthly account statements
    """

    @property
    def name(self) -> str:
        return "financial"

    @property
    def description(self) -> str:
        return "Banking domain with accounts, transactions, loans, and fraud detection"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build financial 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"financial_{self._schema_mode}",
                "description": f"Financial services domain — {self._schema_mode} schema",
                "domain": "financial",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── branch ──────────────────────────────────────
                "branch": {
                    "description": "Bank branches with locations",
                    "primary_key": ["branch_id"],
                    "columns": {
                        "branch_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "branch_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "branch_names",
                            },
                        },
                        "city": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "us_zip_locations",
                                "field": "city",
                            },
                        },
                        "state": {
                            "type": "string",
                            "max_length": 2,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "state",
                            },
                        },
                        "zip_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "zip",
                            },
                        },
                        "lat": {
                            "type": "float",
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lat",
                            },
                        },
                        "lng": {
                            "type": "float",
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lng",
                            },
                        },
                        "opened_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2000-01-01",
                                "end": "2024-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("branch.is_active", {"true": 0.92, "false": 0.08}),
                            },
                        },
                    },
                },

                # ── transaction_category ────────────────────────
                "transaction_category": {
                    "description": "Transaction category hierarchy",
                    "primary_key": ["category_id"],
                    "columns": {
                        "category_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "category_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "transaction_categories",
                            },
                        },
                        "parent_category_id": {
                            "type": "integer",
                            "nullable": True,
                            "generator": {
                                "strategy": "self_referencing",
                                "pk_column": "category_id",
                                "root_count": 8,
                                "max_depth": 2,
                            },
                        },
                        "level": {
                            "type": "integer",
                            "generator": {
                                "strategy": "self_ref_field",
                                "field": "level",
                            },
                        },
                    },
                },

                # ── customer ────────────────────────────────────
                "customer": {
                    "description": "Bank customers with demographics and credit tiers",
                    "primary_key": ["customer_id"],
                    "columns": {
                        "customer_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "first_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "first_name"},
                        },
                        "last_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "last_name"},
                        },
                        "email": {
                            "type": "string",
                            "max_length": 255,
                            "nullable": True,
                            "null_rate": 0.03,
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "phone": {
                            "type": "string",
                            "max_length": 20,
                            "nullable": True,
                            "null_rate": 0.10,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                        "date_of_birth": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "1945-01-01",
                                "end": "2005-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "credit_tier": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("customer.credit_tier", {
                                    "Excellent": 0.22,
                                    "Good": 0.35,
                                    "Fair": 0.28,
                                    "Poor": 0.15,
                                }),
                            },
                        },
                        "customer_since": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2010-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                        "primary_branch_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "branch.branch_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("customer.is_active", {"true": 0.88, "false": 0.12}),
                            },
                        },
                    },
                },

                # ── account ─────────────────────────────────────
                "account": {
                    "description": "Customer bank accounts",
                    "primary_key": ["account_id"],
                    "columns": {
                        "account_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "customer_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "customer.customer_id",
                            },
                        },
                        "account_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("account.account_type", {
                                    "Checking": 0.45,
                                    "Savings": 0.30,
                                    "Money Market": 0.10,
                                    "CD": 0.08,
                                    "Investment": 0.07,
                                }),
                            },
                        },
                        "opened_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2015-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                        "current_balance": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 8.5,
                                "sigma": 1.8,
                                "min": 0.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                        "currency": {
                            "type": "string",
                            "max_length": 3,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"USD": 1.0},
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("account.status", {
                                    "Active": 0.85,
                                    "Dormant": 0.08,
                                    "Closed": 0.05,
                                    "Frozen": 0.02,
                                }),
                            },
                        },
                    },
                },

                # ── card ────────────────────────────────────────
                "card": {
                    "description": "Credit and debit cards linked to accounts",
                    "primary_key": ["card_id"],
                    "columns": {
                        "card_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "account_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "account.account_id",
                            },
                        },
                        "card_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("card.card_type", {
                                    "Debit": 0.55,
                                    "Credit": 0.35,
                                    "Prepaid": 0.10,
                                }),
                            },
                        },
                        "card_network": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("card.card_network", {
                                    "Visa": 0.52,
                                    "Mastercard": 0.24,
                                    "Amex": 0.15,
                                    "Discover": 0.09,
                                }),
                            },
                        },
                        "last_four": {
                            "type": "string",
                            "max_length": 4,
                            "generator": {
                                "strategy": "pattern",
                                "format": "{seq:4}",
                            },
                        },
                        "issued_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                        "expiry_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "issued_date",
                                "operation": "add_days",
                                "days": 1095,
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"true": 0.90, "false": 0.10},
                            },
                        },
                    },
                },

                # ── transaction ─────────────────────────────────
                "transaction": {
                    "description": "Financial transactions (deposits, withdrawals, transfers, payments)",
                    "primary_key": ["transaction_id"],
                    "columns": {
                        "transaction_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "account_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "account.account_id",
                                **self._dist("transaction.account_id", {
                                    "distribution": "pareto",
                                    "alpha": 1.2,
                                    "max_per_parent": 200,
                                }),
                            },
                        },
                        "category_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "transaction_category.category_id",
                                "distribution": "zipf",
                                "alpha": 1.4,
                            },
                        },
                        "transaction_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("transaction.transaction_type", {
                                    "Deposit": 0.25,
                                    "Withdrawal": 0.20,
                                    "Transfer": 0.18,
                                    "Payment": 0.22,
                                    "Fee": 0.05,
                                    "Interest": 0.05,
                                    "Refund": 0.05,
                                }),
                            },
                        },
                        "amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 4.2,
                                "sigma": 1.5,
                                "min": 0.01,
                                "max": 100000.0,
                                "round": 2,
                            },
                        },
                        "transaction_date": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "seasonal",
                                "month_weights": self._dist("transaction.transaction_date.month", {
                                    "Jan": 0.078, "Feb": 0.072, "Mar": 0.083, "Apr": 0.082,
                                    "May": 0.085, "Jun": 0.083, "Jul": 0.084, "Aug": 0.085,
                                    "Sep": 0.082, "Oct": 0.084, "Nov": 0.088, "Dec": 0.094,
                                }),
                                "day_of_week_weights": self._dist("transaction.transaction_date.day_of_week", {
                                    "Mon": 0.165, "Tue": 0.160, "Wed": 0.155,
                                    "Thu": 0.155, "Fri": 0.160, "Sat": 0.105, "Sun": 0.100,
                                }),
                            },
                        },
                        "channel": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("transaction.channel", {
                                    "Online": 0.35,
                                    "Mobile": 0.30,
                                    "ATM": 0.15,
                                    "Branch": 0.12,
                                    "Phone": 0.05,
                                    "Wire": 0.03,
                                }),
                            },
                        },
                        "merchant_name": {
                            "type": "string",
                            "max_length": 200,
                            "nullable": True,
                            "null_rate": 0.30,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "merchant_names",
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("transaction.status", {
                                    "Completed": 0.92,
                                    "Pending": 0.04,
                                    "Failed": 0.02,
                                    "Reversed": 0.02,
                                }),
                            },
                        },
                    },
                },

                # ── loan ────────────────────────────────────────
                "loan": {
                    "description": "Customer loans (mortgage, auto, personal, student)",
                    "primary_key": ["loan_id"],
                    "columns": {
                        "loan_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "customer_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "customer.customer_id",
                            },
                        },
                        "loan_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("loan.loan_type", {
                                    "Mortgage": 0.35,
                                    "Auto": 0.25,
                                    "Personal": 0.20,
                                    "Student": 0.12,
                                    "Business": 0.08,
                                }),
                            },
                        },
                        "principal_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 10.5,
                                "sigma": 1.2,
                                "min": 1000.0,
                                "max": 2000000.0,
                                "round": 2,
                            },
                        },
                        "interest_rate": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 2.5,
                                "max": 18.0,
                                "round": 2,
                            },
                        },
                        "term_months": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("loan.term_months", {
                                    "12": 0.05,
                                    "24": 0.08,
                                    "36": 0.12,
                                    "48": 0.10,
                                    "60": 0.15,
                                    "120": 0.10,
                                    "180": 0.10,
                                    "240": 0.10,
                                    "360": 0.20,
                                }),
                            },
                        },
                        "origination_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2018-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                        "outstanding_balance": {
                            "type": "float",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "principal_amount",
                                "operation": "multiply",
                                "params": {"factor_min": 0.10, "factor_max": 0.95},
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("loan.status", {
                                    "Current": 0.78,
                                    "Paid Off": 0.10,
                                    "Delinquent": 0.06,
                                    "Default": 0.03,
                                    "Forbearance": 0.03,
                                }),
                            },
                        },
                    },
                },

                # ── loan_payment ────────────────────────────────
                "loan_payment": {
                    "description": "Monthly loan payments",
                    "primary_key": ["payment_id"],
                    "columns": {
                        "payment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "loan_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "loan.loan_id",
                            },
                        },
                        "payment_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "payment_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 7.0,
                                "sigma": 1.0,
                                "min": 50.0,
                                "max": 50000.0,
                                "round": 2,
                            },
                        },
                        "principal_portion": {
                            "type": "float",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "payment_amount",
                                "operation": "multiply",
                                "params": {"factor_min": 0.30, "factor_max": 0.70},
                            },
                        },
                        "interest_portion": {
                            "type": "float",
                            "generator": {
                                "strategy": "formula",
                                "expression": "payment_amount - principal_portion",
                            },
                        },
                        "payment_method": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("loan_payment.payment_method", {
                                    "Auto-debit": 0.55,
                                    "Online": 0.25,
                                    "Check": 0.10,
                                    "Branch": 0.07,
                                    "Phone": 0.03,
                                }),
                            },
                        },
                    },
                },

                # ── fraud_flag ──────────────────────────────────
                "fraud_flag": {
                    "description": "Suspicious transaction flags for fraud detection",
                    "primary_key": ["flag_id"],
                    "columns": {
                        "flag_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "transaction_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "transaction.transaction_id",
                            },
                        },
                        "flag_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("fraud_flag.flag_type", {
                                    "Unusual Amount": 0.30,
                                    "Unusual Location": 0.25,
                                    "Rapid Succession": 0.15,
                                    "Account Takeover": 0.10,
                                    "Card Not Present": 0.10,
                                    "Velocity Check": 0.10,
                                }),
                            },
                        },
                        "risk_score": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.50,
                                "max": 1.00,
                                "round": 3,
                            },
                        },
                        "flagged_date": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "resolution": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("fraud_flag.resolution", {
                                    "Confirmed Fraud": 0.15,
                                    "False Positive": 0.55,
                                    "Under Review": 0.20,
                                    "Customer Verified": 0.10,
                                }),
                            },
                        },
                    },
                },

                # ── statement ───────────────────────────────────
                "statement": {
                    "description": "Monthly account statements",
                    "primary_key": ["statement_id"],
                    "columns": {
                        "statement_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "account_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "account.account_id",
                            },
                        },
                        "statement_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2024-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                        "opening_balance": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 8.0,
                                "sigma": 1.5,
                                "min": 0.0,
                                "max": 2000000.0,
                                "round": 2,
                            },
                        },
                        "closing_balance": {
                            "type": "float",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "opening_balance",
                                "operation": "multiply",
                                "params": {"factor_min": 0.85, "factor_max": 1.15},
                            },
                        },
                        "total_deposits": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 7.5,
                                "sigma": 1.2,
                                "min": 0.0,
                                "max": 500000.0,
                                "round": 2,
                            },
                        },
                        "total_withdrawals": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 7.3,
                                "sigma": 1.2,
                                "min": 0.0,
                                "max": 500000.0,
                                "round": 2,
                            },
                        },
                        "fees_charged": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 2.0,
                                "sigma": 1.0,
                                "min": 0.0,
                                "max": 500.0,
                                "round": 2,
                            },
                        },
                    },
                },
            },

            # ── relationships ───────────────────────────────
            "relationships": [
                {
                    "name": "customer_primary_branch",
                    "parent": "branch",
                    "child": "customer",
                    "parent_key": "branch_id",
                    "child_key": "primary_branch_id",
                },
                {
                    "name": "customer_accounts",
                    "parent": "customer",
                    "child": "account",
                    "parent_key": "customer_id",
                    "child_key": "customer_id",
                },
                {
                    "name": "account_cards",
                    "parent": "account",
                    "child": "card",
                    "parent_key": "account_id",
                    "child_key": "account_id",
                },
                {
                    "name": "account_transactions",
                    "parent": "account",
                    "child": "transaction",
                    "parent_key": "account_id",
                    "child_key": "account_id",
                },
                {
                    "name": "transaction_category_ref",
                    "parent": "transaction_category",
                    "child": "transaction",
                    "parent_key": "category_id",
                    "child_key": "category_id",
                },
                {
                    "name": "customer_loans",
                    "parent": "customer",
                    "child": "loan",
                    "parent_key": "customer_id",
                    "child_key": "customer_id",
                },
                {
                    "name": "loan_payments",
                    "parent": "loan",
                    "child": "loan_payment",
                    "parent_key": "loan_id",
                    "child_key": "loan_id",
                },
                {
                    "name": "transaction_fraud_flags",
                    "parent": "transaction",
                    "child": "fraud_flag",
                    "parent_key": "transaction_id",
                    "child_key": "transaction_id",
                },
                {
                    "name": "account_statements",
                    "parent": "account",
                    "child": "statement",
                    "parent_key": "account_id",
                    "child_key": "account_id",
                },
            ],

            # ── business rules ──────────────────────────────
            "business_rules": [
                {
                    "name": "account_opened_after_customer_since",
                    "description": "Account opened_date must be >= customer customer_since",
                    "type": "temporal_order",
                    "tables": ["account", "customer"],
                    "join_key": "customer_id",
                    "earlier": "customer_since",
                    "later": "opened_date",
                },
                {
                    "name": "loan_after_customer_since",
                    "description": "Loan origination_date must be >= customer customer_since",
                    "type": "temporal_order",
                    "tables": ["loan", "customer"],
                    "join_key": "customer_id",
                    "earlier": "customer_since",
                    "later": "origination_date",
                },
                {
                    "name": "outstanding_leq_principal",
                    "description": "Loan outstanding_balance must be <= principal_amount",
                    "type": "column_comparison",
                    "table": "loan",
                    "left": "outstanding_balance",
                    "operator": "<=",
                    "right": "principal_amount",
                },
            ],

            # ── generation config ───────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"customer": 100, "transaction": 1000},
                    "small": {"customer": 1000, "transaction": 10000},
                    "medium": {"customer": 50000, "transaction": 500000},
                    "large": {"customer": 500000, "transaction": 5000000},
                    "xlarge": {"customer": 5000000, "transaction": 100000000},
                    "warehouse": {"customer": 10000000, "transaction": 2000000000},
                },
                "derived_counts": {
                    "branch": {"fixed": 200},
                    "transaction_category": {"fixed": 40},
                    "account": {"per_parent": "customer", "ratio": self._ratio("account_per_customer", 2.2)},
                    "card": {"per_parent": "account", "ratio": self._ratio("card_per_account", 0.8)},
                    "loan": {"per_parent": "customer", "ratio": self._ratio("loan_per_customer", 0.4)},
                    "loan_payment": {"per_parent": "loan", "ratio": self._ratio("loan_payment_per_loan", 12)},
                    "fraud_flag": {"per_parent": "transaction", "ratio": self._ratio("fraud_flag_per_transaction", 0.02)},
                    "statement": {"per_parent": "account", "ratio": self._ratio("statement_per_account", 6)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the Financial domain.

        Produces:
          - dim_customer (from customer)
          - dim_branch   (from branch)
          - dim_account  (from account)
          - dim_category (from transaction_category)
          - fact_transaction  (from transaction)
          - fact_loan_payment (from loan_payment + loan)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_customer": DimSpec(
                    source="customer",
                    sk="sk_customer",
                    nk="customer_id",
                ),
                "dim_branch": DimSpec(
                    source="branch",
                    sk="sk_branch",
                    nk="branch_id",
                ),
                "dim_account": DimSpec(
                    source="account",
                    sk="sk_account",
                    nk="account_id",
                ),
                "dim_category": DimSpec(
                    source="transaction_category",
                    sk="sk_category",
                    nk="category_id",
                ),
            },
            facts={
                "fact_transaction": FactSpec(
                    primary="transaction",
                    fk_map={
                        "account_id": "dim_account",
                        "category_id": "dim_category",
                    },
                    date_cols=["transaction_date"],
                ),
                "fact_loan_payment": FactSpec(
                    primary="loan_payment",
                    joins=[{
                        "table": "loan",
                        "left_on": "loan_id",
                        "right_on": "loan_id",
                    }],
                    fk_map={"customer_id": "dim_customer"},
                    date_cols=["payment_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Financial domain.

        Maps source table names to Microsoft Common Data Model entity names.
        """
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "customer": "Contact",
            "branch": "Account",
            "account": "FinancialAccount",
            "card": "FinancialProduct",
            "transaction": "Transaction",
            "loan": "Loan",
            "loan_payment": "Payment",
            "fraud_flag": "Alert",
            "transaction_category": "Category",
            "statement": "Document",
        })
