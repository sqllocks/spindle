"""Telecom domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class TelecomDomain(Domain):
    """Telecom domain — subscribers, service lines, usage, billing, and churn.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - plan: Service plans
        - device_model: Phone/device models
        - subscriber: Customers
        - service_line: Phone lines
        - usage_record: CDR/usage
        - billing: Monthly bills
        - payment: Bill payments
        - network_event: Network events
        - churn_indicator: Churn predictions
    """

    @property
    def name(self) -> str:
        return "telecom"

    @property
    def description(self) -> str:
        return "Telecom domain with subscribers, service lines, usage records, billing, and churn"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build telecom 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"telecom_{self._schema_mode}",
                "description": f"Telecom domain — {self._schema_mode} schema",
                "domain": "telecom",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── plan ───────────────────────────────────────────
                "plan": {
                    "description": "Telecom service plans",
                    "primary_key": ["plan_id"],
                    "columns": {
                        "plan_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "plan_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "plan_types",
                            },
                        },
                        "plan_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("plan.plan_type", {
                                    "Prepaid": 0.25,
                                    "Postpaid": 0.35,
                                    "Family": 0.25,
                                    "Business": 0.15,
                                }),
                            },
                        },
                        "monthly_rate": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 20.0,
                                "max": 200.0,
                                "round": 2,
                            },
                        },
                        "data_limit_gb": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("plan.data_limit_gb", {
                                    "5": 0.10,
                                    "10": 0.15,
                                    "25": 0.20,
                                    "50": 0.20,
                                    "100": 0.15,
                                    "Unlimited": 0.20,
                                }),
                            },
                        },
                        "talk_minutes": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("plan.talk_minutes", {
                                    "300": 0.15,
                                    "500": 0.20,
                                    "1000": 0.30,
                                    "Unlimited": 0.35,
                                }),
                            },
                        },
                    },
                },

                # ── device_model ───────────────────────────────────
                "device_model": {
                    "description": "Phone and device models",
                    "primary_key": ["model_id"],
                    "columns": {
                        "model_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "model_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "device_models",
                            },
                        },
                        "manufacturer": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("device_model.manufacturer", {
                                    "Samsung": 0.28,
                                    "Apple": 0.35,
                                    "Google": 0.15,
                                    "OnePlus": 0.10,
                                    "Motorola": 0.12,
                                }),
                            },
                        },
                        "device_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("device_model.device_type", {
                                    "Smartphone": 0.70,
                                    "Tablet": 0.12,
                                    "Hotspot": 0.08,
                                    "Watch": 0.10,
                                }),
                            },
                        },
                        "retail_price": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 6.0,
                                "sigma": 0.8,
                                "min": 50.00,
                                "max": 2000.00,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── subscriber ─────────────────────────────────────
                "subscriber": {
                    "description": "Telecom customers",
                    "primary_key": ["subscriber_id"],
                    "columns": {
                        "subscriber_id": {
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
                            "null_rate": 0.05,
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "phone": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                        "address_city": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "us_zip_locations",
                                "field": "city",
                            },
                        },
                        "address_state": {
                            "type": "string",
                            "max_length": 2,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "state",
                            },
                        },
                        "address_zip": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "zip",
                            },
                        },
                        "account_status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("subscriber.account_status", {
                                    "Active": 0.85,
                                    "Suspended": 0.05,
                                    "Cancelled": 0.10,
                                }),
                            },
                        },
                        "credit_class": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("subscriber.credit_class", {
                                    "A": 0.30,
                                    "B": 0.35,
                                    "C": 0.25,
                                    "D": 0.10,
                                }),
                            },
                        },
                        "signup_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2018-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                    },
                },

                # ── service_line ───────────────────────────────────
                "service_line": {
                    "description": "Phone/data lines linked to subscribers",
                    "primary_key": ["line_id"],
                    "columns": {
                        "line_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "subscriber_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "subscriber.subscriber_id",
                            },
                        },
                        "plan_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "plan.plan_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "device_model_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "device_model.model_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "phone_number": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {
                                "strategy": "pattern",
                                "format": "555-{seq:7}",
                            },
                        },
                        "activation_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("service_line.status", {
                                    "Active": 0.82,
                                    "Suspended": 0.08,
                                    "Disconnected": 0.10,
                                }),
                            },
                        },
                        "is_primary": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("service_line.is_primary", {"true": 0.55, "false": 0.45}),
                            },
                        },
                    },
                },

                # ── usage_record ───────────────────────────────────
                "usage_record": {
                    "description": "Call detail records and usage data",
                    "primary_key": ["record_id"],
                    "columns": {
                        "record_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "line_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "service_line.line_id",
                            },
                        },
                        "record_date": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "record_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("usage_record.record_type", {
                                    "Voice": 0.25,
                                    "Data": 0.45,
                                    "SMS": 0.25,
                                    "MMS": 0.05,
                                }),
                            },
                        },
                        "duration_seconds": {
                            "type": "integer",
                            "nullable": True,
                            "null_rate": 0.70,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 4.5,
                                "sigma": 1.2,
                                "min": 1,
                                "max": 7200,
                            },
                        },
                        "data_mb": {
                            "type": "float",
                            "nullable": True,
                            "null_rate": 0.50,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.0,
                                "sigma": 1.5,
                                "min": 0.01,
                                "max": 5000.00,
                                "round": 2,
                            },
                        },
                        "destination": {
                            "type": "string",
                            "max_length": 20,
                            "nullable": True,
                            "null_rate": 0.45,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                    },
                },

                # ── billing ────────────────────────────────────────
                "billing": {
                    "description": "Monthly subscriber bills",
                    "primary_key": ["bill_id"],
                    "columns": {
                        "bill_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "subscriber_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "subscriber.subscriber_id",
                            },
                        },
                        "billing_period_start": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "billing_period_end": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "billing_period_start",
                                "operation": "add_days",
                                "days": 30,
                            },
                        },
                        "total_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 4.5,
                                "sigma": 0.5,
                                "min": 20.00,
                                "max": 500.00,
                                "round": 2,
                            },
                        },
                        "payment_status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("billing.payment_status", {
                                    "Paid": 0.75,
                                    "Pending": 0.10,
                                    "Overdue": 0.10,
                                    "Partial": 0.05,
                                }),
                            },
                        },
                        "due_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "billing_period_end",
                                "operation": "add_days",
                                "days": 15,
                            },
                        },
                    },
                },

                # ── payment ────────────────────────────────────────
                "payment": {
                    "description": "Bill payments",
                    "primary_key": ["payment_id"],
                    "columns": {
                        "payment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "bill_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "billing.bill_id",
                            },
                        },
                        "payment_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-15",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 4.3,
                                "sigma": 0.5,
                                "min": 10.0,
                                "max": 500.0,
                                "round": 2,
                            },
                        },
                        "payment_method": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("payment.payment_method", {
                                    "Auto-Pay": 0.40,
                                    "Online": 0.25,
                                    "In-Store": 0.15,
                                    "Phone": 0.10,
                                    "Mail": 0.10,
                                }),
                            },
                        },
                    },
                },

                # ── network_event ──────────────────────────────────
                "network_event": {
                    "description": "Network events and incidents",
                    "primary_key": ["event_id"],
                    "columns": {
                        "event_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "line_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "service_line.line_id",
                            },
                        },
                        "event_type": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "network_event_types",
                            },
                        },
                        "event_timestamp": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "cell_tower_id": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {
                                "strategy": "pattern",
                                "format": "TWR-{seq:5}",
                            },
                        },
                        "signal_strength_dbm": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": -120,
                                "max": -50,
                            },
                        },
                    },
                },

                # ── churn_indicator ────────────────────────────────
                "churn_indicator": {
                    "description": "Churn prediction indicators per subscriber",
                    "primary_key": ["indicator_id"],
                    "columns": {
                        "indicator_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "subscriber_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "subscriber.subscriber_id",
                            },
                        },
                        "assessment_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "churn_score": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.0,
                                "max": 1.0,
                                "round": 3,
                            },
                        },
                        "risk_level": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("churn_indicator.risk_level", {
                                    "Low": 0.40,
                                    "Medium": 0.30,
                                    "High": 0.20,
                                    "Very High": 0.10,
                                }),
                            },
                        },
                        "primary_factor": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("churn_indicator.primary_factor", {
                                    "Price": 0.25,
                                    "Service Quality": 0.20,
                                    "Competition": 0.20,
                                    "Coverage": 0.15,
                                    "Life Event": 0.20,
                                }),
                            },
                        },
                        "months_remaining": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0,
                                "max": 24,
                            },
                        },
                    },
                },
            },

            # ── relationships ──────────────────────────────────────
            "relationships": [
                {
                    "name": "service_line_subscriber",
                    "parent": "subscriber",
                    "child": "service_line",
                    "parent_key": "subscriber_id",
                    "child_key": "subscriber_id",
                },
                {
                    "name": "service_line_plan",
                    "parent": "plan",
                    "child": "service_line",
                    "parent_key": "plan_id",
                    "child_key": "plan_id",
                },
                {
                    "name": "service_line_device_model",
                    "parent": "device_model",
                    "child": "service_line",
                    "parent_key": "model_id",
                    "child_key": "device_model_id",
                },
                {
                    "name": "usage_record_line",
                    "parent": "service_line",
                    "child": "usage_record",
                    "parent_key": "line_id",
                    "child_key": "line_id",
                },
                {
                    "name": "billing_subscriber",
                    "parent": "subscriber",
                    "child": "billing",
                    "parent_key": "subscriber_id",
                    "child_key": "subscriber_id",
                },
                {
                    "name": "payment_billing",
                    "parent": "billing",
                    "child": "payment",
                    "parent_key": "bill_id",
                    "child_key": "bill_id",
                },
                {
                    "name": "network_event_line",
                    "parent": "service_line",
                    "child": "network_event",
                    "parent_key": "line_id",
                    "child_key": "line_id",
                },
                {
                    "name": "churn_indicator_subscriber",
                    "parent": "subscriber",
                    "child": "churn_indicator",
                    "parent_key": "subscriber_id",
                    "child_key": "subscriber_id",
                },
            ],

            # ── business rules ─────────────────────────────────────
            "business_rules": [
                {
                    "name": "billing_total_positive",
                    "type": "constraint",
                    "table": "billing",
                    "rule": "total_amount > 0",
                },
                {
                    "name": "payment_amount_positive",
                    "type": "constraint",
                    "table": "payment",
                    "rule": "amount > 0",
                },
                {
                    "name": "churn_score_range",
                    "description": "Churn score must be between 0 and 1",
                    "type": "constraint",
                    "table": "churn_indicator",
                    "rule": "churn_score >= 0 AND churn_score <= 1",
                },
            ],

            # ── generation config ──────────────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"subscriber": 200},
                    "small": {"subscriber": 2000},
                    "medium": {"subscriber": 20000},
                    "large": {"subscriber": 200000},
                    "warehouse": {"subscriber": 2000000},
                },
                "derived_counts": {
                    "plan": {"fixed": 20},
                    "device_model": {"fixed": 40},
                    "service_line": {"per_parent": "subscriber", "ratio": self._ratio("service_line_per_subscriber", 1.8)},
                    "usage_record": {"per_parent": "service_line", "ratio": self._ratio("usage_record_per_line", 30.0)},
                    "billing": {"per_parent": "subscriber", "ratio": self._ratio("billing_per_subscriber", 6.0)},
                    "payment": {"per_parent": "billing", "ratio": self._ratio("payment_per_billing", 1.0)},
                    "network_event": {"per_parent": "service_line", "ratio": self._ratio("network_event_per_line", 2.0)},
                    "churn_indicator": {"per_parent": "subscriber", "ratio": self._ratio("churn_indicator_per_subscriber", 1.0)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)
