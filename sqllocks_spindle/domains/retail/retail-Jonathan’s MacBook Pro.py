"""Retail domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class RetailDomain(Domain):
    """Retail / E-Commerce domain.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships
        - star: Star schema for analytics (Phase 0+)

    Tables (3NF mode):
        - customer: Individual customers
        - address: Customer shipping/billing addresses (1:N → customer)
        - product_category: Product category hierarchy
        - product: Individual SKUs
        - store: Physical and online stores
        - promotion: Marketing promotions
        - order: Order headers (FK → customer, address, store)
        - order_line: Order line items (FK → order, product)
        - return: Return transactions (FK → order)
    """

    @property
    def name(self) -> str:
        return "retail"

    @property
    def description(self) -> str:
        return "Retail / E-Commerce domain with customers, products, orders, and returns"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build retail 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"retail_{self._schema_mode}",
                "description": f"Retail domain — {self._schema_mode} schema",
                "domain": "retail",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                "customer": {
                    "description": "Individual customers",
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
                            "null_rate": 0.05,
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "gender": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"M": 0.48, "F": 0.50, "NB": 0.02},
                            },
                        },
                        "loyalty_tier": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {
                                    "Basic": 0.80,
                                    "Silver": 0.12,
                                    "Gold": 0.06,
                                    "Platinum": 0.02,
                                },
                            },
                        },
                        "signup_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "is_active": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"true": 0.85, "false": 0.15},
                            },
                        },
                    },
                },
                "address": {
                    "description": "Customer shipping and billing addresses",
                    "primary_key": ["address_id"],
                    "columns": {
                        "address_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "customer_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "customer.customer_id",
                                "distribution": "uniform",
                            },
                        },
                        "address_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"shipping": 0.60, "billing": 0.30, "both": 0.10},
                            },
                        },
                        "street": {
                            "type": "string",
                            "generator": {"strategy": "faker", "provider": "street_address"},
                        },
                        # city is the record_sample anchor — samples one ZIP record per row
                        # and stashes all fields (zip, state, lat, lng) for the columns below.
                        "city": {
                            "type": "string",
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
                            "type": "decimal",
                            "precision": 9,
                            "scale": 6,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lat",
                            },
                        },
                        "lng": {
                            "type": "decimal",
                            "precision": 9,
                            "scale": 6,
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lng",
                            },
                        },
                    },
                },
                "product_category": {
                    "description": "Product category hierarchy — 3 levels (department > category > subcategory)",
                    "primary_key": ["category_id"],
                    "columns": {
                        "category_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "category_name": {
                            "type": "string",
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "categories",
                            },
                        },
                        # self_referencing builds the hierarchy and stashes level info
                        "parent_category_id": {
                            "type": "integer",
                            "nullable": True,
                            "null_rate": 0.0,
                            "generator": {
                                "strategy": "self_referencing",
                                "pk_column": "category_id",
                                "levels": 3,
                                "root_count": 8,
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
                "product": {
                    "description": "Individual products (SKUs)",
                    "primary_key": ["product_id"],
                    "columns": {
                        "product_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "product_name": {
                            "type": "string",
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "product_names",
                            },
                        },
                        "category_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "product_category.category_id",
                            },
                        },
                        "unit_price": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 3.0,
                                    "sigma": 1.0,
                                    "min": 0.99,
                                    "max": 999.99,
                                },
                            },
                        },
                        "cost": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "unit_price",
                                "rule": "multiply",
                                "params": {"factor_min": 0.30, "factor_max": 0.70},
                            },
                        },
                        "product_status": {
                            "type": "string",
                            "generator": {
                                "strategy": "lifecycle",
                                "phases": {
                                    "introduced": 0.10,
                                    "active": 0.75,
                                    "discontinued": 0.15,
                                },
                            },
                        },
                    },
                },
                "store": {
                    "description": "Physical and online store locations",
                    "primary_key": ["store_id"],
                    "columns": {
                        "store_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "store_name": {
                            "type": "string",
                            "generator": {
                                "strategy": "pattern",
                                "format": "Store #{seq:4}",
                            },
                        },
                        "store_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {
                                    "physical": 0.85,
                                    "online": 0.10,
                                    "warehouse": 0.05,
                                },
                            },
                        },
                        "city": {
                            "type": "string",
                            "generator": {"strategy": "faker", "provider": "city"},
                        },
                        "state": {
                            "type": "string",
                            "max_length": 2,
                            "generator": {"strategy": "faker", "provider": "state_abbr"},
                        },
                    },
                },
                "promotion": {
                    "description": "Marketing promotions",
                    "primary_key": ["promotion_id"],
                    "columns": {
                        "promotion_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "promo_name": {
                            "type": "string",
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "promo_names",
                            },
                        },
                        "promo_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {
                                    "percent_off": 0.40,
                                    "bogo": 0.20,
                                    "fixed_amount": 0.25,
                                    "bundle": 0.10,
                                    "clearance": 0.05,
                                },
                            },
                        },
                        "discount_pct": {
                            "type": "decimal",
                            "precision": 5,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "params": {"min": 5.0, "max": 50.0},
                            },
                        },
                        "start_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                        "end_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "uniform",
                                "range_ref": "model.date_range",
                            },
                        },
                    },
                },
                "order": {
                    "description": "Customer orders (header)",
                    "primary_key": ["order_id"],
                    "columns": {
                        "order_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "customer_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "customer.customer_id",
                                "distribution": "pareto",
                                "params": {"alpha": 1.2, "max_per_parent": 50},
                            },
                        },
                        "shipping_address_id": {
                            "type": "integer",
                            "nullable": True,
                            "null_rate": 0.10,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "address.address_id",
                                "distribution": "uniform",
                                # Only pick an address belonging to this order's customer
                                "constrained_by": "customer_id",
                            },
                        },
                        "store_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "store.store_id",
                                "distribution": "zipf",
                                "params": {"alpha": 1.3},
                            },
                        },
                        "order_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "seasonal",
                                "range_ref": "model.date_range",
                                "profiles": {
                                    "month": {
                                        "Jan": 0.06, "Feb": 0.06, "Mar": 0.07,
                                        "Apr": 0.08, "May": 0.08, "Jun": 0.08,
                                        "Jul": 0.08, "Aug": 0.09, "Sep": 0.08,
                                        "Oct": 0.08, "Nov": 0.11, "Dec": 0.13,
                                    },
                                    "day_of_week": {
                                        "Mon": 0.13, "Tue": 0.14, "Wed": 0.14,
                                        "Thu": 0.14, "Fri": 0.16, "Sat": 0.17,
                                        "Sun": 0.12,
                                    },
                                    "hour_of_day": {
                                        "distribution": "bimodal",
                                        "peaks": [11, 20],
                                        "std_dev": 2,
                                    },
                                },
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {
                                    "completed": 0.82,
                                    "shipped": 0.08,
                                    "processing": 0.04,
                                    "cancelled": 0.05,
                                    "returned": 0.01,
                                },
                            },
                        },
                        "order_total": {
                            "type": "decimal",
                            "precision": 12,
                            "scale": 2,
                            "generator": {
                                "strategy": "computed",
                                "rule": "sum_children",
                                "child_table": "order_line",
                                "child_column": "line_total",
                            },
                        },
                    },
                },
                "order_line": {
                    "description": "Individual line items within an order",
                    "primary_key": ["order_line_id"],
                    "columns": {
                        "order_line_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "order_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "order.order_id",
                            },
                        },
                        "product_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "product.product_id",
                                "distribution": "zipf",
                                "params": {"alpha": 1.5},
                            },
                        },
                        "quantity": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "geometric",
                                "params": {"p": 0.6, "min": 1, "max": 20},
                            },
                        },
                        "unit_price": {
                            "type": "decimal",
                            "precision": 10,
                            "scale": 2,
                            "generator": {
                                "strategy": "lookup",
                                "source_table": "product",
                                "source_column": "unit_price",
                                "via": "product_id",
                            },
                        },
                        # discount_percent: look up from promotion if one is applied,
                        # otherwise 0. promotion_id is on order, not order_line — for
                        # Phase 0 simplicity we use a weighted distribution.
                        "discount_percent": {
                            "type": "decimal",
                            "precision": 5,
                            "scale": 2,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {
                                    "0.0": 0.70,   # no discount
                                    "5.0": 0.08,
                                    "10.0": 0.10,
                                    "15.0": 0.05,
                                    "20.0": 0.04,
                                    "25.0": 0.02,
                                    "50.0": 0.01,
                                },
                            },
                        },
                        "line_total": {
                            "type": "decimal",
                            "precision": 12,
                            "scale": 2,
                            "generator": {
                                "strategy": "formula",
                                "expression": "quantity * unit_price * (1 - discount_percent / 100)",
                            },
                        },
                    },
                },
                "return": {
                    "description": "Return transactions",
                    "primary_key": ["return_id"],
                    "columns": {
                        "return_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "order_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "order.order_id",
                            },
                        },
                        "return_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "derived",
                                "source": "order.order_date",
                                "via": "order_id",
                                "rule": "add_days",
                                "params": {
                                    "distribution": "log_normal",
                                    "mean": 2.0,
                                    "sigma": 0.8,
                                    "min": 1,
                                    "max": 90,
                                },
                            },
                        },
                        "reason": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {
                                    "defective": 0.15,
                                    "wrong_size": 0.25,
                                    "not_as_described": 0.20,
                                    "changed_mind": 0.30,
                                    "arrived_late": 0.05,
                                    "other": 0.05,
                                },
                            },
                        },
                        # refund_amount is computed post-generation by copying
                        # order.order_total for the associated order_id.
                        "refund_amount": {
                            "type": "decimal",
                            "precision": 12,
                            "scale": 2,
                            "generator": {
                                "strategy": "computed",
                                "rule": "lookup_parent",
                                "parent_table": "order",
                                "parent_column": "order_total",
                                "via": "order_id",
                            },
                        },
                    },
                },
            },
            "relationships": [
                {
                    "name": "customer_addresses",
                    "parent": "customer",
                    "child": "address",
                    "parent_columns": ["customer_id"],
                    "child_columns": ["customer_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "order_shipping_address",
                    "parent": "address",
                    "child": "order",
                    "parent_columns": ["address_id"],
                    "child_columns": ["shipping_address_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "product_in_category",
                    "parent": "product_category",
                    "child": "product",
                    "parent_columns": ["category_id"],
                    "child_columns": ["category_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "customer_orders",
                    "parent": "customer",
                    "child": "order",
                    "parent_columns": ["customer_id"],
                    "child_columns": ["customer_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "order_store",
                    "parent": "store",
                    "child": "order",
                    "parent_columns": ["store_id"],
                    "child_columns": ["store_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "order_lines",
                    "parent": "order",
                    "child": "order_line",
                    "parent_columns": ["order_id"],
                    "child_columns": ["order_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "order_line_product",
                    "parent": "product",
                    "child": "order_line",
                    "parent_columns": ["product_id"],
                    "child_columns": ["product_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "order_returns",
                    "parent": "order",
                    "child": "return",
                    "parent_columns": ["order_id"],
                    "child_columns": ["order_id"],
                    "type": "one_to_many",
                },
            ],
            "business_rules": [
                {
                    "name": "line_total_positive",
                    "type": "constraint",
                    "table": "order_line",
                    "rule": "line_total > 0",
                },
                {
                    "name": "order_date_after_signup",
                    "type": "cross_table",
                    "rule": "order.order_date >= customer.signup_date",
                    "via": "customer_id",
                },
                {
                    "name": "return_after_order",
                    "type": "cross_table",
                    "rule": "return.return_date > order.order_date",
                    "via": "order_id",
                },
                {
                    "name": "cost_less_than_price",
                    "type": "cross_column",
                    "table": "product",
                    "rule": "cost < unit_price",
                },
            ],
            "generation": {
                "scale": "small",
                "scales": {
                    "small": {"customer": 1000, "product": 500, "order": 5000},
                    "medium": {"customer": 50000, "product": 5000, "order": 500000},
                    "large": {"customer": 500000, "product": 25000, "order": 5000000},
                    "xlarge": {"customer": 5000000, "product": 100000, "order": 100000000},
                },
                "derived_counts": {
                    "address": {"per_parent": "customer", "ratio": 1.5},
                    "product_category": {"fixed": 50},
                    "store": {"fixed": 150},
                    "promotion": {"per_year": 50},
                    "order_line": {"per_parent": "order", "ratio": 2.5},
                    "return": {"per_parent": "order", "ratio": 0.08},
                },
                "output": {"format": "dataframe"},
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)
