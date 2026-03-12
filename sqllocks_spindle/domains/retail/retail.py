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
        - address: Customer addresses (1:N)
        - product_category: 3-level category hierarchy
        - product: Individual SKUs
        - store: Physical and online stores
        - promotion: Marketing promotions
        - order: Order headers
        - order_line: Order line items
        - return: Return transactions
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
                                "values": self._dist("customer.gender", {"M": 0.49, "F": 0.51}),
                            },
                        },
                        "loyalty_tier": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("customer.loyalty_tier", {
                                    "Basic": 0.55,
                                    "Silver": 0.25,
                                    "Gold": 0.13,
                                    "Platinum": 0.07,
                                }),
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
                                "values": self._dist("customer.is_active", {"true": 0.85, "false": 0.15}),
                            },
                        },
                    },
                },
                "address": {
                    "description": "Customer addresses — one customer can have multiple (billing, shipping)",
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
                            },
                        },
                        "address_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("address.address_type", {
                                    "billing": 0.50,
                                    "shipping": 0.40,
                                    "both": 0.10,
                                }),
                            },
                        },
                        "street": {
                            "type": "string",
                            "generator": {"strategy": "faker", "provider": "street_address"},
                        },
                        # record_sample anchor: samples a US ZIP record, stashes all fields
                        "city": {
                            "type": "string",
                            "generator": {
                                "strategy": "record_sample",
                                "dataset": "us_zip_locations",
                                "field": "city",
                            },
                        },
                        # record_field: read correlated fields from the sampled record
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
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "zip",
                            },
                        },
                        "lat": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lat",
                            },
                        },
                        "lng": {
                            "type": "decimal",
                            "generator": {
                                "strategy": "record_field",
                                "dataset": "us_zip_locations",
                                "field": "lng",
                            },
                        },
                        "is_primary": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "first_per_parent",
                                "parent_column": "customer_id",
                                "default": True,
                            },
                        },
                    },
                },
                "product_category": {
                    "description": "Product category hierarchy — 3 levels",
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
                        "parent_category_id": {
                            "type": "integer",
                            "nullable": True,
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
                                    "active": 0.75,
                                    "discontinued": 0.15,
                                    "introduced": 0.10,
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
                                "values": self._dist("store.store_type", {
                                    "physical": 0.85,
                                    "online": 0.10,
                                    "warehouse": 0.05,
                                }),
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
                                "values": self._dist("promotion.promotion_type", {
                                    "percent_off": 0.40,
                                    "bogo": 0.15,
                                    "fixed_amount": 0.20,
                                    "bundle": 0.10,
                                    "free_shipping": 0.15,
                                }),
                            },
                        },
                        "discount_pct": {
                            "type": "decimal",
                            "precision": 5,
                            "scale": 2,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("promotion.discount_pct", {
                                    5.0: 0.05,
                                    10.0: 0.25,
                                    15.0: 0.20,
                                    20.0: 0.25,
                                    25.0: 0.10,
                                    50.0: 0.15,
                                }),
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
                                "strategy": "derived",
                                "source": "start_date",
                                "rule": "add_days",
                                "params": {"distribution": "uniform", "min": 3, "max": 30},
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
                            "generator": (lambda d: {
                                "strategy": "foreign_key",
                                "ref": "customer.customer_id",
                                "distribution": d.get("distribution", "pareto"),
                                "params": {
                                    k: v for k, v in d.items() if k != "distribution"
                                },
                            })(self._dist("order.customer_id", {"distribution": "pareto", "alpha": 1.16, "max_per_parent": 50})),
                        },
                        "store_id": {
                            "type": "integer",
                            "generator": (lambda d: {
                                "strategy": "foreign_key",
                                "ref": "store.store_id",
                                "distribution": d.get("distribution", "zipf"),
                                "params": {
                                    k: v for k, v in d.items() if k != "distribution"
                                },
                            })(self._dist("order.store_id", {"distribution": "zipf", "alpha": 1.3})),
                        },
                        "shipping_address_id": {
                            "type": "integer",
                            "nullable": True,
                            "null_rate": 0.0,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "address.address_id",
                                "constrained_by": "customer_id",
                            },
                        },
                        "promotion_id": {
                            "type": "integer",
                            "nullable": True,
                            "null_rate": 0.70,
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "promotion.promotion_id",
                            },
                        },
                        "order_date": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "seasonal",
                                "range_ref": "model.date_range",
                                "profiles": {
                                    "month": self._dist("order.order_date.month", {
                                        "Jan": 0.071, "Feb": 0.068, "Mar": 0.079,
                                        "Apr": 0.079, "May": 0.083, "Jun": 0.083,
                                        "Jul": 0.085, "Aug": 0.088, "Sep": 0.079,
                                        "Oct": 0.083, "Nov": 0.096, "Dec": 0.106,
                                    }),
                                    "day_of_week": self._dist("order.order_date.day_of_week", {
                                        "Mon": 0.155, "Tue": 0.152, "Wed": 0.148,
                                        "Thu": 0.153, "Fri": 0.147, "Sat": 0.115,
                                        "Sun": 0.130,
                                    }),
                                    "hour_of_day": self._dist("order.order_date.hour_of_day", {
                                        "distribution": "bimodal",
                                        "peaks": [12, 20],
                                        "std_dev": 2,
                                    }),
                                },
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("order.status", {
                                    "completed": 0.77,
                                    "shipped": 0.08,
                                    "processing": 0.02,
                                    "cancelled": 0.04,
                                    "returned": 0.09,
                                }),
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
                            "generator": (lambda d: {
                                "strategy": "foreign_key",
                                "ref": "product.product_id",
                                "distribution": d.get("distribution", "zipf"),
                                "params": {
                                    k: v for k, v in d.items() if k != "distribution"
                                },
                            })(self._dist("order_line.product_id", {"distribution": "zipf", "alpha": 1.5})),
                        },
                        "promotion_id": {
                            "type": "integer",
                            "nullable": True,
                            "generator": {
                                "strategy": "lookup",
                                "source_table": "order",
                                "source_column": "promotion_id",
                                "via": "order_id",
                            },
                        },
                        "quantity": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "geometric",
                                "params": (lambda d: {k: v for k, v in d.items() if k != "distribution"})(
                                    self._dist("order_line.quantity", {"distribution": "geometric", "p": 0.6, "min": 1, "max": 20})
                                ),
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
                        "discount_percent": {
                            "type": "decimal",
                            "precision": 5,
                            "scale": 2,
                            "generator": {
                                "strategy": "conditional",
                                "condition": "promotion_id IS NOT NULL",
                                "true_generator": {
                                    "strategy": "lookup",
                                    "source_table": "promotion",
                                    "source_column": "discount_pct",
                                    "via": "promotion_id",
                                },
                                "false_generator": {"fixed": 0.0},
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
                                "values": self._dist("return.reason", {
                                    "wrong_size": 0.25,
                                    "changed_mind": 0.22,
                                    "not_as_described": 0.20,
                                    "defective": 0.15,
                                    "damaged_shipping": 0.10,
                                    "arrived_late": 0.05,
                                    "other": 0.03,
                                }),
                            },
                        },
                        "refund_amount": {
                            "type": "decimal",
                            "precision": 12,
                            "scale": 2,
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "params": {
                                    "mean": 3.5,
                                    "sigma": 1.0,
                                    "min": 5.00,
                                    "max": 2000.00,
                                },
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
                    "name": "order_address",
                    "parent": "address",
                    "child": "order",
                    "parent_columns": ["address_id"],
                    "child_columns": ["shipping_address_id"],
                    "type": "one_to_many",
                },
                {
                    "name": "order_promotion",
                    "parent": "promotion",
                    "child": "order",
                    "parent_columns": ["promotion_id"],
                    "child_columns": ["promotion_id"],
                    "type": "one_to_many",
                    "optional": True,
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
                {
                    "name": "refund_leq_order_total",
                    "type": "cross_table",
                    "rule": "return.refund_amount <= order.order_total",
                    "via": "order_id",
                },
                {
                    "name": "line_total_positive",
                    "type": "constraint",
                    "table": "order_line",
                    "rule": "line_total > 0",
                },
            ],
            "generation": {
                "scale": "small",
                "scales": {
                    "fabric_demo": {"customer": 200, "product": 100, "order": 1000},
                    "small": {"customer": 1000, "product": 500, "order": 5000},
                    "medium": {"customer": 50000, "product": 5000, "order": 500000},
                    "large": {"customer": 500000, "product": 25000, "order": 5000000},
                    "xlarge": {"customer": 5000000, "product": 100000, "order": 100000000},
                    "warehouse": {"customer": 1000000, "product": 50000, "order": 10000000},
                },
                "derived_counts": {
                    "address": {"per_parent": "customer", "ratio": self._ratio("address_per_customer", 1.5)},
                    "product_category": {"fixed": 50},
                    "store": {"fixed": 150},
                    "promotion": {"per_year": 50},
                    "order_line": {"per_parent": "order", "ratio": self._ratio("order_line_per_order", 2.5)},
                    "return": {"per_parent": "order", "ratio": self._ratio("return_per_order", 0.17)},
                },
                "output": {"format": "dataframe"},
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the Retail domain.

        Produces:
          - dim_customer (from customer)
          - dim_product  (from product, enriched with product_category)
          - dim_store    (from store)
          - dim_promotion (from promotion)
          - dim_date     (generated from order_date / return_date)
          - fact_sale    (from order_line + order)
          - fact_return  (from return + order)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_customer": DimSpec(
                    source="customer",
                    sk="sk_customer",
                    nk="customer_id",
                ),
                "dim_product": DimSpec(
                    source="product",
                    sk="sk_product",
                    nk="product_id",
                    enrich=[{
                        "table": "product_category",
                        "left_on": "category_id",
                        "right_on": "category_id",
                        "prefix": "cat_",
                    }],
                ),
                "dim_store": DimSpec(
                    source="store",
                    sk="sk_store",
                    nk="store_id",
                ),
                "dim_promotion": DimSpec(
                    source="promotion",
                    sk="sk_promotion",
                    nk="promotion_id",
                ),
            },
            facts={
                "fact_sale": FactSpec(
                    primary="order_line",
                    joins=[{"table": "order", "left_on": "order_id", "right_on": "order_id"}],
                    fk_map={
                        "customer_id": "dim_customer",
                        "product_id": "dim_product",
                        "store_id": "dim_store",
                        "promotion_id": "dim_promotion",
                    },
                    date_cols=["order_date"],
                ),
                "fact_return": FactSpec(
                    primary="return",
                    joins=[{"table": "order", "left_on": "order_id", "right_on": "order_id"}],
                    fk_map={
                        "product_id": "dim_product",
                        "customer_id": "dim_customer",
                        "store_id": "dim_store",
                    },
                    date_cols=["return_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Retail domain.

        Maps source table names to Microsoft Common Data Model entity names.
        """
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "customer": "Contact",
            "address": "CustomerAddress",
            "product_category": "ProductCategory",
            "product": "Product",
            "store": "Store",
            "promotion": "Campaign",
            "order": "SalesOrder",
            "order_line": "SalesOrderProduct",
            "return": "ReturnOrder",
        })
