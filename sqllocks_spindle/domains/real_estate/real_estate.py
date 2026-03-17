"""Real estate domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class RealEstateDomain(Domain):
    """Real estate domain — properties, listings, transactions, and inspections.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - neighborhood: Neighborhoods with demographics
        - agent: Real estate agents
        - property: Physical properties
        - listing: MLS listings
        - showing: Property showings
        - offer: Purchase offers
        - transaction: Closed sales
        - inspection: Property inspections
        - appraisal: Property appraisals
    """

    @property
    def name(self) -> str:
        return "real_estate"

    @property
    def description(self) -> str:
        return "Real estate domain with properties, listings, offers, transactions, and inspections"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build real estate 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"real_estate_{self._schema_mode}",
                "description": f"Real estate domain — {self._schema_mode} schema",
                "domain": "real_estate",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── neighborhood ─────────────────────────────────
                "neighborhood": {
                    "description": "Neighborhoods with demographics",
                    "primary_key": ["neighborhood_id"],
                    "columns": {
                        "neighborhood_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "neighborhood_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "neighborhoods",
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
                        "median_income": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 11.0,
                                "sigma": 0.5,
                                "min": 25000.0,
                                "max": 500000.0,
                                "round": 2,
                            },
                        },
                        "walk_score": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 10,
                                "max": 100,
                            },
                        },
                    },
                },

                # ── agent ────────────────────────────────────────
                "agent": {
                    "description": "Real estate agents",
                    "primary_key": ["agent_id"],
                    "columns": {
                        "agent_id": {
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
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "phone": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                        "license_number": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {
                                "strategy": "pattern",
                                "format": "RE-{seq:6}",
                            },
                        },
                        "brokerage": {
                            "type": "string",
                            "max_length": 150,
                            "generator": {"strategy": "faker", "provider": "company"},
                        },
                        "years_experience": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 1,
                                "max": 30,
                            },
                        },
                        "specialization": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("agent.specialization", {
                                    "Residential": 0.45,
                                    "Commercial": 0.20,
                                    "Luxury": 0.15,
                                    "Investment": 0.12,
                                    "New Construction": 0.08,
                                }),
                            },
                        },
                    },
                },

                # ── property ─────────────────────────────────────
                "property": {
                    "description": "Physical properties",
                    "primary_key": ["property_id"],
                    "columns": {
                        "property_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "neighborhood_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "neighborhood.neighborhood_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "property_type": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "property_types",
                            },
                        },
                        "address": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {"strategy": "faker", "provider": "street_address"},
                        },
                        "bedrooms": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("property.bedrooms", {
                                    "1": 0.08,
                                    "2": 0.18,
                                    "3": 0.35,
                                    "4": 0.25,
                                    "5": 0.10,
                                    "6": 0.04,
                                }),
                            },
                        },
                        "bathrooms": {
                            "type": "float",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("property.bathrooms", {
                                    "1.0": 0.12,
                                    "1.5": 0.15,
                                    "2.0": 0.30,
                                    "2.5": 0.22,
                                    "3.0": 0.12,
                                    "3.5": 0.05,
                                    "4.0": 0.04,
                                }),
                            },
                        },
                        "sqft": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 7.3,
                                "sigma": 0.4,
                                "min": 500,
                                "max": 10000,
                            },
                        },
                        "lot_size_sqft": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 8.5,
                                "sigma": 0.6,
                                "min": 1000,
                                "max": 200000,
                            },
                        },
                        "year_built": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 1950,
                                "max": 2025,
                            },
                        },
                        "assessed_value": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 12.2,
                                "sigma": 0.7,
                                "min": 50000.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── listing ──────────────────────────────────────
                "listing": {
                    "description": "MLS property listings",
                    "primary_key": ["listing_id"],
                    "columns": {
                        "listing_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "property_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "property.property_id",
                            },
                        },
                        "agent_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "agent.agent_id",
                                "distribution": "zipf",
                                "alpha": 1.4,
                            },
                        },
                        "list_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "seasonal",
                                "month_weights": self._dist("listing.list_date.month", {
                                    "Jan": 0.06, "Feb": 0.06, "Mar": 0.09, "Apr": 0.10,
                                    "May": 0.11, "Jun": 0.10, "Jul": 0.09, "Aug": 0.09,
                                    "Sep": 0.08, "Oct": 0.08, "Nov": 0.07, "Dec": 0.07,
                                }),
                            },
                        },
                        "list_price": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 12.5,
                                "sigma": 0.7,
                                "min": 50000.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("listing.status", {
                                    "Active": 0.20,
                                    "Pending": 0.10,
                                    "Sold": 0.45,
                                    "Withdrawn": 0.10,
                                    "Expired": 0.15,
                                }),
                            },
                        },
                        "days_on_market": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.5,
                                "sigma": 0.8,
                                "min": 1,
                                "max": 365,
                            },
                        },
                        "mls_number": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {
                                "strategy": "pattern",
                                "format": "MLS-{seq:7}",
                            },
                        },
                    },
                },

                # ── showing ──────────────────────────────────────
                "showing": {
                    "description": "Property showings",
                    "primary_key": ["showing_id"],
                    "columns": {
                        "showing_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "listing_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "listing.listing_id",
                            },
                        },
                        "showing_date": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "agent_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "agent.agent_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "feedback": {
                            "type": "string",
                            "max_length": 500,
                            "nullable": True,
                            "null_rate": 0.25,
                            "generator": {"strategy": "faker", "provider": "sentence"},
                        },
                        "buyer_interest_level": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("showing.buyer_interest_level", {
                                    "High": 0.20,
                                    "Medium": 0.35,
                                    "Low": 0.30,
                                    "None": 0.15,
                                }),
                            },
                        },
                    },
                },

                # ── offer ────────────────────────────────────────
                "offer": {
                    "description": "Purchase offers on listings",
                    "primary_key": ["offer_id"],
                    "columns": {
                        "offer_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "listing_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "listing.listing_id",
                            },
                        },
                        "offer_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "offer_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 12.4,
                                "sigma": 0.7,
                                "min": 40000.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("offer.status", {
                                    "Pending": 0.15,
                                    "Accepted": 0.30,
                                    "Rejected": 0.25,
                                    "Countered": 0.20,
                                    "Withdrawn": 0.10,
                                }),
                            },
                        },
                        "contingencies": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("offer.contingencies", {
                                    "Inspection": 0.30,
                                    "Financing": 0.28,
                                    "Appraisal": 0.22,
                                    "None": 0.20,
                                }),
                            },
                        },
                    },
                },

                # ── transaction ───────────────────────────────────
                "transaction": {
                    "description": "Closed real estate transactions",
                    "primary_key": ["transaction_id"],
                    "columns": {
                        "transaction_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "listing_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "listing.listing_id",
                            },
                        },
                        "sale_price": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 12.5,
                                "sigma": 0.7,
                                "min": 50000.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                        "close_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-03-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "buyer_agent_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "agent.agent_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "seller_agent_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "agent.agent_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "commission_rate": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 2.5,
                                "max": 6.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── inspection ────────────────────────────────────
                "inspection": {
                    "description": "Property inspections for transactions",
                    "primary_key": ["inspection_id"],
                    "columns": {
                        "inspection_id": {
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
                        "inspection_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-03-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "inspector_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                        "inspection_type": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "inspection_items",
                            },
                        },
                        "result": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("inspection.result", {
                                    "Pass": 0.60,
                                    "Fail": 0.10,
                                    "Conditional": 0.30,
                                }),
                            },
                        },
                        "findings_count": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0,
                                "max": 15,
                            },
                        },
                    },
                },

                # ── appraisal ─────────────────────────────────────
                "appraisal": {
                    "description": "Property appraisals for transactions",
                    "primary_key": ["appraisal_id"],
                    "columns": {
                        "appraisal_id": {
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
                        "appraisal_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-03-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "appraised_value": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 12.5,
                                "sigma": 0.7,
                                "min": 50000.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                        "appraiser_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                        "condition_rating": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("appraisal.condition_rating", {
                                    "Excellent": 0.15,
                                    "Good": 0.50,
                                    "Fair": 0.30,
                                    "Poor": 0.05,
                                }),
                            },
                        },
                    },
                },
            },

            # ── relationships ────────────────────────────────────
            "relationships": [
                {
                    "name": "property_neighborhood",
                    "parent": "neighborhood",
                    "child": "property",
                    "parent_key": "neighborhood_id",
                    "child_key": "neighborhood_id",
                },
                {
                    "name": "listing_property",
                    "parent": "property",
                    "child": "listing",
                    "parent_key": "property_id",
                    "child_key": "property_id",
                },
                {
                    "name": "listing_agent",
                    "parent": "agent",
                    "child": "listing",
                    "parent_key": "agent_id",
                    "child_key": "agent_id",
                },
                {
                    "name": "showing_listing",
                    "parent": "listing",
                    "child": "showing",
                    "parent_key": "listing_id",
                    "child_key": "listing_id",
                },
                {
                    "name": "showing_agent",
                    "parent": "agent",
                    "child": "showing",
                    "parent_key": "agent_id",
                    "child_key": "agent_id",
                },
                {
                    "name": "offer_listing",
                    "parent": "listing",
                    "child": "offer",
                    "parent_key": "listing_id",
                    "child_key": "listing_id",
                },
                {
                    "name": "transaction_listing",
                    "parent": "listing",
                    "child": "transaction",
                    "parent_key": "listing_id",
                    "child_key": "listing_id",
                },
                {
                    "name": "transaction_buyer_agent",
                    "parent": "agent",
                    "child": "transaction",
                    "parent_key": "agent_id",
                    "child_key": "buyer_agent_id",
                },
                {
                    "name": "transaction_seller_agent",
                    "parent": "agent",
                    "child": "transaction",
                    "parent_key": "agent_id",
                    "child_key": "seller_agent_id",
                },
                {
                    "name": "inspection_transaction",
                    "parent": "transaction",
                    "child": "inspection",
                    "parent_key": "transaction_id",
                    "child_key": "transaction_id",
                },
                {
                    "name": "appraisal_transaction",
                    "parent": "transaction",
                    "child": "appraisal",
                    "parent_key": "transaction_id",
                    "child_key": "transaction_id",
                },
            ],

            # ── business rules ───────────────────────────────────
            "business_rules": [
                {
                    "name": "list_price_positive",
                    "type": "constraint",
                    "table": "listing",
                    "rule": "list_price > 0",
                },
                {
                    "name": "sale_price_positive",
                    "type": "constraint",
                    "table": "transaction",
                    "rule": "sale_price > 0",
                },
                {
                    "name": "sqft_positive",
                    "type": "constraint",
                    "table": "property",
                    "rule": "sqft > 0",
                },
            ],

            # ── generation config ────────────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"property": 100},
                    "small": {"property": 1000},
                    "medium": {"property": 10000},
                    "large": {"property": 100000},
                    "xlarge": {"property": 5000000},
                    "warehouse": {"property": 1000000},
                    "xxl": {"property": 20000000},
                    "xxxl": {"property": 100000000},
                },
                "derived_counts": {
                    "neighborhood": {"fixed": 50},
                    "agent": {"fixed": 100},
                    "listing": {"per_parent": "property", "ratio": self._ratio("listing_per_property", 1.5)},
                    "showing": {"per_parent": "listing", "ratio": self._ratio("showing_per_listing", 5.0)},
                    "offer": {"per_parent": "listing", "ratio": self._ratio("offer_per_listing", 1.5)},
                    "transaction": {"per_parent": "listing", "ratio": self._ratio("transaction_per_listing", 0.4)},
                    "inspection": {"per_parent": "transaction", "ratio": self._ratio("inspection_per_transaction", 1.0)},
                    "appraisal": {"per_parent": "transaction", "ratio": self._ratio("appraisal_per_transaction", 1.0)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the Real Estate domain.

        Produces:
          - dim_property (from property, enriched with neighborhood)
          - dim_agent    (from agent)
          - dim_listing  (from listing)
          - dim_date     (generated from close_date / showing_date)
          - fact_transaction (from transaction)
          - fact_showing     (from showing)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_property": DimSpec(
                    source="property",
                    sk="sk_property",
                    nk="property_id",
                    enrich=[{
                        "table": "neighborhood",
                        "left_on": "neighborhood_id",
                        "right_on": "neighborhood_id",
                        "prefix": "nbr_",
                    }],
                ),
                "dim_agent": DimSpec(
                    source="agent",
                    sk="sk_agent",
                    nk="agent_id",
                ),
                "dim_listing": DimSpec(
                    source="listing",
                    sk="sk_listing",
                    nk="listing_id",
                ),
            },
            facts={
                "fact_transaction": FactSpec(
                    primary="transaction",
                    fk_map={
                        "listing_id": "dim_listing",
                    },
                    date_cols=["close_date"],
                ),
                "fact_showing": FactSpec(
                    primary="showing",
                    fk_map={
                        "listing_id": "dim_listing",
                    },
                    date_cols=["showing_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Real Estate domain."""
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "neighborhood": "Location",
            "agent": "Worker",
            "property": "RealEstateProperty",
            "listing": "Listing",
            "showing": "Appointment",
            "offer": "Proposal",
            "transaction": "Transaction",
            "inspection": "Inspection",
            "appraisal": "Assessment",
        })
