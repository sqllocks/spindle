"""Insurance domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class InsuranceDomain(Domain):
    """Insurance domain with policies, claims, and underwriting.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - agent: Insurance agents with licensing info (fixed: 100)
        - policyholder: Insurance customers with demographics (anchor: 1000 small)
        - policy_type: Policy categories and base premiums (fixed: 30)
        - policy: Insurance policies with coverage details
        - coverage: Coverage line items per policy
        - claim: Claims filed against policies
        - claim_payment: Payouts on claims
        - premium_payment: Premium payments by policyholders
        - underwriting: Risk assessment records per policy
    """

    @property
    def name(self) -> str:
        return "insurance"

    @property
    def description(self) -> str:
        return "Insurance domain with policies, claims, underwriting, and premium management"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build insurance 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"insurance_{self._schema_mode}",
                "description": f"Insurance domain — {self._schema_mode} schema",
                "domain": "insurance",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── agent ─────────────────────────────────────────
                "agent": {
                    "description": "Insurance agents with licensing info",
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
                        "license_number": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "AGT-{seq:6}",
                            },
                        },
                        "specialization": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("agent.specialization", {
                                    "P&C": 0.30,
                                    "Life": 0.20,
                                    "Health": 0.20,
                                    "Commercial": 0.15,
                                    "Multi-Line": 0.15,
                                }),
                            },
                        },
                        "years_experience": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 1,
                                "max": 35,
                                "round": 0,
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("agent.is_active", {"true": 0.90, "false": 0.10}),
                            },
                        },
                    },
                },

                # ── policyholder ──────────────────────────────────
                "policyholder": {
                    "description": "Insurance customers with demographics",
                    "primary_key": ["policyholder_id"],
                    "columns": {
                        "policyholder_id": {
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
                            "nullable": True,
                            "null_rate": 0.08,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                        "date_of_birth": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "1940-01-01",
                                "end": "2005-12-31",
                                "pattern": "uniform",
                            },
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
                        "credit_score": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "normal",
                                "mean": 700,
                                "sigma": 80,
                                "min": 300,
                                "max": 850,
                                "round": 0,
                            },
                        },
                        "agent_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "agent.agent_id",
                            },
                        },
                    },
                },

                # ── policy_type ───────────────────────────────────
                "policy_type": {
                    "description": "Policy categories and base premiums",
                    "primary_key": ["policy_type_id"],
                    "columns": {
                        "policy_type_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "type_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "policy_types",
                            },
                        },
                        "category": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("policy_type.category", {
                                    "Auto": 0.25,
                                    "Home": 0.20,
                                    "Life": 0.18,
                                    "Health": 0.15,
                                    "Commercial": 0.15,
                                    "Umbrella": 0.07,
                                }),
                            },
                        },
                        "base_premium_min": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 200.0,
                                "max": 2000.0,
                                "round": 2,
                            },
                        },
                        "base_premium_max": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 2000.0,
                                "max": 15000.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── policy ────────────────────────────────────────
                "policy": {
                    "description": "Insurance policies",
                    "primary_key": ["policy_id"],
                    "columns": {
                        "policy_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "policyholder_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "policyholder.policyholder_id",
                            },
                        },
                        "policy_type_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "policy_type.policy_type_id",
                            },
                        },
                        "policy_number": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "POL-{seq:8}",
                            },
                        },
                        "effective_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "expiration_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "effective_date",
                                "operation": "add_days",
                                "days": 365,
                            },
                        },
                        "premium_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 7.0,
                                "sigma": 0.8,
                                "min": 200.0,
                                "max": 50000.0,
                                "round": 2,
                            },
                        },
                        "deductible": {
                            "type": "float",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("policy.deductible", {
                                    "500": 0.30,
                                    "1000": 0.35,
                                    "2000": 0.20,
                                    "5000": 0.15,
                                }),
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("policy.status", {
                                    "Active": 0.72,
                                    "Cancelled": 0.08,
                                    "Expired": 0.15,
                                    "Lapsed": 0.05,
                                }),
                            },
                        },
                    },
                },

                # ── coverage ──────────────────────────────────────
                "coverage": {
                    "description": "Coverage line items per policy",
                    "primary_key": ["coverage_id"],
                    "columns": {
                        "coverage_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "policy_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "policy.policy_id",
                            },
                        },
                        "coverage_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("coverage.coverage_type", {
                                    "Liability": 0.25,
                                    "Collision": 0.18,
                                    "Comprehensive": 0.17,
                                    "Medical": 0.15,
                                    "Property": 0.15,
                                    "Uninsured": 0.10,
                                }),
                            },
                        },
                        "coverage_limit": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 11.0,
                                "sigma": 1.2,
                                "min": 5000.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                        "coverage_premium": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 5.5,
                                "sigma": 0.8,
                                "min": 50.0,
                                "max": 10000.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── claim ─────────────────────────────────────────
                "claim": {
                    "description": "Claims filed against policies",
                    "primary_key": ["claim_id"],
                    "columns": {
                        "claim_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "policy_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "policy.policy_id",
                            },
                        },
                        "claim_number": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "CLM-{seq:8}",
                            },
                        },
                        "incident_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "filed_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "incident_date",
                                "operation": "add_days",
                                "days": 7,
                            },
                        },
                        "claim_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 8.5,
                                "sigma": 1.5,
                                "min": 100.0,
                                "max": 1000000.0,
                                "round": 2,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("claim.status", {
                                    "Open": 0.10,
                                    "Under Review": 0.15,
                                    "Approved": 0.40,
                                    "Denied": 0.10,
                                    "Closed": 0.25,
                                }),
                            },
                        },
                        "cause": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "claim_categories",
                            },
                        },
                    },
                },

                # ── claim_payment ─────────────────────────────────
                "claim_payment": {
                    "description": "Payouts on claims",
                    "primary_key": ["payment_id"],
                    "columns": {
                        "payment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "claim_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "claim.claim_id",
                            },
                        },
                        "payment_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "payment_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 7.5,
                                "sigma": 1.5,
                                "min": 50.0,
                                "max": 500000.0,
                                "round": 2,
                            },
                        },
                        "payment_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("claim_payment.payment_type", {
                                    "Check": 0.20,
                                    "EFT": 0.70,
                                    "Wire": 0.10,
                                }),
                            },
                        },
                    },
                },

                # ── premium_payment ───────────────────────────────
                "premium_payment": {
                    "description": "Premium payments by policyholders",
                    "primary_key": ["payment_id"],
                    "columns": {
                        "payment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "policy_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "policy.policy_id",
                            },
                        },
                        "payment_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 5.5,
                                "sigma": 0.8,
                                "min": 25.0,
                                "max": 10000.0,
                                "round": 2,
                            },
                        },
                        "payment_method": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("premium_payment.payment_method", {
                                    "Auto-Pay": 0.45,
                                    "Online": 0.30,
                                    "Mail": 0.15,
                                    "Agent": 0.10,
                                }),
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("premium_payment.status", {
                                    "Paid": 0.82,
                                    "Pending": 0.08,
                                    "Late": 0.07,
                                    "Returned": 0.03,
                                }),
                            },
                        },
                    },
                },

                # ── underwriting ──────────────────────────────────
                "underwriting": {
                    "description": "Risk assessment records per policy",
                    "primary_key": ["underwriting_id"],
                    "columns": {
                        "underwriting_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "policy_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "policy.policy_id",
                            },
                        },
                        "assessed_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "risk_score": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0,
                                "max": 100,
                                "round": 0,
                            },
                        },
                        "risk_tier": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("underwriting.risk_tier", {
                                    "Low": 0.35,
                                    "Medium": 0.35,
                                    "High": 0.20,
                                    "Very High": 0.10,
                                }),
                            },
                        },
                        "notes": {
                            "type": "string",
                            "max_length": 500,
                            "nullable": True,
                            "null_rate": 0.40,
                            "generator": {"strategy": "faker", "provider": "sentence"},
                        },
                    },
                },
            },

            # ── relationships ─────────────────────────────────
            "relationships": [
                {
                    "name": "policyholder_agent",
                    "parent": "agent",
                    "child": "policyholder",
                    "parent_key": "agent_id",
                    "child_key": "agent_id",
                },
                {
                    "name": "policy_policyholder",
                    "parent": "policyholder",
                    "child": "policy",
                    "parent_key": "policyholder_id",
                    "child_key": "policyholder_id",
                },
                {
                    "name": "policy_type_ref",
                    "parent": "policy_type",
                    "child": "policy",
                    "parent_key": "policy_type_id",
                    "child_key": "policy_type_id",
                },
                {
                    "name": "coverage_policy",
                    "parent": "policy",
                    "child": "coverage",
                    "parent_key": "policy_id",
                    "child_key": "policy_id",
                },
                {
                    "name": "claim_policy",
                    "parent": "policy",
                    "child": "claim",
                    "parent_key": "policy_id",
                    "child_key": "policy_id",
                },
                {
                    "name": "claim_payment_claim",
                    "parent": "claim",
                    "child": "claim_payment",
                    "parent_key": "claim_id",
                    "child_key": "claim_id",
                },
                {
                    "name": "premium_payment_policy",
                    "parent": "policy",
                    "child": "premium_payment",
                    "parent_key": "policy_id",
                    "child_key": "policy_id",
                },
                {
                    "name": "underwriting_policy",
                    "parent": "policy",
                    "child": "underwriting",
                    "parent_key": "policy_id",
                    "child_key": "policy_id",
                },
            ],

            # ── business rules ────────────────────────────────
            "business_rules": [
                {
                    "name": "claim_after_policy_effective",
                    "description": "Claim incident_date must be >= policy effective_date",
                    "type": "temporal_order",
                    "tables": ["claim", "policy"],
                    "join_key": "policy_id",
                    "earlier": "effective_date",
                    "later": "incident_date",
                },
                {
                    "name": "filed_after_incident",
                    "description": "Claim filed_date must be >= incident_date",
                    "type": "temporal_order",
                    "tables": ["claim"],
                    "join_key": "claim_id",
                    "earlier": "incident_date",
                    "later": "filed_date",
                },
            ],

            # ── generation config ─────────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"policyholder": 100},
                    "small": {"policyholder": 1000},
                    "medium": {"policyholder": 10000},
                    "large": {"policyholder": 100000},
                    "warehouse": {"policyholder": 1000000},
                },
                "derived_counts": {
                    "agent": {"fixed": 100},
                    "policy_type": {"fixed": 30},
                    "policy": {"per_parent": "policyholder", "ratio": self._ratio("policy_per_policyholder", 1.8)},
                    "coverage": {"per_parent": "policy", "ratio": self._ratio("coverage_per_policy", 2.5)},
                    "claim": {"per_parent": "policy", "ratio": self._ratio("claim_per_policy", 0.3)},
                    "claim_payment": {"per_parent": "claim", "ratio": self._ratio("claim_payment_per_claim", 1.5)},
                    "premium_payment": {"per_parent": "policy", "ratio": self._ratio("premium_payment_per_policy", 6.0)},
                    "underwriting": {"per_parent": "policy", "ratio": self._ratio("underwriting_per_policy", 1.0)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the Insurance domain.

        Produces:
          - dim_policyholder (from policyholder)
          - dim_agent        (from agent)
          - dim_policy_type  (from policy_type)
          - dim_policy       (from policy)
          - fact_claim         (from claim)
          - fact_claim_payment (from claim_payment + claim)
          - fact_premium       (from premium_payment)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_policyholder": DimSpec(
                    source="policyholder",
                    sk="sk_policyholder",
                    nk="policyholder_id",
                ),
                "dim_agent": DimSpec(
                    source="agent",
                    sk="sk_agent",
                    nk="agent_id",
                ),
                "dim_policy_type": DimSpec(
                    source="policy_type",
                    sk="sk_policy_type",
                    nk="policy_type_id",
                ),
                "dim_policy": DimSpec(
                    source="policy",
                    sk="sk_policy",
                    nk="policy_id",
                ),
            },
            facts={
                "fact_claim": FactSpec(
                    primary="claim",
                    fk_map={"policy_id": "dim_policy"},
                    date_cols=["filed_date"],
                ),
                "fact_claim_payment": FactSpec(
                    primary="claim_payment",
                    joins=[{
                        "table": "claim",
                        "left_on": "claim_id",
                        "right_on": "claim_id",
                    }],
                    fk_map={"policy_id": "dim_policy"},
                    date_cols=["payment_date"],
                ),
                "fact_premium": FactSpec(
                    primary="premium_payment",
                    fk_map={"policy_id": "dim_policy"},
                    date_cols=["payment_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Insurance domain.

        Maps source table names to Microsoft Common Data Model entity names.
        """
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "agent": "Worker",
            "policyholder": "Contact",
            "policy_type": "Category",
            "policy": "Contract",
            "coverage": "ContractDetail",
            "claim": "Case",
            "claim_payment": "Payment",
            "premium_payment": "Payment",
            "underwriting": "Assessment",
        })
