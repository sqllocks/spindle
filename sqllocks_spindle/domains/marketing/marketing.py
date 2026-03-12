"""Marketing domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class MarketingDomain(Domain):
    """Marketing domain with campaigns, contacts, leads, and conversions.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - campaign_type: Campaign categories (fixed: 15)
        - industry: Target industries (fixed: 25)
        - campaign: Marketing campaigns (anchor: 200 small)
        - lead_source: Lead origins (fixed: 20)
        - contact: Marketing contacts
        - lead: Qualified leads from contacts
        - opportunity: Sales opportunities from leads
        - email_send: Email send events per campaign
        - web_visit: Website visits per contact
        - conversion: Conversion events from leads
    """

    @property
    def name(self) -> str:
        return "marketing"

    @property
    def description(self) -> str:
        return "Marketing domain with campaigns, contacts, leads, opportunities, and conversions"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build marketing 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"marketing_{self._schema_mode}",
                "description": f"Marketing domain — {self._schema_mode} schema",
                "domain": "marketing",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── campaign_type ─────────────────────────────────
                "campaign_type": {
                    "description": "Campaign categories",
                    "primary_key": ["type_id"],
                    "columns": {
                        "type_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "type_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "campaign_types",
                            },
                        },
                        "channel": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("campaign_type.channel", {
                                    "Email": 0.25,
                                    "Social": 0.20,
                                    "PPC": 0.18,
                                    "Content": 0.15,
                                    "Event": 0.12,
                                    "Direct Mail": 0.10,
                                }),
                            },
                        },
                        "avg_conversion_rate": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.01,
                                "max": 0.15,
                                "round": 4,
                            },
                        },
                    },
                },

                # ── industry ──────────────────────────────────────
                "industry": {
                    "description": "Target industries",
                    "primary_key": ["industry_id"],
                    "columns": {
                        "industry_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "industry_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "industry_names",
                            },
                        },
                        "sector": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("industry.sector", {
                                    "Technology": 0.22,
                                    "Healthcare": 0.18,
                                    "Finance": 0.16,
                                    "Manufacturing": 0.15,
                                    "Retail": 0.15,
                                    "Services": 0.14,
                                }),
                            },
                        },
                    },
                },

                # ── campaign ──────────────────────────────────────
                "campaign": {
                    "description": "Marketing campaigns",
                    "primary_key": ["campaign_id"],
                    "columns": {
                        "campaign_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "campaign_name": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {
                                "strategy": "pattern",
                                "format": "CMP-{seq:4}",
                            },
                        },
                        "campaign_type_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "campaign_type.type_id",
                            },
                        },
                        "start_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "end_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "start_date",
                                "operation": "add_days",
                                "days": 30,
                            },
                        },
                        "budget": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 9.5,
                                "sigma": 1.0,
                                "min": 1000.0,
                                "max": 500000.0,
                                "round": 2,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("campaign.status", {
                                    "Active": 0.25,
                                    "Completed": 0.45,
                                    "Paused": 0.15,
                                    "Draft": 0.15,
                                }),
                            },
                        },
                    },
                },

                # ── lead_source ───────────────────────────────────
                "lead_source": {
                    "description": "Lead origins",
                    "primary_key": ["source_id"],
                    "columns": {
                        "source_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "source_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "lead_sources",
                            },
                        },
                        "source_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("lead_source.source_type", {
                                    "Inbound": 0.35,
                                    "Outbound": 0.25,
                                    "Referral": 0.22,
                                    "Partner": 0.18,
                                }),
                            },
                        },
                        "cost_per_lead": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.5,
                                "sigma": 0.8,
                                "min": 5.0,
                                "max": 500.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── contact ───────────────────────────────────────
                "contact": {
                    "description": "Marketing contacts",
                    "primary_key": ["contact_id"],
                    "columns": {
                        "contact_id": {
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
                            "null_rate": 0.15,
                            "generator": {"strategy": "faker", "provider": "phone_number"},
                        },
                        "company": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {"strategy": "faker", "provider": "company"},
                        },
                        "industry_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "industry.industry_id",
                            },
                        },
                        "lead_source_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "lead_source.source_id",
                            },
                        },
                        "created_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                    },
                },

                # ── lead ──────────────────────────────────────────
                "lead": {
                    "description": "Qualified leads from contacts",
                    "primary_key": ["lead_id"],
                    "columns": {
                        "lead_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "contact_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "contact.contact_id",
                            },
                        },
                        "campaign_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "campaign.campaign_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "lead_score": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0,
                                "max": 100,
                                "round": 0,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("lead.status", {
                                    "New": 0.20,
                                    "Contacted": 0.25,
                                    "Qualified": 0.20,
                                    "Unqualified": 0.15,
                                    "Converted": 0.20,
                                }),
                            },
                        },
                        "created_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                    },
                },

                # ── opportunity ────────────────────────────────────
                "opportunity": {
                    "description": "Sales opportunities from leads",
                    "primary_key": ["opp_id"],
                    "columns": {
                        "opp_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "lead_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "lead.lead_id",
                            },
                        },
                        "opp_name": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {
                                "strategy": "pattern",
                                "format": "OPP-{seq:6}",
                            },
                        },
                        "stage": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("opportunity.stage", {
                                    "Prospecting": 0.15,
                                    "Qualification": 0.18,
                                    "Proposal": 0.20,
                                    "Negotiation": 0.17,
                                    "Closed Won": 0.18,
                                    "Closed Lost": 0.12,
                                }),
                            },
                        },
                        "amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 9.0,
                                "sigma": 1.2,
                                "min": 500.0,
                                "max": 500000.0,
                                "round": 2,
                            },
                        },
                        "close_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-06-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "probability": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.05,
                                "max": 0.95,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── email_send ────────────────────────────────────
                "email_send": {
                    "description": "Email send events per campaign",
                    "primary_key": ["send_id"],
                    "columns": {
                        "send_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "campaign_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "campaign.campaign_id",
                            },
                        },
                        "contact_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "contact.contact_id",
                            },
                        },
                        "sent_date": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "opened": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("email_send.opened", {"true": 0.25, "false": 0.75}),
                            },
                        },
                        "clicked": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("email_send.clicked", {"true": 0.08, "false": 0.92}),
                            },
                        },
                        "bounced": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("email_send.bounced", {"true": 0.03, "false": 0.97}),
                            },
                        },
                    },
                },

                # ── web_visit ─────────────────────────────────────
                "web_visit": {
                    "description": "Website visits per contact",
                    "primary_key": ["visit_id"],
                    "columns": {
                        "visit_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "contact_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "contact.contact_id",
                            },
                        },
                        "visit_date": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "page_url": {
                            "type": "string",
                            "max_length": 500,
                            "generator": {"strategy": "faker", "provider": "uri"},
                        },
                        "referrer": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("web_visit.referrer", {
                                    "Organic": 0.30,
                                    "Paid": 0.22,
                                    "Social": 0.18,
                                    "Direct": 0.18,
                                    "Email": 0.12,
                                }),
                            },
                        },
                        "duration_seconds": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 4.5,
                                "sigma": 1.2,
                                "min": 1.0,
                                "max": 3600.0,
                                "round": 0,
                            },
                        },
                        "pages_viewed": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 1,
                                "max": 20,
                                "round": 0,
                            },
                        },
                    },
                },

                # ── conversion ────────────────────────────────────
                "conversion": {
                    "description": "Conversion events from leads",
                    "primary_key": ["conversion_id"],
                    "columns": {
                        "conversion_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "lead_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "lead.lead_id",
                            },
                        },
                        "campaign_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "campaign.campaign_id",
                            },
                        },
                        "conversion_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "conversion_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("conversion.conversion_type", {
                                    "Purchase": 0.30,
                                    "Trial": 0.25,
                                    "Demo": 0.25,
                                    "Signup": 0.20,
                                }),
                            },
                        },
                        "revenue": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 7.5,
                                "sigma": 1.2,
                                "min": 0.0,
                                "max": 100000.0,
                                "round": 2,
                            },
                        },
                    },
                },
            },

            # ── relationships ─────────────────────────────────
            "relationships": [
                {
                    "name": "campaign_campaign_type",
                    "parent": "campaign_type",
                    "child": "campaign",
                    "parent_key": "type_id",
                    "child_key": "campaign_type_id",
                },
                {
                    "name": "contact_industry",
                    "parent": "industry",
                    "child": "contact",
                    "parent_key": "industry_id",
                    "child_key": "industry_id",
                },
                {
                    "name": "contact_lead_source",
                    "parent": "lead_source",
                    "child": "contact",
                    "parent_key": "source_id",
                    "child_key": "lead_source_id",
                },
                {
                    "name": "lead_contact",
                    "parent": "contact",
                    "child": "lead",
                    "parent_key": "contact_id",
                    "child_key": "contact_id",
                },
                {
                    "name": "lead_campaign",
                    "parent": "campaign",
                    "child": "lead",
                    "parent_key": "campaign_id",
                    "child_key": "campaign_id",
                },
                {
                    "name": "opportunity_lead",
                    "parent": "lead",
                    "child": "opportunity",
                    "parent_key": "lead_id",
                    "child_key": "lead_id",
                },
                {
                    "name": "email_send_campaign",
                    "parent": "campaign",
                    "child": "email_send",
                    "parent_key": "campaign_id",
                    "child_key": "campaign_id",
                },
                {
                    "name": "email_send_contact",
                    "parent": "contact",
                    "child": "email_send",
                    "parent_key": "contact_id",
                    "child_key": "contact_id",
                },
                {
                    "name": "web_visit_contact",
                    "parent": "contact",
                    "child": "web_visit",
                    "parent_key": "contact_id",
                    "child_key": "contact_id",
                },
                {
                    "name": "conversion_lead",
                    "parent": "lead",
                    "child": "conversion",
                    "parent_key": "lead_id",
                    "child_key": "lead_id",
                },
                {
                    "name": "conversion_campaign",
                    "parent": "campaign",
                    "child": "conversion",
                    "parent_key": "campaign_id",
                    "child_key": "campaign_id",
                },
            ],

            # ── business rules ────────────────────────────────
            "business_rules": [
                {
                    "name": "budget_positive",
                    "description": "Campaign budget must be positive",
                    "type": "constraint",
                    "table": "campaign",
                    "rule": "budget > 0",
                },
                {
                    "name": "lead_score_range",
                    "description": "Lead score must be 0-100",
                    "type": "constraint",
                    "table": "lead",
                    "rule": "lead_score >= 0 AND lead_score <= 100",
                },
            ],

            # ── generation config ─────────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"campaign": 20},
                    "small": {"campaign": 200},
                    "medium": {"campaign": 2000},
                    "large": {"campaign": 20000},
                    "warehouse": {"campaign": 200000},
                },
                "derived_counts": {
                    "campaign_type": {"fixed": 15},
                    "industry": {"fixed": 25},
                    "lead_source": {"fixed": 20},
                    "contact": {"per_parent": "campaign", "ratio": self._ratio("contact_per_campaign", 25.0)},
                    "lead": {"per_parent": "contact", "ratio": self._ratio("lead_per_contact", 0.4)},
                    "opportunity": {"per_parent": "lead", "ratio": self._ratio("opportunity_per_lead", 0.5)},
                    "email_send": {"per_parent": "campaign", "ratio": self._ratio("email_send_per_campaign", 50.0)},
                    "web_visit": {"per_parent": "contact", "ratio": self._ratio("web_visit_per_contact", 5.0)},
                    "conversion": {"per_parent": "lead", "ratio": self._ratio("conversion_per_lead", 0.3)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)
