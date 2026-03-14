"""Manufacturing domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class ManufacturingDomain(Domain):
    """Manufacturing domain — production lines, work orders, quality, and equipment.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - production_line: Manufacturing lines
        - product: Products
        - bom: Bill of materials
        - work_order: Production work orders
        - quality_check: QC inspections
        - defect: Defect records
        - equipment: Machines and tools
        - downtime_event: Downtime tracking
        - production_metric: Production KPIs
    """

    @property
    def name(self) -> str:
        return "manufacturing"

    @property
    def description(self) -> str:
        return "Manufacturing domain with production lines, work orders, quality control, and equipment"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build manufacturing 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"manufacturing_{self._schema_mode}",
                "description": f"Manufacturing domain — {self._schema_mode} schema",
                "domain": "manufacturing",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── production_line ─────────────────────────────────
                "production_line": {
                    "description": "Manufacturing production lines",
                    "primary_key": ["line_id"],
                    "columns": {
                        "line_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "line_name": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "LINE-{seq:3}",
                            },
                        },
                        "facility": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("production_line.facility", {
                                    "Plant A": 0.30,
                                    "Plant B": 0.28,
                                    "Plant C": 0.22,
                                    "Plant D": 0.20,
                                }),
                            },
                        },
                        "line_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("production_line.line_type", {
                                    "Assembly": 0.30,
                                    "Machining": 0.25,
                                    "Packaging": 0.20,
                                    "Testing": 0.15,
                                    "Finishing": 0.10,
                                }),
                            },
                        },
                        "capacity_per_hour": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 50,
                                "max": 500,
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("production_line.is_active", {"true": 0.90, "false": 0.10}),
                            },
                        },
                    },
                },

                # ── product ────────────────────────────────────────
                "product": {
                    "description": "Manufactured products",
                    "primary_key": ["product_id"],
                    "columns": {
                        "product_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "product_name": {
                            "type": "string",
                            "max_length": 30,
                            "generator": {
                                "strategy": "pattern",
                                "format": "PRD-{seq:5}",
                            },
                        },
                        "product_category": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "material_types",
                            },
                        },
                        "unit_cost": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 4.5,
                                "sigma": 1.2,
                                "min": 1.00,
                                "max": 10000.00,
                                "round": 2,
                            },
                        },
                        "sell_price": {
                            "type": "float",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "unit_cost",
                                "operation": "multiply",
                                "params": {"factor_min": 1.3, "factor_max": 2.5},
                            },
                        },
                        "weight_kg": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 1.5,
                                "sigma": 1.0,
                                "min": 0.01,
                                "max": 500.00,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── bom ────────────────────────────────────────────
                "bom": {
                    "description": "Bill of materials — components per product",
                    "primary_key": ["bom_id"],
                    "columns": {
                        "bom_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "product_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "product.product_id",
                            },
                        },
                        "component_name": {
                            "type": "string",
                            "max_length": 30,
                            "generator": {
                                "strategy": "pattern",
                                "format": "CMP-{seq:5}",
                            },
                        },
                        "quantity_required": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 1,
                                "max": 50,
                            },
                        },
                        "unit_of_measure": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("bom.unit_of_measure", {
                                    "EA": 0.40,
                                    "KG": 0.20,
                                    "LB": 0.15,
                                    "M": 0.10,
                                    "LT": 0.15,
                                }),
                            },
                        },
                        "unit_cost": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.0,
                                "sigma": 1.3,
                                "min": 0.01,
                                "max": 5000.00,
                                "round": 2,
                            },
                        },
                        "is_critical": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("bom.is_critical", {"true": 0.25, "false": 0.75}),
                            },
                        },
                    },
                },

                # ── work_order ─────────────────────────────────────
                "work_order": {
                    "description": "Production work orders",
                    "primary_key": ["wo_id"],
                    "columns": {
                        "wo_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "work_order_number": {
                            "type": "string",
                            "max_length": 15,
                            "generator": {
                                "strategy": "pattern",
                                "format": "WO-{seq:6}",
                            },
                        },
                        "product_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "product.product_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "production_line_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "production_line.line_id",
                            },
                        },
                        "quantity_planned": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 5.0,
                                "sigma": 1.0,
                                "min": 1,
                                "max": 10000,
                            },
                        },
                        "quantity_produced": {
                            "type": "integer",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "quantity_planned",
                                "operation": "multiply",
                                "params": {"factor_min": 0.85, "factor_max": 1.0},
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
                                "days": 14,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("work_order.status", {
                                    "Open": 0.10,
                                    "In Progress": 0.15,
                                    "Completed": 0.55,
                                    "On Hold": 0.10,
                                    "Cancelled": 0.10,
                                }),
                            },
                        },
                        "priority": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("work_order.priority", {
                                    "High": 0.20,
                                    "Medium": 0.55,
                                    "Low": 0.25,
                                }),
                            },
                        },
                    },
                },

                # ── quality_check ──────────────────────────────────
                "quality_check": {
                    "description": "Quality control inspections",
                    "primary_key": ["check_id"],
                    "columns": {
                        "check_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "wo_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "work_order.wo_id",
                            },
                        },
                        "check_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "check_type": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "operation_types",
                            },
                        },
                        "result": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("quality_check.result", {
                                    "Pass": 0.82,
                                    "Fail": 0.08,
                                    "Rework": 0.10,
                                }),
                            },
                        },
                        "defect_count": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 0.5,
                                "sigma": 0.8,
                                "min": 0,
                                "max": 50,
                            },
                        },
                        "inspector_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                    },
                },

                # ── defect ─────────────────────────────────────────
                "defect": {
                    "description": "Defect records from quality checks",
                    "primary_key": ["defect_id"],
                    "columns": {
                        "defect_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "check_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "quality_check.check_id",
                            },
                        },
                        "defect_code": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "defect_codes",
                            },
                        },
                        "severity": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("defect.severity", {
                                    "Critical": 0.05,
                                    "Major": 0.15,
                                    "Minor": 0.50,
                                    "Cosmetic": 0.30,
                                }),
                            },
                        },
                        "root_cause": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("defect.root_cause", {
                                    "Material": 0.25,
                                    "Process": 0.25,
                                    "Equipment": 0.20,
                                    "Human": 0.20,
                                    "Design": 0.10,
                                }),
                            },
                        },
                        "disposition": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("defect.disposition", {
                                    "Scrap": 0.25,
                                    "Rework": 0.35,
                                    "Use As-Is": 0.25,
                                    "Return to Supplier": 0.15,
                                }),
                            },
                        },
                    },
                },

                # ── equipment ──────────────────────────────────────
                "equipment": {
                    "description": "Machines, tools, and equipment",
                    "primary_key": ["equipment_id"],
                    "columns": {
                        "equipment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "equipment_name": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "EQP-{seq:4}",
                            },
                        },
                        "equipment_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("equipment.equipment_type", {
                                    "CNC": 0.25,
                                    "Press": 0.18,
                                    "Conveyor": 0.15,
                                    "Robot": 0.15,
                                    "Oven": 0.12,
                                    "Tester": 0.15,
                                }),
                            },
                        },
                        "production_line_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "production_line.line_id",
                            },
                        },
                        "purchase_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2015-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("equipment.status", {
                                    "Operational": 0.80,
                                    "Under Maintenance": 0.15,
                                    "Decommissioned": 0.05,
                                }),
                            },
                        },
                        "last_maintenance_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2023-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                    },
                },

                # ── downtime_event ─────────────────────────────────
                "downtime_event": {
                    "description": "Equipment downtime tracking",
                    "primary_key": ["event_id"],
                    "columns": {
                        "event_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "equipment_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "equipment.equipment_id",
                            },
                        },
                        "start_time": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "duration_minutes": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.5,
                                "sigma": 1.0,
                                "min": 5,
                                "max": 1440,
                            },
                        },
                        "cause": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("downtime_event.cause", {
                                    "Breakdown": 0.25,
                                    "Planned Maintenance": 0.25,
                                    "Material Shortage": 0.15,
                                    "Changeover": 0.15,
                                    "Power Outage": 0.10,
                                    "Quality Issue": 0.10,
                                }),
                            },
                        },
                        "impact_level": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("downtime_event.impact_level", {
                                    "High": 0.20,
                                    "Medium": 0.50,
                                    "Low": 0.30,
                                }),
                            },
                        },
                    },
                },

                # ── production_metric ──────────────────────────────
                "production_metric": {
                    "description": "Production KPIs per work order",
                    "primary_key": ["metric_id"],
                    "columns": {
                        "metric_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "wo_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "work_order.wo_id",
                            },
                        },
                        "metric_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "oee_score": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.50,
                                "max": 0.99,
                                "round": 3,
                            },
                        },
                        "yield_rate": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.85,
                                "max": 0.99,
                                "round": 3,
                            },
                        },
                        "cycle_time_seconds": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 4.0,
                                "sigma": 0.8,
                                "min": 5.0,
                                "max": 3600.0,
                                "round": 1,
                            },
                        },
                        "scrap_rate": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.001,
                                "max": 0.10,
                                "round": 4,
                            },
                        },
                    },
                },
            },

            # ── relationships ──────────────────────────────────────
            "relationships": [
                {
                    "name": "bom_product",
                    "parent": "product",
                    "child": "bom",
                    "parent_key": "product_id",
                    "child_key": "product_id",
                },
                {
                    "name": "work_order_product",
                    "parent": "product",
                    "child": "work_order",
                    "parent_key": "product_id",
                    "child_key": "product_id",
                },
                {
                    "name": "work_order_production_line",
                    "parent": "production_line",
                    "child": "work_order",
                    "parent_key": "line_id",
                    "child_key": "production_line_id",
                },
                {
                    "name": "quality_check_work_order",
                    "parent": "work_order",
                    "child": "quality_check",
                    "parent_key": "wo_id",
                    "child_key": "wo_id",
                },
                {
                    "name": "defect_quality_check",
                    "parent": "quality_check",
                    "child": "defect",
                    "parent_key": "check_id",
                    "child_key": "check_id",
                },
                {
                    "name": "equipment_production_line",
                    "parent": "production_line",
                    "child": "equipment",
                    "parent_key": "line_id",
                    "child_key": "production_line_id",
                },
                {
                    "name": "downtime_event_equipment",
                    "parent": "equipment",
                    "child": "downtime_event",
                    "parent_key": "equipment_id",
                    "child_key": "equipment_id",
                },
                {
                    "name": "production_metric_work_order",
                    "parent": "work_order",
                    "child": "production_metric",
                    "parent_key": "wo_id",
                    "child_key": "wo_id",
                },
            ],

            # ── business rules ─────────────────────────────────────
            "business_rules": [
                {
                    "name": "quantity_produced_leq_planned",
                    "description": "Work order quantity_produced must be <= quantity_planned",
                    "type": "column_comparison",
                    "table": "work_order",
                    "left": "quantity_produced",
                    "operator": "<=",
                    "right": "quantity_planned",
                },
                {
                    "name": "sell_price_gte_unit_cost",
                    "description": "Product sell_price must be >= unit_cost",
                    "type": "column_comparison",
                    "table": "product",
                    "left": "sell_price",
                    "operator": ">=",
                    "right": "unit_cost",
                },
            ],

            # ── generation config ──────────────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"work_order": 50},
                    "small": {"work_order": 500},
                    "medium": {"work_order": 5000},
                    "large": {"work_order": 50000},
                    "warehouse": {"work_order": 5000000},
                },
                "derived_counts": {
                    "production_line": {"fixed": 20},
                    "product": {"fixed": 100},
                    "equipment": {"fixed": 80},
                    "bom": {"per_parent": "product", "ratio": self._ratio("bom_per_product", 5.0)},
                    "quality_check": {"per_parent": "work_order", "ratio": self._ratio("quality_check_per_work_order", 3.0)},
                    "defect": {"per_parent": "quality_check", "ratio": self._ratio("defect_per_quality_check", 0.3)},
                    "downtime_event": {"per_parent": "equipment", "ratio": self._ratio("downtime_event_per_equipment", 4.0)},
                    "production_metric": {"per_parent": "work_order", "ratio": self._ratio("production_metric_per_work_order", 5.0)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the Manufacturing domain.

        Produces:
          - dim_product         (from product)
          - dim_production_line (from production_line)
          - dim_equipment       (from equipment)
          - dim_date            (generated from start_date / check_date / start_time)
          - fact_work_order     (from work_order)
          - fact_quality        (from quality_check + work_order)
          - fact_downtime       (from downtime_event)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_product": DimSpec(
                    source="product",
                    sk="sk_product",
                    nk="product_id",
                ),
                "dim_production_line": DimSpec(
                    source="production_line",
                    sk="sk_line",
                    nk="line_id",
                ),
                "dim_equipment": DimSpec(
                    source="equipment",
                    sk="sk_equipment",
                    nk="equipment_id",
                ),
            },
            facts={
                "fact_work_order": FactSpec(
                    primary="work_order",
                    fk_map={
                        "product_id": "dim_product",
                        "line_id": "dim_production_line",
                    },
                    date_cols=["start_date"],
                ),
                "fact_quality": FactSpec(
                    primary="quality_check",
                    joins=[{
                        "table": "work_order",
                        "left_on": "wo_id",
                        "right_on": "wo_id",
                    }],
                    fk_map={
                        "product_id": "dim_product",
                        "line_id": "dim_production_line",
                    },
                    date_cols=["check_date"],
                ),
                "fact_downtime": FactSpec(
                    primary="downtime_event",
                    fk_map={
                        "equipment_id": "dim_equipment",
                    },
                    date_cols=["start_time"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Manufacturing domain."""
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "production_line": "BusinessUnit",
            "product": "Product",
            "bom": "BillOfMaterials",
            "work_order": "WorkOrder",
            "quality_check": "QualityOrder",
            "defect": "Case",
            "equipment": "Asset",
            "downtime_event": "Incident",
            "production_metric": "Observation",
        })
