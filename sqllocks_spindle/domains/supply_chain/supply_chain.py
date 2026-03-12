"""Supply chain domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class SupplyChainDomain(Domain):
    """Supply chain and logistics domain.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - warehouse: Distribution centers with locations
        - supplier: Material and product suppliers
        - material: Raw materials and components
        - purchase_order: Purchase orders to suppliers
        - purchase_order_line: PO line items
        - inventory: Stock levels by warehouse and material
        - shipment: Inbound/outbound shipments
        - shipment_event: Shipment tracking events
        - quality_inspection: QA inspection results
        - demand_forecast: Forecasted demand by period
    """

    @property
    def name(self) -> str:
        return "supply_chain"

    @property
    def description(self) -> str:
        return "Supply chain domain with warehouses, purchasing, inventory, and logistics"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build supply chain 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"supply_chain_{self._schema_mode}",
                "description": f"Supply chain domain — {self._schema_mode} schema",
                "domain": "supply_chain",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── warehouse ─────────────────────────────────
                "warehouse": {
                    "description": "Distribution centers with locations",
                    "primary_key": ["warehouse_id"],
                    "columns": {
                        "warehouse_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "warehouse_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "pattern",
                                "format": "DC-{seq:4}",
                            },
                        },
                        "warehouse_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("warehouse.warehouse_type", {
                                    "Distribution Center": 0.40,
                                    "Regional Hub": 0.25,
                                    "Cross-Dock": 0.15,
                                    "Cold Storage": 0.12,
                                    "Fulfillment Center": 0.08,
                                }),
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
                        "capacity_sqft": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 11.0,
                                "sigma": 0.8,
                                "min": 10000,
                                "max": 500000,
                                "round": 0,
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("warehouse.is_active", {"true": 0.94, "false": 0.06}),
                            },
                        },
                    },
                },

                # ── supplier ──────────────────────────────────
                "supplier": {
                    "description": "Material and product suppliers",
                    "primary_key": ["supplier_id"],
                    "columns": {
                        "supplier_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "supplier_name": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {"strategy": "faker", "provider": "company"},
                        },
                        "contact_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                        "contact_email": {
                            "type": "string",
                            "max_length": 255,
                            "generator": {"strategy": "faker", "provider": "company_email"},
                        },
                        "country": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("supplier.country", {
                                    "United States": 0.35,
                                    "China": 0.20,
                                    "Germany": 0.10,
                                    "Mexico": 0.10,
                                    "Japan": 0.08,
                                    "South Korea": 0.05,
                                    "India": 0.05,
                                    "Taiwan": 0.04,
                                    "Canada": 0.03,
                                }),
                            },
                        },
                        "lead_time_days": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.0,
                                "sigma": 0.7,
                                "min": 1,
                                "max": 120,
                                "round": 0,
                            },
                        },
                        "reliability_score": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.60,
                                "max": 0.99,
                                "round": 2,
                            },
                        },
                        "is_active": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("supplier.is_active", {"true": 0.90, "false": 0.10}),
                            },
                        },
                    },
                },

                # ── material ──────────────────────────────────
                "material": {
                    "description": "Raw materials and components",
                    "primary_key": ["material_id"],
                    "columns": {
                        "material_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "material_name": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {
                                "strategy": "pattern",
                                "format": "MAT-{seq:5}",
                            },
                        },
                        "category": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "material_categories",
                            },
                        },
                        "unit_of_measure": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("material.unit_of_measure", {
                                    "EA": 0.40,
                                    "KG": 0.20,
                                    "LB": 0.15,
                                    "LT": 0.10,
                                    "M": 0.08,
                                    "FT": 0.07,
                                }),
                            },
                        },
                        "unit_cost": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.5,
                                "sigma": 1.5,
                                "min": 0.01,
                                "max": 50000.0,
                                "round": 2,
                            },
                        },
                        "reorder_point": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 10,
                                "max": 500,
                                "round": 0,
                            },
                        },
                        "is_hazardous": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": {"true": 0.08, "false": 0.92},
                            },
                        },
                    },
                },

                # ── purchase_order ─────────────────────────────
                "purchase_order": {
                    "description": "Purchase orders to suppliers",
                    "primary_key": ["po_id"],
                    "columns": {
                        "po_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "po_number": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "PO-{seq:6}",
                            },
                        },
                        "supplier_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "supplier.supplier_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "order_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "expected_delivery_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "order_date",
                                "operation": "add_days",
                                "days": 21,
                            },
                        },
                        "total_amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 9.0,
                                "sigma": 1.5,
                                "min": 100.0,
                                "max": 5000000.0,
                                "round": 2,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("purchase_order.status", {
                                    "Approved": 0.30,
                                    "Shipped": 0.20,
                                    "Delivered": 0.35,
                                    "Cancelled": 0.05,
                                    "Pending": 0.10,
                                }),
                            },
                        },
                        "priority": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("purchase_order.priority", {
                                    "Standard": 0.60,
                                    "Expedited": 0.25,
                                    "Critical": 0.10,
                                    "Low": 0.05,
                                }),
                            },
                        },
                    },
                },

                # ── purchase_order_line ────────────────────────
                "purchase_order_line": {
                    "description": "Purchase order line items",
                    "primary_key": ["po_line_id"],
                    "columns": {
                        "po_line_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "po_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "purchase_order.po_id",
                            },
                        },
                        "material_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "material.material_id",
                                "distribution": "zipf",
                                "alpha": 1.1,
                            },
                        },
                        "quantity_ordered": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 5.0,
                                "sigma": 1.2,
                                "min": 1,
                                "max": 10000,
                                "round": 0,
                            },
                        },
                        "quantity_received": {
                            "type": "integer",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "quantity_ordered",
                                "operation": "multiply",
                                "params": {"factor_min": 0.85, "factor_max": 1.00},
                            },
                        },
                        "unit_price": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 3.5,
                                "sigma": 1.5,
                                "min": 0.10,
                                "max": 50000.0,
                                "round": 2,
                            },
                        },
                        "line_total": {
                            "type": "float",
                            "generator": {
                                "strategy": "formula",
                                "expression": "quantity_ordered * unit_price",
                            },
                        },
                    },
                },

                # ── inventory ─────────────────────────────────
                "inventory": {
                    "description": "Stock levels by warehouse and material",
                    "primary_key": ["inventory_id"],
                    "columns": {
                        "inventory_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "warehouse_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "warehouse.warehouse_id",
                                "distribution": "zipf",
                                "alpha": 1.1,
                            },
                        },
                        "material_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "material.material_id",
                            },
                        },
                        "quantity_on_hand": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 6.0,
                                "sigma": 1.5,
                                "min": 0,
                                "max": 100000,
                                "round": 0,
                            },
                        },
                        "quantity_reserved": {
                            "type": "integer",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "quantity_on_hand",
                                "operation": "multiply",
                                "params": {"factor_min": 0.0, "factor_max": 0.40},
                            },
                        },
                        "last_counted_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2024-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "bin_location": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "A{seq:2}-R{seq:2}-S{seq:2}",
                            },
                        },
                    },
                },

                # ── shipment ──────────────────────────────────
                "shipment": {
                    "description": "Inbound and outbound shipments",
                    "primary_key": ["shipment_id"],
                    "columns": {
                        "shipment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "shipment_number": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "SHP-{seq:6}",
                            },
                        },
                        "po_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "purchase_order.po_id",
                            },
                        },
                        "warehouse_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "warehouse.warehouse_id",
                                "distribution": "zipf",
                                "alpha": 1.1,
                            },
                        },
                        "carrier_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "carrier_names",
                            },
                        },
                        "shipping_method": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "shipping_methods",
                            },
                        },
                        "ship_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "delivery_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "derived",
                                "source": "ship_date",
                                "operation": "add_days",
                                "days": 5,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("shipment.status", {
                                    "In Transit": 0.20,
                                    "Delivered": 0.55,
                                    "Pending Pickup": 0.10,
                                    "Delayed": 0.08,
                                    "Returned": 0.04,
                                    "Lost": 0.03,
                                }),
                            },
                        },
                        "weight_kg": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 5.0,
                                "sigma": 1.5,
                                "min": 0.5,
                                "max": 50000.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── shipment_event ────────────────────────────
                "shipment_event": {
                    "description": "Shipment tracking events",
                    "primary_key": ["event_id"],
                    "columns": {
                        "event_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "shipment_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "shipment.shipment_id",
                            },
                        },
                        "event_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("shipment_event.event_type", {
                                    "Picked Up": 0.20,
                                    "In Transit": 0.25,
                                    "At Hub": 0.15,
                                    "Out for Delivery": 0.15,
                                    "Delivered": 0.15,
                                    "Exception": 0.05,
                                    "Returned to Sender": 0.05,
                                }),
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
                        "location": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {"strategy": "faker", "provider": "city"},
                        },
                        "notes": {
                            "type": "string",
                            "max_length": 500,
                            "nullable": True,
                            "null_rate": 0.60,
                            "generator": {"strategy": "faker", "provider": "sentence"},
                        },
                    },
                },

                # ── quality_inspection ────────────────────────
                "quality_inspection": {
                    "description": "QA inspection results",
                    "primary_key": ["inspection_id"],
                    "columns": {
                        "inspection_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "shipment_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "shipment.shipment_id",
                            },
                        },
                        "inspector_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                        "inspection_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2022-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "result": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("quality_inspection.result", {
                                    "Pass": 0.75,
                                    "Fail": 0.10,
                                    "Conditional Pass": 0.10,
                                    "Pending Retest": 0.05,
                                }),
                            },
                        },
                        "defect_count": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 0.5,
                                "sigma": 1.0,
                                "min": 0,
                                "max": 100,
                                "round": 0,
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

                # ── demand_forecast ───────────────────────────
                "demand_forecast": {
                    "description": "Forecasted demand by period",
                    "primary_key": ["forecast_id"],
                    "columns": {
                        "forecast_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "material_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "material.material_id",
                            },
                        },
                        "forecast_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2024-01-01",
                                "end": "2026-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "forecast_quantity": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 6.0,
                                "sigma": 1.2,
                                "min": 1,
                                "max": 50000,
                                "round": 0,
                            },
                        },
                        "confidence_level": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.50,
                                "max": 0.99,
                                "round": 2,
                            },
                        },
                        "forecast_method": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("demand_forecast.forecast_method", {
                                    "Moving Average": 0.30,
                                    "Exponential Smoothing": 0.25,
                                    "ARIMA": 0.20,
                                    "Machine Learning": 0.15,
                                    "Manual": 0.10,
                                }),
                            },
                        },
                    },
                },
            },

            # ── relationships ───────────────────────────────
            "relationships": [
                {
                    "name": "po_supplier",
                    "parent": "supplier",
                    "child": "purchase_order",
                    "parent_key": "supplier_id",
                    "child_key": "supplier_id",
                },
                {
                    "name": "po_line_po",
                    "parent": "purchase_order",
                    "child": "purchase_order_line",
                    "parent_key": "po_id",
                    "child_key": "po_id",
                },
                {
                    "name": "po_line_material",
                    "parent": "material",
                    "child": "purchase_order_line",
                    "parent_key": "material_id",
                    "child_key": "material_id",
                },
                {
                    "name": "inventory_warehouse",
                    "parent": "warehouse",
                    "child": "inventory",
                    "parent_key": "warehouse_id",
                    "child_key": "warehouse_id",
                },
                {
                    "name": "inventory_material",
                    "parent": "material",
                    "child": "inventory",
                    "parent_key": "material_id",
                    "child_key": "material_id",
                },
                {
                    "name": "shipment_po",
                    "parent": "purchase_order",
                    "child": "shipment",
                    "parent_key": "po_id",
                    "child_key": "po_id",
                },
                {
                    "name": "shipment_warehouse",
                    "parent": "warehouse",
                    "child": "shipment",
                    "parent_key": "warehouse_id",
                    "child_key": "warehouse_id",
                },
                {
                    "name": "shipment_event_shipment",
                    "parent": "shipment",
                    "child": "shipment_event",
                    "parent_key": "shipment_id",
                    "child_key": "shipment_id",
                },
                {
                    "name": "quality_inspection_shipment",
                    "parent": "shipment",
                    "child": "quality_inspection",
                    "parent_key": "shipment_id",
                    "child_key": "shipment_id",
                },
                {
                    "name": "demand_forecast_material",
                    "parent": "material",
                    "child": "demand_forecast",
                    "parent_key": "material_id",
                    "child_key": "material_id",
                },
            ],

            # ── business rules ──────────────────────────────
            "business_rules": [
                {
                    "name": "received_leq_ordered",
                    "description": "Received quantity must be <= ordered quantity",
                    "type": "column_comparison",
                    "table": "purchase_order_line",
                    "left": "quantity_received",
                    "operator": "<=",
                    "right": "quantity_ordered",
                },
                {
                    "name": "shipment_delivery_after_ship",
                    "description": "Delivery date must be >= ship date",
                    "type": "column_comparison",
                    "table": "shipment",
                    "left": "ship_date",
                    "operator": "<=",
                    "right": "delivery_date",
                },
            ],

            # ── generation config ─────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"purchase_order": 200},
                    "small": {"purchase_order": 2000},
                    "medium": {"purchase_order": 20000},
                    "large": {"purchase_order": 200000},
                    "xlarge": {"purchase_order": 2000000},
                    "warehouse": {"purchase_order": 20000000},
                },
                "derived_counts": {
                    "warehouse": {"fixed": 50},
                    "supplier": {"fixed": 200},
                    "material": {"fixed": 300},
                    "purchase_order_line": {"per_parent": "purchase_order", "ratio": self._ratio("po_line_per_po", 3.0)},
                    "inventory": {"per_parent": "material", "ratio": self._ratio("inventory_per_material", 3.0)},
                    "shipment": {"per_parent": "purchase_order", "ratio": self._ratio("shipment_per_po", 1.2)},
                    "shipment_event": {"per_parent": "shipment", "ratio": self._ratio("event_per_shipment", 4.0)},
                    "quality_inspection": {"per_parent": "shipment", "ratio": self._ratio("inspection_per_shipment", 0.3)},
                    "demand_forecast": {"per_parent": "material", "ratio": self._ratio("forecast_per_material", 6.0)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)
