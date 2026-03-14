"""IoT domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class IoTDomain(Domain):
    """Internet of Things domain — devices, sensors, readings, alerts, and maintenance.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - device_type: IoT device type catalog (fixed: 20)
        - location: Deployment locations (fixed: 100)
        - device: Deployed IoT devices (anchor: 500 small)
        - sensor: Device sensors (per_parent: device, ratio: 2.5)
        - reading: Sensor readings (per_parent: sensor, ratio: 20.0)
        - alert: Device alerts (per_parent: device, ratio: 0.5)
        - maintenance_log: Maintenance records (per_parent: device, ratio: 1.5)
        - command: Device commands (per_parent: device, ratio: 3.0)
    """

    @property
    def name(self) -> str:
        return "iot"

    @property
    def description(self) -> str:
        return "IoT domain with devices, sensors, readings, alerts, and maintenance"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build IoT 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"iot_{self._schema_mode}",
                "description": f"IoT domain — {self._schema_mode} schema",
                "domain": "iot",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2022-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── device_type ───────────────────────────────
                "device_type": {
                    "description": "IoT device type catalog",
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
                                "dataset": "device_types",
                            },
                        },
                        "manufacturer": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "company"},
                        },
                        "category": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("device_type.category", {
                                    "Industrial": 0.30,
                                    "Consumer": 0.25,
                                    "Automotive": 0.15,
                                    "Medical": 0.15,
                                    "Smart Home": 0.15,
                                }),
                            },
                        },
                        "protocol": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("device_type.protocol", {
                                    "MQTT": 0.35,
                                    "HTTP": 0.25,
                                    "CoAP": 0.15,
                                    "AMQP": 0.15,
                                    "Modbus": 0.10,
                                }),
                            },
                        },
                    },
                },

                # ── location ─────────────────────────────────
                "location": {
                    "description": "Deployment locations",
                    "primary_key": ["location_id"],
                    "columns": {
                        "location_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "location_name": {
                            "type": "string",
                            "max_length": 150,
                            "generator": {
                                "strategy": "pattern",
                                "format": "LOC-{seq:4}",
                            },
                        },
                        "facility_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("location.facility_type", {
                                    "Factory": 0.25,
                                    "Warehouse": 0.25,
                                    "Office": 0.20,
                                    "Field": 0.15,
                                    "Residential": 0.15,
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
                    },
                },

                # ── device ───────────────────────────────────
                "device": {
                    "description": "Deployed IoT devices",
                    "primary_key": ["device_id"],
                    "columns": {
                        "device_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "serial_number": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "DEV-{seq:6}",
                            },
                        },
                        "device_type_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "device_type.type_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "location_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "location.location_id",
                                "distribution": "zipf",
                                "alpha": 1.1,
                            },
                        },
                        "firmware_version": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "pattern",
                                "format": "v{seq:1}.{seq:1}.{seq:1}",
                            },
                        },
                        "install_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2020-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("device.status", {
                                    "Active": 0.72,
                                    "Inactive": 0.10,
                                    "Maintenance": 0.10,
                                    "Decommissioned": 0.08,
                                }),
                            },
                        },
                        "battery_level": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 0.0,
                                "max": 100.0,
                                "round": 1,
                            },
                        },
                    },
                },

                # ── sensor ───────────────────────────────────
                "sensor": {
                    "description": "Device sensors",
                    "primary_key": ["sensor_id"],
                    "columns": {
                        "sensor_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "device_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "device.device_id",
                            },
                        },
                        "sensor_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("sensor.sensor_type", {
                                    "Temperature": 0.18,
                                    "Humidity": 0.15,
                                    "Pressure": 0.14,
                                    "Vibration": 0.12,
                                    "Flow": 0.10,
                                    "Level": 0.10,
                                    "Proximity": 0.11,
                                    "Light": 0.10,
                                }),
                            },
                        },
                        "unit_of_measure": {
                            "type": "string",
                            "max_length": 20,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("sensor.unit_of_measure", {
                                    "C": 0.18,
                                    "%RH": 0.15,
                                    "Pa": 0.14,
                                    "mm/s": 0.12,
                                    "L/min": 0.10,
                                    "m": 0.10,
                                    "cm": 0.11,
                                    "lux": 0.10,
                                }),
                            },
                        },
                        "min_range": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": -50.0,
                                "max": 0.0,
                                "round": 2,
                            },
                        },
                        "max_range": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 50.0,
                                "max": 500.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── reading ──────────────────────────────────
                "reading": {
                    "description": "Sensor readings",
                    "primary_key": ["reading_id"],
                    "columns": {
                        "reading_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "sensor_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "sensor.sensor_id",
                                "distribution": "zipf",
                                "alpha": 1.1,
                            },
                        },
                        "reading_value": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "normal",
                                "mean": 25.0,
                                "sigma": 15.0,
                                "min": -50.0,
                                "max": 500.0,
                                "round": 3,
                            },
                        },
                        "reading_timestamp": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2024-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "quality_flag": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("reading.quality_flag", {
                                    "Good": 0.90,
                                    "Suspect": 0.07,
                                    "Bad": 0.03,
                                }),
                            },
                        },
                    },
                },

                # ── alert ────────────────────────────────────
                "alert": {
                    "description": "Device alerts",
                    "primary_key": ["alert_id"],
                    "columns": {
                        "alert_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "device_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "device.device_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "alert_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("alert.alert_type", {
                                    "Threshold": 0.30,
                                    "Anomaly": 0.25,
                                    "Connectivity": 0.20,
                                    "Battery": 0.15,
                                    "Maintenance": 0.10,
                                }),
                            },
                        },
                        "severity": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("alert.severity", {
                                    "Critical": 0.10,
                                    "High": 0.20,
                                    "Medium": 0.40,
                                    "Low": 0.30,
                                }),
                            },
                        },
                        "triggered_at": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2024-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "resolved_at": {
                            "type": "datetime",
                            "nullable": True,
                            "null_rate": 0.25,
                            "generator": {
                                "strategy": "derived",
                                "source": "triggered_at",
                                "rule": "add_days",
                                "params": {"distribution": "uniform", "min": 0, "max": 7},
                            },
                        },
                        "acknowledged": {
                            "type": "boolean",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("alert.acknowledged", {"true": 0.70, "false": 0.30}),
                            },
                        },
                    },
                },

                # ── maintenance_log ──────────────────────────
                "maintenance_log": {
                    "description": "Device maintenance records",
                    "primary_key": ["log_id"],
                    "columns": {
                        "log_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "device_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "device.device_id",
                            },
                        },
                        "maintenance_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("maintenance_log.maintenance_type", {
                                    "Preventive": 0.35,
                                    "Corrective": 0.25,
                                    "Calibration": 0.20,
                                    "Firmware Update": 0.20,
                                }),
                            },
                        },
                        "performed_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2023-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "performed_by": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                        "notes": {
                            "type": "string",
                            "max_length": 500,
                            "nullable": True,
                            "null_rate": 0.30,
                            "generator": {"strategy": "faker", "provider": "sentence"},
                        },
                    },
                },

                # ── command ──────────────────────────────────
                "command": {
                    "description": "Device commands",
                    "primary_key": ["command_id"],
                    "columns": {
                        "command_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "device_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "device.device_id",
                            },
                        },
                        "command_type": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("command.command_type", {
                                    "Reboot": 0.20,
                                    "Configure": 0.25,
                                    "Update": 0.20,
                                    "Reset": 0.15,
                                    "Calibrate": 0.20,
                                }),
                            },
                        },
                        "issued_at": {
                            "type": "datetime",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2024-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("command.status", {
                                    "Sent": 0.15,
                                    "Acknowledged": 0.20,
                                    "Executed": 0.55,
                                    "Failed": 0.10,
                                }),
                            },
                        },
                        "issued_by": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {"strategy": "faker", "provider": "name"},
                        },
                    },
                },
            },

            # ── relationships ───────────────────────────────
            "relationships": [
                {
                    "name": "device_device_type",
                    "parent": "device_type",
                    "child": "device",
                    "parent_key": "type_id",
                    "child_key": "device_type_id",
                },
                {
                    "name": "device_location",
                    "parent": "location",
                    "child": "device",
                    "parent_key": "location_id",
                    "child_key": "location_id",
                },
                {
                    "name": "sensor_device",
                    "parent": "device",
                    "child": "sensor",
                    "parent_key": "device_id",
                    "child_key": "device_id",
                },
                {
                    "name": "reading_sensor",
                    "parent": "sensor",
                    "child": "reading",
                    "parent_key": "sensor_id",
                    "child_key": "sensor_id",
                },
                {
                    "name": "alert_device",
                    "parent": "device",
                    "child": "alert",
                    "parent_key": "device_id",
                    "child_key": "device_id",
                },
                {
                    "name": "maintenance_log_device",
                    "parent": "device",
                    "child": "maintenance_log",
                    "parent_key": "device_id",
                    "child_key": "device_id",
                },
                {
                    "name": "command_device",
                    "parent": "device",
                    "child": "command",
                    "parent_key": "device_id",
                    "child_key": "device_id",
                },
            ],

            # ── business rules ──────────────────────────────
            "business_rules": [
                {
                    "name": "battery_level_range",
                    "description": "Battery level must be between 0 and 100",
                    "type": "constraint",
                    "table": "device",
                    "rule": "battery_level >= 0 AND battery_level <= 100",
                },
                {
                    "name": "max_range_exceeds_min",
                    "description": "Sensor max_range must be >= min_range",
                    "type": "column_comparison",
                    "table": "sensor",
                    "left": "max_range",
                    "operator": ">=",
                    "right": "min_range",
                },
            ],

            # ── generation config ─────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"device": 50},
                    "small": {"device": 500},
                    "medium": {"device": 5000},
                    "large": {"device": 50000},
                    "xlarge": {"device": 500000},
                    "warehouse": {"device": 5000000},
                },
                "derived_counts": {
                    "device_type": {"fixed": 20},
                    "location": {"fixed": 100},
                    "sensor": {"per_parent": "device", "ratio": self._ratio("sensor_per_device", 2.5)},
                    "reading": {"per_parent": "sensor", "ratio": self._ratio("reading_per_sensor", 20.0)},
                    "alert": {"per_parent": "device", "ratio": self._ratio("alert_per_device", 0.5)},
                    "maintenance_log": {"per_parent": "device", "ratio": self._ratio("maintenance_per_device", 1.5)},
                    "command": {"per_parent": "device", "ratio": self._ratio("command_per_device", 3.0)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the IoT domain.

        Produces:
          - dim_device   (from device, enriched with device_type)
          - dim_location (from location)
          - dim_sensor   (from sensor)
          - fact_reading (from reading)
          - fact_alert   (from alert)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_device": DimSpec(
                    source="device",
                    sk="sk_device",
                    nk="device_id",
                    enrich=[{
                        "table": "device_type",
                        "left_on": "type_id",
                        "right_on": "type_id",
                        "prefix": "type_",
                    }],
                ),
                "dim_location": DimSpec(
                    source="location",
                    sk="sk_location",
                    nk="location_id",
                ),
                "dim_sensor": DimSpec(
                    source="sensor",
                    sk="sk_sensor",
                    nk="sensor_id",
                ),
            },
            facts={
                "fact_reading": FactSpec(
                    primary="reading",
                    fk_map={"sensor_id": "dim_sensor"},
                    date_cols=["reading_timestamp"],
                ),
                "fact_alert": FactSpec(
                    primary="alert",
                    fk_map={"device_id": "dim_device"},
                    date_cols=["triggered_at"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the IoT domain.

        Maps source table names to Microsoft Common Data Model entity names.
        """
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "device_type": "Category",
            "location": "Location",
            "device": "Asset",
            "sensor": "Component",
            "reading": "Observation",
            "alert": "Alert",
            "maintenance_log": "WorkOrder",
            "command": "Command",
        })
