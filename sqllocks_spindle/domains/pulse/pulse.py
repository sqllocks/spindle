"""Pulse rideshare domain.

Branded synthetic dataset for the four-store Fabric demo. Core entities are
generated here (rider, driver, vehicle, trip) with realistic seasonality baked
into ``trip.requested_at``. Downstream telemetry (driver_pings, trip_events,
surge_signals) and finance marts (fact_revenue_daily, fact_driver_earnings) are
DERIVED from trips by the Pulse simulator / streaming notebook (see SPEC §5),
not generated as base tables.

4 metros (city_id 1-4): Seattle, Austin, Atlanta, Chicago.
"""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SchemaParser, SpindleSchema


class PulseDomain(Domain):
    """Pulse rideshare domain — riders, drivers, vehicles, trips.

    Tables (3NF mode):
        - rider:   passengers (current state)
        - driver:  drivers (current state)
        - vehicle: one per driver
        - trip:    the seasonal fact — requested_at carries diurnal/weekly/annual pattern
    """

    @property
    def name(self) -> str:
        return "pulse"

    @property
    def description(self) -> str:
        return "Pulse rideshare — riders, drivers, vehicles, seasonal trips across 4 US metros"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        # city_id weighting across the 4 metros (Seattle/Austin/Atlanta/Chicago)
        city_weights = self._dist("city_id", {"1": 0.28, "2": 0.22, "3": 0.25, "4": 0.25})

        schema_dict = {
            "model": {
                "name": "pulse_3nf",
                "description": "Pulse rideshare — 3NF schema",
                "domain": "pulse",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2024-06-01", "end": "2026-06-01"},
            },
            "tables": {
                "rider": {
                    "description": "Passengers (current state)",
                    "primary_key": ["rider_id"],
                    "columns": {
                        "rider_id": {"type": "integer", "generator": {"strategy": "sequence", "start": 1}},
                        "full_name": {"type": "string", "max_length": 100,
                                      "generator": {"strategy": "faker", "provider": "name"}},
                        "email": {"type": "string", "max_length": 255, "nullable": True, "null_rate": 0.03,
                                  "generator": {"strategy": "faker", "provider": "email"}},
                        "phone": {"type": "string", "max_length": 20,
                                  "generator": {"strategy": "faker", "provider": "phone_number"}},
                        "home_city_id": {"type": "integer",
                                         "generator": {"strategy": "weighted_enum", "values": city_weights}},
                        "rating": {"type": "decimal", "precision": 2, "scale": 1,
                                   "generator": {"strategy": "weighted_enum", "values": self._dist("rider.rating", {
                                       4.5: 0.10, 4.6: 0.12, 4.7: 0.18, 4.8: 0.24, 4.9: 0.22, 5.0: 0.14})}},
                        "joined_at": {"type": "timestamp",
                                      "generator": {"strategy": "temporal", "pattern": "uniform",
                                                    "range_ref": "model.date_range"}},
                        "payment_default": {"type": "string", "max_length": 12,
                                            "generator": {"strategy": "weighted_enum", "values": self._dist(
                                                "rider.payment_default", {"card": 0.68, "wallet": 0.27, "cash": 0.05})}},
                    },
                },
                "driver": {
                    "description": "Drivers (current state)",
                    "primary_key": ["driver_id"],
                    "columns": {
                        "driver_id": {"type": "integer", "generator": {"strategy": "sequence", "start": 1}},
                        "full_name": {"type": "string", "max_length": 100,
                                      "generator": {"strategy": "faker", "provider": "name"}},
                        "email": {"type": "string", "max_length": 255, "nullable": True, "null_rate": 0.02,
                                  "generator": {"strategy": "faker", "provider": "email"}},
                        "phone": {"type": "string", "max_length": 20,
                                  "generator": {"strategy": "faker", "provider": "phone_number"}},
                        "home_city_id": {"type": "integer",
                                         "generator": {"strategy": "weighted_enum", "values": city_weights}},
                        "rating": {"type": "decimal", "precision": 2, "scale": 1,
                                   "generator": {"strategy": "weighted_enum", "values": self._dist("driver.rating", {
                                       4.6: 0.08, 4.7: 0.14, 4.8: 0.26, 4.9: 0.30, 5.0: 0.22})}},
                        "status": {"type": "string", "max_length": 12,
                                   "generator": {"strategy": "weighted_enum", "values": self._dist("driver.status", {
                                       "offline": 0.45, "available": 0.30, "enroute": 0.12, "on_trip": 0.13})}},
                        "onboarded_at": {"type": "timestamp",
                                         "generator": {"strategy": "temporal", "pattern": "uniform",
                                                       "range_ref": "model.date_range"}},
                        "is_active": {"type": "string",
                                      "generator": {"strategy": "weighted_enum", "values": self._dist(
                                          "driver.is_active", {"true": 0.88, "false": 0.12})}},
                    },
                },
                "vehicle": {
                    "description": "One vehicle per driver",
                    "primary_key": ["vehicle_id"],
                    "columns": {
                        "vehicle_id": {"type": "integer", "generator": {"strategy": "sequence", "start": 1}},
                        "driver_id": {"type": "integer",
                                      "generator": {"strategy": "foreign_key", "ref": "driver.driver_id"}},
                        "make": {"type": "string", "max_length": 30,
                                 "generator": {"strategy": "weighted_enum", "values": self._dist("vehicle.make", {
                                     "Toyota": 0.22, "Honda": 0.18, "Ford": 0.12, "Chevrolet": 0.10,
                                     "Tesla": 0.10, "Nissan": 0.10, "Hyundai": 0.09, "Kia": 0.09})}},
                        "model": {"type": "string", "max_length": 30,
                                  "generator": {"strategy": "weighted_enum", "values": self._dist("vehicle.model", {
                                      "Camry": 0.16, "Civic": 0.14, "Model 3": 0.12, "Accord": 0.12,
                                      "Corolla": 0.12, "Altima": 0.10, "Fusion": 0.12, "Elantra": 0.12})}},
                        "year": {"type": "integer",
                                 "generator": {"strategy": "weighted_enum", "values": self._dist("vehicle.year", {
                                     2017: 0.08, 2018: 0.12, 2019: 0.16, 2020: 0.18,
                                     2021: 0.16, 2022: 0.14, 2023: 0.10, 2024: 0.06})}},
                        "color": {"type": "string", "max_length": 20,
                                  "generator": {"strategy": "weighted_enum", "values": self._dist("vehicle.color", {
                                      "White": 0.26, "Black": 0.22, "Silver": 0.18, "Gray": 0.14,
                                      "Blue": 0.10, "Red": 0.06, "Other": 0.04})}},
                        "license_plate": {"type": "string", "max_length": 10,
                                          "generator": {"strategy": "pattern", "format": "{seq:6}"}},
                        "capacity": {"type": "integer",
                                     "generator": {"strategy": "weighted_enum", "values": self._dist(
                                         "vehicle.capacity", {4: 0.82, 6: 0.18})}},
                        "vehicle_class": {"type": "string", "max_length": 10,
                                          "generator": {"strategy": "weighted_enum", "values": self._dist(
                                              "vehicle.vehicle_class",
                                              {"standard": 0.62, "xl": 0.16, "premium": 0.12, "ev": 0.10})}},
                        "city_id": {"type": "integer",
                                    "generator": {"strategy": "lookup", "source_table": "driver",
                                                  "source_column": "home_city_id", "via": "driver_id"}},
                    },
                },
                "trip": {
                    "description": "Trips — the seasonal fact (requested_at carries diurnal/weekly/annual pattern)",
                    "primary_key": ["trip_id"],
                    "columns": {
                        "trip_id": {"type": "integer", "generator": {"strategy": "sequence", "start": 1}},
                        "rider_id": {"type": "integer",
                                     "generator": {"strategy": "foreign_key", "ref": "rider.rider_id",
                                                   "distribution": "pareto",
                                                   "params": {"alpha": 1.16, "max_per_parent": 400}}},
                        "driver_id": {"type": "integer",
                                      "generator": {"strategy": "foreign_key", "ref": "driver.driver_id",
                                                    "distribution": "zipf", "params": {"alpha": 1.2}}},
                        "city_id": {"type": "integer",
                                    "generator": {"strategy": "lookup", "source_table": "rider",
                                                  "source_column": "home_city_id", "via": "rider_id"}},
                        "requested_at": {
                            "type": "timestamp",
                            "generator": {
                                "strategy": "temporal",
                                "pattern": "seasonal",
                                "range_ref": "model.date_range",
                                "profiles": {
                                    "month": self._dist("trip.requested_at.month", {
                                        "Jan": 0.06, "Feb": 0.06, "Mar": 0.08, "Apr": 0.08,
                                        "May": 0.09, "Jun": 0.10, "Jul": 0.11, "Aug": 0.10,
                                        "Sep": 0.08, "Oct": 0.08, "Nov": 0.07, "Dec": 0.09}),
                                    "day_of_week": self._dist("trip.requested_at.day_of_week", {
                                        "Mon": 0.12, "Tue": 0.12, "Wed": 0.13, "Thu": 0.14,
                                        "Fri": 0.18, "Sat": 0.19, "Sun": 0.12}),
                                    "hour_of_day": self._dist("trip.requested_at.hour_of_day", {
                                        "distribution": "bimodal", "peaks": [8, 18], "std_dev": 2}),
                                },
                            },
                        },
                        "status": {"type": "string", "max_length": 12,
                                   "generator": {"strategy": "weighted_enum", "values": self._dist("trip.status", {
                                       "completed": 0.86, "cancelled": 0.10, "no_driver": 0.04})}},
                        "distance_mi": {"type": "decimal", "precision": 6, "scale": 2,
                                        "generator": {"strategy": "distribution", "distribution": "log_normal",
                                                      "params": {"mean": 1.4, "sigma": 0.7, "min": 0.4, "max": 60.0}}},
                        "duration_min": {"type": "integer",
                                         "generator": {"strategy": "correlated", "source_column": "distance_mi",
                                                       "rule": "multiply",
                                                       "params": {"factor_min": 2.4, "factor_max": 4.2}}},
                        "surge_mult": {"type": "decimal", "precision": 4, "scale": 2,
                                       "generator": {"strategy": "weighted_enum", "values": self._dist("trip.surge_mult", {
                                           1.0: 0.70, 1.2: 0.12, 1.5: 0.09, 2.0: 0.06, 2.5: 0.02, 3.5: 0.01})}},
                        "fare": {"type": "decimal", "precision": 8, "scale": 2,
                                 "generator": {"strategy": "formula",
                                               "expression": "(2.5 + distance_mi * 1.75 + duration_min * 0.35) * surge_mult"}},
                        "tip": {"type": "decimal", "precision": 8, "scale": 2,
                                "generator": {"strategy": "correlated", "source_column": "fare", "rule": "multiply",
                                              "params": {"factor_min": 0.0, "factor_max": 0.22}}},
                        "payment_type": {"type": "string", "max_length": 12,
                                         "generator": {"strategy": "lookup", "source_table": "rider",
                                                       "source_column": "payment_default", "via": "rider_id"}},
                        "rating_given": {"type": "integer", "nullable": True, "null_rate": 0.18,
                                         "generator": {"strategy": "weighted_enum", "values": self._dist(
                                             "trip.rating_given", {5: 0.72, 4: 0.16, 3: 0.06, 2: 0.03, 1: 0.03})}},
                    },
                },
            },
            "relationships": [
                {"name": "driver_vehicle", "parent": "driver", "child": "vehicle",
                 "parent_columns": ["driver_id"], "child_columns": ["driver_id"], "type": "one_to_many"},
                {"name": "rider_trips", "parent": "rider", "child": "trip",
                 "parent_columns": ["rider_id"], "child_columns": ["rider_id"], "type": "one_to_many"},
                {"name": "driver_trips", "parent": "driver", "child": "trip",
                 "parent_columns": ["driver_id"], "child_columns": ["driver_id"], "type": "one_to_many"},
            ],
            "business_rules": [
                {"name": "trip_after_rider_join", "type": "cross_table",
                 "rule": "trip.requested_at >= rider.joined_at", "via": "rider_id"},
                {"name": "fare_positive", "type": "constraint", "table": "trip", "rule": "fare > 0"},
            ],
            "generation": {
                "scale": "small",
                "scales": {
                    "fabric_demo": {"rider": 500, "driver": 100, "vehicle": 100, "trip": 5000},
                    "small": {"rider": 2000, "driver": 300, "vehicle": 300, "trip": 20000},
                    "medium": {"rider": 50000, "driver": 5000, "vehicle": 5000, "trip": 500000},
                    "large": {"rider": 50000, "driver": 5000, "vehicle": 5000, "trip": 2000000},
                    "xlarge": {"rider": 200000, "driver": 20000, "vehicle": 20000, "trip": 10000000},
                },
                "derived_counts": {
                    "vehicle": {"per_parent": "driver", "ratio": self._ratio("vehicle_per_driver", 1.0)},
                    "trip": {"per_parent": "rider", "ratio": self._ratio("trip_per_rider", 10.0)},
                },
                "output": {"format": "dataframe"},
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)
