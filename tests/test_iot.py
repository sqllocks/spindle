"""Integration tests for the IoT domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.iot import IoTDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=IoTDomain(), scale="small", seed=42)


class TestIoTStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "device_type", "location", "device", "sensor",
            "reading", "alert", "maintenance_log", "command",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["device_type"]) == 20
        assert len(r["location"]) == 100
        assert len(r["device"]) == 500
        assert len(r["sensor"]) == 1250
        assert len(r["reading"]) == 25000
        assert len(r["alert"]) == 250
        assert len(r["maintenance_log"]) == 750
        assert len(r["command"]) == 1500

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("device_type") < order.index("device")
        assert order.index("location") < order.index("device")
        assert order.index("device") < order.index("sensor")
        assert order.index("sensor") < order.index("reading")
        assert order.index("device") < order.index("alert")
        assert order.index("device") < order.index("maintenance_log")
        assert order.index("device") < order.index("command")


class TestIoTIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_device_id_is_unique(self, result_small):
        assert result_small["device"]["device_id"].is_unique

    def test_sensor_id_is_unique(self, result_small):
        assert result_small["sensor"]["sensor_id"].is_unique

    def test_device_type_id_is_unique(self, result_small):
        assert result_small["device_type"]["type_id"].is_unique

    def test_location_id_is_unique(self, result_small):
        assert result_small["location"]["location_id"].is_unique

    def test_device_device_type_fk_valid(self, result_small):
        type_ids = set(result_small["device_type"]["type_id"])
        device_type_ids = set(result_small["device"]["device_type_id"])
        assert device_type_ids.issubset(type_ids)

    def test_device_location_fk_valid(self, result_small):
        location_ids = set(result_small["location"]["location_id"])
        device_location_ids = set(result_small["device"]["location_id"])
        assert device_location_ids.issubset(location_ids)

    def test_sensor_device_fk_valid(self, result_small):
        device_ids = set(result_small["device"]["device_id"])
        sensor_device_ids = set(result_small["sensor"]["device_id"])
        assert sensor_device_ids.issubset(device_ids)

    def test_reading_sensor_fk_valid(self, result_small):
        sensor_ids = set(result_small["sensor"]["sensor_id"])
        reading_sensor_ids = set(result_small["reading"]["sensor_id"])
        assert reading_sensor_ids.issubset(sensor_ids)

    def test_alert_device_fk_valid(self, result_small):
        device_ids = set(result_small["device"]["device_id"])
        alert_device_ids = set(result_small["alert"]["device_id"])
        assert alert_device_ids.issubset(device_ids)


class TestIoTDistributions:
    def test_device_status_distribution(self, result_small):
        statuses = result_small["device"]["status"].value_counts(normalize=True)
        assert 0.62 <= statuses.get("Active", 0) <= 0.82

    def test_device_type_category_in_set(self, result_small):
        categories = set(result_small["device_type"]["category"].unique())
        valid = {"Industrial", "Consumer", "Automotive", "Medical", "Smart Home"}
        assert categories.issubset(valid)

    def test_device_type_protocol_in_set(self, result_small):
        protocols = set(result_small["device_type"]["protocol"].unique())
        valid = {"MQTT", "HTTP", "CoAP", "AMQP", "Modbus"}
        assert protocols.issubset(valid)

    def test_alert_severity_distribution(self, result_small):
        severities = result_small["alert"]["severity"].value_counts(normalize=True)
        assert 0.30 <= severities.get("Medium", 0) <= 0.50

    def test_alert_severity_in_set(self, result_small):
        severities = set(result_small["alert"]["severity"].unique())
        valid = {"Critical", "High", "Medium", "Low"}
        assert severities.issubset(valid)

    def test_command_status_distribution(self, result_small):
        statuses = result_small["command"]["status"].value_counts(normalize=True)
        assert 0.45 <= statuses.get("Executed", 0) <= 0.65

    def test_command_status_in_set(self, result_small):
        statuses = set(result_small["command"]["status"].unique())
        valid = {"Sent", "Acknowledged", "Executed", "Failed"}
        assert statuses.issubset(valid)

    def test_reading_quality_flag_in_set(self, result_small):
        flags = set(result_small["reading"]["quality_flag"].unique())
        valid = {"Good", "Suspect", "Bad"}
        assert flags.issubset(valid)

    def test_reading_quality_flag_distribution(self, result_small):
        flags = result_small["reading"]["quality_flag"].value_counts(normalize=True)
        assert 0.85 <= flags.get("Good", 0) <= 0.95

    def test_facility_type_in_set(self, result_small):
        types = set(result_small["location"]["facility_type"].unique())
        valid = {"Factory", "Warehouse", "Office", "Field", "Residential"}
        assert types.issubset(valid)


class TestIoTBusinessRules:
    def test_battery_level_range(self, result_small):
        devices = result_small["device"]
        assert (devices["battery_level"] >= 0.0).all(), "Battery level below 0"
        assert (devices["battery_level"] <= 100.01).all(), "Battery level above 100"

    def test_sensor_max_range_geq_min_range(self, result_small):
        sensors = result_small["sensor"]
        violations = (sensors["max_range"] < sensors["min_range"] - 0.01).sum()
        assert violations == 0, f"{violations} sensors have max_range < min_range"

    def test_reading_value_range(self, result_small):
        readings = result_small["reading"]
        assert (readings["reading_value"] >= -50.0).all()
        assert (readings["reading_value"] <= 500.01).all()


class TestIoTReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=IoTDomain(), scale="small", seed=99)
        r2 = s.generate(domain=IoTDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
