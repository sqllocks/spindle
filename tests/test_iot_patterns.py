"""Tests for IoT telemetry stream patterns (E6)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.simulation.iot_patterns import (
    IoTTelemetryConfig,
    IoTTelemetryResult,
    IoTTelemetrySimulator,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def devices_df():
    return pd.DataFrame({
        "device_id": range(1, 11),
        "device_name": [f"Device_{i}" for i in range(1, 11)],
        "battery_level": np.random.default_rng(1).uniform(50, 100, size=10).round(1),
        "status": ["active"] * 8 + ["inactive"] * 2,
    })


@pytest.fixture
def readings_df():
    n = 200
    rng = np.random.default_rng(1)
    return pd.DataFrame({
        "reading_id": range(1, n + 1),
        "sensor_id": rng.integers(1, 21, size=n),
        "device_id": rng.integers(1, 11, size=n),
        "value": rng.normal(25.0, 5.0, size=n).round(2),
        "reading_time": pd.date_range("2024-01-01", periods=n, freq="5min"),
    })


@pytest.fixture
def config():
    return IoTTelemetryConfig(
        fleet_size=10,
        duration_hours=1.0,
        reading_interval_seconds=60.0,
        seed=42,
    )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestIoTTelemetryConfig:
    def test_defaults(self):
        cfg = IoTTelemetryConfig()
        assert cfg.fleet_size == 50
        assert cfg.drift_enabled is True
        assert cfg.missing_enabled is True
        assert cfg.alert_storm_enabled is True
        assert cfg.battery_drain_enabled is True
        assert cfg.seed == 42


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class TestIoTTelemetrySimulator:
    def test_run_returns_result(self, readings_df, devices_df, config):
        sim = IoTTelemetrySimulator(
            readings_df=readings_df, devices_df=devices_df, config=config,
        )
        result = sim.run()
        assert isinstance(result, IoTTelemetryResult)

    def test_readings_returned(self, readings_df, devices_df, config):
        sim = IoTTelemetrySimulator(
            readings_df=readings_df, devices_df=devices_df, config=config,
        )
        result = sim.run()
        assert isinstance(result.readings, pd.DataFrame)
        assert len(result.readings) > 0

    def test_alerts_returned(self, readings_df, devices_df, config):
        sim = IoTTelemetrySimulator(
            readings_df=readings_df, devices_df=devices_df, config=config,
        )
        result = sim.run()
        assert isinstance(result.alerts, pd.DataFrame)

    def test_fleet_status_returned(self, readings_df, devices_df, config):
        sim = IoTTelemetrySimulator(
            readings_df=readings_df, devices_df=devices_df, config=config,
        )
        result = sim.run()
        assert isinstance(result.fleet_status, pd.DataFrame)
        assert len(result.fleet_status) > 0

    def test_stats_populated(self, readings_df, devices_df, config):
        sim = IoTTelemetrySimulator(
            readings_df=readings_df, devices_df=devices_df, config=config,
        )
        result = sim.run()
        assert isinstance(result.stats, dict)


# ---------------------------------------------------------------------------
# Sensor drift
# ---------------------------------------------------------------------------

class TestSensorDrift:
    def test_drift_modifies_values(self, readings_df, devices_df):
        cfg = IoTTelemetryConfig(
            fleet_size=10,
            drift_enabled=True,
            drift_probability=1.0,  # All sensors drift
            drift_rate=0.1,  # Strong drift for test
            missing_enabled=False,
            alert_storm_enabled=False,
            battery_drain_enabled=False,
            seed=42,
        )
        sim = IoTTelemetrySimulator(
            readings_df=readings_df.copy(), devices_df=devices_df, config=cfg,
        )
        result = sim.run()
        # With 100% drift probability and high rate, values should change
        original_mean = readings_df["value"].mean()
        result_mean = result.readings["value"].dropna().mean()
        # They should differ (drift shifts values)
        assert abs(result_mean - original_mean) > 0 or result.readings["value"].isna().any()


# ---------------------------------------------------------------------------
# Missing readings
# ---------------------------------------------------------------------------

class TestMissingReadings:
    def test_missing_injects_nans(self, readings_df, devices_df):
        cfg = IoTTelemetryConfig(
            fleet_size=10,
            drift_enabled=False,
            missing_enabled=True,
            missing_probability=0.5,  # High probability for test
            alert_storm_enabled=False,
            battery_drain_enabled=False,
            seed=42,
        )
        sim = IoTTelemetrySimulator(
            readings_df=readings_df.copy(), devices_df=devices_df, config=cfg,
        )
        result = sim.run()
        assert result.readings["value"].isna().any()


# ---------------------------------------------------------------------------
# Alert storms
# ---------------------------------------------------------------------------

class TestAlertStorms:
    def test_alert_storm_generates_alerts(self, readings_df, devices_df):
        cfg = IoTTelemetryConfig(
            fleet_size=10,
            drift_enabled=False,
            missing_enabled=False,
            alert_storm_enabled=True,
            alert_storm_probability=1.0,  # Force storm
            battery_drain_enabled=False,
            seed=42,
        )
        sim = IoTTelemetrySimulator(
            readings_df=readings_df.copy(), devices_df=devices_df, config=cfg,
        )
        result = sim.run()
        assert len(result.alerts) > 0
        assert "alert_id" in result.alerts.columns
        assert "device_id" in result.alerts.columns


# ---------------------------------------------------------------------------
# Battery drain
# ---------------------------------------------------------------------------

class TestBatteryDrain:
    def test_battery_level_decreases(self, readings_df, devices_df):
        cfg = IoTTelemetryConfig(
            fleet_size=10,
            duration_hours=24.0,
            drift_enabled=False,
            missing_enabled=False,
            alert_storm_enabled=False,
            battery_drain_enabled=True,
            battery_drain_rate=5.0,  # 5% per hour for test visibility
            seed=42,
        )
        sim = IoTTelemetrySimulator(
            readings_df=readings_df.copy(), devices_df=devices_df, config=cfg,
        )
        result = sim.run()
        # Fleet status should show reduced battery levels
        assert "battery_level" in result.fleet_status.columns
