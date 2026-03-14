"""IoT telemetry stream patterns — sensor drift, missing readings, alert storms, fleet sim.

Generates realistic IoT streaming data anomalies that can be layered on top
of base IoT domain data produced by the generator.

Usage::

    from sqllocks_spindle.simulation.iot_patterns import IoTTelemetrySimulator, IoTTelemetryConfig

    cfg = IoTTelemetryConfig(fleet_size=50, duration_hours=24)
    result = IoTTelemetrySimulator(readings_df=readings, devices_df=devices, config=cfg).run()
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class IoTTelemetryConfig:
    """Configuration for :class:`IoTTelemetrySimulator`.

    Args:
        fleet_size: Number of active devices in the simulation.
        duration_hours: Total simulation window in hours.
        reading_interval_seconds: Nominal seconds between consecutive readings.
        drift_enabled: Whether to apply gradual sensor drift.
        drift_rate: Per-reading drift magnitude (additive random-walk step size).
        drift_probability: Fraction of sensors that will experience drift.
        missing_enabled: Whether to inject missing (NaN) readings.
        missing_probability: Per-reading chance of a value being set to NaN.
        alert_storm_enabled: Whether to generate alert storm bursts.
        alert_storm_probability: Per-hour chance of an alert storm starting.
        alert_storm_duration_minutes: How long each alert storm lasts.
        alert_storm_rate_multiplier: Alert frequency multiplier during a storm.
        battery_drain_enabled: Whether to simulate battery drain over time.
        battery_drain_rate: Battery percentage lost per hour.
        seed: Random seed for reproducibility.
    """

    fleet_size: int = 50
    duration_hours: float = 24.0
    reading_interval_seconds: float = 60.0
    drift_enabled: bool = True
    drift_rate: float = 0.001
    drift_probability: float = 0.10
    missing_enabled: bool = True
    missing_probability: float = 0.05
    alert_storm_enabled: bool = True
    alert_storm_probability: float = 0.02
    alert_storm_duration_minutes: float = 15.0
    alert_storm_rate_multiplier: float = 10.0
    battery_drain_enabled: bool = True
    battery_drain_rate: float = 0.1
    seed: int = 42


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class IoTTelemetryResult:
    """Result of :meth:`IoTTelemetrySimulator.run`.

    Attributes:
        readings: Modified readings DataFrame with drift and missing values applied.
        alerts: Generated alert events including alert-storm bursts.
        fleet_status: Per-device status snapshot at the end of the simulation.
        stats: Summary statistics dictionary.
    """

    readings: pd.DataFrame
    alerts: pd.DataFrame
    fleet_status: pd.DataFrame
    stats: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"IoTTelemetryResult(readings={len(self.readings)}, "
            f"alerts={len(self.alerts)}, fleet={len(self.fleet_status)}, "
            f"stats_keys={list(self.stats.keys())})"
        )


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class IoTTelemetrySimulator:
    """Simulate realistic IoT telemetry anomalies on top of generated data.

    Takes pre-generated *readings* and *devices* DataFrames (from the IoT
    domain generator) and applies configurable anomaly layers: sensor drift,
    missing readings, alert storms, and battery drain.

    Args:
        readings_df: Base readings DataFrame.  Expected to contain at least
            ``device_id`` (or ``sensor_id``), a numeric ``value`` column, and
            a ``reading_time`` (or ``created_at``) timestamp column.
        devices_df: Devices DataFrame.  Expected to contain at least
            ``device_id`` and optionally ``battery_level``.
        config: :class:`IoTTelemetryConfig`.

    Example::

        sim = IoTTelemetrySimulator(readings_df=readings, devices_df=devices)
        result = sim.run()
        result.readings.head()
    """

    # Column name mappings — the simulator normalises internally
    _VALUE_CANDIDATES = ("value", "reading_value", "sensor_value", "measurement")
    _TIME_CANDIDATES = ("reading_time", "created_at", "timestamp", "read_at")
    _DEVICE_ID = "device_id"
    _SENSOR_ID = "sensor_id"

    def __init__(
        self,
        readings_df: pd.DataFrame,
        devices_df: pd.DataFrame,
        config: IoTTelemetryConfig | None = None,
    ) -> None:
        self._config = config or IoTTelemetryConfig()
        self._rng = np.random.default_rng(self._config.seed)

        # Work on copies to avoid mutating caller data
        self._readings = readings_df.copy()
        self._devices = devices_df.copy()

        # Resolve column names
        self._value_col = self._resolve_col(self._readings, self._VALUE_CANDIDATES, "value")
        self._time_col = self._resolve_col(self._readings, self._TIME_CANDIDATES, "reading_time")

        # Ensure the device column exists on readings (fall back to sensor_id)
        if self._DEVICE_ID not in self._readings.columns and self._SENSOR_ID in self._readings.columns:
            self._reading_device_col = self._SENSOR_ID
        else:
            self._reading_device_col = self._DEVICE_ID

        # Tracking containers
        self._alerts: list[dict[str, Any]] = []
        self._drift_devices: set[Any] = set()
        self._missing_count: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> IoTTelemetryResult:
        """Execute all enabled simulation layers and return results."""
        cfg = self._config

        if cfg.drift_enabled:
            self._apply_sensor_drift()

        if cfg.missing_enabled:
            self._inject_missing_readings()

        if cfg.alert_storm_enabled:
            self._generate_alert_storms()

        if cfg.battery_drain_enabled:
            self._simulate_battery_drain()

        fleet_status = self._build_fleet_status()
        alerts_df = self._build_alerts_dataframe()

        stats = self._compile_stats(fleet_status, alerts_df)

        return IoTTelemetryResult(
            readings=self._readings,
            alerts=alerts_df,
            fleet_status=fleet_status,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Sensor drift — cumulative random walk
    # ------------------------------------------------------------------

    def _apply_sensor_drift(self) -> None:
        """Gradually shift reading values for selected sensors via cumulative random walk."""
        cfg = self._config
        device_ids = self._readings[self._reading_device_col].unique()

        # Select which devices will drift
        n_drift = max(1, int(len(device_ids) * cfg.drift_probability))
        drift_ids = self._rng.choice(device_ids, size=min(n_drift, len(device_ids)), replace=False)
        self._drift_devices = set(drift_ids)

        for did in drift_ids:
            mask = self._readings[self._reading_device_col] == did
            n_readings = mask.sum()
            if n_readings == 0:
                continue

            # Cumulative random walk: each step is Normal(0, drift_rate)
            steps = self._rng.normal(loc=0.0, scale=cfg.drift_rate, size=n_readings)
            cumulative_drift = np.cumsum(steps)

            # Apply drift additively to the value column
            self._readings.loc[mask, self._value_col] = (
                self._readings.loc[mask, self._value_col].values + cumulative_drift
            )

    # ------------------------------------------------------------------
    # Missing readings
    # ------------------------------------------------------------------

    def _inject_missing_readings(self) -> None:
        """Set some reading values to NaN to simulate missing data."""
        cfg = self._config
        n_total = len(self._readings)
        if n_total == 0:
            return

        # Generate a boolean mask for which readings go missing
        missing_mask = self._rng.random(size=n_total) < cfg.missing_probability
        self._missing_count = int(missing_mask.sum())

        self._readings.loc[missing_mask, self._value_col] = np.nan

    # ------------------------------------------------------------------
    # Alert storms
    # ------------------------------------------------------------------

    _ALERT_TYPES = [
        ("threshold_exceeded", "critical"),
        ("sensor_malfunction", "critical"),
        ("connectivity_lost", "warning"),
        ("battery_low", "warning"),
        ("temperature_spike", "critical"),
        ("vibration_anomaly", "warning"),
        ("data_quality_issue", "info"),
        ("firmware_error", "critical"),
    ]

    _ALERT_MESSAGES = {
        "threshold_exceeded": "Sensor reading exceeded configured threshold",
        "sensor_malfunction": "Sensor reporting erratic values",
        "connectivity_lost": "Device lost network connectivity",
        "battery_low": "Battery level below minimum threshold",
        "temperature_spike": "Temperature reading abnormally high",
        "vibration_anomaly": "Unusual vibration pattern detected",
        "data_quality_issue": "Reading quality score below acceptable range",
        "firmware_error": "Device firmware reported an internal error",
    }

    def _generate_alert_storms(self) -> None:
        """Create burst alert DataFrames during randomly selected storm windows."""
        cfg = self._config
        device_ids = self._devices[self._DEVICE_ID].unique()
        if len(device_ids) == 0:
            return

        # Determine storm windows: iterate each hour of the simulation
        n_hours = int(np.ceil(cfg.duration_hours))
        base_time = self._infer_start_time()

        for hour_idx in range(n_hours):
            if self._rng.random() >= cfg.alert_storm_probability:
                continue

            # Storm hits — pick a random device subset
            storm_device_count = max(1, int(len(device_ids) * 0.2))
            storm_devices = self._rng.choice(
                device_ids,
                size=min(storm_device_count, len(device_ids)),
                replace=False,
            )

            storm_start = base_time + timedelta(hours=hour_idx)
            storm_end = storm_start + timedelta(minutes=cfg.alert_storm_duration_minutes)

            # Generate alerts at an elevated rate within the storm window
            base_alert_interval_sec = cfg.reading_interval_seconds
            storm_interval_sec = base_alert_interval_sec / cfg.alert_storm_rate_multiplier
            n_storm_alerts = max(
                1,
                int(cfg.alert_storm_duration_minutes * 60 / storm_interval_sec),
            )

            for device_id in storm_devices:
                for i in range(n_storm_alerts):
                    offset_sec = self._rng.uniform(0, cfg.alert_storm_duration_minutes * 60)
                    alert_time = storm_start + timedelta(seconds=offset_sec)

                    alert_type, severity = self._ALERT_TYPES[
                        self._rng.integers(0, len(self._ALERT_TYPES))
                    ]

                    self._alerts.append({
                        "alert_id": str(uuid.uuid4()),
                        "device_id": device_id,
                        "alert_type": alert_type,
                        "severity": severity,
                        "triggered_at": alert_time,
                        "message": self._ALERT_MESSAGES[alert_type],
                    })

        # Also generate baseline (non-storm) alerts for normal operation
        self._generate_baseline_alerts(base_time, device_ids)

    def _generate_baseline_alerts(
        self,
        base_time: datetime,
        device_ids: np.ndarray,
    ) -> None:
        """Generate a sparse scattering of normal-operation alerts."""
        cfg = self._config
        # Roughly 1 alert per device per 8 hours on average
        alerts_per_device = max(1, int(cfg.duration_hours / 8))

        for device_id in device_ids:
            n_alerts = self._rng.poisson(alerts_per_device)
            for _ in range(n_alerts):
                offset_sec = self._rng.uniform(0, cfg.duration_hours * 3600)
                alert_time = base_time + timedelta(seconds=offset_sec)

                alert_type, severity = self._ALERT_TYPES[
                    self._rng.integers(0, len(self._ALERT_TYPES))
                ]

                self._alerts.append({
                    "alert_id": str(uuid.uuid4()),
                    "device_id": device_id,
                    "alert_type": alert_type,
                    "severity": severity,
                    "triggered_at": alert_time,
                    "message": self._ALERT_MESSAGES[alert_type],
                })

    # ------------------------------------------------------------------
    # Battery drain
    # ------------------------------------------------------------------

    def _simulate_battery_drain(self) -> None:
        """Decrement battery_level on the devices DataFrame over the simulation window."""
        cfg = self._config

        # Ensure battery_level column exists
        if "battery_level" not in self._devices.columns:
            self._devices["battery_level"] = self._rng.uniform(60.0, 100.0, size=len(self._devices))

        total_drain = cfg.battery_drain_rate * cfg.duration_hours

        for idx in self._devices.index:
            # Each device drains at a slightly different rate (jitter +/- 30%)
            jitter = self._rng.uniform(0.7, 1.3)
            drain = total_drain * jitter
            current = self._devices.at[idx, "battery_level"]
            self._devices.at[idx, "battery_level"] = max(0.0, current - drain)

    # ------------------------------------------------------------------
    # Fleet status
    # ------------------------------------------------------------------

    def _build_fleet_status(self) -> pd.DataFrame:
        """Snapshot of each device: status, battery, last reading, drift flag."""
        device_ids = self._devices[self._DEVICE_ID].unique()
        records: list[dict[str, Any]] = []

        for device_id in device_ids:
            battery = self._get_device_battery(device_id)
            last_reading = self._get_last_reading_time(device_id)
            missing_rate = self._get_missing_rate(device_id)
            drift_detected = device_id in self._drift_devices

            # Determine status
            if battery <= 0.0:
                status = "offline"
            elif battery < 15.0 or missing_rate > 0.5:
                status = "degraded"
            else:
                status = "online"

            records.append({
                "device_id": device_id,
                "status": status,
                "battery_level": round(battery, 2),
                "last_reading_at": last_reading,
                "drift_detected": drift_detected,
            })

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_col(
        df: pd.DataFrame,
        candidates: tuple[str, ...],
        fallback: str,
    ) -> str:
        """Return the first column name from *candidates* that exists in *df*."""
        for c in candidates:
            if c in df.columns:
                return c
        return fallback

    def _infer_start_time(self) -> datetime:
        """Infer the simulation start time from the readings DataFrame."""
        if self._time_col in self._readings.columns:
            col = self._readings[self._time_col]
            if pd.api.types.is_datetime64_any_dtype(col):
                return col.min().to_pydatetime()
            try:
                return pd.to_datetime(col).min().to_pydatetime()
            except Exception:
                pass
        return datetime(2024, 1, 1)

    def _get_device_battery(self, device_id: Any) -> float:
        """Return battery level for *device_id* from the devices DataFrame."""
        mask = self._devices[self._DEVICE_ID] == device_id
        if mask.any() and "battery_level" in self._devices.columns:
            return float(self._devices.loc[mask, "battery_level"].iloc[0])
        return 100.0

    def _get_last_reading_time(self, device_id: Any) -> Any:
        """Return the most recent reading timestamp for a device."""
        if self._time_col not in self._readings.columns:
            return None

        mask = self._readings[self._reading_device_col] == device_id
        subset = self._readings.loc[mask, self._time_col]
        if subset.empty:
            return None

        try:
            return pd.to_datetime(subset).max()
        except Exception:
            return subset.iloc[-1]

    def _get_missing_rate(self, device_id: Any) -> float:
        """Return the fraction of NaN readings for a device."""
        if self._value_col not in self._readings.columns:
            return 0.0

        mask = self._readings[self._reading_device_col] == device_id
        subset = self._readings.loc[mask, self._value_col]
        if subset.empty:
            return 0.0

        return float(subset.isna().mean())

    def _build_alerts_dataframe(self) -> pd.DataFrame:
        """Convert the internal alerts list to a sorted DataFrame."""
        if not self._alerts:
            return pd.DataFrame(
                columns=["alert_id", "device_id", "alert_type", "severity", "triggered_at", "message"]
            )

        df = pd.DataFrame(self._alerts)
        df["triggered_at"] = pd.to_datetime(df["triggered_at"])
        df = df.sort_values("triggered_at").reset_index(drop=True)
        return df

    def _compile_stats(
        self,
        fleet_status: pd.DataFrame,
        alerts_df: pd.DataFrame,
    ) -> dict[str, Any]:
        """Build summary statistics dictionary."""
        status_counts = fleet_status["status"].value_counts().to_dict() if not fleet_status.empty else {}

        return {
            "total_readings": len(self._readings),
            "missing_readings_injected": self._missing_count,
            "missing_rate": round(self._missing_count / max(1, len(self._readings)), 4),
            "drifting_sensors": len(self._drift_devices),
            "total_alerts": len(alerts_df),
            "alert_types": alerts_df["alert_type"].value_counts().to_dict() if not alerts_df.empty else {},
            "fleet_online": status_counts.get("online", 0),
            "fleet_degraded": status_counts.get("degraded", 0),
            "fleet_offline": status_counts.get("offline", 0),
            "avg_battery_level": round(float(fleet_status["battery_level"].mean()), 2) if not fleet_status.empty else 0.0,
            "config_seed": self._config.seed,
            "config_duration_hours": self._config.duration_hours,
        }
