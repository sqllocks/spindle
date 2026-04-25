"""Operational logs / observability patterns — trace/span IDs, latency spikes, outage storms.

Generate realistic application log and observability event streams with
distributed tracing, latency distributions, error bursts, and outage
simulation.

Usage::

    from sqllocks_spindle.simulation.operational_log_patterns import (
        OperationalLogSimulator, OperationalLogConfig,
    )

    cfg = OperationalLogConfig(service_count=5, duration_hours=24)
    result = OperationalLogSimulator(config=cfg).run()
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Default service / endpoint definitions
# ---------------------------------------------------------------------------

DEFAULT_SERVICES = [
    {"name": "api-gateway", "port": 8080, "tier": "edge"},
    {"name": "auth-service", "port": 8081, "tier": "middleware"},
    {"name": "order-service", "port": 8082, "tier": "core"},
    {"name": "inventory-service", "port": 8083, "tier": "core"},
    {"name": "payment-service", "port": 8084, "tier": "core"},
    {"name": "notification-service", "port": 8085, "tier": "support"},
    {"name": "search-service", "port": 8086, "tier": "core"},
    {"name": "analytics-service", "port": 8087, "tier": "support"},
]

DEFAULT_ENDPOINTS = [
    "/api/v1/orders",
    "/api/v1/orders/{id}",
    "/api/v1/products",
    "/api/v1/products/search",
    "/api/v1/users/auth",
    "/api/v1/users/profile",
    "/api/v1/cart",
    "/api/v1/checkout",
    "/api/v1/inventory/check",
    "/api/v1/payments/process",
    "/api/v1/notifications/send",
    "/healthz",
    "/readyz",
]

DEFAULT_HTTP_METHODS = [
    ("GET", 0.50),
    ("POST", 0.25),
    ("PUT", 0.10),
    ("DELETE", 0.05),
    ("PATCH", 0.05),
    ("OPTIONS", 0.03),
    ("HEAD", 0.02),
]

LOG_LEVELS = [
    ("INFO", 0.70),
    ("WARN", 0.15),
    ("ERROR", 0.10),
    ("DEBUG", 0.04),
    ("FATAL", 0.01),
]

ERROR_MESSAGES = [
    "Connection timeout after 30000ms",
    "Circuit breaker open for downstream service",
    "Rate limit exceeded (429)",
    "Database connection pool exhausted",
    "Upstream service returned 503",
    "SSL handshake failed",
    "Request payload exceeds max size",
    "Invalid authentication token",
    "Deadlock detected on table lock",
    "Out of memory: heap space",
    "DNS resolution failed",
    "Kafka producer send failed: NOT_LEADER_FOR_PARTITION",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class OperationalLogConfig:
    """Configuration for operational log simulation.

    Args:
        service_count: Number of services (uses first N from default list).
        services: Override service definitions.
        duration_hours: Time span of the simulation.
        start_time: Simulation start (ISO format).
        events_per_hour: Base event rate per service per hour.
        latency_mean_ms: Mean request latency (log-normal distribution).
        latency_std_ms: Std dev for log-normal latency.
        latency_spike_enabled: Inject latency spikes.
        latency_spike_probability: Per-hour chance of a latency spike.
        latency_spike_multiplier: Multiplier for latency during spikes.
        latency_spike_duration_minutes: Duration of spike window.
        outage_enabled: Inject outage storms.
        outage_probability: Per-hour chance of an outage starting.
        outage_duration_minutes: How long the outage lasts.
        outage_error_rate: Fraction of requests that fail during outage.
        trace_enabled: Generate distributed trace/span IDs.
        trace_depth_mean: Mean number of spans per trace.
        error_burst_enabled: Inject error bursts.
        error_burst_probability: Per-hour chance of error burst.
        error_burst_count: Errors per burst.
        seed: Random seed.
    """
    service_count: int = 5
    services: list[dict[str, Any]] = field(default_factory=list)
    duration_hours: float = 24.0
    start_time: str = "2024-01-01T00:00:00"
    events_per_hour: float = 100.0
    latency_mean_ms: float = 50.0
    latency_std_ms: float = 30.0
    latency_spike_enabled: bool = True
    latency_spike_probability: float = 0.05
    latency_spike_multiplier: float = 10.0
    latency_spike_duration_minutes: float = 15.0
    outage_enabled: bool = True
    outage_probability: float = 0.02
    outage_duration_minutes: float = 30.0
    outage_error_rate: float = 0.80
    trace_enabled: bool = True
    trace_depth_mean: float = 3.5
    error_burst_enabled: bool = True
    error_burst_probability: float = 0.03
    error_burst_count: int = 50
    seed: int = 42


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class OperationalLogResult:
    """Result of an operational log simulation.

    Attributes:
        logs: All log events sorted by timestamp.
        traces: Distributed trace records (trace_id, spans).
        service_health: Per-service health summary.
        stats: Aggregate statistics.
    """
    logs: pd.DataFrame
    traces: pd.DataFrame
    service_health: pd.DataFrame
    stats: dict[str, Any]


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class OperationalLogSimulator:
    """Generate synthetic operational / observability log data.

    Produces structured log events with distributed tracing, latency
    distributions, error bursts, and outage simulation.
    """

    def __init__(self, config: OperationalLogConfig | None = None) -> None:
        self._config = config or OperationalLogConfig()
        self._rng = np.random.default_rng(self._config.seed)

    def run(self) -> OperationalLogResult:
        """Execute the operational log simulation."""
        cfg = self._config
        services = cfg.services if cfg.services else DEFAULT_SERVICES[:cfg.service_count]
        start = datetime.fromisoformat(cfg.start_time)

        # Pre-compute anomaly windows
        spike_windows = self._compute_windows(cfg.latency_spike_probability, cfg.latency_spike_duration_minutes)
        outage_windows = self._compute_windows(cfg.outage_probability, cfg.outage_duration_minutes)
        error_burst_hours = self._compute_burst_hours(cfg.error_burst_probability)

        all_logs: list[dict[str, Any]] = []
        all_traces: list[dict[str, Any]] = []

        # Generate events hour by hour
        for hour in range(int(cfg.duration_hours)):
            hour_start = start + timedelta(hours=hour)
            in_spike = hour in spike_windows
            in_outage = hour in outage_windows
            is_error_burst = hour in error_burst_hours

            for svc in services:
                n_events = max(1, int(self._rng.poisson(cfg.events_per_hour)))

                for _ in range(n_events):
                    ts = hour_start + timedelta(seconds=float(self._rng.uniform(0, 3600)))
                    log = self._generate_log_event(svc, ts, in_spike, in_outage)
                    all_logs.append(log)

                    # Distributed traces
                    if cfg.trace_enabled and self._rng.random() < 0.3:
                        trace = self._generate_trace(svc, services, ts)
                        all_traces.extend(trace)

                # Error burst injection
                if is_error_burst and cfg.error_burst_enabled:
                    for _ in range(cfg.error_burst_count):
                        ts = hour_start + timedelta(seconds=float(self._rng.uniform(0, 300)))
                        log = self._generate_error_event(svc, ts)
                        all_logs.append(log)

        logs_df = pd.DataFrame(all_logs)
        if not logs_df.empty:
            logs_df = logs_df.sort_values("timestamp").reset_index(drop=True)

        traces_df = pd.DataFrame(all_traces)
        if not traces_df.empty:
            traces_df = traces_df.sort_values("timestamp").reset_index(drop=True)

        service_health = self._compute_service_health(all_logs, services)
        stats = self._compute_stats(all_logs, all_traces)

        return OperationalLogResult(
            logs=logs_df,
            traces=traces_df,
            service_health=service_health,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Event generation
    # ------------------------------------------------------------------

    def _generate_log_event(
        self,
        service: dict[str, Any],
        timestamp: datetime,
        in_spike: bool,
        in_outage: bool,
    ) -> dict[str, Any]:
        """Generate a single structured log event."""
        cfg = self._config

        # Latency
        latency = float(self._rng.lognormal(
            np.log(cfg.latency_mean_ms), cfg.latency_std_ms / cfg.latency_mean_ms,
        ))
        if in_spike:
            latency *= cfg.latency_spike_multiplier
        latency = round(max(0.5, latency), 2)

        # Status code
        if in_outage and self._rng.random() < cfg.outage_error_rate:
            status_code = int(self._rng.choice([500, 502, 503, 504]))
            level = "ERROR"
            message = self._rng.choice(ERROR_MESSAGES)
        else:
            status_code = self._pick_status_code()
            level = self._pick_level(status_code)
            message = f"Handled request in {latency}ms"

        method = self._pick_weighted(DEFAULT_HTTP_METHODS)
        endpoint = self._rng.choice(DEFAULT_ENDPOINTS)

        return {
            "log_id": str(uuid.uuid4()),
            "timestamp": timestamp,
            "service": service["name"],
            "tier": service.get("tier", "unknown"),
            "level": level,
            "method": method,
            "endpoint": endpoint,
            "status_code": status_code,
            "latency_ms": latency,
            "message": message,
            "trace_id": str(uuid.uuid4()) if cfg.trace_enabled else None,
            "span_id": str(uuid.uuid4())[:16] if cfg.trace_enabled else None,
            "is_spike": in_spike,
            "is_outage": in_outage,
        }

    def _generate_error_event(
        self,
        service: dict[str, Any],
        timestamp: datetime,
    ) -> dict[str, Any]:
        """Generate a single error log event for burst injection."""
        return {
            "log_id": str(uuid.uuid4()),
            "timestamp": timestamp,
            "service": service["name"],
            "tier": service.get("tier", "unknown"),
            "level": "ERROR",
            "method": self._pick_weighted(DEFAULT_HTTP_METHODS),
            "endpoint": self._rng.choice(DEFAULT_ENDPOINTS),
            "status_code": int(self._rng.choice([500, 502, 503, 429])),
            "latency_ms": round(float(self._rng.uniform(5000, 30000)), 2),
            "message": self._rng.choice(ERROR_MESSAGES),
            "trace_id": str(uuid.uuid4()),
            "span_id": str(uuid.uuid4())[:16],
            "is_spike": False,
            "is_outage": False,
        }

    def _generate_trace(
        self,
        entry_service: dict[str, Any],
        all_services: list[dict[str, Any]],
        start_time: datetime,
    ) -> list[dict[str, Any]]:
        """Generate a distributed trace with multiple spans."""
        cfg = self._config
        trace_id = str(uuid.uuid4())
        depth = max(1, int(self._rng.poisson(cfg.trace_depth_mean)))
        depth = min(depth, len(all_services))

        # Pick a chain of services
        svc_indices = self._rng.choice(len(all_services), size=depth, replace=False)
        # Ensure entry service is first
        svc_chain = [entry_service] + [all_services[i] for i in svc_indices if all_services[i]["name"] != entry_service["name"]]
        svc_chain = svc_chain[:depth]

        spans: list[dict[str, Any]] = []
        parent_span_id = None
        current_time = start_time

        for i, svc in enumerate(svc_chain):
            span_id = str(uuid.uuid4())[:16]
            latency = max(0.5, float(self._rng.lognormal(
                np.log(cfg.latency_mean_ms), cfg.latency_std_ms / cfg.latency_mean_ms,
            )))

            spans.append({
                "trace_id": trace_id,
                "span_id": span_id,
                "parent_span_id": parent_span_id,
                "service": svc["name"],
                "operation": self._rng.choice(DEFAULT_ENDPOINTS),
                "timestamp": current_time,
                "duration_ms": round(latency, 2),
                "status": "OK" if self._rng.random() > 0.05 else "ERROR",
                "depth": i,
            })

            parent_span_id = span_id
            current_time += timedelta(milliseconds=latency)

        return spans

    # ------------------------------------------------------------------
    # Anomaly window computation
    # ------------------------------------------------------------------

    def _compute_windows(self, probability: float, duration_minutes: float) -> set[int]:
        """Return set of hour indices that fall within anomaly windows."""
        cfg = self._config
        hours = set()
        window_hours = max(1, int(duration_minutes / 60) + 1)
        for h in range(int(cfg.duration_hours)):
            if self._rng.random() < probability:
                for offset in range(window_hours):
                    hours.add(h + offset)
        return hours

    def _compute_burst_hours(self, probability: float) -> set[int]:
        """Return set of hour indices where error bursts fire."""
        cfg = self._config
        return {h for h in range(int(cfg.duration_hours)) if self._rng.random() < probability}

    # ------------------------------------------------------------------
    # Health & stats
    # ------------------------------------------------------------------

    def _compute_service_health(
        self,
        logs: list[dict[str, Any]],
        services: list[dict[str, Any]],
    ) -> pd.DataFrame:
        """Compute per-service health metrics."""
        rows = []
        for svc in services:
            svc_logs = [l for l in logs if l["service"] == svc["name"]]
            total = len(svc_logs)
            errors = sum(1 for l in svc_logs if l.get("status_code", 200) >= 500)
            latencies = [l["latency_ms"] for l in svc_logs if "latency_ms" in l]

            rows.append({
                "service": svc["name"],
                "tier": svc.get("tier", "unknown"),
                "total_requests": total,
                "error_count": errors,
                "error_rate": round(errors / max(total, 1), 4),
                "p50_latency_ms": round(float(np.percentile(latencies, 50)), 2) if latencies else 0,
                "p95_latency_ms": round(float(np.percentile(latencies, 95)), 2) if latencies else 0,
                "p99_latency_ms": round(float(np.percentile(latencies, 99)), 2) if latencies else 0,
                "mean_latency_ms": round(float(np.mean(latencies)), 2) if latencies else 0,
            })

        return pd.DataFrame(rows)

    def _compute_stats(
        self,
        logs: list[dict[str, Any]],
        traces: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Compute aggregate statistics."""
        total = len(logs)
        errors = sum(1 for l in logs if l.get("status_code", 200) >= 500)
        spikes = sum(1 for l in logs if l.get("is_spike"))
        outages = sum(1 for l in logs if l.get("is_outage"))
        unique_traces = len(set(t["trace_id"] for t in traces)) if traces else 0

        return {
            "total_events": total,
            "total_errors": errors,
            "error_rate": round(errors / max(total, 1), 4),
            "spike_events": spikes,
            "outage_events": outages,
            "total_traces": unique_traces,
            "total_spans": len(traces),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _pick_weighted(self, options: list[tuple[str, float]]) -> str:
        values = [v for v, _ in options]
        weights = np.array([w for _, w in options])
        weights = weights / weights.sum()
        return str(self._rng.choice(values, p=weights))

    def _pick_status_code(self) -> int:
        codes = [200, 201, 204, 301, 302, 400, 401, 403, 404, 500, 502, 503]
        weights = np.array([0.60, 0.10, 0.05, 0.02, 0.02, 0.05, 0.03, 0.02, 0.04, 0.03, 0.02, 0.02])
        weights = weights / weights.sum()
        return int(self._rng.choice(codes, p=weights))

    def _pick_level(self, status_code: int) -> str:
        if status_code >= 500:
            return "ERROR"
        if status_code >= 400:
            return "WARN"
        return "INFO"
