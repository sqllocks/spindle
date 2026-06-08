"""Pulse rideshare simulation layer — derive telemetry + marts, inject variability.

Takes the base Pulse domain tables (rider, driver, vehicle, trip) and produces the
store-shaped outputs the 4-store demo needs (SPEC §4/§5):

  Eventhouse / KQL  -> driver_pings, trip_events, surge_signals   (live telemetry)
  Warehouse         -> fact_revenue_daily, fact_driver_earnings   (finance marts)

It also enriches `trip` with geography (pickup/dropoff lat/lon from city centroids +
jitter) and lifecycle timestamps (accepted/started/completed, wait, ETA promised vs
actual) so the SQL Database + Lakehouse slices are complete.

Modeled on the shipped IoTTelemetrySimulator / ClickstreamSimulator pattern.

Usage::

    from sqllocks_spindle.domains.pulse import PulseDomain
    from sqllocks_spindle.engine.generator import Spindle
    from sqllocks_spindle.simulation.pulse_patterns import PulseDemandSimulator, PulseDemandConfig

    r = Spindle().generate(domain=PulseDomain(), scale="small", seed=7)
    sim = PulseDemandSimulator(tables=r.tables, config=PulseDemandConfig())
    out = sim.run()   # dict of DataFrames, incl. enriched "trip"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

# 4 metros (city_id -> name, lat, lon). Matches SPEC §1 / PulseDomain.
CITY_CENTROIDS: dict[int, tuple[str, str, float, float]] = {
    1: ("Seattle", "WA", 47.6062, -122.3321),
    2: ("Austin", "TX", 30.2672, -97.7431),
    3: ("Atlanta", "GA", 33.7490, -84.3880),
    4: ("Chicago", "IL", 41.8781, -87.6298),
}
_METRO_RADIUS_DEG = 0.14  # ~ metro-sized scatter around centroid
_PLATFORM_FEE = 0.25      # platform take rate for driver earnings


@dataclass
class PulseDemandConfig:
    """Variability dials for the Pulse simulator (SPEC §4)."""
    seed: int = 42
    # SURGE — demand shocks (the IoT "alert storm" analog)
    surge_events_per_week: float = 3.0
    surge_multiplier_range: tuple[float, float] = (1.3, 3.5)
    surge_duration_minutes: tuple[int, int] = (30, 120)
    surge_recent_days: int = 7          # window surge_signals covers
    surge_bucket_minutes: int = 10
    # QUALITY / noise
    eta_noise_minutes: float = 2.5
    gps_jitter_meters: float = 15.0
    # LIVE map window — driver_pings emitted for trips in the most recent window
    live_window_minutes: int = 120
    ping_interval_seconds: int = 20
    max_live_trips: int = 400           # cap pings volume for the demo


@dataclass
class PulseSimResult:
    tables: dict[str, pd.DataFrame]
    stats: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return "PulseSimResult(" + ", ".join(
            f"{k}={len(v)}" for k, v in self.tables.items()) + ")"


class PulseDemandSimulator:
    _GPS_JITTER_DEG = 1.0 / 111_000.0  # ~ meters -> degrees latitude

    def __init__(self, tables: dict[str, pd.DataFrame], config: PulseDemandConfig | None = None):
        self._cfg = config or PulseDemandConfig()
        self._rng = np.random.default_rng(self._cfg.seed)
        self._rider = tables["rider"].copy()
        self._driver = tables["driver"].copy()
        self._vehicle = tables["vehicle"].copy()
        self._trip = tables["trip"].copy()

    # ------------------------------------------------------------------ run
    def run(self) -> PulseSimResult:
        self._enrich_trips()
        events = self._build_trip_events()
        surge = self._build_surge_signals()
        pings = self._build_driver_pings()
        rev = self._build_revenue_daily()
        earn = self._build_driver_earnings()

        tables = {
            "trip": self._trip,
            "trip_events": events,
            "surge_signals": surge,
            "driver_pings": pings,
            "fact_revenue_daily": rev,
            "fact_driver_earnings": earn,
        }
        stats = {k: len(v) for k, v in tables.items()}
        stats["live_now"] = str(self._now())
        return PulseSimResult(tables=tables, stats=stats)

    # -------------------------------------------------------------- helpers
    def _now(self) -> pd.Timestamp:
        return pd.to_datetime(self._trip["requested_at"]).max()

    def _centroid(self, city_id: float) -> tuple[float, float]:
        name, st, lat, lon = CITY_CENTROIDS.get(int(city_id), CITY_CENTROIDS[1])
        return lat, lon

    def _scatter(self, n: int, city_ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        lats = np.empty(n)
        lons = np.empty(n)
        for cid, (_, _, clat, clon) in CITY_CENTROIDS.items():
            mask = city_ids == cid
            k = int(mask.sum())
            if k:
                lats[mask] = clat + self._rng.normal(0, _METRO_RADIUS_DEG, k)
                lons[mask] = clon + self._rng.normal(0, _METRO_RADIUS_DEG, k)
        # any unmapped city_id -> Seattle
        unmapped = ~np.isin(city_ids, list(CITY_CENTROIDS))
        if unmapped.any():
            k = int(unmapped.sum())
            lats[unmapped] = CITY_CENTROIDS[1][2] + self._rng.normal(0, _METRO_RADIUS_DEG, k)
            lons[unmapped] = CITY_CENTROIDS[1][3] + self._rng.normal(0, _METRO_RADIUS_DEG, k)
        return np.round(lats, 6), np.round(lons, 6)

    # ------------------------------------------------------------- enrich
    def _enrich_trips(self) -> None:
        t = self._trip
        n = len(t)
        # Coerce Decimal / nullable columns to float up front — Fabric's pandas/numpy
        # raises on .round() over object-dtype Decimal columns (local pandas tolerated it).
        for c in ("distance_mi", "surge_mult", "fare", "tip", "rating_given", "duration_min"):
            if c in t.columns:
                t[c] = pd.to_numeric(t[c], errors="coerce")
        cid = t["city_id"].to_numpy()
        req = pd.to_datetime(t["requested_at"])
        dur = t["duration_min"].astype(float).to_numpy()

        # geography
        plat, plon = self._scatter(n, cid)
        t["pickup_lat"], t["pickup_lon"] = plat, plon
        bearing = self._rng.uniform(0, 2 * np.pi, n)
        span = (dur / 60.0) * 0.02  # rough degrees of travel ~ duration
        t["dropoff_lat"] = np.round(plat + np.cos(bearing) * span, 6)
        t["dropoff_lon"] = np.round(plon + np.sin(bearing) * span, 6)

        # lifecycle timestamps
        accept_s = self._rng.uniform(5, 120, n)
        wait_min = np.round(self._rng.uniform(1, 8, n), 2)
        accepted = req + pd.to_timedelta(accept_s, unit="s")
        started = accepted + pd.to_timedelta(wait_min, unit="m")
        completed = started + pd.to_timedelta(dur, unit="m")

        status = t["status"].to_numpy()
        is_done = status == "completed"
        is_cancel = status == "cancelled"
        is_nodriver = status == "no_driver"

        t["accepted_at"] = pd.Series(accepted).where(~is_nodriver)
        t["started_at"] = pd.Series(started).where(is_done)
        t["completed_at"] = pd.Series(completed).where(is_done)
        t["wait_min"] = np.where(is_done, wait_min, np.nan)
        t["trip_date"] = req.dt.date

        # ETA promised vs actual (noise -> talkable accuracy tile)
        eta_promised = np.round(dur * self._rng.uniform(0.9, 1.1, n), 1)
        eta_actual = np.round(dur + self._rng.normal(0, self._cfg.eta_noise_minutes, n), 1)
        t["eta_promised_min"] = eta_promised
        t["eta_actual_min"] = np.clip(eta_actual, 1, None)
        t["is_cancelled"] = is_cancel
        t["cancel_reason"] = np.where(
            is_cancel,
            self._rng.choice(["rider_no_show", "driver_cancel", "long_wait", "changed_mind"], n),
            None,
        )
        # null out fare/tip for non-completed
        t.loc[~is_done, ["fare", "tip"]] = np.nan
        t["source_store"] = "SQL Database"

    # --------------------------------------------------------- trip_events
    def _build_trip_events(self) -> pd.DataFrame:
        t = self._trip
        rows = []
        def add(sub, etype, ts_col):
            ts = pd.to_datetime(sub[ts_col])
            ok = ts.notna()
            rows.append(pd.DataFrame({
                "event_id": [f"{tid}-{etype}" for tid in sub.loc[ok, "trip_id"]],
                "trip_id": sub.loc[ok, "trip_id"].to_numpy(),
                "ts": ts[ok].to_numpy(),
                "event_type": etype,
                "driver_id": sub.loc[ok, "driver_id"].to_numpy(),
                "rider_id": sub.loc[ok, "rider_id"].to_numpy(),
                "city_id": sub.loc[ok, "city_id"].to_numpy(),
                "lat": sub.loc[ok, "pickup_lat"].to_numpy(),
                "lon": sub.loc[ok, "pickup_lon"].to_numpy(),
            }))
        add(t, "requested", "requested_at")
        add(t[t["status"] != "no_driver"], "accepted", "accepted_at")
        add(t[t["status"] == "completed"], "started", "started_at")
        add(t[t["status"] == "completed"], "completed", "completed_at")
        cancels = t[t["status"] == "cancelled"].copy()
        cancels["cancel_ts"] = pd.to_datetime(cancels["accepted_at"]) + pd.to_timedelta(
            self._rng.uniform(30, 300, len(cancels)), unit="s")
        add(cancels, "cancelled", "cancel_ts")
        ev = pd.concat(rows, ignore_index=True).sort_values("ts").reset_index(drop=True)
        ev["source_store"] = "Eventhouse"
        return ev

    # ------------------------------------------------------- surge_signals
    def _build_surge_signals(self) -> pd.DataFrame:
        cfg = self._cfg
        now = self._now()
        start = now - timedelta(days=cfg.surge_recent_days)
        grid = pd.date_range(start, now, freq=f"{cfg.surge_bucket_minutes}min")
        rows = []
        for cid in CITY_CENTROIDS:
            mult = np.ones(len(grid))
            # inject surge events (Poisson over the window)
            exp_events = cfg.surge_events_per_week * (cfg.surge_recent_days / 7.0)
            n_events = self._rng.poisson(exp_events)
            for _ in range(n_events):
                start_idx = self._rng.integers(0, len(grid))
                dur_min = self._rng.integers(*cfg.surge_duration_minutes)
                span = max(1, dur_min // cfg.surge_bucket_minutes)
                peak = self._rng.uniform(*cfg.surge_multiplier_range)
                end_idx = min(len(grid), start_idx + span)
                mult[start_idx:end_idx] = np.maximum(mult[start_idx:end_idx], peak)
            active = self._rng.integers(20, 200, len(grid))
            open_req = np.round(active * mult * self._rng.uniform(0.3, 1.5, len(grid))).astype(int)
            trig = np.where(mult > 1.0,
                            self._rng.choice(["weather", "event", "imbalance"], len(grid)),
                            "baseline")
            rows.append(pd.DataFrame({
                "signal_id": [f"{cid}-{i}" for i in range(len(grid))],
                "ts": grid, "city_id": cid,
                "zone": self._rng.choice(["downtown", "airport", "north", "south", "east", "west"], len(grid)),
                "multiplier": np.round(mult, 2),
                "trigger": trig, "active_drivers": active, "open_requests": open_req,
            }))
        s = pd.concat(rows, ignore_index=True)
        s["source_store"] = "Eventhouse"
        return s

    # -------------------------------------------------------- driver_pings
    def _build_driver_pings(self) -> pd.DataFrame:
        cfg = self._cfg
        now = self._now()
        t = self._trip
        # live trips = completed trips overlapping the recent window
        done = t[t["status"] == "completed"].copy()
        st = pd.to_datetime(done["started_at"]); en = pd.to_datetime(done["completed_at"])
        live = done[(en >= now - timedelta(minutes=cfg.live_window_minutes)) & (st <= now)]
        if len(live) > cfg.max_live_trips:
            live = live.sample(cfg.max_live_trips, random_state=cfg.seed)
        rows = []
        jitter = cfg.gps_jitter_meters * self._GPS_JITTER_DEG
        for _, tr in live.iterrows():
            s = pd.to_datetime(tr["started_at"]); e = pd.to_datetime(tr["completed_at"])
            steps = max(2, int((e - s).total_seconds() // cfg.ping_interval_seconds))
            steps = min(steps, 60)
            frac = np.linspace(0, 1, steps)
            lat = tr["pickup_lat"] + (tr["dropoff_lat"] - tr["pickup_lat"]) * frac + self._rng.normal(0, jitter, steps)
            lon = tr["pickup_lon"] + (tr["dropoff_lon"] - tr["pickup_lon"]) * frac + self._rng.normal(0, jitter, steps)
            ts = [s + (e - s) * f for f in frac]
            rows.append(pd.DataFrame({
                "ping_id": [f"{tr['trip_id']}-{i}" for i in range(steps)],
                "ts": ts, "driver_id": tr["driver_id"], "trip_id": tr["trip_id"],
                "city_id": tr["city_id"], "lat": np.round(lat, 6), "lon": np.round(lon, 6),
                "heading": np.round(self._rng.uniform(0, 360, steps), 1),
                "speed_mph": np.round(np.clip(self._rng.normal(22, 8, steps), 0, 80), 1),
                "status": "on_trip",
            }))
        if not rows:
            pings = pd.DataFrame(columns=["ping_id", "ts", "driver_id", "trip_id", "city_id",
                                          "lat", "lon", "heading", "speed_mph", "status"])
        else:
            pings = pd.concat(rows, ignore_index=True).sort_values("ts").reset_index(drop=True)
        pings["source_store"] = "Eventhouse"
        return pings

    # ----------------------------------------------------- revenue_daily
    def _build_revenue_daily(self) -> pd.DataFrame:
        t = self._trip
        t = t.assign(_date_key=pd.to_datetime(t["requested_at"]).dt.strftime("%Y%m%d").astype(int))
        done = t[t["status"] == "completed"]
        g = done.groupby(["_date_key", "city_id"])
        rev = g.agg(
            completed_trips=("trip_id", "count"),
            gross_revenue=("fare", "sum"),
            surge_revenue=("fare", lambda s: float(s[done.loc[s.index, "surge_mult"] > 1.0].sum())),
            avg_fare=("fare", "mean"),
            avg_surge_mult=("surge_mult", "mean"),
            unique_riders=("rider_id", "nunique"),
            unique_drivers=("driver_id", "nunique"),
        ).reset_index()
        allg = t.groupby(["_date_key", "city_id"]).agg(
            trips=("trip_id", "count"),
            cancelled_trips=("is_cancelled", "sum"),
        ).reset_index()
        rev = rev.merge(allg, on=["_date_key", "city_id"], how="right").fillna(0)
        rev = rev.rename(columns={"_date_key": "date_key"})
        rev["net_revenue"] = np.round(rev["gross_revenue"] * (1 - _PLATFORM_FEE), 2)
        for c in ["gross_revenue", "surge_revenue", "avg_fare", "avg_surge_mult"]:
            rev[c] = rev[c].round(2)
        rev["source_store"] = "Warehouse"
        return rev

    # ---------------------------------------------------- driver_earnings
    def _build_driver_earnings(self) -> pd.DataFrame:
        t = self._trip
        done = t[t["status"] == "completed"].assign(
            _date_key=pd.to_datetime(t.loc[t["status"] == "completed", "requested_at"]).dt.strftime("%Y%m%d").astype(int))
        g = done.groupby(["_date_key", "driver_id"])
        earn = g.agg(
            city_id=("city_id", "first"),
            trips=("trip_id", "count"),
            gross=("fare", "sum"),
            tips=("tip", "sum"),
            online_hours=("duration_min", lambda s: round(float(s.sum()) / 60.0, 2)),
            avg_rating=("rating_given", "mean"),
        ).reset_index().rename(columns={"_date_key": "date_key"})
        earn["platform_fee"] = np.round(earn["gross"] * _PLATFORM_FEE, 2)
        earn["payout"] = np.round(earn["gross"] * (1 - _PLATFORM_FEE) + earn["tips"], 2)
        earn["utilization_pct"] = np.round(np.clip(earn["online_hours"] / 8.0 * 100, 0, 100), 2)
        earn["gross"] = earn["gross"].round(2); earn["tips"] = earn["tips"].round(2)
        earn["avg_rating"] = earn["avg_rating"].round(1)
        earn["source_store"] = "Warehouse"
        return earn
