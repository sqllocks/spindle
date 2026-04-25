"""Clickstream / web telemetry patterns — sessions, page views, funnels, bot traffic.

Generates realistic web analytics data that simulates user browsing behavior
including multi-page sessions, conversion funnels, and bot/crawler traffic.

Usage::

    from sqllocks_spindle.simulation.clickstream_patterns import (
        ClickstreamSimulator, ClickstreamConfig,
    )

    cfg = ClickstreamConfig(users=1000, duration_hours=24)
    result = ClickstreamSimulator(config=cfg).run()
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Bot user-agent pool
# ---------------------------------------------------------------------------

_BOT_USER_AGENTS: list[str] = [
    "Googlebot/2.1",
    "Bingbot/2.0",
    "Amazonbot/0.1",
    "GPTBot/1.0",
    "AhrefsBot/7.0",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class ClickstreamConfig:
    """Configuration for :class:`ClickstreamSimulator`.

    Args:
        users: Number of distinct users to simulate.
        duration_hours: Total simulation window in hours.
        avg_sessions_per_user: Average number of sessions each user starts.
        avg_pages_per_session: Average page views for non-bounced sessions.
        bounce_rate: Fraction of human sessions that are single-page bounces.
        funnel_enabled: Whether to track conversion funnel progression.
        funnel_stages: Ordered list of funnel stage names.
        funnel_drop_rate: Per-stage probability of a user abandoning.
        bot_traffic_enabled: Whether to inject bot/crawler sessions.
        bot_fraction: Fraction of all sessions that come from bots.
        bot_pages_per_session: Number of pages a bot crawls per session.
        page_pool: URL templates available for page views.
        referrer_sources: Traffic sources for session attribution.
        device_types: Device type labels for human sessions.
        seed: Random seed for reproducibility.
    """

    users: int = 1000
    duration_hours: float = 24.0
    avg_sessions_per_user: float = 2.5
    avg_pages_per_session: float = 5.0
    bounce_rate: float = 0.35
    funnel_enabled: bool = True
    funnel_stages: list[str] = field(default_factory=lambda: [
        "landing", "product", "cart", "checkout", "confirmation",
    ])
    funnel_drop_rate: float = 0.40
    bot_traffic_enabled: bool = True
    bot_fraction: float = 0.15
    bot_pages_per_session: int = 50
    page_pool: list[str] = field(default_factory=lambda: [
        "/", "/products", "/products/{id}", "/cart", "/checkout",
        "/about", "/contact", "/blog", "/blog/{slug}", "/search",
        "/account", "/faq",
    ])
    referrer_sources: list[str] = field(default_factory=lambda: [
        "direct", "google", "bing", "facebook", "twitter", "email", "reddit",
    ])
    device_types: list[str] = field(default_factory=lambda: [
        "desktop", "mobile", "tablet",
    ])
    seed: int = 42


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class ClickstreamResult:
    """Result of :meth:`ClickstreamSimulator.run`.

    Attributes:
        sessions: One row per session with metadata.
        page_views: One row per page view with timestamps and dwell times.
        funnels: Funnel progression records per session.
        stats: Summary statistics dictionary.
    """

    sessions: pd.DataFrame
    page_views: pd.DataFrame
    funnels: pd.DataFrame
    stats: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"ClickstreamResult(sessions={len(self.sessions)}, "
            f"page_views={len(self.page_views)}, "
            f"funnels={len(self.funnels)}, "
            f"stats_keys={list(self.stats.keys())})"
        )


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class ClickstreamSimulator:
    """Generate realistic web clickstream data.

    Produces sessions, page views, conversion funnels, and bot traffic
    for a configurable number of users over a simulation window.

    Args:
        config: :class:`ClickstreamConfig` (uses defaults if ``None``).

    Example::

        cfg = ClickstreamConfig(users=500, duration_hours=12)
        sim = ClickstreamSimulator(cfg)
        result = sim.run()
        print(result.stats)
    """

    def __init__(self, config: ClickstreamConfig | None = None) -> None:
        self._config = config or ClickstreamConfig()
        self._rng = np.random.default_rng(self._config.seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> ClickstreamResult:
        """Execute the simulation and return a :class:`ClickstreamResult`."""
        sessions_df = self._generate_sessions()
        page_views_df = self._generate_page_views(sessions_df)
        funnels_df = (
            self._generate_funnels(sessions_df)
            if self._config.funnel_enabled
            else _empty_funnels()
        )

        # Back-fill session duration from actual page views
        sessions_df = self._fill_session_durations(sessions_df, page_views_df)

        stats = self._build_stats(sessions_df, page_views_df, funnels_df)

        return ClickstreamResult(
            sessions=sessions_df,
            page_views=page_views_df,
            funnels=funnels_df,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Session generation
    # ------------------------------------------------------------------

    def _generate_sessions(self) -> pd.DataFrame:
        """Create session records for human users and (optionally) bots."""
        cfg = self._config

        # Generate user IDs
        user_ids = [f"user_{i:06d}" for i in range(cfg.users)]

        # Determine session count per user (Poisson)
        sessions_per_user = self._rng.poisson(
            lam=cfg.avg_sessions_per_user, size=cfg.users,
        )
        sessions_per_user = np.maximum(sessions_per_user, 1)

        sim_start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0,
        )

        records: list[dict[str, Any]] = []

        for user_idx, n_sessions in enumerate(sessions_per_user):
            for _ in range(int(n_sessions)):
                started_at = self._random_session_time(sim_start, cfg.duration_hours)
                is_bounce = self._rng.random() < cfg.bounce_rate
                device = str(self._rng.choice(cfg.device_types))
                referrer = str(self._rng.choice(cfg.referrer_sources))

                records.append({
                    "session_id": str(uuid.uuid4()),
                    "user_id": user_ids[user_idx],
                    "started_at": started_at,
                    "device_type": device,
                    "referrer": referrer,
                    "is_bot": False,
                    "is_bounce": is_bounce,
                    "user_agent": None,
                    "duration_seconds": 0.0,  # filled later
                })

        # Bot sessions
        if cfg.bot_traffic_enabled:
            n_human = len(records)
            # bot_fraction is fraction of total, so n_bot = n_human * frac / (1 - frac)
            n_bot = max(1, int(n_human * cfg.bot_fraction / max(0.01, 1.0 - cfg.bot_fraction)))

            for _ in range(n_bot):
                started_at = self._random_session_time(sim_start, cfg.duration_hours)
                bot_ua = str(self._rng.choice(_BOT_USER_AGENTS))

                records.append({
                    "session_id": str(uuid.uuid4()),
                    "user_id": f"bot_{uuid.uuid4().hex[:8]}",
                    "started_at": started_at,
                    "device_type": "bot",
                    "referrer": "direct",
                    "is_bot": True,
                    "is_bounce": False,
                    "user_agent": bot_ua,
                    "duration_seconds": 0.0,
                })

        df = pd.DataFrame(records)
        df["started_at"] = pd.to_datetime(df["started_at"], utc=True)
        df = df.sort_values("started_at").reset_index(drop=True)
        return df

    def _random_session_time(
        self,
        sim_start: datetime,
        duration_hours: float,
    ) -> datetime:
        """Pick a random session start time weighted toward business hours.

        Uses a sinusoidal distribution that peaks around 12:00 (noon) and
        troughs around 03:00 (early morning).
        """
        # Rejection sampling with sinusoidal weighting
        while True:
            offset_hours = float(self._rng.uniform(0, duration_hours))
            candidate = sim_start + timedelta(hours=offset_hours)
            hour_of_day = candidate.hour + candidate.minute / 60.0

            # Sinusoidal weight: peak at hour 12, trough at hour 0
            # w(h) = 0.5 + 0.5 * sin(pi * (h - 6) / 12)  peaks at h=12
            weight = 0.5 + 0.5 * np.sin(np.pi * (hour_of_day - 6.0) / 12.0)
            # Clamp to [0.1, 1.0] so overnight isn't completely zero
            weight = max(0.1, float(weight))

            if self._rng.random() < weight:
                return candidate

    # ------------------------------------------------------------------
    # Page view generation
    # ------------------------------------------------------------------

    def _generate_page_views(self, sessions_df: pd.DataFrame) -> pd.DataFrame:
        """For each session, generate a sequence of page views."""
        cfg = self._config
        records: list[dict[str, Any]] = []

        for _, session in sessions_df.iterrows():
            session_id = session["session_id"]
            user_id = session["user_id"]
            started_at = pd.Timestamp(session["started_at"])
            is_bot = session["is_bot"]
            is_bounce = session["is_bounce"]

            if is_bot:
                n_pages = cfg.bot_pages_per_session
            elif is_bounce:
                n_pages = 1
            else:
                n_pages = max(2, int(self._rng.poisson(lam=cfg.avg_pages_per_session)))

            current_time = started_at
            prev_url: str | None = None

            for page_idx in range(n_pages):
                page_url = self._random_page_url()

                if is_bot:
                    # Bots have very short dwell times
                    dwell = float(self._rng.uniform(0.1, 0.5))
                else:
                    # Human dwell times: lognormal, median ~15s, range ~5-120s
                    dwell = float(np.clip(
                        self._rng.lognormal(mean=2.7, sigma=0.8), 5.0, 120.0,
                    ))

                referrer_url = prev_url if prev_url else session["referrer"]

                records.append({
                    "view_id": str(uuid.uuid4()),
                    "session_id": session_id,
                    "user_id": user_id,
                    "page_url": page_url,
                    "timestamp": current_time,
                    "time_on_page_seconds": round(dwell, 2),
                    "referrer_url": referrer_url,
                })

                current_time = current_time + pd.Timedelta(seconds=dwell)
                prev_url = page_url

        df = pd.DataFrame(records)
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df = df.sort_values("timestamp").reset_index(drop=True)
        return df

    def _random_page_url(self) -> str:
        """Pick a random page URL from the pool, expanding template placeholders."""
        cfg = self._config
        template = str(self._rng.choice(cfg.page_pool))

        if "{id}" in template:
            template = template.replace("{id}", str(self._rng.integers(1, 501)))
        if "{slug}" in template:
            template = template.replace("{slug}", f"post-{self._rng.integers(1, 501)}")

        return template

    # ------------------------------------------------------------------
    # Funnel generation
    # ------------------------------------------------------------------

    def _generate_funnels(self, sessions_df: pd.DataFrame) -> pd.DataFrame:
        """Track which sessions progress through the conversion funnel.

        Only non-bot, non-bounce sessions are eligible for funnel tracking.
        Each stage has an independent ``funnel_drop_rate`` chance of the
        user abandoning before reaching the next stage.
        """
        cfg = self._config
        eligible = sessions_df[(~sessions_df["is_bot"]) & (~sessions_df["is_bounce"])]

        if eligible.empty or not cfg.funnel_stages:
            return _empty_funnels()

        records: list[dict[str, Any]] = []

        for _, session in eligible.iterrows():
            session_id = session["session_id"]
            user_id = session["user_id"]
            current_time = pd.Timestamp(session["started_at"])

            for stage_order, stage in enumerate(cfg.funnel_stages):
                is_final = stage_order == len(cfg.funnel_stages) - 1

                records.append({
                    "session_id": session_id,
                    "user_id": user_id,
                    "stage": stage,
                    "stage_order": stage_order,
                    "reached_at": current_time,
                    "converted": is_final,
                })

                # Advance time for the next stage
                dwell = float(self._rng.uniform(10.0, 60.0))
                current_time = current_time + pd.Timedelta(seconds=dwell)

                # Check for drop-off (not on the final stage — already recorded it)
                if not is_final and self._rng.random() < cfg.funnel_drop_rate:
                    break

        df = pd.DataFrame(records)
        if not df.empty:
            df["reached_at"] = pd.to_datetime(df["reached_at"], utc=True)
        return df

    # ------------------------------------------------------------------
    # Post-processing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _fill_session_durations(
        sessions_df: pd.DataFrame,
        page_views_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """Compute actual session duration from page view timestamps."""
        if page_views_df.empty:
            return sessions_df

        pv_agg = page_views_df.groupby("session_id")["time_on_page_seconds"].sum()
        sessions_df = sessions_df.copy()
        sessions_df["duration_seconds"] = (
            sessions_df["session_id"]
            .map(pv_agg)
            .fillna(0.0)
            .round(2)
        )
        return sessions_df

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def _build_stats(
        self,
        sessions_df: pd.DataFrame,
        page_views_df: pd.DataFrame,
        funnels_df: pd.DataFrame,
    ) -> dict[str, Any]:
        """Build summary statistics dictionary."""
        cfg = self._config

        total_sessions = len(sessions_df)
        bot_sessions = int(sessions_df["is_bot"].sum()) if not sessions_df.empty else 0
        human_sessions = total_sessions - bot_sessions
        bounce_sessions = int(sessions_df["is_bounce"].sum()) if not sessions_df.empty else 0

        # Funnel conversion rate
        if not funnels_df.empty and cfg.funnel_stages:
            final_stage = cfg.funnel_stages[-1]
            converted = int(funnels_df[
                (funnels_df["stage"] == final_stage) & (funnels_df["converted"])
            ]["session_id"].nunique())
            funnel_eligible = int(funnels_df["session_id"].nunique())
            conversion_rate = round(converted / max(1, funnel_eligible), 4)
        else:
            converted = 0
            funnel_eligible = 0
            conversion_rate = 0.0

        return {
            "total_sessions": total_sessions,
            "human_sessions": human_sessions,
            "bot_sessions": bot_sessions,
            "bounce_sessions": bounce_sessions,
            "bounce_rate_actual": round(bounce_sessions / max(1, human_sessions), 4),
            "total_page_views": len(page_views_df),
            "avg_pages_per_session": round(
                len(page_views_df) / max(1, total_sessions), 2,
            ),
            "funnel_eligible_sessions": funnel_eligible,
            "funnel_conversions": converted,
            "funnel_conversion_rate": conversion_rate,
            "unique_users": (
                int(sessions_df["user_id"].nunique()) if not sessions_df.empty else 0
            ),
            "device_type_distribution": (
                sessions_df["device_type"].value_counts().to_dict()
                if not sessions_df.empty
                else {}
            ),
            "referrer_distribution": (
                sessions_df["referrer"].value_counts().to_dict()
                if not sessions_df.empty
                else {}
            ),
            "duration_hours": cfg.duration_hours,
            "seed": cfg.seed,
        }


# ---------------------------------------------------------------------------
# Empty DataFrame factories (keep column schemas consistent)
# ---------------------------------------------------------------------------

def _empty_funnels() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "session_id", "user_id", "stage", "stage_order",
        "reached_at", "converted",
    ])
