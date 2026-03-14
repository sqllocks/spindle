"""Tests for clickstream / web telemetry patterns (E5)."""

from __future__ import annotations

import pandas as pd
import pytest

from sqllocks_spindle.simulation.clickstream_patterns import (
    ClickstreamConfig,
    ClickstreamResult,
    ClickstreamSimulator,
)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestClickstreamConfig:
    def test_defaults(self):
        cfg = ClickstreamConfig()
        assert cfg.users == 1000
        assert cfg.bot_traffic_enabled is True
        assert cfg.bounce_rate == 0.35
        assert cfg.seed == 42
        assert cfg.funnel_enabled is True


# ---------------------------------------------------------------------------
# Simulator basics
# ---------------------------------------------------------------------------

class TestClickstreamSimulator:
    @pytest.fixture
    def small_config(self):
        return ClickstreamConfig(
            users=10,
            duration_hours=24.0,
            avg_sessions_per_user=2.0,
            seed=42,
        )

    def test_run_returns_result(self, small_config):
        sim = ClickstreamSimulator(config=small_config)
        result = sim.run()
        assert isinstance(result, ClickstreamResult)

    def test_page_views_returned(self, small_config):
        result = ClickstreamSimulator(config=small_config).run()
        assert isinstance(result.page_views, pd.DataFrame)
        assert len(result.page_views) > 0

    def test_sessions_returned(self, small_config):
        result = ClickstreamSimulator(config=small_config).run()
        assert isinstance(result.sessions, pd.DataFrame)
        assert len(result.sessions) > 0

    def test_funnels_returned(self, small_config):
        result = ClickstreamSimulator(config=small_config).run()
        assert isinstance(result.funnels, pd.DataFrame)

    def test_stats_populated(self, small_config):
        result = ClickstreamSimulator(config=small_config).run()
        assert "total_page_views" in result.stats
        assert "total_sessions" in result.stats

    def test_deterministic_with_seed(self):
        cfg = ClickstreamConfig(users=5, avg_sessions_per_user=2.0, seed=123)
        r1 = ClickstreamSimulator(config=cfg).run()
        r2 = ClickstreamSimulator(config=cfg).run()
        assert r1.stats["total_sessions"] == r2.stats["total_sessions"]
        assert len(r1.page_views) == len(r2.page_views)


# ---------------------------------------------------------------------------
# Page views
# ---------------------------------------------------------------------------

class TestPageViews:
    @pytest.fixture
    def result(self):
        cfg = ClickstreamConfig(users=10, avg_sessions_per_user=3.0, seed=42)
        return ClickstreamSimulator(config=cfg).run()

    def test_has_session_id(self, result):
        assert "session_id" in result.page_views.columns

    def test_has_user_id(self, result):
        assert "user_id" in result.page_views.columns

    def test_has_timestamp(self, result):
        assert "timestamp" in result.page_views.columns

    def test_has_page_url(self, result):
        assert "page_url" in result.page_views.columns


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

class TestSessions:
    @pytest.fixture
    def result(self):
        cfg = ClickstreamConfig(users=10, avg_sessions_per_user=3.0, seed=42)
        return ClickstreamSimulator(config=cfg).run()

    def test_has_session_id(self, result):
        assert "session_id" in result.sessions.columns

    def test_has_user_id(self, result):
        assert "user_id" in result.sessions.columns

    def test_has_device_type(self, result):
        assert "device_type" in result.sessions.columns

    def test_has_is_bot(self, result):
        assert "is_bot" in result.sessions.columns


# ---------------------------------------------------------------------------
# Bot traffic
# ---------------------------------------------------------------------------

class TestBotTraffic:
    def test_bots_present_when_enabled(self):
        cfg = ClickstreamConfig(
            users=10,
            avg_sessions_per_user=3.0,
            bot_traffic_enabled=True,
            bot_fraction=0.20,
            seed=42,
        )
        result = ClickstreamSimulator(config=cfg).run()
        bot_sessions = result.sessions[result.sessions["is_bot"] == True]
        assert len(bot_sessions) > 0

    def test_no_bots_when_disabled(self):
        cfg = ClickstreamConfig(
            users=10,
            avg_sessions_per_user=2.0,
            bot_traffic_enabled=False,
            seed=42,
        )
        result = ClickstreamSimulator(config=cfg).run()
        bot_sessions = result.sessions[result.sessions["is_bot"] == True]
        assert len(bot_sessions) == 0


# ---------------------------------------------------------------------------
# Funnel
# ---------------------------------------------------------------------------

class TestFunnel:
    def test_funnel_has_stages(self):
        cfg = ClickstreamConfig(users=20, avg_sessions_per_user=3.0, seed=42)
        result = ClickstreamSimulator(config=cfg).run()
        if len(result.funnels) > 0:
            assert "stage" in result.funnels.columns

    def test_funnel_disabled_returns_empty(self):
        cfg = ClickstreamConfig(
            users=10,
            avg_sessions_per_user=2.0,
            funnel_enabled=False,
            seed=42,
        )
        result = ClickstreamSimulator(config=cfg).run()
        assert len(result.funnels) == 0

    def test_conversions_tracked_in_stats(self):
        cfg = ClickstreamConfig(
            users=20,
            avg_sessions_per_user=3.0,
            funnel_drop_rate=0.1,
            seed=42,
        )
        result = ClickstreamSimulator(config=cfg).run()
        assert "funnel_conversion_rate" in result.stats


# ---------------------------------------------------------------------------
# Bounce rate
# ---------------------------------------------------------------------------

class TestBounceRate:
    def test_bounce_rate_reflected_in_stats(self):
        cfg = ClickstreamConfig(
            users=20,
            avg_sessions_per_user=5.0,
            bounce_rate=0.80,
            bot_traffic_enabled=False,
            seed=42,
        )
        result = ClickstreamSimulator(config=cfg).run()
        assert result.stats["bounce_rate_actual"] > 0.3
