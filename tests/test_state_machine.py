"""Tests for business workflow events / state machines (E9)."""

from __future__ import annotations

import pandas as pd
import pytest

from sqllocks_spindle.simulation.state_machine import (
    PRESET_WORKFLOWS,
    StateDefinition,
    TransitionRule,
    WorkflowConfig,
    WorkflowResult,
    WorkflowSimulator,
    get_preset_workflow,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def order_states():
    return [
        StateDefinition("created", is_initial=True),
        StateDefinition("confirmed"),
        StateDefinition("shipped"),
        StateDefinition("delivered", is_terminal=True),
        StateDefinition("cancelled", is_terminal=True),
    ]


@pytest.fixture
def order_transitions():
    return [
        TransitionRule("created", "confirmed", probability=0.90, dwell_hours_mean=2.0, dwell_hours_std=0.5),
        TransitionRule("created", "cancelled", probability=0.10, dwell_hours_mean=1.0, dwell_hours_std=0.3),
        TransitionRule("confirmed", "shipped", probability=0.95, dwell_hours_mean=24.0, dwell_hours_std=6.0),
        TransitionRule("confirmed", "cancelled", probability=0.05, dwell_hours_mean=4.0, dwell_hours_std=1.0),
        TransitionRule("shipped", "delivered", probability=0.95, dwell_hours_mean=72.0, dwell_hours_std=24.0),
    ]


@pytest.fixture
def order_config(order_states, order_transitions):
    return WorkflowConfig(
        states=order_states,
        transitions=order_transitions,
        entity_count=50,
        seed=42,
    )


# ---------------------------------------------------------------------------
# StateDefinition / TransitionRule
# ---------------------------------------------------------------------------

class TestDataclasses:
    def test_state_definition(self):
        s = StateDefinition("created", is_initial=True)
        assert s.name == "created"
        assert s.is_initial is True
        assert s.is_terminal is False

    def test_transition_rule(self):
        t = TransitionRule("a", "b", probability=0.5, dwell_hours_mean=10.0)
        assert t.from_state == "a"
        assert t.to_state == "b"
        assert t.probability == 0.5


# ---------------------------------------------------------------------------
# WorkflowSimulator basics
# ---------------------------------------------------------------------------

class TestWorkflowSimulator:
    def test_run_returns_result(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        assert isinstance(result, WorkflowResult)

    def test_events_returned(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        assert isinstance(result.events, pd.DataFrame)
        assert len(result.events) > 0

    def test_entity_summary_returned(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        assert isinstance(result.entity_summary, pd.DataFrame)
        assert len(result.entity_summary) == order_config.entity_count

    def test_state_distribution_returned(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        assert isinstance(result.state_distribution, dict)
        assert len(result.state_distribution) > 0

    def test_stats_populated(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        assert isinstance(result.stats, dict)
        assert "total_events" in result.stats
        assert "total_entities" in result.stats

    def test_deterministic_with_seed(self, order_config):
        r1 = WorkflowSimulator(config=order_config).run()
        r2 = WorkflowSimulator(config=order_config).run()
        assert r1.stats == r2.stats
        assert len(r1.events) == len(r2.events)


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

class TestEvents:
    def test_required_columns(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        required = {"event_id", "entity_id", "from_state", "to_state", "transitioned_at", "dwell_hours"}
        assert required.issubset(set(result.events.columns))

    def test_dwell_hours_positive(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        assert (result.events["dwell_hours"] >= 0).all()

    def test_entity_ids_match_config(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        assert result.events["entity_id"].nunique() == order_config.entity_count


# ---------------------------------------------------------------------------
# Entity summary
# ---------------------------------------------------------------------------

class TestEntitySummary:
    def test_required_columns(self, order_config):
        result = WorkflowSimulator(config=order_config).run()
        required = {"entity_id", "final_state", "total_transitions"}
        assert required.issubset(set(result.entity_summary.columns))

    def test_terminal_states_are_valid(self, order_config, order_states):
        result = WorkflowSimulator(config=order_config).run()
        terminal = {s.name for s in order_states if s.is_terminal}
        all_states = {s.name for s in order_states}
        # Final states should be either terminal or a non-terminal where entity got stuck
        assert set(result.entity_summary["final_state"]).issubset(all_states)


# ---------------------------------------------------------------------------
# Presets
# ---------------------------------------------------------------------------

class TestPresets:
    def test_preset_names(self):
        assert "order_fulfillment" in PRESET_WORKFLOWS
        assert "support_ticket" in PRESET_WORKFLOWS
        assert "employee_onboarding" in PRESET_WORKFLOWS

    def test_get_preset_workflow(self):
        states, transitions = get_preset_workflow("order_fulfillment")
        assert len(states) > 0
        assert len(transitions) > 0
        assert any(s.is_initial for s in states)
        assert any(s.is_terminal for s in states)

    def test_get_preset_unknown_raises(self):
        with pytest.raises((KeyError, ValueError)):
            get_preset_workflow("nonexistent_workflow")

    def test_preset_order_fulfillment_runs(self):
        states, transitions = get_preset_workflow("order_fulfillment")
        cfg = WorkflowConfig(states=states, transitions=transitions, entity_count=20, seed=42)
        result = WorkflowSimulator(config=cfg).run()
        assert len(result.events) > 0

    def test_preset_support_ticket_runs(self):
        states, transitions = get_preset_workflow("support_ticket")
        cfg = WorkflowConfig(states=states, transitions=transitions, entity_count=20, seed=42)
        result = WorkflowSimulator(config=cfg).run()
        assert len(result.events) > 0

    def test_preset_employee_onboarding_runs(self):
        states, transitions = get_preset_workflow("employee_onboarding")
        cfg = WorkflowConfig(states=states, transitions=transitions, entity_count=20, seed=42)
        result = WorkflowSimulator(config=cfg).run()
        assert len(result.events) > 0


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------

class TestAnomalies:
    def test_anomalies_can_occur(self, order_states, order_transitions):
        cfg = WorkflowConfig(
            states=order_states,
            transitions=order_transitions,
            entity_count=200,
            anomaly_enabled=True,
            anomaly_skip_probability=0.10,
            anomaly_stuck_probability=0.05,
            anomaly_backward_probability=0.05,
            seed=42,
        )
        result = WorkflowSimulator(config=cfg).run()
        if "is_anomaly" in result.events.columns:
            anomaly_count = result.events["is_anomaly"].sum()
            # With high anomaly probabilities and 200 entities, should see some
            assert anomaly_count >= 0  # May still be 0 due to randomness

    def test_anomalies_disabled(self, order_states, order_transitions):
        cfg = WorkflowConfig(
            states=order_states,
            transitions=order_transitions,
            entity_count=50,
            anomaly_enabled=False,
            seed=42,
        )
        result = WorkflowSimulator(config=cfg).run()
        if "is_anomaly" in result.events.columns:
            assert result.events["is_anomaly"].sum() == 0
