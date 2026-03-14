"""Business workflow events — state machines with dwell times.

Generate realistic event streams for business processes where entities
transition through defined states (e.g. order: created->confirmed->shipped->
delivered->returned) with configurable dwell times between transitions.

Usage::

    from sqllocks_spindle.simulation.state_machine import (
        WorkflowSimulator, WorkflowConfig, StateDefinition, TransitionRule,
    )

    states = [
        StateDefinition("created", is_initial=True),
        StateDefinition("confirmed"),
        StateDefinition("shipped"),
        StateDefinition("delivered", is_terminal=True),
        StateDefinition("cancelled", is_terminal=True),
        StateDefinition("returned", is_terminal=True),
    ]
    transitions = [
        TransitionRule("created", "confirmed", probability=0.90, dwell_hours_mean=2.0, dwell_hours_std=0.5),
        TransitionRule("created", "cancelled", probability=0.10, dwell_hours_mean=1.0, dwell_hours_std=0.3),
        TransitionRule("confirmed", "shipped", probability=0.95, dwell_hours_mean=24.0, dwell_hours_std=6.0),
        TransitionRule("confirmed", "cancelled", probability=0.05, dwell_hours_mean=4.0, dwell_hours_std=1.0),
        TransitionRule("shipped", "delivered", probability=0.92, dwell_hours_mean=72.0, dwell_hours_std=24.0),
        TransitionRule("shipped", "returned", probability=0.08, dwell_hours_mean=120.0, dwell_hours_std=36.0),
    ]
    cfg = WorkflowConfig(states=states, transitions=transitions, entity_count=1000)
    result = WorkflowSimulator(config=cfg).run()
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Data definitions
# ---------------------------------------------------------------------------

@dataclass
class StateDefinition:
    """A single state in a business workflow.

    Args:
        name: Unique name for this state.
        is_initial: Whether entities can start in this state.
        is_terminal: Whether this state ends the workflow (no outgoing transitions).
        metadata: Arbitrary key-value pairs attached to the state.
    """

    name: str
    is_initial: bool = False
    is_terminal: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TransitionRule:
    """A directed edge in the workflow state machine.

    Args:
        from_state: Source state name.
        to_state: Destination state name.
        probability: Relative weight for this transition (normalised per source state).
        dwell_hours_mean: Mean hours spent in *from_state* before this transition fires.
        dwell_hours_std: Standard deviation for the dwell-time normal distribution.
        min_dwell_hours: Hard floor on sampled dwell time.
    """

    from_state: str
    to_state: str
    probability: float = 1.0
    dwell_hours_mean: float = 1.0
    dwell_hours_std: float = 0.5
    min_dwell_hours: float = 0.1


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class WorkflowConfig:
    """Configuration for :class:`WorkflowSimulator`.

    Args:
        states: List of state definitions for the workflow.
        transitions: List of transition rules connecting states.
        entity_count: Number of entities to simulate.
        entity_prefix: Prefix for generated entity IDs.
        start_time: ISO-format start timestamp for the simulation.
        max_transitions_per_entity: Safety limit to prevent infinite loops.
        anomaly_enabled: Whether to inject anomalous transitions.
        anomaly_skip_probability: Chance of skipping a state in the workflow.
        anomaly_stuck_probability: Chance of an entity getting stuck (no further transitions).
        anomaly_backward_probability: Chance of transitioning backward to a previous state.
        seed: Random seed for reproducibility.
    """

    states: list[StateDefinition] = field(default_factory=list)
    transitions: list[TransitionRule] = field(default_factory=list)
    entity_count: int = 100
    entity_prefix: str = "entity"
    start_time: str = "2024-01-01T00:00:00"
    max_transitions_per_entity: int = 20
    anomaly_enabled: bool = True
    anomaly_skip_probability: float = 0.02
    anomaly_stuck_probability: float = 0.01
    anomaly_backward_probability: float = 0.01
    seed: int = 42


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class WorkflowResult:
    """Result of :meth:`WorkflowSimulator.run`.

    Attributes:
        events: All transition events sorted by (entity_id, transitioned_at).
        entity_summary: Per-entity summary with final state and timing.
        state_distribution: Count of entities in each final state.
        stats: Aggregate statistics dictionary.
    """

    events: pd.DataFrame
    entity_summary: pd.DataFrame
    state_distribution: dict[str, int] = field(default_factory=dict)
    stats: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"WorkflowResult(events={len(self.events)}, "
            f"entities={len(self.entity_summary)}, "
            f"stats_keys={list(self.stats.keys())})"
        )


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class WorkflowSimulator:
    """Simulate business-process event streams using a state machine.

    Entities are created in an initial state and transition through the
    workflow according to probabilistic :class:`TransitionRule` definitions
    until they reach a terminal state, get stuck (anomaly), or hit the
    per-entity transition safety limit.

    Args:
        config: :class:`WorkflowConfig` describing the workflow graph and
            simulation parameters.

    Example::

        cfg = WorkflowConfig(states=states, transitions=transitions, entity_count=500)
        sim = WorkflowSimulator(config=cfg)
        result = sim.run()
        result.events.head()
    """

    def __init__(self, config: WorkflowConfig) -> None:
        self._config = config
        self._rng = np.random.default_rng(config.seed)

        # Build lookup structures
        self._state_map: dict[str, StateDefinition] = {s.name: s for s in config.states}
        self._initial_states = [s.name for s in config.states if s.is_initial]
        self._terminal_states = {s.name for s in config.states if s.is_terminal}
        self._non_terminal_states = [s.name for s in config.states if not s.is_terminal]

        # Group transitions by from_state and normalise probabilities
        self._transitions_by_state: dict[str, list[TransitionRule]] = {}
        self._probs_by_state: dict[str, np.ndarray] = {}
        self._build_transition_index()

        # Track visited states per entity (for backward anomaly)
        self._entity_history: list[str] = []

    # ------------------------------------------------------------------
    # Internal setup
    # ------------------------------------------------------------------

    def _build_transition_index(self) -> None:
        """Group transitions by source state and pre-compute normalised probabilities."""
        from collections import defaultdict

        grouped: dict[str, list[TransitionRule]] = defaultdict(list)
        for rule in self._config.transitions:
            grouped[rule.from_state].append(rule)

        for state, rules in grouped.items():
            self._transitions_by_state[state] = rules
            raw = np.array([r.probability for r in rules], dtype=np.float64)
            total = raw.sum()
            self._probs_by_state[state] = raw / total if total > 0 else raw

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> WorkflowResult:
        """Execute the full simulation and return a :class:`WorkflowResult`."""
        cfg = self._config
        base_time = datetime.fromisoformat(cfg.start_time)

        all_events: list[dict[str, Any]] = []

        for i in range(cfg.entity_count):
            entity_id = f"{cfg.entity_prefix}_{i:06d}"
            events = self._simulate_entity(entity_id, base_time)
            all_events.extend(events)

        # Build events DataFrame
        if all_events:
            events_df = pd.DataFrame(all_events)
            events_df["transitioned_at"] = pd.to_datetime(events_df["transitioned_at"])
            events_df = events_df.sort_values(
                ["entity_id", "transitioned_at"]
            ).reset_index(drop=True)
        else:
            events_df = pd.DataFrame(
                columns=[
                    "event_id", "entity_id", "from_state", "to_state",
                    "transitioned_at", "dwell_hours", "is_anomaly", "anomaly_type",
                ]
            )

        # Build entity summary and stats
        entity_summary = self._build_entity_summary(events_df)
        state_distribution = self._build_state_distribution(entity_summary)
        stats = self._compile_stats(events_df, entity_summary)

        return WorkflowResult(
            events=events_df,
            entity_summary=entity_summary,
            state_distribution=state_distribution,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Entity simulation
    # ------------------------------------------------------------------

    def _simulate_entity(
        self,
        entity_id: str,
        base_time: datetime,
    ) -> list[dict[str, Any]]:
        """Simulate one entity traversing the state machine.

        Returns a list of event dicts for this entity.
        """
        cfg = self._config
        events: list[dict[str, Any]] = []

        # Pick an initial state
        if not self._initial_states:
            return events
        initial = self._initial_states[
            self._rng.integers(0, len(self._initial_states))
        ]

        current_state = initial
        current_time = base_time + timedelta(
            hours=float(self._rng.uniform(0, 1))
        )
        visited: list[str] = [current_state]

        for _ in range(cfg.max_transitions_per_entity):
            # Terminal state — stop
            if current_state in self._terminal_states:
                break

            # Pick the normal next transition
            rule = self._pick_transition(current_state)
            if rule is None:
                # No outgoing transitions — entity is stuck (not anomaly, just dead-end)
                break

            normal_next = rule.to_state

            # Anomaly injection
            if cfg.anomaly_enabled:
                actual_next, anomaly_type, is_anomaly = self._maybe_inject_anomaly(
                    entity_id, current_state, normal_next, visited,
                )
            else:
                actual_next = normal_next
                anomaly_type = ""
                is_anomaly = False

            # If stuck anomaly was triggered, stop processing
            if anomaly_type == "stuck":
                events.append({
                    "event_id": str(uuid.uuid4()),
                    "entity_id": entity_id,
                    "from_state": current_state,
                    "to_state": current_state,
                    "transitioned_at": current_time,
                    "dwell_hours": 0.0,
                    "is_anomaly": True,
                    "anomaly_type": "stuck",
                })
                break

            # Compute dwell time — use the original rule even for anomalies
            dwell = self._compute_dwell(rule)
            transition_time = current_time + timedelta(hours=dwell)

            events.append({
                "event_id": str(uuid.uuid4()),
                "entity_id": entity_id,
                "from_state": current_state,
                "to_state": actual_next,
                "transitioned_at": transition_time,
                "dwell_hours": round(dwell, 4),
                "is_anomaly": is_anomaly,
                "anomaly_type": anomaly_type,
            })

            current_state = actual_next
            current_time = transition_time
            visited.append(current_state)

        return events

    def _pick_transition(self, state: str) -> TransitionRule | None:
        """Select the next transition from *state* using normalised probabilities.

        Returns ``None`` if no transitions are defined for this state.
        """
        rules = self._transitions_by_state.get(state)
        if not rules:
            return None

        probs = self._probs_by_state[state]
        idx = int(self._rng.choice(len(rules), p=probs))
        return rules[idx]

    def _compute_dwell(self, rule: TransitionRule) -> float:
        """Sample a dwell time from a normal distribution, clamped to the minimum."""
        sample = float(self._rng.normal(rule.dwell_hours_mean, rule.dwell_hours_std))
        return max(rule.min_dwell_hours, sample)

    def _maybe_inject_anomaly(
        self,
        entity_id: str,
        current_state: str,
        normal_next_state: str,
        visited: list[str],
    ) -> tuple[str, str, bool]:
        """Possibly inject an anomaly, returning (actual_next_state, anomaly_type, is_anomaly).

        Anomaly types:
        - **skip**: jump past the normal next state to a downstream state.
        - **stuck**: entity stops transitioning (caller should break).
        - **backward**: entity returns to a previously visited state.
        """
        cfg = self._config
        roll = float(self._rng.random())
        threshold = 0.0

        # Check stuck
        threshold += cfg.anomaly_stuck_probability
        if roll < threshold:
            return current_state, "stuck", True

        # Check skip — jump to a state reachable *from* the normal next state
        threshold += cfg.anomaly_skip_probability
        if roll < threshold:
            skip_rules = self._transitions_by_state.get(normal_next_state)
            if skip_rules:
                skip_target = skip_rules[
                    self._rng.integers(0, len(skip_rules))
                ].to_state
                return skip_target, "skip", True

        # Check backward — go to a previously visited non-terminal state
        threshold += cfg.anomaly_backward_probability
        if roll < threshold:
            backward_candidates = [
                s for s in visited
                if s != current_state and s not in self._terminal_states
            ]
            if backward_candidates:
                target = backward_candidates[
                    self._rng.integers(0, len(backward_candidates))
                ]
                return target, "backward", True

        # No anomaly
        return normal_next_state, "", False

    # ------------------------------------------------------------------
    # Summary builders
    # ------------------------------------------------------------------

    def _build_entity_summary(self, events_df: pd.DataFrame) -> pd.DataFrame:
        """Build a per-entity summary DataFrame."""
        cfg = self._config

        if events_df.empty:
            return pd.DataFrame(
                columns=[
                    "entity_id", "initial_state", "final_state",
                    "total_transitions", "total_hours", "is_complete",
                ]
            )

        records: list[dict[str, Any]] = []

        for entity_id, group in events_df.groupby("entity_id", sort=True):
            group_sorted = group.sort_values("transitioned_at")
            first_row = group_sorted.iloc[0]
            last_row = group_sorted.iloc[-1]

            initial_state = first_row["from_state"]
            final_state = last_row["to_state"]
            total_transitions = len(group_sorted)
            total_hours = float(group_sorted["dwell_hours"].sum())
            is_complete = final_state in self._terminal_states

            records.append({
                "entity_id": entity_id,
                "initial_state": initial_state,
                "final_state": final_state,
                "total_transitions": total_transitions,
                "total_hours": round(total_hours, 4),
                "is_complete": is_complete,
            })

        # Also add entities with zero events (shouldn't happen normally, but be safe)
        seen = {r["entity_id"] for r in records}
        for i in range(cfg.entity_count):
            eid = f"{cfg.entity_prefix}_{i:06d}"
            if eid not in seen:
                init = self._initial_states[0] if self._initial_states else ""
                records.append({
                    "entity_id": eid,
                    "initial_state": init,
                    "final_state": init,
                    "total_transitions": 0,
                    "total_hours": 0.0,
                    "is_complete": False,
                })

        return pd.DataFrame(records).sort_values("entity_id").reset_index(drop=True)

    def _build_state_distribution(self, entity_summary: pd.DataFrame) -> dict[str, int]:
        """Count of entities in each final state."""
        if entity_summary.empty:
            return {}
        return entity_summary["final_state"].value_counts().to_dict()

    def _compile_stats(
        self,
        events_df: pd.DataFrame,
        entity_summary: pd.DataFrame,
    ) -> dict[str, Any]:
        """Build aggregate statistics dictionary."""
        anomaly_count = int(events_df["is_anomaly"].sum()) if not events_df.empty else 0

        completed = entity_summary[entity_summary["is_complete"]] if not entity_summary.empty else entity_summary
        mean_completion_hours = (
            round(float(completed["total_hours"].mean()), 4)
            if not completed.empty else 0.0
        )

        return {
            "total_events": len(events_df),
            "total_entities": len(entity_summary),
            "anomaly_count": anomaly_count,
            "mean_completion_hours": mean_completion_hours,
            "config_seed": self._config.seed,
        }


# ---------------------------------------------------------------------------
# Preset workflows
# ---------------------------------------------------------------------------

def _preset_order_fulfillment() -> tuple[list[StateDefinition], list[TransitionRule]]:
    """Order fulfillment: created -> confirmed -> shipped -> delivered."""
    states = [
        StateDefinition("created", is_initial=True),
        StateDefinition("confirmed"),
        StateDefinition("shipped"),
        StateDefinition("delivered", is_terminal=True),
        StateDefinition("cancelled", is_terminal=True),
        StateDefinition("returned", is_terminal=True),
    ]
    transitions = [
        TransitionRule("created", "confirmed", probability=0.90, dwell_hours_mean=2.0, dwell_hours_std=0.5),
        TransitionRule("created", "cancelled", probability=0.10, dwell_hours_mean=1.0, dwell_hours_std=0.3),
        TransitionRule("confirmed", "shipped", probability=0.95, dwell_hours_mean=24.0, dwell_hours_std=6.0),
        TransitionRule("confirmed", "cancelled", probability=0.05, dwell_hours_mean=4.0, dwell_hours_std=1.0),
        TransitionRule("shipped", "delivered", probability=0.92, dwell_hours_mean=72.0, dwell_hours_std=24.0),
        TransitionRule("shipped", "returned", probability=0.08, dwell_hours_mean=120.0, dwell_hours_std=36.0),
    ]
    return states, transitions


def _preset_support_ticket() -> tuple[list[StateDefinition], list[TransitionRule]]:
    """Support ticket: opened -> triaged -> in_progress -> resolved -> closed."""
    states = [
        StateDefinition("opened", is_initial=True),
        StateDefinition("triaged"),
        StateDefinition("in_progress"),
        StateDefinition("escalated"),
        StateDefinition("resolved"),
        StateDefinition("closed", is_terminal=True),
        StateDefinition("reopened"),
    ]
    transitions = [
        TransitionRule("opened", "triaged", probability=0.95, dwell_hours_mean=1.0, dwell_hours_std=0.5),
        TransitionRule("opened", "closed", probability=0.05, dwell_hours_mean=0.5, dwell_hours_std=0.2),
        TransitionRule("triaged", "in_progress", probability=0.80, dwell_hours_mean=4.0, dwell_hours_std=2.0),
        TransitionRule("triaged", "escalated", probability=0.20, dwell_hours_mean=2.0, dwell_hours_std=1.0),
        TransitionRule("in_progress", "resolved", probability=0.85, dwell_hours_mean=8.0, dwell_hours_std=4.0),
        TransitionRule("in_progress", "escalated", probability=0.15, dwell_hours_mean=6.0, dwell_hours_std=3.0),
        TransitionRule("escalated", "in_progress", probability=0.70, dwell_hours_mean=12.0, dwell_hours_std=6.0),
        TransitionRule("escalated", "resolved", probability=0.30, dwell_hours_mean=24.0, dwell_hours_std=8.0),
        TransitionRule("resolved", "closed", probability=0.90, dwell_hours_mean=2.0, dwell_hours_std=1.0),
        TransitionRule("resolved", "reopened", probability=0.10, dwell_hours_mean=48.0, dwell_hours_std=24.0),
        TransitionRule("reopened", "in_progress", probability=1.0, dwell_hours_mean=2.0, dwell_hours_std=1.0),
    ]
    return states, transitions


def _preset_employee_onboarding() -> tuple[list[StateDefinition], list[TransitionRule]]:
    """Employee onboarding: applied -> screening -> interview -> offer -> hired."""
    states = [
        StateDefinition("applied", is_initial=True),
        StateDefinition("screening"),
        StateDefinition("interview"),
        StateDefinition("offer"),
        StateDefinition("hired", is_terminal=True),
        StateDefinition("rejected", is_terminal=True),
        StateDefinition("withdrawn", is_terminal=True),
    ]
    transitions = [
        TransitionRule("applied", "screening", probability=0.70, dwell_hours_mean=48.0, dwell_hours_std=24.0),
        TransitionRule("applied", "rejected", probability=0.25, dwell_hours_mean=72.0, dwell_hours_std=24.0),
        TransitionRule("applied", "withdrawn", probability=0.05, dwell_hours_mean=24.0, dwell_hours_std=12.0),
        TransitionRule("screening", "interview", probability=0.60, dwell_hours_mean=120.0, dwell_hours_std=48.0),
        TransitionRule("screening", "rejected", probability=0.35, dwell_hours_mean=96.0, dwell_hours_std=36.0),
        TransitionRule("screening", "withdrawn", probability=0.05, dwell_hours_mean=48.0, dwell_hours_std=24.0),
        TransitionRule("interview", "offer", probability=0.40, dwell_hours_mean=72.0, dwell_hours_std=24.0),
        TransitionRule("interview", "rejected", probability=0.50, dwell_hours_mean=48.0, dwell_hours_std=24.0),
        TransitionRule("interview", "withdrawn", probability=0.10, dwell_hours_mean=24.0, dwell_hours_std=12.0),
        TransitionRule("offer", "hired", probability=0.80, dwell_hours_mean=168.0, dwell_hours_std=72.0),
        TransitionRule("offer", "rejected", probability=0.05, dwell_hours_mean=120.0, dwell_hours_std=48.0),
        TransitionRule("offer", "withdrawn", probability=0.15, dwell_hours_mean=96.0, dwell_hours_std=48.0),
    ]
    return states, transitions


PRESET_WORKFLOWS: dict[str, Any] = {
    "order_fulfillment": _preset_order_fulfillment,
    "support_ticket": _preset_support_ticket,
    "employee_onboarding": _preset_employee_onboarding,
}


def get_preset_workflow(name: str) -> tuple[list[StateDefinition], list[TransitionRule]]:
    """Return ``(states, transitions)`` for a named preset workflow.

    Available presets: ``order_fulfillment``, ``support_ticket``,
    ``employee_onboarding``.

    Raises:
        KeyError: If *name* is not a recognised preset.
    """
    if name not in PRESET_WORKFLOWS:
        available = ", ".join(sorted(PRESET_WORKFLOWS.keys()))
        raise KeyError(f"Unknown preset {name!r}. Available: {available}")
    return PRESET_WORKFLOWS[name]()
