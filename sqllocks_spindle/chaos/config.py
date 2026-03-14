"""Chaos Engine configuration for Spindle.

Defines the knobs that control chaos injection: intensity presets,
per-category weights, escalation curves, and warmup windows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ChaosCategory(Enum):
    """Categories of chaos that can be injected into generated data."""

    SCHEMA = "schema"
    VALUE = "value"
    FILE = "file"
    REFERENTIAL = "referential"
    TEMPORAL = "temporal"
    VOLUME = "volume"


# ---------------------------------------------------------------------------
# Intensity presets — each maps to a probability multiplier applied to all
# base injection probabilities.
# ---------------------------------------------------------------------------

INTENSITY_PRESETS: dict[str, float] = {
    "calm": 0.25,
    "moderate": 1.0,
    "stormy": 2.5,
    "hurricane": 5.0,
}

# ---------------------------------------------------------------------------
# Default per-category settings used when the caller does not supply them.
# ---------------------------------------------------------------------------

_DEFAULT_CATEGORIES: dict[str, dict[str, Any]] = {
    ChaosCategory.SCHEMA.value: {"enabled": True, "weight": 0.10},
    ChaosCategory.VALUE.value: {"enabled": True, "weight": 0.15},
    ChaosCategory.FILE.value: {"enabled": True, "weight": 0.08},
    ChaosCategory.REFERENTIAL.value: {"enabled": True, "weight": 0.10},
    ChaosCategory.TEMPORAL.value: {"enabled": True, "weight": 0.12},
    ChaosCategory.VOLUME.value: {"enabled": True, "weight": 0.08},
}


@dataclass
class ChaosOverride:
    """Per-issue override that forces a specific chaos event on a given day.

    Attributes:
        day: The simulation day on which to inject.
        category: Which :class:`ChaosCategory` to inject.
        params: Extra parameters forwarded to the mutator.
    """

    day: int
    category: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChaosConfig:
    """Top-level configuration for the Chaos Engine.

    Attributes:
        enabled: Master switch.  When ``False`` the engine is a no-op.
        intensity: One of ``calm``, ``moderate``, ``stormy``, ``hurricane``.
        seed: Seed for the chaos RNG (independent of the main generation seed).
        warmup_days: Number of days at the start with *no* chaos.
        chaos_start_day: First day chaos may fire (must be > warmup_days).
        escalation: How injection probability grows over time.
            ``gradual`` — linear ramp from 0 to full over the first 30 chaos days.
            ``random`` — uniform random draw each day.
            ``front-loaded`` — full probability from day 1, decaying over time.
        categories: Per-category configuration.  Keys are
            :class:`ChaosCategory` value strings; values are dicts with
            ``enabled`` (bool) and ``weight`` (float 0-1).
        overrides: Explicit per-day overrides that bypass probability checks.
        breaking_change_day: Day on which schema-breaking mutations are
            allowed (column drops / renames).  Before this day only additive
            schema changes are injected.
    """

    enabled: bool = False
    intensity: str = "moderate"
    seed: int = 42
    warmup_days: int = 7
    chaos_start_day: int = 8
    escalation: str = "gradual"
    categories: dict[str, dict[str, Any]] = field(
        default_factory=lambda: dict(_DEFAULT_CATEGORIES)
    )
    overrides: list[ChaosOverride] = field(default_factory=list)
    breaking_change_day: int = 20

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def intensity_multiplier(self) -> float:
        """Return the numeric multiplier for the current intensity preset."""
        return INTENSITY_PRESETS.get(self.intensity, 1.0)

    def is_category_enabled(self, category: str) -> bool:
        """Return ``True`` if the given category string is enabled."""
        cat = self.categories.get(category, {})
        return cat.get("enabled", False)

    def category_weight(self, category: str) -> float:
        """Return the base weight for a category (0.0 if missing/disabled)."""
        cat = self.categories.get(category, {})
        if not cat.get("enabled", False):
            return 0.0
        return cat.get("weight", 0.0)

    def overrides_for_day(self, day: int) -> list[ChaosOverride]:
        """Return any explicit overrides scheduled for *day*."""
        return [o for o in self.overrides if o.day == day]

    def validate(self) -> list[str]:
        """Return a list of validation error messages (empty = valid)."""
        errors: list[str] = []
        if self.intensity not in INTENSITY_PRESETS:
            errors.append(
                f"Unknown intensity '{self.intensity}'. "
                f"Choose from: {', '.join(INTENSITY_PRESETS)}"
            )
        if self.escalation not in ("gradual", "random", "front-loaded"):
            errors.append(
                f"Unknown escalation '{self.escalation}'. "
                f"Choose from: gradual, random, front-loaded"
            )
        if self.chaos_start_day <= self.warmup_days:
            errors.append(
                f"chaos_start_day ({self.chaos_start_day}) must be > "
                f"warmup_days ({self.warmup_days})"
            )
        for name in self.categories:
            if name not in {c.value for c in ChaosCategory}:
                errors.append(f"Unknown category '{name}' in categories dict")
        return errors
