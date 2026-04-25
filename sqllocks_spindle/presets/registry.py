"""Preset registry for named composite domain configurations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PresetDef:
    """Definition of a named composite preset."""

    name: str
    description: str
    domains: list[str]  # domain names
    shared_entities: dict[str, dict[str, Any]] = field(default_factory=dict)


class PresetRegistry:
    """Registry of named composite domain presets."""

    def __init__(self) -> None:
        self._presets: dict[str, PresetDef] = {}

    def register(self, preset: PresetDef) -> None:
        """Register a preset definition."""
        self._presets[preset.name] = preset

    def get(self, name: str) -> PresetDef:
        """Get a preset by name. Raises KeyError if not found."""
        if name not in self._presets:
            raise KeyError(
                f"Unknown preset '{name}'. Available: {list(self._presets.keys())}"
            )
        return self._presets[name]

    def list(self) -> list[PresetDef]:
        """Return all registered presets."""
        return list(self._presets.values())

    @property
    def available(self) -> list[str]:
        """Return names of all registered presets."""
        return list(self._presets.keys())
