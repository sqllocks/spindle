"""Spindle composite domain presets."""

from sqllocks_spindle.presets.registry import PresetDef, PresetRegistry
from sqllocks_spindle.presets.builtin import get_preset, list_presets

__all__ = [
    "PresetDef",
    "PresetRegistry",
    "get_preset",
    "list_presets",
]
