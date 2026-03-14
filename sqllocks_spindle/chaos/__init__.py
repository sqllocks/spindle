"""Chaos Engine for Spindle — deterministic data-quality issue injection.

Public API::

    from sqllocks_spindle.chaos import ChaosEngine, ChaosConfig, ChaosCategory

    cfg = ChaosConfig(enabled=True, intensity="stormy")
    engine = ChaosEngine(cfg)
    df = engine.corrupt_dataframe(df, day=15)
"""

from sqllocks_spindle.chaos.categories import (
    ChaosMutator,
    FileChaosMutator,
    ReferentialChaosMutator,
    SchemaChaosMutator,
    TemporalChaosMutator,
    ValueChaosMutator,
    VolumeChaosMutator,
)
from sqllocks_spindle.chaos.config import (
    INTENSITY_PRESETS,
    ChaosCategory,
    ChaosConfig,
    ChaosOverride,
)
from sqllocks_spindle.chaos.engine import ChaosEngine

__all__ = [
    # Engine
    "ChaosEngine",
    # Config
    "ChaosConfig",
    "ChaosCategory",
    "ChaosOverride",
    "INTENSITY_PRESETS",
    # Mutators
    "ChaosMutator",
    "SchemaChaosMutator",
    "ValueChaosMutator",
    "FileChaosMutator",
    "ReferentialChaosMutator",
    "TemporalChaosMutator",
    "VolumeChaosMutator",
]
