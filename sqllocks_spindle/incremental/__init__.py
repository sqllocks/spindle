"""Incremental generation engine for Spindle — C1 continue support + C5 time-travel."""

from sqllocks_spindle.incremental.continue_config import ContinueConfig
from sqllocks_spindle.incremental.continue_engine import ContinueEngine, DeltaResult
from sqllocks_spindle.incremental.time_travel import (
    Snapshot,
    TimeTravelConfig,
    TimeTravelEngine,
    TimeTravelResult,
)

__all__ = [
    "ContinueEngine",
    "ContinueConfig",
    "DeltaResult",
    "TimeTravelEngine",
    "TimeTravelConfig",
    "TimeTravelResult",
    "Snapshot",
]
