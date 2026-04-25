"""Base strategy interface and registry."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.engine.id_manager import IDManager
from sqllocks_spindle.schema.parser import ColumnDef

logger = logging.getLogger(__name__)


class GenerationContext:
    """Context passed to strategies during generation.

    Provides access to the RNG, ID manager, current table state,
    model config, and other strategies' output for the current row batch.
    """

    def __init__(
        self,
        rng: np.random.Generator,
        id_manager: IDManager,
        model_config: dict[str, Any],
        row_count: int,
    ):
        self.rng = rng
        self.id_manager = id_manager
        self.model_config = model_config
        self.row_count = row_count
        # Columns already generated for this table in this batch
        self.current_table: dict[str, np.ndarray] = {}
        # Name of the current table being generated
        self.current_table_name: str = ""


class Strategy(ABC):
    """Base class for all column generation strategies."""

    @abstractmethod
    def generate(
        self,
        column: ColumnDef,
        config: dict[str, Any],
        ctx: GenerationContext,
    ) -> np.ndarray:
        """Generate values for a column.

        Args:
            column: The column definition from the schema.
            config: The generator config dict from the column def.
            ctx: The generation context with RNG, ID manager, etc.

        Returns:
            numpy array of generated values with length ctx.row_count.
        """
        ...

    def apply_nulls(
        self,
        values: np.ndarray,
        column: ColumnDef,
        ctx: GenerationContext,
    ) -> np.ndarray:
        """Apply null values based on the column's null_rate."""
        if not column.nullable or column.null_rate <= 0:
            return values

        mask = ctx.rng.random(len(values)) < column.null_rate
        # Convert to object array to support None.
        # For datetime arrays, .astype(object) returns ints (ns epoch);
        # convert via pd.Series to preserve Timestamp objects.
        if np.issubdtype(values.dtype, np.datetime64):
            result = np.array(pd.Series(values).astype(object).values, dtype=object)
        else:
            result = values.astype(object)
        result[mask] = None
        return result


class StrategyRegistry:
    """Registry mapping strategy names to Strategy instances."""

    def __init__(self):
        self._strategies: dict[str, Strategy] = {}

    def register(self, name: str, strategy: Strategy) -> None:
        self._strategies[name] = strategy

    def get(self, name: str) -> Strategy:
        if name not in self._strategies:
            raise KeyError(
                f"Unknown strategy '{name}'. "
                f"Available: {list(self._strategies.keys())}"
            )
        return self._strategies[name]

    def has(self, name: str) -> bool:
        return name in self._strategies

    def load_entrypoint_plugins(self, group: str = "spindle.strategies") -> None:
        """Discover and register strategies from installed entrypoint plugins.

        Third-party packages can register custom strategies by defining an
        entrypoint in their ``pyproject.toml``::

            [project.entry-points."spindle.strategies"]
            my_strategy = "my_package.strategies:MyStrategy"

        The entrypoint value must be a class implementing :class:`Strategy`.
        """
        try:
            from importlib.metadata import entry_points
        except ImportError:
            return

        eps = entry_points()
        # Python 3.12+ returns SelectableGroups; 3.9-3.11 returns dict
        if hasattr(eps, "select"):
            plugin_eps = eps.select(group=group)
        else:
            plugin_eps = eps.get(group, [])

        for ep in plugin_eps:
            try:
                cls = ep.load()
                instance = cls()
                self.register(ep.name, instance)
                logger.info("Loaded plugin strategy '%s' from %s", ep.name, ep.value)
            except Exception as exc:
                logger.warning("Failed to load plugin strategy '%s': %s", ep.name, exc)

    @property
    def available(self) -> list[str]:
        return list(self._strategies.keys())
