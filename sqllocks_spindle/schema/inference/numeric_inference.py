"""Upgrade numeric columns from generic uniform/normal to semantically appropriate distributions.

Examines each column's ColumnSemantic and replaces placeholder distribution
parameters with realistic shapes: log-normal for money, geometric for quantities,
bounded normal for percentages, and context-dependent normal for measurements.
"""

from __future__ import annotations

import re

from sqllocks_spindle.schema.inference import (
    ColumnSemantic,
    InferenceContext,
    TableRole,
)

# ---------------------------------------------------------------------------
# Measurement heuristics — map column-name substrings to (mean, std)
# ---------------------------------------------------------------------------

_MEASUREMENT_HINTS: dict[str, tuple[float, float]] = {
    "weight": (5.0, 3.0),
    "height": (170.0, 15.0),
    "width": (50.0, 20.0),
    "length": (100.0, 40.0),
    "depth": (10.0, 5.0),
    "sqft": (1500.0, 500.0),
    "square_feet": (1500.0, 500.0),
    "lot_size": (8000.0, 3000.0),
    "area": (1200.0, 400.0),
    "volume": (500.0, 200.0),
    "distance": (50.0, 30.0),
    "duration": (60.0, 30.0),
    "temperature": (72.0, 10.0),
    "size": (10.0, 5.0),
}

_DEFAULT_MEASUREMENT = (50.0, 20.0)


def _is_placeholder_distribution(gen: dict) -> bool:
    """Return True if the generator looks like a generic placeholder distribution.

    We only overwrite columns whose current strategy is "distribution" with
    basic uniform or normal params that were auto-assigned by the DDL parser
    rather than intentionally configured by the user.
    """
    if gen.get("strategy") != "distribution":
        return False

    dist = gen.get("distribution", "")
    params = gen.get("params", {})

    # Uniform with round/generic bounds
    if dist == "uniform":
        return True

    # Normal with generic-looking params (parser defaults or round numbers)
    if dist == "normal":
        mean = params.get("mean", 0)
        std = params.get("std", 1)
        # Consider it generic if mean and std are both round integers
        if isinstance(mean, (int, float)) and isinstance(std, (int, float)):
            if mean == int(mean) and std == int(std):
                return True

    return False


class NumericDistributionInferrer:
    """Upgrade numeric column generators based on column semantics.

    Only modifies columns whose current strategy is "distribution" with
    placeholder parameters. Columns that already have well-configured
    strategies (weighted_enum, derived, etc.) are left untouched.
    """

    def analyze(self, ctx: InferenceContext) -> None:
        for table_name, table_def in ctx.schema.tables.items():
            semantics = ctx.column_semantics.get(table_name, {})

            for col_name, col_def in table_def.columns.items():
                semantic = semantics.get(col_name)
                if semantic is None:
                    continue

                gen = col_def.generator
                if not _is_placeholder_distribution(gen):
                    continue

                new_gen = self._infer_distribution(semantic, col_name)
                if new_gen is not None:
                    col_def.generator = new_gen
                    ctx.annotate(
                        table=table_name,
                        column=col_name,
                        rule_id=f"ND-{semantic.name}",
                        description=(
                            f"Upgraded to {new_gen['distribution']} distribution "
                            f"based on {semantic.name} semantic"
                        ),
                        confidence=0.75,
                    )

    # ------------------------------------------------------------------
    # Per-semantic distribution selection
    # ------------------------------------------------------------------

    def _infer_distribution(
        self, semantic: ColumnSemantic, col_name: str
    ) -> dict | None:
        if semantic == ColumnSemantic.MONETARY:
            return self._monetary()
        if semantic == ColumnSemantic.QUANTITY:
            return self._quantity()
        if semantic == ColumnSemantic.PERCENTAGE:
            return self._percentage()
        if semantic == ColumnSemantic.MEASUREMENT:
            return self._measurement(col_name)
        if semantic == ColumnSemantic.RATING:
            return self._rating(col_name)
        return None

    # --- Monetary: right-skewed prices/costs ---

    @staticmethod
    def _monetary() -> dict:
        return {
            "strategy": "distribution",
            "distribution": "log_normal",
            "params": {"mean": 4.0, "sigma": 1.2, "min": 0.01, "max": 99999},
        }

    # --- Quantity: geometric / log-normal for counts ---

    @staticmethod
    def _quantity() -> dict:
        return {
            "strategy": "distribution",
            "distribution": "log_normal",
            "params": {"mean": 1.5, "sigma": 0.8, "min": 1, "max": 1000},
        }

    # --- Percentage: bounded normal 0-100 ---

    @staticmethod
    def _percentage() -> dict:
        return {
            "strategy": "distribution",
            "distribution": "normal",
            "params": {"mean": 10, "std": 5, "min": 0, "max": 100},
        }

    # --- Measurement: context-dependent normal ---

    @staticmethod
    def _measurement(col_name: str) -> dict:
        lower = col_name.lower()
        mean, std = _DEFAULT_MEASUREMENT

        for keyword, (m, s) in _MEASUREMENT_HINTS.items():
            if keyword in lower:
                mean, std = m, s
                break

        return {
            "strategy": "distribution",
            "distribution": "normal",
            "params": {"mean": mean, "std": std},
        }

    # --- Rating: normal centered on 3.5 for 5-point scales ---

    @staticmethod
    def _rating(col_name: str) -> dict:
        lower = col_name.lower()

        # Default 5-point scale for columns with "stars" or "rating"
        if "stars" in lower or "rating" in lower:
            return {
                "strategy": "distribution",
                "distribution": "normal",
                "params": {"mean": 3.5, "std": 1.0, "min": 1, "max": 5},
            }

        # Generic score/rank — wider range
        return {
            "strategy": "distribution",
            "distribution": "normal",
            "params": {"mean": 3.5, "std": 1.0, "min": 1, "max": 5},
        }
