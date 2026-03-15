"""Upgrade temporal columns from uniform to context-appropriate date/time patterns.

Transaction dates get seasonal Q4 bias and weekday weighting, end dates become
derived from their paired start date, and birth dates get an age-appropriate
range.  Audit and generic timestamps are left as uniform.
"""

from __future__ import annotations

from sqllocks_spindle.schema.inference import (
    ColumnSemantic,
    InferenceContext,
    TableRole,
)

# ---------------------------------------------------------------------------
# Seasonal transaction profile — slight Q4 bias with weekday weighting
# ---------------------------------------------------------------------------

_SEASONAL_TRANSACTION: dict = {
    "strategy": "temporal",
    "pattern": "seasonal",
    "profiles": {
        "month": {
            "Jan": 0.071, "Feb": 0.068, "Mar": 0.083, "Apr": 0.082,
            "May": 0.085, "Jun": 0.083, "Jul": 0.084, "Aug": 0.085,
            "Sep": 0.082, "Oct": 0.084, "Nov": 0.088, "Dec": 0.106,
        },
        "day_of_week": {
            "Mon": 0.165, "Tue": 0.160, "Wed": 0.155, "Thu": 0.155,
            "Fri": 0.160, "Sat": 0.105, "Sun": 0.100,
        },
    },
}

# ---------------------------------------------------------------------------
# Temporal semantic constants that we leave alone
# ---------------------------------------------------------------------------

_KEEP_UNIFORM: frozenset[ColumnSemantic] = frozenset({
    ColumnSemantic.TEMPORAL_AUDIT,
    ColumnSemantic.TEMPORAL_START,
    ColumnSemantic.TEMPORAL_GENERIC,
})


def _is_placeholder_temporal(gen: dict) -> bool:
    """Return True if the generator is a placeholder that should be upgraded.

    Matches:
    - ``temporal`` with ``uniform`` or unset pattern (DDL parser default)
    - ``faker`` strategy (DDL parser fallback for string date columns)
    - ``distribution`` strategy (DDL parser fallback for numeric columns
      that the column classifier identifies as temporal)
    """
    strategy = gen.get("strategy")

    # Faker/distribution on a temporal-classified column is a DDL parser placeholder
    if strategy in ("faker", "distribution"):
        return True

    if strategy != "temporal":
        return False
    # Already configured with seasonal profiles or derived rules — skip
    if gen.get("pattern") not in ("uniform", None):
        return False
    return True


def _find_start_column(
    table_def, semantics: dict[str, ColumnSemantic]
) -> str | None:
    """Find the first TEMPORAL_START column in the same table."""
    for col_name, semantic in semantics.items():
        if semantic == ColumnSemantic.TEMPORAL_START:
            return col_name
    return None


def _compute_birth_range(date_range: dict[str, str]) -> dict:
    """Build a uniform temporal generator with an age-appropriate date range.

    Birth dates span from 65 years before the model end date to 18 years
    before the model end date, producing adults of working age.
    """
    end_str = date_range.get("end", "2025-12-31")

    # Parse the year from the end date string
    try:
        end_year = int(end_str[:4])
    except (ValueError, IndexError):
        end_year = 2025

    birth_start = f"{end_year - 65}-01-01"
    birth_end = f"{end_year - 18}-12-31"

    return {
        "strategy": "temporal",
        "pattern": "uniform",
        "date_range": {"start": birth_start, "end": birth_end},
    }


class TemporalPatternInferrer:
    """Upgrade temporal column generators based on column and table semantics.

    Applies the following rules:

    - TEMPORAL_TRANSACTION on TRANSACTION tables: seasonal with Q4 bias and
      weekday weighting.
    - TEMPORAL_AUDIT: kept as uniform (audit timestamps have no seasonality).
    - TEMPORAL_START: kept as uniform within the model date range.
    - TEMPORAL_END: derived from the nearest TEMPORAL_START column in the
      same table via an ``add_days`` rule.
    - TEMPORAL_BIRTH: uniform with a custom range (65 years to 18 years
      before the model end date).
    - TEMPORAL_GENERIC: kept as uniform.
    """

    def analyze(self, ctx: InferenceContext) -> None:
        date_range = ctx.schema.model.date_range

        for table_name, table_def in ctx.schema.tables.items():
            semantics = ctx.column_semantics.get(table_name, {})
            table_role = ctx.table_roles.get(table_name, TableRole.UNKNOWN)

            for col_name, col_def in table_def.columns.items():
                semantic = semantics.get(col_name)
                if semantic is None:
                    continue

                gen = col_def.generator
                if not _is_placeholder_temporal(gen):
                    continue

                new_gen = self._infer_temporal(
                    semantic, table_role, col_name,
                    table_def, semantics, date_range,
                )
                if new_gen is not None:
                    col_def.generator = new_gen
                    ctx.annotate(
                        table=table_name,
                        column=col_name,
                        rule_id=f"TP-{semantic.name}",
                        description=(
                            f"Upgraded temporal pattern to "
                            f"'{new_gen.get('pattern', new_gen.get('strategy'))}' "
                            f"based on {semantic.name} semantic"
                        ),
                        confidence=0.75,
                    )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _infer_temporal(
        self,
        semantic: ColumnSemantic,
        table_role: TableRole,
        col_name: str,
        table_def,
        semantics: dict[str, ColumnSemantic],
        date_range: dict[str, str],
    ) -> dict | None:

        # --- Transaction date on a transaction-type table ---
        if semantic == ColumnSemantic.TEMPORAL_TRANSACTION:
            if table_role in (
                TableRole.TRANSACTION,
                TableRole.TRANSACTION_DETAIL,
                TableRole.FACT,
            ):
                return dict(_SEASONAL_TRANSACTION)  # shallow copy
            # Transaction date on a non-transaction table — still apply
            # seasonal patterns (it's a date that represents a business event)
            return dict(_SEASONAL_TRANSACTION)

        # --- Audit timestamps — no change ---
        if semantic == ColumnSemantic.TEMPORAL_AUDIT:
            return None  # keep uniform

        # --- Start dates — no change ---
        if semantic == ColumnSemantic.TEMPORAL_START:
            return None  # keep uniform within date_range

        # --- End dates — derive from nearest start column ---
        if semantic == ColumnSemantic.TEMPORAL_END:
            return self._end_date_derived(table_def, semantics, col_name)

        # --- Birth dates — age-appropriate range ---
        if semantic == ColumnSemantic.TEMPORAL_BIRTH:
            return _compute_birth_range(date_range)

        # --- Generic — no change ---
        if semantic == ColumnSemantic.TEMPORAL_GENERIC:
            return None

        return None

    # ------------------------------------------------------------------
    # End-date derivation
    # ------------------------------------------------------------------

    @staticmethod
    def _end_date_derived(
        table_def,
        semantics: dict[str, ColumnSemantic],
        end_col_name: str,
    ) -> dict | None:
        """Build a derived generator that adds days to the paired start column."""
        start_col = _find_start_column(table_def, semantics)

        if start_col is None:
            # No start column found — cannot derive; leave as-is
            return None

        return {
            "strategy": "derived",
            "source_column": start_col,
            "rule": "add_days",
            "params": {"min": 1, "max": 365},
        }
