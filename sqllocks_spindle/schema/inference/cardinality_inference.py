"""Infer parent:child cardinality ratios and scale presets for row generation.

Sets derived_counts so child tables scale proportionally to their parents,
and sets scale presets (small/medium/large) for root entity tables.
"""

from __future__ import annotations

import re

from sqllocks_spindle.schema.inference import InferenceContext, TableRole

# ---------------------------------------------------------------------------
# Name patterns for relationship classification
# ---------------------------------------------------------------------------

_ADDRESS_CONTACT_PATTERNS = re.compile(
    r"(address|contact|phone|email|location|site)", re.IGNORECASE
)
_RETURN_REFUND_PATTERNS = re.compile(
    r"(return|refund|reversal|chargeback|credit_memo|void)", re.IGNORECASE
)
_LOG_EVENT_PATTERNS = re.compile(
    r"(log|audit|history|event|tracking|changelog|activity|notification)",
    re.IGNORECASE,
)


class CardinalityInferrer:
    """Set parent:child ratios in derived_counts and scale presets for root tables."""

    def analyze(self, ctx: InferenceContext) -> None:
        for table_name, table_def in ctx.schema.tables.items():
            table_role = ctx.table_roles.get(table_name, TableRole.UNKNOWN)
            parents = ctx.parents_of.get(table_name, [])

            # CA-06: Lookup/Reference tables — fixed count based on column count
            if table_role in (TableRole.LOOKUP, TableRole.DIMENSION):
                fixed = self._lookup_fixed_count(table_def)
                ctx.schema.generation.derived_counts[table_name] = {"fixed": fixed}
                ctx.annotate(
                    table=table_name, column=None,
                    rule_id="CA-06",
                    description=f"Lookup/reference table — fixed {fixed} rows",
                    confidence=0.85,
                )
                continue

            # CA-07: Hierarchy tables — fixed 50
            if table_role == TableRole.HIERARCHY:
                ctx.schema.generation.derived_counts[table_name] = {"fixed": 50}
                ctx.annotate(
                    table=table_name, column=None,
                    rule_id="CA-07",
                    description="Hierarchy table — fixed 50 rows",
                    confidence=0.8,
                )
                continue

            # Root tables (no FK parents) — set scale presets
            if not parents:
                self._set_scale_presets(ctx, table_name)
                continue

            # Tables with parents — determine ratio
            parent_table = self._pick_primary_parent(parents, ctx)
            parent_role = ctx.table_roles.get(parent_table, TableRole.UNKNOWN)

            ratio, rule_id, desc = self._infer_ratio(
                table_name, table_role, parent_table, parent_role, ctx,
            )

            ctx.schema.generation.derived_counts[table_name] = {
                "per_parent": {"ratio": ratio, "parent": parent_table},
            }
            ctx.annotate(
                table=table_name, column=None,
                rule_id=rule_id,
                description=desc,
                confidence=0.8,
            )

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _lookup_fixed_count(table_def) -> int:
        """Heuristic: more columns → larger reference table (20-200)."""
        col_count = len(table_def.columns)
        if col_count <= 3:
            return 20
        if col_count <= 5:
            return 50
        if col_count <= 8:
            return 100
        return 200

    @staticmethod
    def _set_scale_presets(ctx: InferenceContext, table_name: str) -> None:
        """Set small/medium/large scale presets for a root entity table."""
        ctx.schema.generation.scales.setdefault("small", {})[table_name] = 1_000
        ctx.schema.generation.scales.setdefault("medium", {})[table_name] = 50_000
        ctx.schema.generation.scales.setdefault("large", {})[table_name] = 500_000
        ctx.annotate(
            table=table_name, column=None,
            rule_id="CA-SCALE",
            description="Root table — scale presets set (1K / 50K / 500K)",
            confidence=0.9,
        )

    @staticmethod
    def _pick_primary_parent(parents: list[str], ctx: InferenceContext) -> str:
        """Pick the most significant parent (prefer entity/transaction over lookup)."""
        priority = {
            TableRole.ENTITY: 0,
            TableRole.TRANSACTION: 1,
            TableRole.FACT: 2,
        }
        scored = sorted(
            parents,
            key=lambda p: priority.get(ctx.table_roles.get(p, TableRole.UNKNOWN), 99),
        )
        return scored[0]

    def _infer_ratio(
        self,
        table_name: str,
        table_role: TableRole,
        parent_table: str,
        parent_role: TableRole,
        ctx: InferenceContext,
    ) -> tuple[float, str, str]:
        """Return (ratio, rule_id, description)."""

        # CA-08: Bridge/Junction — ratio 3.0 per smaller parent
        if table_role == TableRole.BRIDGE:
            return (
                3.0, "CA-08",
                f"Bridge table — ratio 3.0 per parent ({parent_table})",
            )

        # CA-01: Entity -> Address/Contact child
        if parent_role == TableRole.ENTITY and _ADDRESS_CONTACT_PATTERNS.search(table_name):
            return (
                1.5, "CA-01",
                f"Entity ({parent_table}) -> address/contact — ratio 1.5",
            )

        # CA-04: Transaction -> Return/Refund
        if parent_role == TableRole.TRANSACTION and _RETURN_REFUND_PATTERNS.search(table_name):
            return (
                0.15, "CA-04",
                f"Transaction ({parent_table}) -> return/refund — ratio 0.15",
            )

        # CA-03: Transaction -> Detail (line items)
        if table_role == TableRole.TRANSACTION_DETAIL:
            return (
                2.5, "CA-03",
                f"Transaction ({parent_table}) -> detail — ratio 2.5",
            )

        # CA-05: Entity -> Log/Event
        if parent_role == TableRole.ENTITY and (
            table_role == TableRole.LOG or _LOG_EVENT_PATTERNS.search(table_name)
        ):
            return (
                10.0, "CA-05",
                f"Entity ({parent_table}) -> log/event — ratio 10.0",
            )

        # CA-02: Entity -> Transaction
        if parent_role == TableRole.ENTITY and table_role == TableRole.TRANSACTION:
            return (
                5.0, "CA-02",
                f"Entity ({parent_table}) -> transaction — ratio 5.0",
            )

        # CA-05 broader: Any parent -> Log/Event
        if table_role == TableRole.LOG or _LOG_EVENT_PATTERNS.search(table_name):
            return (
                10.0, "CA-05",
                f"Parent ({parent_table}) -> log/event — ratio 10.0",
            )

        # CA-04 broader: Any parent -> Return/Refund
        if _RETURN_REFUND_PATTERNS.search(table_name):
            return (
                0.15, "CA-04",
                f"Parent ({parent_table}) -> return/refund — ratio 0.15",
            )

        # CA-09: Default fallback
        return (
            3.0, "CA-09",
            f"Default child ratio — 3.0 per parent ({parent_table})",
        )
