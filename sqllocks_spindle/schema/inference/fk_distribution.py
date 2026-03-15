"""Upgrade FK distribution strategies from blanket defaults to context-appropriate distributions.

Uses table roles, column semantics, and naming patterns to assign realistic
distribution parameters (pareto, zipf, uniform) to foreign key columns.
"""

from __future__ import annotations

import re

from sqllocks_spindle.schema.inference import InferenceContext, TableRole, ColumnSemantic

# ---------------------------------------------------------------------------
# FK column name patterns
# ---------------------------------------------------------------------------

_ASSIGNED_PATTERNS = re.compile(
    r"(assigned_to|approved_by|created_by|reviewed_by|managed_by|owned_by|"
    r"updated_by|modified_by|submitted_by|handled_by)", re.IGNORECASE
)
_ADDRESS_FK_PATTERNS = re.compile(
    r"(address|location|site|facility|warehouse|branch|office)", re.IGNORECASE
)


class FKDistributionInferrer:
    """Assign context-appropriate distributions to FK generator dicts."""

    def analyze(self, ctx: InferenceContext) -> None:
        for table_name, table_def in ctx.schema.tables.items():
            table_role = ctx.table_roles.get(table_name, TableRole.UNKNOWN)

            for col_name, col_def in table_def.columns.items():
                if not col_def.is_foreign_key:
                    continue

                gen = col_def.generator
                parent_table = col_def.fk_ref_table
                if parent_table is None:
                    continue

                parent_role = ctx.table_roles.get(parent_table, TableRole.UNKNOWN)

                dist, params, rule_id, desc = self._infer(
                    col_name, table_name, table_role, parent_table, parent_role,
                    col_def, table_def, ctx,
                )

                gen["distribution"] = dist
                gen["params"] = params

                # FK-04: Nullable FK — add null_rate
                if col_def.nullable:
                    gen.setdefault("null_rate", 0.15)
                    ctx.annotate(
                        table=table_name, column=col_name,
                        rule_id="FK-04",
                        description="Nullable FK — added null_rate 0.15",
                        confidence=0.9,
                    )

                ctx.annotate(
                    table=table_name, column=col_name,
                    rule_id=rule_id,
                    description=desc,
                    confidence=0.8,
                )

    def _infer(
        self,
        col_name: str,
        table_name: str,
        table_role: TableRole,
        parent_table: str,
        parent_role: TableRole,
        col_def,
        table_def,
        ctx: InferenceContext,
    ) -> tuple[str, dict, str, str]:
        """Return (distribution, params, rule_id, description)."""

        # FK-05: Self-referencing — keep self_referencing strategy unchanged
        if parent_table == table_name:
            return (
                "self_referencing", {},
                "FK-05", "Self-referencing FK — strategy unchanged",
            )

        # FK-09: Agent/person FK by column name (assigned_to, approved_by, etc.)
        if _ASSIGNED_PATTERNS.search(col_name):
            return (
                "pareto", {"alpha": 2.0},
                "FK-09", f"Agent FK ({col_name}) — pareto(alpha=2.0)",
            )

        # FK-08: FK to address/location table
        if _ADDRESS_FK_PATTERNS.search(parent_table):
            return (
                "zipf", {"alpha": 1.5},
                "FK-08", f"FK to address/location table ({parent_table}) — zipf(alpha=1.5)",
            )

        # FK-06: Bridge table FKs — first FK zipf, second FK pareto
        if table_role == TableRole.BRIDGE:
            fk_cols = [
                c.name for c in table_def.columns.values() if c.is_foreign_key
            ]
            if col_name == fk_cols[0] if fk_cols else False:
                return (
                    "zipf", {"alpha": 1.3},
                    "FK-06", "Bridge table first FK — zipf(alpha=1.3)",
                )
            return (
                "pareto", {"alpha": 1.16},
                "FK-06", "Bridge table second FK — pareto(alpha=1.16)",
            )

        # FK-07: Log/audit child — concentrated activity
        if table_role == TableRole.LOG:
            return (
                "pareto", {"alpha": 1.5},
                "FK-07", "Log/audit child — pareto(alpha=1.5)",
            )

        # FK-10: FK to a Lookup from a Transaction
        if table_role == TableRole.TRANSACTION and parent_role == TableRole.LOOKUP:
            return (
                "zipf", {"alpha": 1.2},
                "FK-10", f"Transaction FK to lookup ({parent_table}) — zipf(alpha=1.2)",
            )

        # FK-01: Entity -> Transaction child
        if parent_role == TableRole.ENTITY and table_role == TableRole.TRANSACTION:
            return (
                "pareto", {"alpha": 1.16, "max_per_parent": 50},
                "FK-01", f"Entity ({parent_table}) -> transaction child — pareto(alpha=1.16)",
            )

        # FK-02: Lookup/Reference parent -> child
        if parent_role in (TableRole.LOOKUP, TableRole.DIMENSION):
            return (
                "zipf", {"alpha": 1.3},
                "FK-02", f"Lookup/reference parent ({parent_table}) -> child — zipf(alpha=1.3)",
            )

        # FK-03: Transaction -> Transaction_Detail
        if (
            parent_role == TableRole.TRANSACTION
            and table_role == TableRole.TRANSACTION_DETAIL
        ):
            return (
                "uniform", {},
                "FK-03", f"Transaction ({parent_table}) -> detail — uniform",
            )

        # FK-01 broader: Entity parent with any child
        if parent_role == TableRole.ENTITY:
            return (
                "pareto", {"alpha": 1.16, "max_per_parent": 50},
                "FK-01", f"Entity ({parent_table}) -> child — pareto(alpha=1.16)",
            )

        # Default: keep pareto with reasonable defaults
        return (
            "pareto", {"alpha": 1.16},
            "FK-00", "Default FK distribution — pareto(alpha=1.16)",
        )
