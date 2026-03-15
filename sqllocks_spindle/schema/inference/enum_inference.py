"""Upgrade STATUS and CATEGORICAL columns from placeholder enums to realistic weighted values.

For STATUS columns the weight profile depends on the table's role (transaction,
entity, or log/audit).  For CATEGORICAL columns the profile is inferred from
the column name (priority, severity, payment_method, gender, country, etc.).
"""

from __future__ import annotations

import re

from sqllocks_spindle.schema.inference import (
    ColumnSemantic,
    InferenceContext,
    TableRole,
)

# ---------------------------------------------------------------------------
# Status profiles keyed by table role
# ---------------------------------------------------------------------------

_TRANSACTION_STATUS: dict[str, float] = {
    "completed": 0.72,
    "pending": 0.10,
    "processing": 0.05,
    "shipped": 0.05,
    "cancelled": 0.05,
    "refunded": 0.03,
}

_ENTITY_STATUS: dict[str, float] = {
    "active": 0.82,
    "inactive": 0.10,
    "suspended": 0.05,
    "closed": 0.03,
}

_LOG_STATUS: dict[str, float] = {
    "success": 0.85,
    "warning": 0.10,
    "error": 0.05,
}

# Mapping from TableRole to status weights
_STATUS_BY_ROLE: dict[TableRole, dict[str, float]] = {
    TableRole.TRANSACTION: _TRANSACTION_STATUS,
    TableRole.TRANSACTION_DETAIL: _TRANSACTION_STATUS,
    TableRole.FACT: _TRANSACTION_STATUS,
    TableRole.LOG: _LOG_STATUS,
    TableRole.ENTITY: _ENTITY_STATUS,
    TableRole.DIMENSION: _ENTITY_STATUS,
    TableRole.HIERARCHY: _ENTITY_STATUS,
    TableRole.LOOKUP: _ENTITY_STATUS,
    TableRole.BRIDGE: _ENTITY_STATUS,
    TableRole.UNKNOWN: _ENTITY_STATUS,
}

# ---------------------------------------------------------------------------
# Categorical profiles keyed by column-name pattern
# ---------------------------------------------------------------------------

_CATEGORICAL_PROFILES: list[tuple[re.Pattern, dict[str, float]]] = [
    (
        re.compile(r"priority", re.IGNORECASE),
        {"low": 0.30, "medium": 0.45, "high": 0.20, "critical": 0.05},
    ),
    (
        re.compile(r"severity", re.IGNORECASE),
        {"info": 0.40, "warning": 0.30, "error": 0.20, "critical": 0.10},
    ),
    (
        re.compile(r"payment.?method|pay.?type|payment.?type", re.IGNORECASE),
        {
            "credit_card": 0.45,
            "debit_card": 0.25,
            "cash": 0.15,
            "bank_transfer": 0.10,
            "other": 0.05,
        },
    ),
    (
        re.compile(r"^(gender|sex)$", re.IGNORECASE),
        {"M": 0.49, "F": 0.51},
    ),
    (
        re.compile(r"^(country|country_code|country_name)$", re.IGNORECASE),
        {
            "US": 0.60,
            "UK": 0.10,
            "CA": 0.08,
            "DE": 0.07,
            "FR": 0.05,
            "AU": 0.05,
            "Other": 0.05,
        },
    ),
    (
        re.compile(r"(level|tier)$", re.IGNORECASE),
        {"basic": 0.55, "silver": 0.25, "gold": 0.13, "platinum": 0.07},
    ),
]

# Fallback for generic "type" columns — zipf-like weights
_GENERIC_TYPE_PROFILE: dict[str, float] = {
    "type_1": 0.35,
    "type_2": 0.25,
    "type_3": 0.20,
    "type_4": 0.12,
    "type_5": 0.08,
}


def _is_placeholder_enum(gen: dict) -> bool:
    """Return True if the generator looks like a placeholder that should be upgraded.

    Placeholder patterns include:
    - ``faker`` strategy (DDL parser fallback for string columns)
    - ``distribution`` strategy (DDL parser fallback for numeric columns)
    - Two-value 50/50 weighted_enum splits
    - Generic ``type_a / type_b`` template assigned by the DDL parser
    """
    strategy = gen.get("strategy")

    # Faker/distribution on a STATUS/CATEGORICAL column is always a placeholder
    if strategy in ("faker", "distribution"):
        return True

    if strategy != "weighted_enum":
        return False

    values = gen.get("values", {})
    if not values:
        return True

    # Two-value 50/50 split is almost certainly a placeholder
    if len(values) == 2:
        weights = list(values.values())
        if abs(weights[0] - weights[1]) < 0.01:
            return True

    # Generic names like type_a, type_b or Active/Inactive with 50/50
    keys_lower = {k.lower() for k in values}
    if keys_lower == {"type_a", "type_b"}:
        return True
    if keys_lower == {"active", "inactive"} and len(values) == 2:
        weights = list(values.values())
        if abs(weights[0] - weights[1]) < 0.01:
            return True

    return False


class EnumInferrer:
    """Upgrade STATUS and CATEGORICAL column generators to realistic weighted enums.

    Only modifies columns classified as STATUS or CATEGORICAL whose current
    generator has placeholder values. Columns with intentionally configured
    enums are left untouched.
    """

    def analyze(self, ctx: InferenceContext) -> None:
        for table_name, table_def in ctx.schema.tables.items():
            semantics = ctx.column_semantics.get(table_name, {})
            table_role = ctx.table_roles.get(table_name, TableRole.UNKNOWN)

            for col_name, col_def in table_def.columns.items():
                semantic = semantics.get(col_name)
                if semantic is None:
                    continue

                gen = col_def.generator
                if not _is_placeholder_enum(gen):
                    continue

                new_gen = self._infer_enum(semantic, table_role, col_name)
                if new_gen is not None:
                    col_def.generator = new_gen
                    ctx.annotate(
                        table=table_name,
                        column=col_name,
                        rule_id=f"EN-{semantic.name}",
                        description=(
                            f"Upgraded to realistic weighted_enum "
                            f"({len(new_gen['values'])} values) "
                            f"based on {semantic.name} semantic / "
                            f"{table_role.name} role"
                        ),
                        confidence=0.7,
                    )

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _infer_enum(
        self,
        semantic: ColumnSemantic,
        table_role: TableRole,
        col_name: str,
    ) -> dict | None:
        if semantic == ColumnSemantic.STATUS:
            return self._status_enum(table_role)
        if semantic == ColumnSemantic.CATEGORICAL:
            return self._categorical_enum(col_name)
        return None

    # ------------------------------------------------------------------
    # Status — role-aware
    # ------------------------------------------------------------------

    @staticmethod
    def _status_enum(table_role: TableRole) -> dict:
        values = _STATUS_BY_ROLE.get(table_role, _ENTITY_STATUS)
        return {"strategy": "weighted_enum", "values": dict(values)}

    # ------------------------------------------------------------------
    # Categorical — name-aware
    # ------------------------------------------------------------------

    @staticmethod
    def _categorical_enum(col_name: str) -> dict:
        for pattern, profile in _CATEGORICAL_PROFILES:
            if pattern.search(col_name):
                return {"strategy": "weighted_enum", "values": dict(profile)}

        # Fallback: generic *type* column gets zipf-like distribution
        if re.search(r"type", col_name, re.IGNORECASE):
            return {"strategy": "weighted_enum", "values": dict(_GENERIC_TYPE_PROFILE)}

        # Ultimate fallback — still use generic type profile for any
        # categorical column that wasn't matched by a specific pattern
        return {"strategy": "weighted_enum", "values": dict(_GENERIC_TYPE_PROFILE)}
