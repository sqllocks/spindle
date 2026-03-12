"""Business rules engine — validate and fix constraint violations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.schema.parser import BusinessRuleDef, SpindleSchema

# Match "left_expr OP right_expr" where OP is >=, <=, >, <, ==
_COMPARISON_RE = re.compile(r"^(.+?)\s*(>=|<=|>|<|==)\s*(.+)$")


def _parse_comparison(rule_str: str) -> tuple[str, str, str]:
    """Parse 'A >= B' into (A, '>=', B). Returns ('','','') on failure."""
    m = _COMPARISON_RE.match(rule_str.strip())
    if m:
        return m.group(1).strip(), m.group(2), m.group(3).strip()
    return "", "", ""


@dataclass
class RuleViolation:
    rule_name: str
    table: str
    violation_count: int
    total_rows: int

    @property
    def violation_rate(self) -> float:
        return self.violation_count / self.total_rows if self.total_rows > 0 else 0

    def __repr__(self) -> str:
        return (
            f"RuleViolation('{self.rule_name}' on {self.table}: "
            f"{self.violation_count}/{self.total_rows} = {self.violation_rate:.1%})"
        )


class BusinessRulesEngine:
    """Validate and optionally fix business rule violations in generated data."""

    def validate(
        self,
        tables: dict[str, pd.DataFrame],
        schema: SpindleSchema,
    ) -> list[RuleViolation]:
        """Check all business rules and return violations."""
        violations = []

        for rule in schema.business_rules:
            if rule.type == "cross_column":
                v = self._check_cross_column(rule, tables)
                if v:
                    violations.append(v)
            elif rule.type == "cross_table":
                v = self._check_cross_table(rule, tables, schema)
                if v:
                    violations.append(v)
            elif rule.type == "constraint":
                v = self._check_constraint(rule, tables)
                if v:
                    violations.append(v)

        return violations

    def fix_violations(
        self,
        tables: dict[str, pd.DataFrame],
        schema: SpindleSchema,
        rng: np.random.Generator,
    ) -> list[RuleViolation]:
        """Attempt to fix business rule violations. Returns remaining violations."""
        for rule in schema.business_rules:
            if rule.type == "cross_table":
                self._fix_cross_table(rule, tables, schema, rng)
            elif rule.type == "cross_column":
                self._fix_cross_column(rule, tables, rng)

        # Re-validate
        return self.validate(tables, schema)

    def _check_cross_column(
        self,
        rule: BusinessRuleDef,
        tables: dict[str, pd.DataFrame],
    ) -> RuleViolation | None:
        if not rule.table or rule.table not in tables:
            return None

        df = tables[rule.table]
        return self._evaluate_rule_expression(rule, df)

    def _check_cross_table(
        self,
        rule: BusinessRuleDef,
        tables: dict[str, pd.DataFrame],
        schema: SpindleSchema,
    ) -> RuleViolation | None:
        rule_str = rule.rule
        via = rule.via

        if not via:
            return None

        left, op, right = _parse_comparison(rule_str)
        if not left or not op or not right:
            return None

        if "." not in left or "." not in right:
            return None

        left_table, left_col = left.split(".", 1)
        right_table, right_col = right.split(".", 1)

        if left_table not in tables or right_table not in tables:
            return None

        left_df = tables[left_table]
        right_df = tables[right_table]

        if left_col not in left_df.columns or right_col not in right_df.columns:
            return None
        if via not in left_df.columns or via not in right_df.columns:
            return None

        # Join on the via column
        merged = left_df[[via, left_col]].merge(
            right_df[[via, right_col]], on=via, how="left"
        )

        # Evaluate comparison
        violation_count = 0
        total = len(merged)

        if op in (">=",):
            violation_count = (merged[left_col] < merged[right_col]).sum()
        elif op in ("<=",):
            violation_count = (merged[left_col] > merged[right_col]).sum()
        elif op == ">":
            violation_count = (merged[left_col] <= merged[right_col]).sum()
        elif op == "<":
            violation_count = (merged[left_col] >= merged[right_col]).sum()

        if violation_count > 0:
            return RuleViolation(
                rule_name=rule.name,
                table=left_table,
                violation_count=int(violation_count),
                total_rows=total,
            )
        return None

    def _check_constraint(
        self,
        rule: BusinessRuleDef,
        tables: dict[str, pd.DataFrame],
    ) -> RuleViolation | None:
        if not rule.table or rule.table not in tables:
            return None
        df = tables[rule.table]
        return self._evaluate_rule_expression(rule, df)

    def _evaluate_rule_expression(
        self,
        rule: BusinessRuleDef,
        df: pd.DataFrame,
    ) -> RuleViolation | None:
        """Simple rule expression evaluator for single-table rules."""
        left, op, right = _parse_comparison(rule.rule)
        if not left or not op or not right:
            return None

        if left not in df.columns:
            return None

        # Right side can be a column name or a numeric literal
        if right in df.columns:
            left_vals = df[left].astype(float)
            right_vals = df[right].astype(float)
            if op == ">":
                violations = (left_vals <= right_vals).sum()
            elif op == ">=":
                violations = (left_vals < right_vals).sum()
            elif op == "<":
                violations = (left_vals >= right_vals).sum()
            elif op == "<=":
                violations = (left_vals > right_vals).sum()
            elif op == "==":
                violations = (left_vals != right_vals).sum()
            else:
                return None
        else:
            try:
                val_num = float(right)
            except ValueError:
                return None
            if op == ">":
                violations = (df[left] <= val_num).sum()
            elif op == ">=":
                violations = (df[left] < val_num).sum()
            elif op == "<":
                violations = (df[left] >= val_num).sum()
            elif op == "<=":
                violations = (df[left] > val_num).sum()
            elif op == "==":
                violations = (df[left] != val_num).sum()
            else:
                return None

        if violations > 0:
            return RuleViolation(
                rule_name=rule.name,
                table=rule.table or "",
                violation_count=int(violations),
                total_rows=len(df),
            )
        return None

    def _fix_cross_column(
        self,
        rule: BusinessRuleDef,
        tables: dict[str, pd.DataFrame],
        rng: np.random.Generator,
    ) -> None:
        """Fix cross-column violations (e.g., cost < unit_price)."""
        if not rule.table or rule.table not in tables:
            return

        df = tables[rule.table]
        left, op, right = _parse_comparison(rule.rule)
        if not left or not right:
            return
        if left not in df.columns or right not in df.columns:
            return

        left_vals = df[left].astype(float)
        right_vals = df[right].astype(float)

        if op == "<":
            mask = left_vals >= right_vals
            if mask.any():
                factors = rng.uniform(0.3, 0.95, size=mask.sum())
                df.loc[mask, left] = (right_vals[mask] * factors).round(2)
        elif op == ">":
            mask = left_vals <= right_vals
            if mask.any():
                factors = rng.uniform(1.05, 2.0, size=mask.sum())
                df.loc[mask, left] = (right_vals[mask] * factors).round(2)

    def _fix_cross_table(
        self,
        rule: BusinessRuleDef,
        tables: dict[str, pd.DataFrame],
        schema: SpindleSchema,
        rng: np.random.Generator,
    ) -> None:
        """Fix cross-table violations by adjusting child values."""
        via = rule.via
        if not via:
            return

        left, op, right = _parse_comparison(rule.rule)
        if not left or not right or "." not in left or "." not in right:
            return

        left_table, left_col = left.split(".", 1)
        right_table, right_col = right.split(".", 1)

        if left_table not in tables or right_table not in tables:
            return

        left_df = tables[left_table]
        right_df = tables[right_table]

        if via not in left_df.columns or via not in right_df.columns:
            return

        # Build lookup from right table
        right_lookup = right_df.set_index(via)[right_col]
        right_vals = left_df[via].map(right_lookup)

        # Detect timestamp type for proper offset
        is_temporal = pd.api.types.is_datetime64_any_dtype(right_vals)
        offset = pd.Timedelta(days=1) if is_temporal else 1

        # Fix violations based on operator
        if op == ">=":
            mask = left_df[left_col] < right_vals
            if mask.any():
                left_df.loc[mask, left_col] = right_vals[mask] + offset

        elif op == ">":
            mask = left_df[left_col] <= right_vals
            if mask.any():
                left_df.loc[mask, left_col] = right_vals[mask] + offset

        elif op == "<=":
            mask = left_df[left_col] > right_vals
            if mask.any():
                # Scale down to a fraction of the right value
                factors = rng.uniform(0.3, 1.0, size=mask.sum())
                left_df.loc[mask, left_col] = (right_vals[mask] * factors).round(2)
