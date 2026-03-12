"""Business rules engine — validate and fix constraint violations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from sqllocks_spindle.schema.parser import BusinessRuleDef, SpindleSchema


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
        # Parse rule like "order.order_date >= customer.signup_date"
        # This is a simplified parser for common patterns
        rule_str = rule.rule
        via = rule.via

        if not via:
            return None

        # Extract table.column patterns — tokenize operators safely
        # (must replace >= and <= BEFORE > and < to avoid double-replacement)
        import re
        parts = re.split(r'\s*(>=|<=|>|<)\s*', rule_str.strip())
        if len(parts) < 3:
            return None

        left = parts[0]  # e.g., "order.order_date"
        op = parts[1]
        right = parts[2]  # e.g., "customer.signup_date"

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

        if op in (">=", "=>"):
            violation_count = (merged[left_col] < merged[right_col]).sum()
        elif op in ("<=", "=<"):
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
        """Simple rule expression evaluator for single-table rules.

        Handles:
            "column op scalar"  e.g. "line_total > 0"
            "col1 op col2"      e.g. "cost < unit_price"
        """
        rule_str = rule.rule

        for op in [">=", "<=", ">", "<", "=="]:
            if op in rule_str:
                parts = rule_str.split(op, 1)
                if len(parts) != 2:
                    break
                left = parts[0].strip()
                right = parts[1].strip()

                if left not in df.columns:
                    break

                try:
                    # Column vs scalar
                    val_num = float(right)
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
                except ValueError:
                    # Column vs column
                    if right not in df.columns:
                        break
                    try:
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
                    except Exception:
                        break

                if violations > 0:
                    return RuleViolation(
                        rule_name=rule.name,
                        table=rule.table or "",
                        violation_count=int(violations),
                        total_rows=len(df),
                    )
                break
        return None

    def _fix_cross_table(
        self,
        rule: BusinessRuleDef,
        tables: dict[str, pd.DataFrame],
        schema: SpindleSchema,
        rng: np.random.Generator,
    ) -> None:
        """Fix cross-table violations by adjusting child values."""
        rule_str = rule.rule
        via = rule.via
        if not via:
            return

        import re
        parts = re.split(r'\s*(>=|<=|>|<)\s*', rule_str.strip())
        if len(parts) < 3 or "." not in parts[0] or "." not in parts[2]:
            return

        left_table, left_col = parts[0].strip().split(".", 1)
        op = parts[1].strip()
        right_table, right_col = parts[2].strip().split(".", 1)

        if left_table not in tables or right_table not in tables:
            return

        left_df = tables[left_table]
        right_df = tables[right_table]

        if via not in left_df.columns or via not in right_df.columns:
            return

        # Build lookup from right table
        right_lookup = right_df.set_index(via)[right_col]
        right_vals = left_df[via].map(right_lookup)

        # Fix violations based on operator
        if op in (">=", "=>"):
            mask = left_df[left_col] < right_vals
            if mask.any():
                # Set left to right + small offset
                offset = pd.Timedelta(days=1) if hasattr(right_vals.iloc[0], 'day') else 1
                left_df.loc[mask, left_col] = right_vals[mask] + offset

        elif op == ">":
            mask = left_df[left_col] <= right_vals
            if mask.any():
                offset = pd.Timedelta(days=1) if hasattr(right_vals.iloc[0], 'day') else 1
                left_df.loc[mask, left_col] = right_vals[mask] + offset

    def _fix_cross_column(
        self,
        rule: BusinessRuleDef,
        tables: dict[str, pd.DataFrame],
        rng: np.random.Generator,
    ) -> None:
        """Fix single-table cross-column violations.

        Handles "col1 < col2" style rules by setting the left column to a
        fraction of the right column where the constraint is violated.
        """
        if not rule.table or rule.table not in tables:
            return

        df = tables[rule.table]
        rule_str = rule.rule

        for op in [">=", "<=", ">", "<"]:
            if op in rule_str:
                parts = rule_str.split(op, 1)
                left = parts[0].strip()
                right = parts[1].strip()

                if left not in df.columns or right not in df.columns:
                    return

                try:
                    left_vals = df[left].astype(float)
                    right_vals = df[right].astype(float)
                except (TypeError, ValueError):
                    return

                if op == "<":
                    # left must be < right; fix violating rows
                    mask = left_vals >= right_vals
                    if mask.any():
                        # Set left = right * random(0.30, 0.70)
                        factors = rng.uniform(0.30, 0.70, size=mask.sum())
                        df.loc[mask, left] = (right_vals[mask] * factors).round(2)
                elif op == ">":
                    mask = left_vals <= right_vals
                    if mask.any():
                        factors = rng.uniform(1.3, 2.0, size=mask.sum())
                        df.loc[mask, left] = (right_vals[mask] * factors).round(2)
                break
