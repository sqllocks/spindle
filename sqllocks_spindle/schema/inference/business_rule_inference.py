"""Infer business rules from schema structure and column semantics.

Scans column semantics and cross-table relationships to generate
BusinessRuleDef entries (date ordering, positivity constraints,
range bounds, etc.) and appends them to ``ctx.schema.business_rules``.
"""

from __future__ import annotations

from sqllocks_spindle.schema.inference import InferenceContext, TableRole, ColumnSemantic
from sqllocks_spindle.schema.parser import BusinessRuleDef


class BusinessRuleInferrer:
    """Infer business rules and append them to the schema.

    Runs late in the pipeline so that table roles, column semantics,
    and relationship graphs are already populated.
    """

    def analyze(self, ctx: InferenceContext) -> None:
        # Track existing rule names to avoid duplicates
        existing: set[str] = {r.name for r in ctx.schema.business_rules}

        for table_name, table_def in ctx.schema.tables.items():
            semantics = ctx.column_semantics.get(table_name, {})

            self._br01_date_ordering(ctx, table_name, semantics, existing)
            self._br02_audit_date_ordering(ctx, table_name, table_def, semantics, existing)
            self._br03_cost_lt_price(ctx, table_name, table_def, semantics, existing)
            self._br05_monetary_positive(ctx, table_name, semantics, existing)
            self._br06_quantity_positive(ctx, table_name, semantics, existing)
            self._br07_percentage_range(ctx, table_name, semantics, existing)
            self._br08_rating_range(ctx, table_name, table_def, semantics, existing)

        # Cross-table rules require relationship walking
        self._br04_cross_table_dates(ctx, existing)

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    def _add_rule(
        self, ctx: InferenceContext, existing: set[str], rule: BusinessRuleDef,
        rule_id: str, description: str, column: str | None = None,
    ) -> None:
        """Append a rule if its name is not already present."""
        if rule.name in existing:
            return
        existing.add(rule.name)
        ctx.schema.business_rules.append(rule)
        ctx.annotate(
            table=rule.table or "", column=column, rule_id=rule_id,
            description=description,
        )

    def _cols_with_semantic(
        self, semantics: dict[str, ColumnSemantic], *targets: ColumnSemantic,
    ) -> list[str]:
        """Return column names matching any of the given semantics."""
        return [c for c, s in semantics.items() if s in targets]

    # -------------------------------------------------------------------
    # BR-01: end_date >= start_date
    # -------------------------------------------------------------------

    def _br01_date_ordering(
        self, ctx: InferenceContext, table: str,
        semantics: dict[str, ColumnSemantic], existing: set[str],
    ) -> None:
        start_cols = self._cols_with_semantic(semantics, ColumnSemantic.TEMPORAL_START)
        end_cols = self._cols_with_semantic(semantics, ColumnSemantic.TEMPORAL_END)
        if not start_cols or not end_cols:
            return

        start_col = start_cols[0]
        end_col = end_cols[0]
        rule_name = f"{table}_date_order"

        self._add_rule(
            ctx, existing,
            BusinessRuleDef(
                name=rule_name,
                type="cross_column",
                rule=f"{end_col} >= {start_col}",
                table=table,
            ),
            rule_id="BR-01",
            description=f"{end_col} >= {start_col}",
            column=end_col,
        )

    # -------------------------------------------------------------------
    # BR-02: modified_at/updated_at >= created_at
    # -------------------------------------------------------------------

    def _br02_audit_date_ordering(
        self, ctx: InferenceContext, table: str, table_def,
        semantics: dict[str, ColumnSemantic], existing: set[str],
    ) -> None:
        audit_cols = self._cols_with_semantic(semantics, ColumnSemantic.TEMPORAL_AUDIT)
        if len(audit_cols) < 2:
            return

        created_col: str | None = None
        modified_col: str | None = None
        for col_name in audit_cols:
            lower = col_name.lower()
            if "created" in lower:
                created_col = col_name
            elif "modified" in lower or "updated" in lower:
                modified_col = col_name

        if not created_col or not modified_col:
            return

        rule_name = f"{table}_audit_date_order"
        self._add_rule(
            ctx, existing,
            BusinessRuleDef(
                name=rule_name,
                type="cross_column",
                rule=f"{modified_col} >= {created_col}",
                table=table,
            ),
            rule_id="BR-02",
            description=f"{modified_col} >= {created_col}",
            column=modified_col,
        )

    # -------------------------------------------------------------------
    # BR-03: cost <= price
    # -------------------------------------------------------------------

    def _br03_cost_lt_price(
        self, ctx: InferenceContext, table: str, table_def,
        semantics: dict[str, ColumnSemantic], existing: set[str],
    ) -> None:
        monetary_cols = self._cols_with_semantic(semantics, ColumnSemantic.MONETARY)
        cost_col: str | None = None
        price_col: str | None = None
        for col_name in monetary_cols:
            lower = col_name.lower()
            if "cost" in lower and cost_col is None:
                cost_col = col_name
            if "price" in lower and price_col is None:
                price_col = col_name

        if not cost_col or not price_col or cost_col == price_col:
            return

        rule_name = f"{table}_cost_lt_price"
        self._add_rule(
            ctx, existing,
            BusinessRuleDef(
                name=rule_name,
                type="cross_column",
                rule=f"{cost_col} <= {price_col}",
                table=table,
            ),
            rule_id="BR-03",
            description=f"{cost_col} <= {price_col}",
            column=cost_col,
        )

    # -------------------------------------------------------------------
    # BR-04: child transaction date >= parent date (cross-table)
    # -------------------------------------------------------------------

    def _br04_cross_table_dates(
        self, ctx: InferenceContext, existing: set[str],
    ) -> None:
        for rel in ctx.schema.relationships:
            child_semantics = ctx.column_semantics.get(rel.child, {})
            parent_semantics = ctx.column_semantics.get(rel.parent, {})

            child_date_cols = self._cols_with_semantic(
                child_semantics, ColumnSemantic.TEMPORAL_TRANSACTION,
            )
            parent_date_cols = self._cols_with_semantic(
                parent_semantics, ColumnSemantic.TEMPORAL_TRANSACTION,
            )
            if not child_date_cols or not parent_date_cols:
                continue

            child_date = child_date_cols[0]
            parent_date = parent_date_cols[0]

            # Determine the FK column linking child to parent
            fk_column: str | None = None
            if rel.child_columns:
                fk_column = rel.child_columns[0]

            rule_name = f"{rel.child}_after_{rel.parent}"
            self._add_rule(
                ctx, existing,
                BusinessRuleDef(
                    name=rule_name,
                    type="cross_table",
                    rule=f"{child_date} >= {parent_date}",
                    table=rel.child,
                    via=fk_column,
                ),
                rule_id="BR-04",
                description=f"{rel.child}.{child_date} >= {rel.parent}.{parent_date} (via {fk_column})",
                column=child_date,
            )

    # -------------------------------------------------------------------
    # BR-05: MONETARY columns >= 0
    # -------------------------------------------------------------------

    def _br05_monetary_positive(
        self, ctx: InferenceContext, table: str,
        semantics: dict[str, ColumnSemantic], existing: set[str],
    ) -> None:
        for col_name in self._cols_with_semantic(semantics, ColumnSemantic.MONETARY):
            rule_name = f"{table}_{col_name}_positive"
            self._add_rule(
                ctx, existing,
                BusinessRuleDef(
                    name=rule_name,
                    type="constraint",
                    rule=f"{col_name} >= 0",
                    table=table,
                ),
                rule_id="BR-05",
                description=f"Monetary column {col_name} >= 0",
                column=col_name,
            )

    # -------------------------------------------------------------------
    # BR-06: QUANTITY columns >= 1
    # -------------------------------------------------------------------

    def _br06_quantity_positive(
        self, ctx: InferenceContext, table: str,
        semantics: dict[str, ColumnSemantic], existing: set[str],
    ) -> None:
        for col_name in self._cols_with_semantic(semantics, ColumnSemantic.QUANTITY):
            rule_name = f"{table}_{col_name}_positive"
            self._add_rule(
                ctx, existing,
                BusinessRuleDef(
                    name=rule_name,
                    type="constraint",
                    rule=f"{col_name} >= 1",
                    table=table,
                ),
                rule_id="BR-06",
                description=f"Quantity column {col_name} >= 1",
                column=col_name,
            )

    # -------------------------------------------------------------------
    # BR-07: PERCENTAGE columns BETWEEN 0 AND 100
    # -------------------------------------------------------------------

    def _br07_percentage_range(
        self, ctx: InferenceContext, table: str,
        semantics: dict[str, ColumnSemantic], existing: set[str],
    ) -> None:
        for col_name in self._cols_with_semantic(semantics, ColumnSemantic.PERCENTAGE):
            rule_name = f"{table}_{col_name}_range"
            self._add_rule(
                ctx, existing,
                BusinessRuleDef(
                    name=rule_name,
                    type="constraint",
                    rule=f"{col_name} BETWEEN 0 AND 100",
                    table=table,
                ),
                rule_id="BR-07",
                description=f"Percentage column {col_name} BETWEEN 0 AND 100",
                column=col_name,
            )

    # -------------------------------------------------------------------
    # BR-08: RATING columns BETWEEN 1 AND 5
    # -------------------------------------------------------------------

    def _br08_rating_range(
        self, ctx: InferenceContext, table: str, table_def,
        semantics: dict[str, ColumnSemantic], existing: set[str],
    ) -> None:
        for col_name in self._cols_with_semantic(semantics, ColumnSemantic.RATING):
            rule_name = f"{table}_{col_name}_range"
            self._add_rule(
                ctx, existing,
                BusinessRuleDef(
                    name=rule_name,
                    type="constraint",
                    rule=f"{col_name} BETWEEN 1 AND 5",
                    table=table,
                ),
                rule_id="BR-08",
                description=f"Rating column {col_name} BETWEEN 1 AND 5",
                column=col_name,
            )
