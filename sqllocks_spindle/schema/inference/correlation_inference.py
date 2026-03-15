"""Detect cross-column correlations and upgrade strategies to formula/correlated/computed.

Scans columns within each table for known pairs and triples (cost+price,
tax+subtotal, quantity*unit_price=total, etc.) and replaces basic generation
strategies with correlated, formula, or computed strategies so the generated
data is internally consistent.
"""

from __future__ import annotations

from sqllocks_spindle.schema.inference import InferenceContext, TableRole, ColumnSemantic


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASIC_STRATEGIES = frozenset({
    "distribution", "uniform", "normal", "log_normal", "geometric",
    "bounded_normal", "random", "sequence", "faker", None,
})


def _is_basic(col_def) -> bool:
    """Return True if the column still has a basic (non-correlated) strategy."""
    strategy = col_def.generator.get("strategy")
    return strategy in _BASIC_STRATEGIES


def _find_col(columns: dict, *patterns: str) -> str | None:
    """Return the first column name that contains any of the given patterns (case-insensitive)."""
    for col_name in columns:
        lower = col_name.lower()
        for pat in patterns:
            if pat in lower:
                return col_name
    return None


def _find_cols(columns: dict, *patterns: str) -> list[str]:
    """Return all column names containing any of the given patterns."""
    matches = []
    for col_name in columns:
        lower = col_name.lower()
        for pat in patterns:
            if pat in lower:
                matches.append(col_name)
                break
    return matches


# ---------------------------------------------------------------------------
# CorrelationInferrer
# ---------------------------------------------------------------------------

class CorrelationInferrer:
    """Detect cross-column correlations and upgrade generation strategies.

    Runs after column classification so that semantic labels are available.
    Only modifies columns that still carry basic (non-correlated) strategies,
    preserving any earlier inference work (e.g. TemporalPatternInferrer).
    """

    def analyze(self, ctx: InferenceContext) -> None:
        for table_name, table_def in ctx.schema.tables.items():
            cols = table_def.columns
            semantics = ctx.column_semantics.get(table_name, {})

            self._cr01_cost_price(ctx, table_name, cols, semantics)
            self._cr02_tax_subtotal(ctx, table_name, cols, semantics)
            self._cr03_discount_price(ctx, table_name, cols, semantics)
            self._cr04_qty_price_total(ctx, table_name, cols, semantics)
            self._cr05_net_gross_tax(ctx, table_name, cols, semantics)
            self._cr09_margin_price_cost(ctx, table_name, cols, semantics)

        # CR-08 requires cross-table analysis — run after per-table pass
        self._cr08_parent_sum_children(ctx)

    # -----------------------------------------------------------------------
    # CR-01: cost correlated to price  (factor 0.30 – 0.70)
    # -----------------------------------------------------------------------

    def _cr01_cost_price(
        self, ctx: InferenceContext, table: str, cols: dict, semantics: dict,
    ) -> None:
        cost_col = _find_col(cols, "cost")
        price_col = _find_col(cols, "price")
        if not cost_col or not price_col or cost_col == price_col:
            return
        if semantics.get(cost_col) != ColumnSemantic.MONETARY:
            return
        if semantics.get(price_col) != ColumnSemantic.MONETARY:
            return
        if not _is_basic(cols[cost_col]):
            return

        cols[cost_col].generator = {
            "strategy": "correlated",
            "source_column": price_col,
            "rule": "multiply",
            "params": {"factor_min": 0.30, "factor_max": 0.70},
        }
        ctx.annotate(
            table=table, column=cost_col, rule_id="CR-01",
            description=f"Cost ({cost_col}) correlated to price ({price_col}) via multiply(0.30–0.70)",
        )

    # -----------------------------------------------------------------------
    # CR-02: tax correlated to subtotal/amount  (factor 0.05 – 0.15)
    # -----------------------------------------------------------------------

    def _cr02_tax_subtotal(
        self, ctx: InferenceContext, table: str, cols: dict, semantics: dict,
    ) -> None:
        tax_col = _find_col(cols, "tax")
        if not tax_col or semantics.get(tax_col) != ColumnSemantic.MONETARY:
            return
        base_col = _find_col(cols, "subtotal", "amount")
        if not base_col or base_col == tax_col:
            return
        if not _is_basic(cols[tax_col]):
            return

        cols[tax_col].generator = {
            "strategy": "correlated",
            "source_column": base_col,
            "rule": "multiply",
            "params": {"factor_min": 0.05, "factor_max": 0.15},
        }
        ctx.annotate(
            table=table, column=tax_col, rule_id="CR-02",
            description=f"Tax ({tax_col}) correlated to ({base_col}) via multiply(0.05–0.15)",
        )

    # -----------------------------------------------------------------------
    # CR-03: discount correlated to price/total  (factor 0.05 – 0.25)
    # -----------------------------------------------------------------------

    def _cr03_discount_price(
        self, ctx: InferenceContext, table: str, cols: dict, semantics: dict,
    ) -> None:
        discount_col = _find_col(cols, "discount")
        if not discount_col or semantics.get(discount_col) != ColumnSemantic.MONETARY:
            return
        base_col = _find_col(cols, "price", "total")
        if not base_col or base_col == discount_col:
            return
        if not _is_basic(cols[discount_col]):
            return

        cols[discount_col].generator = {
            "strategy": "correlated",
            "source_column": base_col,
            "rule": "multiply",
            "params": {"factor_min": 0.05, "factor_max": 0.25},
        }
        ctx.annotate(
            table=table, column=discount_col, rule_id="CR-03",
            description=f"Discount ({discount_col}) correlated to ({base_col}) via multiply(0.05–0.25)",
        )

    # -----------------------------------------------------------------------
    # CR-04: total = quantity * unit_price  (formula)
    # -----------------------------------------------------------------------

    def _cr04_qty_price_total(
        self, ctx: InferenceContext, table: str, cols: dict, semantics: dict,
    ) -> None:
        qty_col = _find_col(cols, "quantity", "qty")
        unit_price_col = _find_col(cols, "unit_price")
        if not qty_col or not unit_price_col:
            return
        total_col = _find_col(cols, "total", "line_total")
        if not total_col or total_col == qty_col or total_col == unit_price_col:
            return
        if not _is_basic(cols[total_col]):
            return

        cols[total_col].generator = {
            "strategy": "formula",
            "expression": f"{qty_col} * {unit_price_col}",
        }
        ctx.annotate(
            table=table, column=total_col, rule_id="CR-04",
            description=f"Total ({total_col}) = {qty_col} * {unit_price_col}",
        )

    # -----------------------------------------------------------------------
    # CR-05: net = gross - tax  (formula)
    # -----------------------------------------------------------------------

    def _cr05_net_gross_tax(
        self, ctx: InferenceContext, table: str, cols: dict, semantics: dict,
    ) -> None:
        net_col = _find_col(cols, "net")
        gross_col = _find_col(cols, "gross")
        tax_col = _find_col(cols, "tax")
        if not net_col or not gross_col or not tax_col:
            return
        if len({net_col, gross_col, tax_col}) < 3:
            return
        if not _is_basic(cols[net_col]):
            return

        cols[net_col].generator = {
            "strategy": "formula",
            "expression": f"{gross_col} - {tax_col}",
        }
        ctx.annotate(
            table=table, column=net_col, rule_id="CR-05",
            description=f"Net ({net_col}) = {gross_col} - {tax_col}",
        )

    # -----------------------------------------------------------------------
    # CR-06 / CR-07: temporal pairs — skip if already derived
    # (Handled by TemporalPatternInferrer; nothing to do here.)
    # -----------------------------------------------------------------------

    # -----------------------------------------------------------------------
    # CR-08: parent total = SUM(child amount)  (computed)
    # -----------------------------------------------------------------------

    def _cr08_parent_sum_children(self, ctx: InferenceContext) -> None:
        for rel in ctx.schema.relationships:
            parent_table = ctx.schema.tables.get(rel.parent)
            child_table = ctx.schema.tables.get(rel.child)
            if not parent_table or not child_table:
                continue

            parent_total = _find_col(parent_table.columns, "total")
            if not parent_total:
                continue
            if not _is_basic(parent_table.columns[parent_total]):
                continue

            child_amount = _find_col(child_table.columns, "amount", "total")
            if not child_amount:
                continue

            parent_table.columns[parent_total].generator = {
                "strategy": "computed",
                "rule": "sum_children",
                "child_table": rel.child,
                "child_column": child_amount,
            }
            ctx.annotate(
                table=rel.parent, column=parent_total, rule_id="CR-08",
                description=(
                    f"Parent total ({parent_total}) computed as SUM({rel.child}.{child_amount})"
                ),
            )

    # -----------------------------------------------------------------------
    # CR-09: margin = price - cost  (formula)
    # -----------------------------------------------------------------------

    def _cr09_margin_price_cost(
        self, ctx: InferenceContext, table: str, cols: dict, semantics: dict,
    ) -> None:
        margin_col = _find_col(cols, "margin")
        price_col = _find_col(cols, "price")
        cost_col = _find_col(cols, "cost")
        if not margin_col or not price_col or not cost_col:
            return
        if len({margin_col, price_col, cost_col}) < 3:
            return
        if not _is_basic(cols[margin_col]):
            return

        cols[margin_col].generator = {
            "strategy": "formula",
            "expression": f"{price_col} - {cost_col}",
        }
        ctx.annotate(
            table=table, column=margin_col, rule_id="CR-09",
            description=f"Margin ({margin_col}) = {price_col} - {cost_col}",
        )
