"""Classify tables by semantic role: ENTITY, TRANSACTION, DETAIL, LOOKUP, etc.

Table role is the keystone inference — FK distributions, temporal patterns,
cardinality ratios, and enum weights all depend on knowing whether a table
is a fact vs dimension vs entity vs line-item detail.
"""

from __future__ import annotations

import re

from sqllocks_spindle.schema.inference import InferenceContext, TableRole


# Patterns that suggest specific table roles
_LOG_PATTERNS = re.compile(
    r"(log|audit|history|event|tracking|changelog|activity)", re.IGNORECASE
)
_DIM_PREFIX = re.compile(r"^(dim_|d_)", re.IGNORECASE)
_FACT_PREFIX = re.compile(r"^(fact_|f_)", re.IGNORECASE)

# Column name patterns that suggest entity tables (people/orgs)
_ENTITY_COLUMN_PATTERNS = {
    "first_name", "last_name", "email", "phone", "date_of_birth",
    "birth_date", "ssn", "username", "login", "signup_date",
    "company_name", "org_name", "contact_name",
}

# Column name patterns that suggest transaction tables
_TRANSACTION_COLUMN_PATTERNS = {
    "order_date", "transaction_date", "invoice_date", "purchase_date",
    "created_date", "total_amount", "subtotal", "grand_total",
    "order_total", "payment_date", "bill_date", "claim_date",
}


class TableClassifier:
    """Assign a TableRole to each table based on schema structure."""

    def analyze(self, ctx: InferenceContext) -> None:
        for table_name, table_def in ctx.schema.tables.items():
            role = self._classify(table_name, table_def, ctx)
            ctx.table_roles[table_name] = role
            ctx.annotate(
                table=table_name, column=None,
                rule_id=f"TC-{role.name}",
                description=f"Classified as {role.name}",
                confidence=0.8,
            )

    def _classify(self, name: str, table_def, ctx: InferenceContext) -> TableRole:
        col_names = {c.lower() for c in table_def.column_names}
        children = ctx.children_of.get(name, [])
        parents = ctx.parents_of.get(name, [])
        fk_count = len(parents)
        child_count = len(children)
        non_pk_cols = len(table_def.columns) - len(table_def.primary_key)

        # TC-09: Explicit dim_ prefix
        if _DIM_PREFIX.match(name):
            return TableRole.DIMENSION

        # TC-10: Explicit fact_ prefix
        if _FACT_PREFIX.match(name):
            return TableRole.FACT

        # TC-03: Self-referencing FK (hierarchy)
        for col in table_def.columns.values():
            if col.is_foreign_key and col.fk_ref_table == name:
                return TableRole.HIERARCHY
            if col.generator.get("strategy") == "self_referencing":
                return TableRole.HIERARCHY

        # TC-06: Log/audit/history tables
        if _LOG_PATTERNS.search(name):
            return TableRole.LOG

        # TC-04: Bridge/junction table — exactly 2 FKs composing the PK
        if fk_count >= 2 and non_pk_cols <= 3:
            fk_cols = {c.name for c in table_def.columns.values() if c.is_foreign_key}
            pk_cols = set(table_def.primary_key)
            if fk_cols & pk_cols and len(fk_cols) >= 2:
                return TableRole.BRIDGE

        # TC-02: Lookup/reference — no FK parents, many children, small column count
        if fk_count == 0 and child_count >= 2 and non_pk_cols <= 5:
            return TableRole.LOOKUP

        # TC-05: Entity — has person/org columns, many children
        entity_hits = col_names & _ENTITY_COLUMN_PATTERNS
        if entity_hits and child_count >= 1:
            return TableRole.ENTITY

        # TC-08: Transaction detail — single FK to a transaction table, small column count
        if fk_count == 1 and non_pk_cols <= 8:
            parent_name = parents[0]
            parent_role = ctx.table_roles.get(parent_name, TableRole.UNKNOWN)
            if parent_role in (TableRole.TRANSACTION, TableRole.UNKNOWN):
                # Check if this looks like a line-item (has quantity/amount columns)
                detail_hints = col_names & {
                    "quantity", "qty", "unit_price", "line_total",
                    "amount", "line_amount", "line_number", "item_number",
                }
                if detail_hints:
                    return TableRole.TRANSACTION_DETAIL

        # TC-07/TC-01: Transaction — FKs to entities, has date + amount columns
        txn_hits = col_names & _TRANSACTION_COLUMN_PATTERNS
        has_amount = any(
            "amount" in c or "total" in c or "price" in c or "cost" in c
            for c in col_names
        )
        has_date = any(
            "date" in c or "_at" in c
            for c in col_names
        )
        if fk_count >= 1 and (txn_hits or (has_amount and has_date)):
            return TableRole.TRANSACTION

        # TC-02 fallback: No FK parents, referenced by others → likely a lookup
        if fk_count == 0 and child_count >= 1:
            return TableRole.LOOKUP

        # Entity fallback: has many children regardless of column patterns
        if child_count >= 3:
            return TableRole.ENTITY

        # Transaction fallback: has FKs and date/amount columns
        if fk_count >= 1 and (has_date or has_amount):
            return TableRole.TRANSACTION

        return TableRole.UNKNOWN
