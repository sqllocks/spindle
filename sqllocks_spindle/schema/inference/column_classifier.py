"""Classify columns by semantic role: MONETARY, QUANTITY, STATUS, TEMPORAL, etc.

Extends the DDL parser's 25 name heuristics to ~80 patterns with context
awareness (table role influences column classification).
"""

from __future__ import annotations

import re

from sqllocks_spindle.schema.inference import (
    ColumnSemantic,
    InferenceContext,
    TableRole,
)

# ---------------------------------------------------------------------------
# Pattern catalogs
# ---------------------------------------------------------------------------

# Monetary column patterns (assign log_normal)
_MONETARY_PATTERNS = re.compile(
    r"(price|cost|amount|total|subtotal|fee|charge|rate|salary|wage|pay|"
    r"revenue|margin|balance|payment|premium|deductible|copay|refund|"
    r"credit|debit|deposit|withdrawal|budget|spend|income|rent|"
    r"commission|bonus|tax_amount|freight|shipping_cost)", re.IGNORECASE
)

# Quantity patterns (assign geometric)
_QUANTITY_PATTERNS = re.compile(
    r"(quantity|qty|count|units|num_|number_of_|stock|inventory|"
    r"on_hand|on_order|allocated|reserved|capacity|headcount)", re.IGNORECASE
)

# Percentage patterns (bounded normal 0-100)
_PERCENTAGE_PATTERNS = re.compile(
    r"(_pct|_percent|_rate|_ratio|discount_pct|tax_rate|margin_pct|"
    r"completion_rate|yield_rate|defect_rate|churn_rate|conversion_rate)",
    re.IGNORECASE,
)

# Measurement patterns (normal distribution)
_MEASUREMENT_PATTERNS = re.compile(
    r"(weight|height|width|length|depth|size|area|volume|"
    r"sqft|square_feet|lot_size|distance|duration|temperature)",
    re.IGNORECASE,
)

# Rating/score patterns
_RATING_PATTERNS = re.compile(
    r"(rating|score|stars|rank|grade|level|tier|priority|severity)",
    re.IGNORECASE,
)

# Status patterns (weighted enum)
_STATUS_PATTERNS = re.compile(
    r"^(status|state|order_status|payment_status|claim_status|"
    r"account_status|ticket_status|job_status|task_status)$", re.IGNORECASE
)

# Categorical/type patterns (weighted enum)
_CATEGORICAL_PATTERNS = re.compile(
    r"(type|category|kind|class|group|segment|channel|method|mode|"
    r"source|reason|department|division|region|territory|zone)", re.IGNORECASE
)

# Boolean flag patterns
_BOOLEAN_PATTERNS = re.compile(
    r"^(is_|has_|can_|should_|was_|did_|flag|active|enabled|deleted|"
    r"verified|approved|published|archived|locked|primary|default)",
    re.IGNORECASE,
)

# Temporal patterns
_TEMPORAL_TXN_PATTERNS = re.compile(
    r"(order_date|purchase_date|transaction_date|invoice_date|"
    r"sale_date|claim_date|booking_date|payment_date|ship_date)",
    re.IGNORECASE,
)
_TEMPORAL_AUDIT_PATTERNS = re.compile(
    r"(created_at|modified_at|updated_at|deleted_at|created_date|"
    r"modified_date|updated_date|last_modified|last_updated|"
    r"date_created|date_modified)", re.IGNORECASE,
)
_TEMPORAL_START_PATTERNS = re.compile(
    r"(start_date|begin_date|effective_date|hire_date|open_date|"
    r"enrollment_date|signup_date|registration_date|issue_date|"
    r"inception_date|commencement)", re.IGNORECASE,
)
_TEMPORAL_END_PATTERNS = re.compile(
    r"(end_date|expir|close_date|due_date|completion_date|"
    r"termination_date|cancellation_date|maturity_date|"
    r"discharge_date|resolved_at|closed_at)", re.IGNORECASE,
)
_TEMPORAL_BIRTH_PATTERNS = re.compile(
    r"(birth_date|dob|date_of_birth|birthdate)", re.IGNORECASE
)

# Code/identifier patterns
_CODE_PATTERNS = re.compile(
    r"(_code|_number|_no$|_num$|_ref$|_key$|_token$|_sku|_upc|_ean)",
    re.IGNORECASE,
)

# Text/description patterns
_TEXT_PATTERNS = re.compile(
    r"(description|comment|note|remarks|reason|summary|detail|body|"
    r"message|feedback|review|bio|abstract|memo)", re.IGNORECASE,
)

# Contact patterns
_EMAIL_PATTERN = re.compile(r"(email|e_mail)", re.IGNORECASE)
_PHONE_PATTERN = re.compile(r"(phone|fax|mobile|cell|tel)", re.IGNORECASE)
_ADDRESS_PATTERN = re.compile(r"(address|addr|street|line1|line2|address_line)", re.IGNORECASE)
_CITY_PATTERN = re.compile(r"^(city|town|municipality)$", re.IGNORECASE)
_STATE_PATTERN = re.compile(r"^(state|province|region|state_code|state_province)$", re.IGNORECASE)
_POSTAL_PATTERN = re.compile(r"(zip|postal|postcode|zip_code|postal_code)", re.IGNORECASE)
_COUNTRY_PATTERN = re.compile(r"^(country|country_code|country_name)$", re.IGNORECASE)
_URL_PATTERN = re.compile(r"(url|website|homepage|link|uri)", re.IGNORECASE)
_NAME_PATTERN = re.compile(r"(name|title|label)$", re.IGNORECASE)


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case for pattern matching.

    Examples: OrderDate → order_date, SalesOrderID → sales_order_id,
    firstName → first_name, HTTPResponse → http_response.
    """
    # Insert underscore before uppercase letters that follow lowercase/digits
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert underscore between consecutive uppercase and following lowercase
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


class ColumnClassifier:
    """Assign a ColumnSemantic to each column based on name, type, and table context."""

    def analyze(self, ctx: InferenceContext) -> None:
        for table_name, table_def in ctx.schema.tables.items():
            table_role = ctx.table_roles.get(table_name, TableRole.UNKNOWN)
            ctx.column_semantics.setdefault(table_name, {})

            for col_name, col_def in table_def.columns.items():
                semantic = self._classify(col_name, col_def, table_role, table_def)
                ctx.column_semantics[table_name][col_name] = semantic

    def _classify(self, name: str, col_def, table_role: TableRole, table_def) -> ColumnSemantic:
        # Normalize CamelCase names for pattern matching
        match_name = _camel_to_snake(name) if name != name.lower() else name
        # PK / FK take priority
        if name in table_def.primary_key:
            return ColumnSemantic.PRIMARY_KEY
        if col_def.is_foreign_key:
            return ColumnSemantic.FOREIGN_KEY

        col_type = col_def.type.lower()
        is_numeric = col_type in ("integer", "float", "decimal", "money")
        is_string = col_type in ("string", "text", "varchar", "nvarchar", "char")
        is_temporal = col_type in ("date", "datetime", "timestamp")
        is_boolean = col_type in ("boolean", "bit")

        # Boolean flags
        if is_boolean or _BOOLEAN_PATTERNS.match(match_name):
            return ColumnSemantic.BOOLEAN_FLAG

        # Temporal (order matters — check specific patterns before generic)
        if is_temporal or match_name.endswith(("_date", "_at", "_time", "_timestamp")):
            if _TEMPORAL_BIRTH_PATTERNS.search(match_name):
                return ColumnSemantic.TEMPORAL_BIRTH
            if _TEMPORAL_TXN_PATTERNS.search(match_name):
                return ColumnSemantic.TEMPORAL_TRANSACTION
            if _TEMPORAL_AUDIT_PATTERNS.search(match_name):
                return ColumnSemantic.TEMPORAL_AUDIT
            if _TEMPORAL_START_PATTERNS.search(match_name):
                return ColumnSemantic.TEMPORAL_START
            if _TEMPORAL_END_PATTERNS.search(match_name):
                return ColumnSemantic.TEMPORAL_END
            # Context-aware: date on a transaction table → transaction date
            if table_role == TableRole.TRANSACTION and "date" in match_name:
                return ColumnSemantic.TEMPORAL_TRANSACTION
            return ColumnSemantic.TEMPORAL_GENERIC

        # Monetary (must check before generic numeric)
        if is_numeric and _MONETARY_PATTERNS.search(match_name):
            return ColumnSemantic.MONETARY
        if col_type == "money":
            return ColumnSemantic.MONETARY

        # Quantity
        if is_numeric and _QUANTITY_PATTERNS.search(match_name):
            return ColumnSemantic.QUANTITY

        # Percentage
        if is_numeric and _PERCENTAGE_PATTERNS.search(match_name):
            return ColumnSemantic.PERCENTAGE

        # Measurement
        if is_numeric and _MEASUREMENT_PATTERNS.search(match_name):
            return ColumnSemantic.MEASUREMENT

        # Rating/score
        if is_numeric and _RATING_PATTERNS.search(match_name):
            return ColumnSemantic.RATING

        # Status (string, short)
        if is_string and _STATUS_PATTERNS.search(match_name):
            return ColumnSemantic.STATUS

        # Categorical/type — relaxed max_length for strong name matches
        if is_string and _CATEGORICAL_PATTERNS.search(match_name):
            max_len = col_def.max_length or 255
            if max_len <= 255:
                return ColumnSemantic.CATEGORICAL

        # Contact info
        if _EMAIL_PATTERN.search(match_name):
            return ColumnSemantic.EMAIL
        if _PHONE_PATTERN.search(match_name):
            return ColumnSemantic.PHONE
        if _ADDRESS_PATTERN.search(match_name):
            return ColumnSemantic.ADDRESS
        if _CITY_PATTERN.match(match_name):
            return ColumnSemantic.CITY
        if _STATE_PATTERN.match(match_name):
            return ColumnSemantic.STATE_CODE
        if _POSTAL_PATTERN.search(match_name):
            return ColumnSemantic.POSTAL_CODE
        if _COUNTRY_PATTERN.match(match_name):
            return ColumnSemantic.COUNTRY
        if _URL_PATTERN.search(match_name):
            return ColumnSemantic.URL

        # Code/identifier
        if is_string and _CODE_PATTERNS.search(match_name):
            return ColumnSemantic.CODE

        # Text/description
        if is_string and _TEXT_PATTERNS.search(match_name):
            return ColumnSemantic.TEXT_DESCRIPTION

        # Name (catch-all for *name* strings that aren't FK or code)
        if is_string and _NAME_PATTERN.search(match_name):
            return ColumnSemantic.NAME

        return ColumnSemantic.UNKNOWN
