"""Parse SQL CREATE TABLE DDL into SpindleSchema objects.

Supports SQL Server / Fabric Warehouse, PostgreSQL, MySQL, and ANSI SQL
via regex-based parsing (no external SQL parser dependency).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from sqllocks_spindle.schema.parser import (
    BusinessRuleDef,
    ColumnDef,
    GenerationConfig,
    ModelDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)

# ---------------------------------------------------------------------------
# SQL type → Spindle generator mapping
# ---------------------------------------------------------------------------

TYPE_MAP: dict[str, dict[str, Any] | None] = {
    # Integer types
    "int": {"strategy": "distribution", "distribution": "uniform", "min": 1, "max": 10000},
    "integer": {"strategy": "distribution", "distribution": "uniform", "min": 1, "max": 10000},
    "bigint": {"strategy": "distribution", "distribution": "uniform", "min": 1, "max": 1000000},
    "smallint": {"strategy": "distribution", "distribution": "uniform", "min": 1, "max": 1000},
    "tinyint": {"strategy": "distribution", "distribution": "uniform", "min": 0, "max": 255},
    "bit": {"strategy": "weighted_enum", "values": {"1": 0.85, "0": 0.15}},
    "boolean": {"strategy": "weighted_enum", "values": {"true": 0.85, "false": 0.15}},
    "bool": {"strategy": "weighted_enum", "values": {"true": 0.85, "false": 0.15}},
    # Date/time
    "datetime": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    "datetime2": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    "date": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    "datetimeoffset": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    "timestamp": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    "timestamptz": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    "time": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    # Decimal/money
    "decimal": {"strategy": "distribution", "distribution": "normal", "mean": 100, "std": 50, "min": 0},
    "numeric": {"strategy": "distribution", "distribution": "normal", "mean": 100, "std": 50, "min": 0},
    "money": {"strategy": "distribution", "distribution": "log_normal", "mean": 4.5, "std": 1.0, "min": 0},
    "smallmoney": {"strategy": "distribution", "distribution": "log_normal", "mean": 3.0, "std": 0.8, "min": 0},
    "float": {"strategy": "distribution", "distribution": "normal", "mean": 0, "std": 1},
    "real": {"strategy": "distribution", "distribution": "normal", "mean": 0, "std": 1},
    "double precision": {"strategy": "distribution", "distribution": "normal", "mean": 0, "std": 1},
    # Identifiers
    "uniqueidentifier": {"strategy": "uuid"},
    "uuid": {"strategy": "uuid"},
    # Binary — skip generation
    "varbinary": None,
    "binary": None,
    "image": None,
    "bytea": None,
}

# ---------------------------------------------------------------------------
# Column name → Spindle generator heuristics (for string types)
# ---------------------------------------------------------------------------

# Exact name matches (case-insensitive)
NAME_EXACT: dict[str, dict[str, Any]] = {
    "first_name": {"strategy": "faker", "provider": "first_name"},
    "firstname": {"strategy": "faker", "provider": "first_name"},
    "last_name": {"strategy": "faker", "provider": "last_name"},
    "lastname": {"strategy": "faker", "provider": "last_name"},
    "email": {"strategy": "faker", "provider": "email"},
    "email_address": {"strategy": "faker", "provider": "email"},
    "phone": {"strategy": "faker", "provider": "phone_number"},
    "phone_number": {"strategy": "faker", "provider": "phone_number"},
    "address": {"strategy": "faker", "provider": "street_address"},
    "street_address": {"strategy": "faker", "provider": "street_address"},
    "city": {"strategy": "faker", "provider": "city"},
    "state": {"strategy": "faker", "provider": "state_abbr"},
    "zip": {"strategy": "faker", "provider": "zipcode"},
    "zip_code": {"strategy": "faker", "provider": "zipcode"},
    "zipcode": {"strategy": "faker", "provider": "zipcode"},
    "postal_code": {"strategy": "faker", "provider": "zipcode"},
    "country": {"strategy": "faker", "provider": "country"},
    "company": {"strategy": "faker", "provider": "company"},
    "company_name": {"strategy": "faker", "provider": "company"},
    "url": {"strategy": "faker", "provider": "url"},
    "website": {"strategy": "faker", "provider": "url"},
    "ssn": {"strategy": "faker", "provider": "ssn"},
    "description": {"strategy": "faker", "provider": "sentence"},
    "notes": {"strategy": "faker", "provider": "sentence"},
    "comment": {"strategy": "faker", "provider": "sentence"},
    "comments": {"strategy": "faker", "provider": "sentence"},
    "username": {"strategy": "faker", "provider": "user_name"},
    "user_name": {"strategy": "faker", "provider": "user_name"},
    "ip_address": {"strategy": "faker", "provider": "ipv4"},
}

# Suffix-based partial matches (case-insensitive)
NAME_SUFFIX: dict[str, dict[str, Any] | str] = {
    "_name": {"strategy": "faker", "provider": "name"},
    "_email": {"strategy": "faker", "provider": "email"},
    "_phone": {"strategy": "faker", "provider": "phone_number"},
    "_date": {"strategy": "temporal", "pattern": "uniform", "range_ref": "model.date_range"},
    "_code": {"strategy": "pattern", "template": "{seq:6}"},
    "_type": {"strategy": "weighted_enum", "values": {"type_a": 0.5, "type_b": 0.3, "type_c": 0.2}},
    "_status": {"strategy": "weighted_enum", "values": {"active": 0.7, "inactive": 0.2, "pending": 0.1}},
    "_id": "fk_candidate",  # sentinel — handled by FK detection
}

# String types that trigger heuristic matching
_STRING_TYPES = {
    "nvarchar", "varchar", "char", "nchar", "text", "ntext",
    "character varying", "character", "clob",
}


@dataclass
class _ParsedColumn:
    """Intermediate representation of a parsed DDL column."""

    name: str
    raw_type: str
    base_type: str
    max_length: int | None = None
    precision: int | None = None
    scale: int | None = None
    nullable: bool = True
    is_identity: bool = False
    is_serial: bool = False
    is_auto_increment: bool = False
    is_primary_key: bool = False
    default: str | None = None


@dataclass
class _ParsedTable:
    """Intermediate representation of a parsed DDL table."""

    name: str
    columns: list[_ParsedColumn] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[dict[str, str]] = field(default_factory=list)


@dataclass
class _ParsedForeignKey:
    """Intermediate representation of a parsed FK constraint."""

    child_table: str
    child_column: str
    parent_table: str
    parent_column: str


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Strip identifier quoting: [brackets], "double quotes", `backticks`
_UNQUOTE = re.compile(r'[\[\]"`]')

# Match CREATE TABLE header (captures table name, body parsed separately)
_CREATE_TABLE_HEADER = re.compile(
    r"CREATE\s+TABLE\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?"
    r"([\w.\[\]\"` ]+?)\s*\(",
    re.IGNORECASE,
)

# Match ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY
_ALTER_FK = re.compile(
    r"ALTER\s+TABLE\s+([\w.\[\]\"` ]+?)\s+"
    r"ADD\s+CONSTRAINT\s+[\w.\[\]\"` ]+\s+"
    r"FOREIGN\s+KEY\s*\(\s*([\w.\[\]\"` ]+)\s*\)\s*"
    r"REFERENCES\s+([\w.\[\]\"` ]+?)\s*\(\s*([\w.\[\]\"` ]+)\s*\)",
    re.IGNORECASE,
)

# Match inline FOREIGN KEY in column block
_INLINE_FK = re.compile(
    r"FOREIGN\s+KEY\s*\(\s*([\w.\[\]\"` ]+)\s*\)\s*"
    r"REFERENCES\s+([\w.\[\]\"` ]+?)\s*\(\s*([\w.\[\]\"` ]+)\s*\)",
    re.IGNORECASE,
)

# Match inline PRIMARY KEY in column block (table-level)
_TABLE_PK = re.compile(
    r"(?:CONSTRAINT\s+[\w.\[\]\"` ]+\s+)?PRIMARY\s+KEY\s*\(\s*([\w,\s.\[\]\"` ]+)\s*\)",
    re.IGNORECASE,
)

# Match SQL type with optional precision/scale/length: e.g. NVARCHAR(50), DECIMAL(18,2)
_TYPE_SPEC = re.compile(
    r"([\w\s]+?)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?$"
)

# IDENTITY(seed, increment)
_IDENTITY = re.compile(r"IDENTITY\s*(?:\(\s*\d+\s*,\s*\d+\s*\))?", re.IGNORECASE)

# SERIAL / BIGSERIAL (PostgreSQL)
_SERIAL_TYPES = {"serial", "bigserial", "smallserial"}


def _unquote(name: str) -> str:
    """Strip SQL identifier quoting and trim whitespace."""
    return _UNQUOTE.sub("", name).strip()


def _extract_table_name(raw: str) -> str:
    """Extract unqualified table name from possibly schema-qualified name."""
    name = _unquote(raw)
    # Handle schema.table
    if "." in name:
        name = name.rsplit(".", 1)[-1]
    return name.strip()


# ---------------------------------------------------------------------------
# DdlParser
# ---------------------------------------------------------------------------

class DdlParser:
    """Parse SQL DDL (CREATE TABLE) into a SpindleSchema.

    Supports SQL Server / Fabric Warehouse, PostgreSQL, MySQL, and ANSI SQL
    via regex-based parsing. No external SQL parser dependency required.

    Usage::

        parser = DdlParser()
        schema = parser.parse_file("my_tables.sql")
        # or
        schema = parser.parse_string("CREATE TABLE ...")
    """

    def parse_file(self, path: str | Path) -> SpindleSchema:
        """Parse a .sql file containing CREATE TABLE statements."""
        path = Path(path)
        sql = path.read_text(encoding="utf-8")
        return self.parse_string(sql)

    def parse_string(self, sql: str) -> SpindleSchema:
        """Parse SQL DDL string into a SpindleSchema."""
        # Step 1: Extract raw table definitions
        parsed_tables = self._extract_tables(sql)

        # Step 2: Extract ALTER TABLE foreign keys
        alter_fks = self._extract_alter_fks(sql)

        # Step 3: Build table name lookup for FK resolution
        table_names = {t.name.lower(): t.name for t in parsed_tables}

        # Step 4: Merge inline + ALTER FKs
        all_fks = self._collect_all_fks(parsed_tables, alter_fks, table_names)

        # Step 5: Build SpindleSchema
        return self._build_schema(parsed_tables, all_fks, table_names)

    # ----- internal: extraction -----

    def _extract_tables(self, sql: str) -> list[_ParsedTable]:
        """Extract all CREATE TABLE definitions from SQL."""
        tables = []
        for match in _CREATE_TABLE_HEADER.finditer(sql):
            raw_name = match.group(1)
            # Find the matching closing paren (handles nested parens like IDENTITY(1,1))
            start = match.end() - 1  # position of the opening '('
            body = self._extract_paren_body(sql, start)
            if body is not None:
                table = self._parse_create_table(raw_name, body)
                tables.append(table)
        return tables

    def _extract_paren_body(self, sql: str, open_pos: int) -> str | None:
        """Extract content between matched parentheses starting at open_pos."""
        if sql[open_pos] != "(":
            return None
        depth = 0
        for i in range(open_pos, len(sql)):
            if sql[i] == "(":
                depth += 1
            elif sql[i] == ")":
                depth -= 1
                if depth == 0:
                    return sql[open_pos + 1 : i].strip()
        return None

    def _parse_create_table(self, raw_name: str, body: str) -> _ParsedTable:
        """Parse a single CREATE TABLE body into a _ParsedTable."""
        table_name = _extract_table_name(raw_name)
        table = _ParsedTable(name=table_name)

        # Split body by top-level commas (respecting parentheses)
        parts = self._split_columns(body)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Check for table-level PRIMARY KEY
            pk_match = _TABLE_PK.search(part)
            if pk_match and not re.match(r"^\w", part.split()[0] if part.split() else "", re.IGNORECASE):
                # This is a standalone PK constraint, not a column
                pk_cols = [_unquote(c.strip()) for c in pk_match.group(1).split(",")]
                table.primary_key = pk_cols
                continue

            # Check for inline FOREIGN KEY constraint (table-level)
            fk_match = _INLINE_FK.search(part)
            if fk_match and part.strip().upper().startswith(("FOREIGN", "CONSTRAINT")):
                child_col = _unquote(fk_match.group(1))
                parent_table = _extract_table_name(fk_match.group(2))
                parent_col = _unquote(fk_match.group(3))
                table.foreign_keys.append({
                    "child_column": child_col,
                    "parent_table": parent_table,
                    "parent_column": parent_col,
                })
                continue

            # Check if this looks like a constraint line (not a column)
            first_word = part.split()[0].upper() if part.split() else ""
            if first_word in ("CONSTRAINT", "UNIQUE", "CHECK", "INDEX"):
                # Check for PK inside constraint
                pk_match2 = _TABLE_PK.search(part)
                if pk_match2:
                    pk_cols = [_unquote(c.strip()) for c in pk_match2.group(1).split(",")]
                    table.primary_key = pk_cols
                continue

            # Parse as column definition
            col = self._parse_column(part)
            if col:
                table.columns.append(col)
                if col.is_primary_key and not table.primary_key:
                    table.primary_key = [col.name]

        return table

    def _parse_column(self, definition: str) -> _ParsedColumn | None:
        """Parse a single column definition string."""
        definition = definition.strip()
        if not definition:
            return None

        # Tokenize: first token is name, then type and modifiers
        # Handle quoted names
        if definition.startswith(("[", '"', "`")):
            # Find the closing quote
            if definition.startswith("["):
                end = definition.index("]") + 1
            elif definition.startswith('"'):
                end = definition.index('"', 1) + 1
            else:
                end = definition.index("`", 1) + 1
            col_name = _unquote(definition[:end])
            rest = definition[end:].strip()
        else:
            parts = definition.split(None, 1)
            if len(parts) < 2:
                return None
            col_name = _unquote(parts[0])
            rest = parts[1]

        if not rest:
            return None

        # Extract type — consume until we hit a keyword or end
        upper_rest = rest.upper()

        # Check for IDENTITY before stripping it
        is_identity = bool(_IDENTITY.search(upper_rest))
        rest_no_identity = _IDENTITY.sub("", rest).strip()

        # Check for AUTO_INCREMENT
        is_auto_increment = "AUTO_INCREMENT" in upper_rest
        rest_clean = re.sub(r"AUTO_INCREMENT", "", rest_no_identity, flags=re.IGNORECASE).strip()

        # Strip constraint keywords to isolate the type
        keywords = [
            "NOT NULL", "NULL", "DEFAULT", "PRIMARY KEY", "REFERENCES",
            "UNIQUE", "CHECK", "COLLATE", "GENERATED",
        ]
        type_part = rest_clean
        for kw in keywords:
            idx = type_part.upper().find(kw)
            if idx > 0:
                type_part = type_part[:idx]
        type_part = type_part.strip().rstrip(",")

        # Parse type spec
        base_type, max_length, precision, scale = self._parse_type(type_part)

        # Check for serial types (PostgreSQL)
        is_serial = base_type.lower() in _SERIAL_TYPES

        # Nullable
        nullable = "NOT NULL" not in upper_rest

        # Inline PRIMARY KEY
        is_pk = "PRIMARY KEY" in upper_rest

        # Default
        default = None
        default_match = re.search(r"DEFAULT\s+(\S+)", rest, re.IGNORECASE)
        if default_match:
            default = default_match.group(1).strip("'\"(),")

        return _ParsedColumn(
            name=col_name,
            raw_type=type_part.strip(),
            base_type=base_type,
            max_length=max_length,
            precision=precision,
            scale=scale,
            nullable=nullable,
            is_identity=is_identity,
            is_serial=is_serial,
            is_auto_increment=is_auto_increment,
            is_primary_key=is_pk,
            default=default,
        )

    def _parse_type(self, type_str: str) -> tuple[str, int | None, int | None, int | None]:
        """Parse a SQL type string into (base_type, max_length, precision, scale)."""
        type_str = type_str.strip()

        match = _TYPE_SPEC.match(type_str)
        if not match:
            return type_str.lower(), None, None, None

        base = match.group(1).strip().lower()
        p1 = int(match.group(2)) if match.group(2) else None
        p2 = int(match.group(3)) if match.group(3) else None

        # Determine what the parameters mean based on type
        if base in ("decimal", "numeric"):
            return base, None, p1, p2
        elif base in _STRING_TYPES or base in ("nvarchar", "varchar", "char", "nchar", "varbinary", "binary"):
            return base, p1, None, None
        else:
            return base, p1, p2, None

    def _split_columns(self, body: str) -> list[str]:
        """Split CREATE TABLE body by top-level commas (respecting parentheses)."""
        parts = []
        depth = 0
        current = []
        for char in body:
            if char == "(":
                depth += 1
                current.append(char)
            elif char == ")":
                depth -= 1
                current.append(char)
            elif char == "," and depth == 0:
                parts.append("".join(current))
                current = []
            else:
                current.append(char)
        if current:
            parts.append("".join(current))
        return parts

    def _extract_alter_fks(self, sql: str) -> list[_ParsedForeignKey]:
        """Extract FOREIGN KEY constraints from ALTER TABLE statements."""
        fks = []
        for match in _ALTER_FK.finditer(sql):
            fks.append(_ParsedForeignKey(
                child_table=_extract_table_name(match.group(1)),
                child_column=_unquote(match.group(2)),
                parent_table=_extract_table_name(match.group(3)),
                parent_column=_unquote(match.group(4)),
            ))
        return fks

    def _collect_all_fks(
        self,
        tables: list[_ParsedTable],
        alter_fks: list[_ParsedForeignKey],
        table_names: dict[str, str],
    ) -> list[_ParsedForeignKey]:
        """Merge inline FKs from tables with ALTER TABLE FKs."""
        all_fks: list[_ParsedForeignKey] = []

        # Inline FKs from CREATE TABLE
        for table in tables:
            for fk in table.foreign_keys:
                all_fks.append(_ParsedForeignKey(
                    child_table=table.name,
                    child_column=fk["child_column"],
                    parent_table=fk["parent_table"],
                    parent_column=fk["parent_column"],
                ))

        # ALTER TABLE FKs
        all_fks.extend(alter_fks)

        return all_fks

    # ----- internal: schema building -----

    def _build_schema(
        self,
        parsed_tables: list[_ParsedTable],
        all_fks: list[_ParsedForeignKey],
        table_names: dict[str, str],
    ) -> SpindleSchema:
        """Build a SpindleSchema from parsed DDL components."""

        # Index FKs by (child_table, child_column) for lookup
        fk_index: dict[tuple[str, str], _ParsedForeignKey] = {}
        for fk in all_fks:
            fk_index[(fk.child_table.lower(), fk.child_column.lower())] = fk

        # Build tables — also track naming-convention FKs discovered during resolution
        convention_fks: list[_ParsedForeignKey] = []
        spindle_tables: dict[str, TableDef] = {}
        for pt in parsed_tables:
            columns: dict[str, ColumnDef] = {}
            for pc in pt.columns:
                generator = self._resolve_generator(pc, pt, fk_index, table_names)
                if generator is None:
                    # Binary column — skip
                    continue

                # Track naming-convention FKs (not in fk_index) for relationship building
                if (
                    generator.get("strategy") == "foreign_key"
                    and (pt.name.lower(), pc.name.lower()) not in fk_index
                ):
                    ref = generator.get("ref", "")
                    if "." in ref:
                        parent_table, parent_col = ref.split(".", 1)
                        convention_fks.append(_ParsedForeignKey(
                            child_table=pt.name,
                            child_column=pc.name,
                            parent_table=parent_table,
                            parent_column=parent_col,
                        ))

                col_type = self._resolve_spindle_type(pc)

                columns[pc.name] = ColumnDef(
                    name=pc.name,
                    type=col_type,
                    generator=generator,
                    nullable=pc.nullable,
                    max_length=pc.max_length,
                    precision=pc.precision,
                    scale=pc.scale,
                )

            spindle_tables[pt.name] = TableDef(
                name=pt.name,
                columns=columns,
                primary_key=pt.primary_key,
            )

        # Build relationships from explicit + naming-convention FKs
        combined_fks = all_fks + convention_fks
        relationships = self._build_relationships(combined_fks, spindle_tables)

        # Build scale defaults
        generation = self._build_generation(spindle_tables)

        model = ModelDef(
            name="ddl_import",
            description="Imported from SQL DDL",
            domain="custom",
            schema_mode="3nf",
            date_range={"start": "2024-01-01", "end": "2025-12-31"},
        )

        return SpindleSchema(
            model=model,
            tables=spindle_tables,
            relationships=relationships,
            business_rules=[],
            generation=generation,
        )

    def _resolve_generator(
        self,
        col: _ParsedColumn,
        table: _ParsedTable,
        fk_index: dict[tuple[str, str], _ParsedForeignKey],
        table_names: dict[str, str],
    ) -> dict[str, Any] | None:
        """Determine the best generator strategy for a column."""

        # 1. Identity / Serial / Auto-increment → sequence
        if col.is_identity or col.is_serial or col.is_auto_increment:
            return {"strategy": "sequence", "start": 1}

        # 2. Explicit FK constraint
        fk_key = (table.name.lower(), col.name.lower())
        if fk_key in fk_index:
            fk = fk_index[fk_key]
            return {
                "strategy": "foreign_key",
                "ref": f"{fk.parent_table}.{fk.parent_column}",
                "distribution": "pareto",
            }

        # 3. Naming-convention FK detection: column ends with _id and matches a table
        col_lower = col.name.lower()
        if col_lower.endswith("_id") and col_lower not in ("id",):
            candidate = col_lower[:-3]  # strip _id
            real_table = self._find_table_by_singular(candidate, table_names)
            if real_table is not None and real_table.lower() != table.name.lower():
                return {
                    "strategy": "foreign_key",
                    "ref": f"{real_table}.{col.name}",
                    "distribution": "pareto",
                }

        # 4. PK column (non-identity) → sequence
        if col.name in table.primary_key and len(table.primary_key) == 1:
            return {"strategy": "sequence", "start": 1}

        # 5. Type-based mapping
        base = col.base_type.lower()

        # Binary types → skip
        if base in ("varbinary", "binary", "image", "bytea"):
            return None

        # String types → use name heuristics
        if base in _STRING_TYPES:
            gen = self._resolve_string_heuristic(col)
            if gen:
                return gen
            # Fallback for strings
            length = col.max_length or 255
            if length <= 10:
                return {"strategy": "pattern", "template": "{seq:6}"}
            return {"strategy": "faker", "provider": "text", "max_nb_chars": min(length, 200)}

        # Direct type mapping
        if base in TYPE_MAP:
            gen = TYPE_MAP[base]
            if gen is None:
                return None
            return dict(gen)  # copy to avoid mutation

        # Fallback
        return {"strategy": "faker", "provider": "text", "max_nb_chars": 50}

    @staticmethod
    def _find_table_by_singular(
        candidate: str, table_names: dict[str, str]
    ) -> str | None:
        """Find a table matching a singular FK candidate, trying plural forms.

        Given ``candidate`` (e.g. ``"order"``), look up in *table_names*
        (keyed by lowercase) for ``order``, ``orders``, ``orderes``, or
        ``orderies`` (for -y → -ies plurals).
        """
        # Exact match (singular table name)
        if candidate in table_names:
            return table_names[candidate]
        # Common English plurals
        if candidate + "s" in table_names:
            return table_names[candidate + "s"]
        if candidate + "es" in table_names:
            return table_names[candidate + "es"]
        if candidate.endswith("y") and candidate[:-1] + "ies" in table_names:
            return table_names[candidate[:-1] + "ies"]
        # Reverse: candidate might already be plural — try singularising
        if candidate.endswith("ies"):
            singular = candidate[:-3] + "y"
            if singular in table_names:
                return table_names[singular]
        if candidate.endswith("ses") or candidate.endswith("xes") or candidate.endswith("zes"):
            singular = candidate[:-2]
            if singular in table_names:
                return table_names[singular]
        if candidate.endswith("s") and not candidate.endswith("ss"):
            singular = candidate[:-1]
            if singular in table_names:
                return table_names[singular]
        return None

    def _resolve_string_heuristic(self, col: _ParsedColumn) -> dict[str, Any] | None:
        """Apply name heuristics for string-type columns."""
        name = col.name.lower()

        # Exact match
        if name in NAME_EXACT:
            return dict(NAME_EXACT[name])

        # Suffix match
        for suffix, gen in NAME_SUFFIX.items():
            if name.endswith(suffix) and gen != "fk_candidate":
                return dict(gen) if isinstance(gen, dict) else None

        return None

    def _resolve_spindle_type(self, col: _ParsedColumn) -> str:
        """Map a parsed SQL column to a Spindle type string."""
        base = col.base_type.lower()

        if base in _SERIAL_TYPES or col.is_identity:
            return "integer"
        if base in ("int", "integer", "bigint", "smallint", "tinyint"):
            return "integer"
        if base in ("bit", "boolean", "bool"):
            return "boolean"
        if base in ("decimal", "numeric", "money", "smallmoney"):
            return "decimal"
        if base in ("float", "real", "double precision"):
            return "float"
        if base in ("datetime", "datetime2", "datetimeoffset", "timestamp", "timestamptz"):
            return "timestamp"
        if base == "date":
            return "date"
        if base == "time":
            return "time"
        if base in ("uniqueidentifier", "uuid"):
            return "uuid"
        if base in _STRING_TYPES:
            return "string"
        if base in ("varbinary", "binary", "image", "bytea"):
            return "binary"
        return "string"

    def _build_relationships(
        self,
        all_fks: list[_ParsedForeignKey],
        tables: dict[str, TableDef],
    ) -> list[RelationshipDef]:
        """Build RelationshipDef list from parsed FKs."""
        relationships = []
        for fk in all_fks:
            if fk.parent_table in tables and fk.child_table in tables:
                relationships.append(RelationshipDef(
                    name=f"fk_{fk.child_table}_{fk.child_column}",
                    parent=fk.parent_table,
                    child=fk.child_table,
                    parent_columns=[fk.parent_column],
                    child_columns=[fk.child_column],
                    type="one_to_many",
                ))
        return relationships

    def _build_generation(self, tables: dict[str, TableDef]) -> GenerationConfig:
        """Build scale defaults based on table relationships."""
        # Identify root tables (no FK dependencies)
        root_tables = set()
        child_tables = set()
        for t in tables.values():
            if not t.fk_dependencies:
                root_tables.add(t.name)
            else:
                child_tables.add(t.name)

        # Build scale presets
        small: dict[str, int] = {}
        medium: dict[str, int] = {}
        large: dict[str, int] = {}
        for name in tables:
            if name in root_tables:
                small[name] = 1000
                medium[name] = 10000
                large[name] = 100000
            else:
                # Estimate based on parent count × ratio
                small[name] = 2500
                medium[name] = 25000
                large[name] = 250000

        return GenerationConfig(
            scale="small",
            scales={"small": small, "medium": medium, "large": large},
        )
