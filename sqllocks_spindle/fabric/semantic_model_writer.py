"""Export Spindle schemas to Power BI / Fabric semantic model .bim files.

Generates TOM (Tabular Object Model) JSON at compatibilityLevel 1604,
including auto-generated DAX measures for fact tables and M expressions
for Lakehouse, Warehouse, or SQL Database sources.

No external dependencies — uses only stdlib json/pathlib.

Usage::

    from sqllocks_spindle.fabric.semantic_model_writer import SemanticModelExporter

    exporter = SemanticModelExporter()
    path = exporter.export_bim(
        schema=schema,
        source_type="lakehouse",
        source_name="MyLakehouse",
        output_path="retail_model.bim",
        include_measures=True,
    )
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqllocks_spindle.schema.parser import (
    ColumnDef,
    RelationshipDef,
    SpindleSchema,
    TableDef,
)

# ---------------------------------------------------------------------------
# Spindle type → TOM dataType mapping
# ---------------------------------------------------------------------------

TOM_TYPE_MAP: dict[str, str] = {
    "integer": "int64",
    "string": "string",
    "decimal": "decimal",
    "timestamp": "dateTime",
    "boolean": "boolean",
    "uuid": "string",
    "float": "double",
    "date": "dateTime",
    "time": "string",
    "binary": "string",
}

# ---------------------------------------------------------------------------
# M expression templates per source type
# ---------------------------------------------------------------------------

_M_LAKEHOUSE = (
    'let\n'
    '    Source = Lakehouse.Contents(null),\n'
    '    Data = Source{{[workspaceId="{{workspace_id}}", '
    'itemObjectId="{{lakehouse_id}}"]}}'
    '[Data],\n'
    '    {table}_Data = Data{{[schema="{table}"]}}'
    '[Data]\n'
    'in\n'
    '    {table}_Data'
)

_M_WAREHOUSE = (
    'let\n'
    '    Source = Sql.Database("{server}", "{database}"),\n'
    '    {schema}_{table} = Source{{[Schema="{schema}", '
    'Item="{table}"]}}[Data]\n'
    'in\n'
    '    {schema}_{table}'
)

_M_SQL_DATABASE = (
    'let\n'
    '    Source = Sql.Database("{server}", "{database}"),\n'
    '    {schema}_{table} = Source{{[Schema="{schema}", '
    'Item="{table}"]}}[Data]\n'
    'in\n'
    '    {schema}_{table}'
)


class SemanticModelExporter:
    """Export a SpindleSchema to a Power BI .bim (TOM JSON) file.

    Generates:
        - Table definitions with typed columns
        - Relationships from SpindleSchema FK definitions
        - Auto-generated DAX measures (SUM, COUNTROWS, AVERAGE)
        - M (Power Query) partition expressions per source type
    """

    def export_bim(
        self,
        schema: SpindleSchema,
        source_type: str = "lakehouse",
        source_name: str = "",
        output_path: str | Path = "model.bim",
        include_measures: bool = True,
        schema_name: str = "dbo",
    ) -> Path:
        """Export schema to a .bim file (TOM JSON format).

        Args:
            schema: A SpindleSchema describing tables and relationships.
            source_type: Data source — ``"lakehouse"``, ``"warehouse"``,
                or ``"sql_database"``.
            source_name: Name of the Lakehouse/Warehouse/Server item.
            output_path: Destination file path for the .bim output.
            include_measures: Generate DAX measures for numeric columns.
            schema_name: SQL schema for warehouse/sql_database sources.

        Returns:
            Path to the written .bim file.
        """
        output_path = Path(output_path)

        model_name = f"Spindle{schema.model.domain.replace('_', ' ').title().replace(' ', '')}"

        # Build TOM model
        tom = self._build_tom(
            schema=schema,
            model_name=model_name,
            source_type=source_type,
            source_name=source_name,
            include_measures=include_measures,
            schema_name=schema_name,
        )

        # Write JSON
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(tom, f, indent=2, ensure_ascii=False)

        return output_path

    def to_dict(
        self,
        schema: SpindleSchema,
        source_type: str = "lakehouse",
        source_name: str = "",
        include_measures: bool = True,
        schema_name: str = "dbo",
    ) -> dict[str, Any]:
        """Return the TOM model as a dict (without writing to disk)."""
        model_name = f"Spindle{schema.model.domain.replace('_', ' ').title().replace(' ', '')}"
        return self._build_tom(
            schema=schema,
            model_name=model_name,
            source_type=source_type,
            source_name=source_name,
            include_measures=include_measures,
            schema_name=schema_name,
        )

    # ----- internal: TOM building -----

    def _build_tom(
        self,
        schema: SpindleSchema,
        model_name: str,
        source_type: str,
        source_name: str,
        include_measures: bool,
        schema_name: str,
    ) -> dict[str, Any]:
        """Build the full TOM JSON structure."""
        from sqllocks_spindle import __version__

        # Identify fact tables (tables that are children in relationships)
        fact_tables = {r.child for r in schema.relationships}

        tables = []
        for tname, tdef in schema.tables.items():
            tom_table = self._build_table(
                tdef=tdef,
                source_type=source_type,
                source_name=source_name,
                schema_name=schema_name,
                is_fact=tname in fact_tables,
                include_measures=include_measures,
                schema=schema,
            )
            tables.append(tom_table)

        relationships = [
            self._build_relationship(rel) for rel in schema.relationships
        ]

        return {
            "name": model_name,
            "compatibilityLevel": 1604,
            "model": {
                "culture": schema.model.locale.replace("_", "-"),
                "tables": tables,
                "relationships": relationships,
                "roles": [],
                "annotations": [
                    {
                        "name": "generated_by",
                        "value": f"Spindle v{__version__}",
                    },
                    {
                        "name": "domain",
                        "value": schema.model.domain,
                    },
                    {
                        "name": "schema_mode",
                        "value": schema.model.schema_mode,
                    },
                ],
            },
        }

    def _build_table(
        self,
        tdef: TableDef,
        source_type: str,
        source_name: str,
        schema_name: str,
        is_fact: bool,
        include_measures: bool,
        schema: SpindleSchema,
    ) -> dict[str, Any]:
        """Build a TOM table definition."""
        columns = [
            self._build_column(cdef, tdef) for cdef in tdef.columns.values()
        ]

        # M expression for partition
        m_expr = self._build_m_expression(
            table_name=tdef.name,
            source_type=source_type,
            source_name=source_name,
            schema_name=schema_name,
        )

        table: dict[str, Any] = {
            "name": tdef.name,
            "columns": columns,
            "partitions": [
                {
                    "name": tdef.name,
                    "source": {
                        "type": "m",
                        "expression": m_expr,
                    },
                },
            ],
        }

        if tdef.description:
            table["description"] = tdef.description

        # Auto-generate measures
        if include_measures:
            measures = self._build_measures(tdef, is_fact, schema)
            if measures:
                table["measures"] = measures

        return table

    def _build_column(self, cdef: ColumnDef, tdef: TableDef) -> dict[str, Any]:
        """Build a TOM column definition."""
        tom_type = TOM_TYPE_MAP.get(cdef.type, "string")

        col: dict[str, Any] = {
            "name": cdef.name,
            "dataType": tom_type,
            "isHidden": False,
        }

        # Mark PK columns
        if cdef.name in tdef.primary_key:
            col["isKey"] = True
            col["summarizeBy"] = "none"

        # Mark FK columns — hide by default (join keys, not interesting to end users)
        if cdef.is_foreign_key:
            col["isHidden"] = True
            col["summarizeBy"] = "none"

        # Suppress auto-summarization for non-measure columns
        if tom_type == "string":
            col["summarizeBy"] = "none"
        elif tom_type == "dateTime":
            col["summarizeBy"] = "none"

        # Source column reference
        col["sourceColumn"] = cdef.name

        return col

    def _build_relationship(self, rel: RelationshipDef) -> dict[str, Any]:
        """Build a TOM relationship definition."""
        return {
            "name": rel.name,
            "fromTable": rel.child,
            "fromColumn": rel.child_columns[0] if rel.child_columns else "",
            "toTable": rel.parent,
            "toColumn": rel.parent_columns[0] if rel.parent_columns else "",
            "crossFilteringBehavior": "oneDirection",
            "isActive": True,
        }

    def _build_m_expression(
        self,
        table_name: str,
        source_type: str,
        source_name: str,
        schema_name: str,
    ) -> str:
        """Build an M (Power Query) expression for a table partition."""
        if source_type == "lakehouse":
            return _M_LAKEHOUSE.format(table=table_name)
        elif source_type == "warehouse":
            return _M_WAREHOUSE.format(
                server=source_name,
                database=source_name,
                schema=schema_name,
                table=table_name,
            )
        elif source_type == "sql_database":
            return _M_SQL_DATABASE.format(
                server=source_name,
                database=source_name,
                schema=schema_name,
                table=table_name,
            )
        else:
            return _M_LAKEHOUSE.format(table=table_name)

    # ----- internal: DAX measure generation -----

    def _build_measures(
        self,
        tdef: TableDef,
        is_fact: bool,
        schema: SpindleSchema,
    ) -> list[dict[str, Any]]:
        """Auto-generate DAX measures for a table."""
        measures: list[dict[str, Any]] = []

        # Row count for every table
        title = _title_case(tdef.name)
        measures.append({
            "name": f"{title} Count",
            "expression": f"COUNTROWS('{tdef.name}')",
            "formatString": "#,0",
        })

        # Numeric measures for fact tables (or any table with numeric columns)
        for cname, cdef in tdef.columns.items():
            # Skip PK and FK columns
            if cname in tdef.primary_key:
                continue
            if cdef.is_foreign_key:
                continue

            col_title = _title_case(cname)

            if cdef.type == "decimal" or cdef.type == "float":
                # SUM
                measures.append({
                    "name": f"Total {col_title}",
                    "expression": f"SUM('{tdef.name}'[{cname}])",
                    "formatString": "#,0.00",
                })
                # AVERAGE
                measures.append({
                    "name": f"Avg {col_title}",
                    "expression": f"AVERAGE('{tdef.name}'[{cname}])",
                    "formatString": "#,0.00",
                })

            elif cdef.type == "integer":
                # SUM for non-key integers (e.g., quantity)
                measures.append({
                    "name": f"Total {col_title}",
                    "expression": f"SUM('{tdef.name}'[{cname}])",
                    "formatString": "#,0",
                })

        return measures


def _title_case(snake_name: str) -> str:
    """Convert snake_case to Title Case."""
    return " ".join(w.capitalize() for w in snake_name.split("_"))
