"""Tests for the semantic model (.bim) exporter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sqllocks_spindle import RetailDomain, HealthcareDomain
from sqllocks_spindle.fabric.semantic_model_writer import SemanticModelExporter


@pytest.fixture()
def exporter():
    return SemanticModelExporter()


@pytest.fixture()
def retail_schema():
    return RetailDomain().get_schema()


class TestSemanticModelExporterToDict:
    def test_returns_dict(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        assert isinstance(result, dict)

    def test_has_model_key(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        assert "model" in result

    def test_has_tables(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        tables = result["model"]["tables"]
        assert len(tables) > 0

    def test_table_names_match_schema(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        bim_names = {t["name"] for t in result["model"]["tables"]}
        schema_names = set(retail_schema.tables.keys())
        assert schema_names.issubset(bim_names)

    def test_tables_have_columns(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        for table in result["model"]["tables"]:
            assert "columns" in table
            assert len(table["columns"]) > 0

    def test_columns_have_data_type(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        for table in result["model"]["tables"]:
            for col in table["columns"]:
                assert "dataType" in col

    def test_valid_data_types(self, exporter, retail_schema):
        valid_types = {"int64", "string", "decimal", "double", "dateTime", "boolean"}
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        for table in result["model"]["tables"]:
            for col in table["columns"]:
                assert col["dataType"] in valid_types, (
                    f"Invalid dataType {col['dataType']} for {table['name']}.{col['name']}"
                )


class TestSemanticModelExporterMeasures:
    def test_measures_included_by_default(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse", include_measures=True)
        has_measures = any(
            "measures" in t and len(t["measures"]) > 0
            for t in result["model"]["tables"]
        )
        assert has_measures

    def test_measures_excluded_when_disabled(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse", include_measures=False)
        has_measures = any(
            "measures" in t and len(t["measures"]) > 0
            for t in result["model"]["tables"]
        )
        assert not has_measures


class TestSemanticModelExporterSourceTypes:
    def test_lakehouse_source(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="lakehouse")
        assert result is not None

    def test_warehouse_source(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="warehouse")
        assert result is not None

    def test_sql_database_source(self, exporter, retail_schema):
        result = exporter.to_dict(retail_schema, source_type="sql_database")
        assert result is not None


class TestSemanticModelExporterFile:
    def test_export_bim_creates_file(self, exporter, retail_schema, tmp_path):
        output = tmp_path / "test_model.bim"
        result_path = exporter.export_bim(
            retail_schema, source_type="lakehouse", output_path=str(output)
        )
        assert Path(result_path).exists()

    def test_export_bim_is_valid_json(self, exporter, retail_schema, tmp_path):
        output = tmp_path / "test_model.bim"
        exporter.export_bim(
            retail_schema, source_type="lakehouse", output_path=str(output)
        )
        with open(output) as f:
            data = json.load(f)
        assert "model" in data

    def test_export_different_domains(self, exporter, tmp_path):
        for domain_cls in [RetailDomain, HealthcareDomain]:
            schema = domain_cls().get_schema()
            output = tmp_path / f"{domain_cls.__name__}.bim"
            exporter.export_bim(schema, source_type="lakehouse", output_path=str(output))
            assert output.exists()
            with open(output) as f:
                data = json.load(f)
            assert len(data["model"]["tables"]) > 0
