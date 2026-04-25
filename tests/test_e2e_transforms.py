"""E2E tests: star schema transforms, CDM export, semantic model .bim export."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sqllocks_spindle import (
    Spindle,
    RetailDomain,
    HealthcareDomain,
    FinancialDomain,
    StarSchemaTransform,
    CdmMapper,
    CdmEntityMap,
)
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain
from sqllocks_spindle.fabric.semantic_model_writer import SemanticModelExporter


STAR_DOMAINS = [
    (RetailDomain, "retail"),
    (HealthcareDomain, "healthcare"),
    (FinancialDomain, "financial"),
    (CapitalMarketsDomain, "capital_markets"),
]


class TestStarSchemaTransform:
    @pytest.mark.parametrize("domain_cls,name", STAR_DOMAINS, ids=[n for _, n in STAR_DOMAINS])
    def test_star_transform_produces_dims_and_facts(self, domain_cls, name):
        domain = domain_cls()
        result = Spindle().generate(domain=domain, scale="small", seed=42)
        star_map = domain.star_schema_map()
        transform = StarSchemaTransform()
        star_result = transform.transform(result.tables, star_map)
        assert len(star_result.dimensions) > 0, f"{name}: no dimensions"
        assert len(star_result.facts) > 0, f"{name}: no facts"

    @pytest.mark.parametrize("domain_cls,name", STAR_DOMAINS, ids=[n for _, n in STAR_DOMAINS])
    def test_star_dims_have_surrogate_keys(self, domain_cls, name):
        domain = domain_cls()
        result = Spindle().generate(domain=domain, scale="small", seed=42)
        star_map = domain.star_schema_map()
        star_result = StarSchemaTransform().transform(result.tables, star_map)
        for dim_name, dim_df in star_result.dimensions.items():
            sk_cols = [c for c in dim_df.columns if c.startswith("sk_")]
            assert len(sk_cols) > 0, f"{dim_name} has no surrogate key"

    @pytest.mark.parametrize("domain_cls,name", STAR_DOMAINS, ids=[n for _, n in STAR_DOMAINS])
    def test_star_facts_not_empty(self, domain_cls, name):
        domain = domain_cls()
        result = Spindle().generate(domain=domain, scale="small", seed=42)
        star_map = domain.star_schema_map()
        star_result = StarSchemaTransform().transform(result.tables, star_map)
        for fact_name, fact_df in star_result.facts.items():
            assert len(fact_df) > 0, f"{fact_name} is empty"


class TestCdmExport:
    @pytest.mark.parametrize("domain_cls,name",
                             [(RetailDomain, "retail"), (HealthcareDomain, "healthcare")],
                             ids=["retail", "healthcare"])
    def test_cdm_export_creates_model_json(self, domain_cls, name, tmp_path):
        domain = domain_cls()
        result = Spindle().generate(domain=domain, scale="small", seed=42)
        entity_map = domain.cdm_map()
        mapper = CdmMapper()
        files = mapper.write_cdm_folder(
            result.tables, str(tmp_path), name, entity_map, fmt="csv"
        )
        model_json = tmp_path / "model.json"
        assert model_json.exists(), "model.json not created"
        with open(model_json) as f:
            data = json.load(f)
        assert "entities" in data or "name" in data


class TestSemanticModelExport:
    def test_bim_export_valid_json(self, tmp_path):
        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
        exporter = SemanticModelExporter()
        output = tmp_path / "test.bim"
        exporter.export_bim(
            result.schema, source_type="lakehouse", output_path=str(output)
        )
        assert output.exists()
        with open(output) as f:
            data = json.load(f)
        assert "model" in data
        assert len(data["model"]["tables"]) > 0

    def test_bim_source_types(self):
        schema = RetailDomain().get_schema()
        exporter = SemanticModelExporter()
        for source_type in ["lakehouse", "warehouse", "sql_database"]:
            result = exporter.to_dict(schema, source_type=source_type)
            assert result is not None
