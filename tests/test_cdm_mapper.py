"""Tests for the CDM mapper (Phase 6)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from sqllocks_spindle import RetailDomain, Spindle
from sqllocks_spindle.transform import CdmEntityMap, CdmMapper
from sqllocks_spindle.transform.cdm_mapper import _cdm_dtype, _to_pascal


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def test_to_pascal_snake_case():
    assert _to_pascal("order_line") == "OrderLine"
    assert _to_pascal("product_category") == "ProductCategory"
    assert _to_pascal("customer") == "Customer"


def test_to_pascal_single_word():
    assert _to_pascal("patient") == "Patient"


def test_cdm_dtype_int():
    s = pd.Series([1, 2, 3], dtype="int64")
    s.name = "id"
    assert _cdm_dtype(s) == "int64"


def test_cdm_dtype_float():
    s = pd.Series([1.0, 2.5], dtype="float64")
    s.name = "amount"
    assert _cdm_dtype(s) == "double"


def test_cdm_dtype_bool():
    s = pd.Series([True, False])
    s.name = "flag"
    assert _cdm_dtype(s) == "boolean"


def test_cdm_dtype_datetime():
    s = pd.Series(pd.to_datetime(["2024-01-01", "2024-01-02"]))
    s.name = "created_at"
    assert _cdm_dtype(s) == "dateTime"


def test_cdm_dtype_string():
    s = pd.Series(["a", "b"])
    s.name = "name"
    assert _cdm_dtype(s) == "string"


def test_cdm_dtype_date_column_name_heuristic():
    s = pd.Series(["2024-01-01", "2024-01-02"])
    s.name = "order_date"
    assert _cdm_dtype(s) == "date"


# ---------------------------------------------------------------------------
# CdmEntityMap
# ---------------------------------------------------------------------------

def test_cdm_entity_map_explicit():
    em = CdmEntityMap({"customer": "Contact", "order": "SalesOrder"})
    assert em.entity_name("customer") == "Contact"
    assert em.entity_name("order") == "SalesOrder"


def test_cdm_entity_map_default_pascal():
    em = CdmEntityMap()
    assert em.entity_name("order_line") == "OrderLine"
    assert em.entity_name("product_category") == "ProductCategory"


def test_cdm_entity_map_fallback_for_unknown():
    em = CdmEntityMap({"customer": "Contact"})
    assert em.entity_name("unknown_table") == "UnknownTable"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def retail_tables():
    spindle = Spindle()
    result = spindle.generate(domain=RetailDomain(), scale="fabric_demo", seed=99)
    return result.tables


# ---------------------------------------------------------------------------
# model.json generation
# ---------------------------------------------------------------------------

def test_to_model_json_basic(retail_tables):
    mapper = CdmMapper()
    model = mapper.to_model_json(retail_tables, domain_name="TestModel")
    assert model["name"] == "TestModel"
    assert "entities" in model
    assert len(model["entities"]) == len(retail_tables)


def test_model_json_entity_names_default_pascal(retail_tables):
    mapper = CdmMapper()
    model = mapper.to_model_json(retail_tables)
    entity_names = {e["name"] for e in model["entities"]}
    # Default: snake_case → PascalCase
    assert "Customer" in entity_names
    assert "Order" in entity_names
    assert "OrderLine" in entity_names


def test_model_json_entity_names_with_map(retail_tables):
    mapper = CdmMapper()
    em = CdmEntityMap({"customer": "Contact", "order": "SalesOrder"})
    model = mapper.to_model_json(retail_tables, entity_map=em)
    entity_names = {e["name"] for e in model["entities"]}
    assert "Contact" in entity_names
    assert "SalesOrder" in entity_names


def test_model_json_entity_has_attributes(retail_tables):
    mapper = CdmMapper()
    model = mapper.to_model_json(retail_tables)
    customer_entity = next(e for e in model["entities"] if e["name"] == "Customer")
    assert len(customer_entity["attributes"]) > 0
    # Each attribute has name and dataType
    for attr in customer_entity["attributes"]:
        assert "name" in attr
        assert "dataType" in attr


def test_model_json_entity_has_partitions(retail_tables):
    mapper = CdmMapper()
    model = mapper.to_model_json(retail_tables)
    entity = model["entities"][0]
    assert "partitions" in entity
    assert len(entity["partitions"]) == 1
    partition = entity["partitions"][0]
    assert "location" in partition
    assert "fileFormatSettings" in partition


def test_model_json_csv_partition_format(retail_tables):
    mapper = CdmMapper()
    model = mapper.to_model_json(retail_tables, fmt="csv")
    entity = model["entities"][0]
    fmt_settings = entity["partitions"][0]["fileFormatSettings"]
    assert fmt_settings["$type"] == "CsvFormatSettings"
    assert fmt_settings["columnHeaders"] is True


def test_model_json_parquet_partition_format(retail_tables):
    mapper = CdmMapper()
    model = mapper.to_model_json(retail_tables, fmt="parquet")
    entity = model["entities"][0]
    fmt_settings = entity["partitions"][0]["fileFormatSettings"]
    assert fmt_settings["$type"] == "ParquetFormatSettings"


def test_model_json_has_version(retail_tables):
    mapper = CdmMapper()
    model = mapper.to_model_json(retail_tables)
    assert model["version"] == "1.0"
    assert "modifiedTime" in model


# ---------------------------------------------------------------------------
# CDM folder write
# ---------------------------------------------------------------------------

def test_write_cdm_folder_creates_model_json(retail_tables):
    mapper = CdmMapper()
    with tempfile.TemporaryDirectory() as tmpdir:
        files = mapper.write_cdm_folder(retail_tables, tmpdir)
        model_path = Path(tmpdir) / "model.json"
        assert model_path.exists()
        assert model_path in files


def test_write_cdm_folder_creates_entity_dirs(retail_tables):
    mapper = CdmMapper()
    with tempfile.TemporaryDirectory() as tmpdir:
        mapper.write_cdm_folder(retail_tables, tmpdir)
        # Each table should have its own subdirectory
        dirs = [p for p in Path(tmpdir).iterdir() if p.is_dir()]
        assert len(dirs) == len(retail_tables)


def test_write_cdm_folder_csv_files_exist(retail_tables):
    mapper = CdmMapper()
    with tempfile.TemporaryDirectory() as tmpdir:
        files = mapper.write_cdm_folder(retail_tables, tmpdir, fmt="csv")
        csv_files = [f for f in files if f.suffix == ".csv"]
        assert len(csv_files) == len(retail_tables)
        for f in csv_files:
            assert f.exists()
            assert f.stat().st_size > 0


def test_write_cdm_folder_model_json_is_valid(retail_tables):
    mapper = CdmMapper()
    with tempfile.TemporaryDirectory() as tmpdir:
        mapper.write_cdm_folder(retail_tables, tmpdir, domain_name="TestRetail")
        model_path = Path(tmpdir) / "model.json"
        with open(model_path) as f:
            model = json.load(f)
        assert model["name"] == "TestRetail"
        assert len(model["entities"]) == len(retail_tables)


def test_write_cdm_folder_location_matches_entity_dir(retail_tables):
    mapper = CdmMapper()
    with tempfile.TemporaryDirectory() as tmpdir:
        mapper.write_cdm_folder(retail_tables, tmpdir, fmt="csv")
        model_path = Path(tmpdir) / "model.json"
        with open(model_path) as f:
            model = json.load(f)
        for entity in model["entities"]:
            partition_loc = entity["partitions"][0]["location"]
            data_path = Path(tmpdir) / partition_loc
            assert data_path.exists(), f"CDM partition file missing: {partition_loc}"


def test_write_cdm_folder_with_entity_map(retail_tables):
    mapper = CdmMapper()
    em = CdmEntityMap({"customer": "Contact", "order": "SalesOrder"})
    with tempfile.TemporaryDirectory() as tmpdir:
        mapper.write_cdm_folder(retail_tables, tmpdir, entity_map=em)
        # Contact directory should exist
        assert (Path(tmpdir) / "Contact").is_dir()
        assert (Path(tmpdir) / "SalesOrder").is_dir()


def test_write_cdm_folder_total_file_count(retail_tables):
    mapper = CdmMapper()
    with tempfile.TemporaryDirectory() as tmpdir:
        files = mapper.write_cdm_folder(retail_tables, tmpdir)
        # 1 model.json + 1 csv per table
        assert len(files) == len(retail_tables) + 1


# ---------------------------------------------------------------------------
# Retail domain cdm_map()
# ---------------------------------------------------------------------------

def test_retail_cdm_map_returns_entity_map():
    domain = RetailDomain()
    em = domain.cdm_map()
    assert isinstance(em, CdmEntityMap)
    assert em.entity_name("customer") == "Contact"
    assert em.entity_name("order") == "SalesOrder"
    assert em.entity_name("order_line") == "SalesOrderProduct"
