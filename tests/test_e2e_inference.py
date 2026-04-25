"""E2E tests: DataProfiler, SchemaBuilder, FidelityComparator, ProfileIO."""

from __future__ import annotations

import pytest
import pandas as pd

from sqllocks_spindle import Spindle, RetailDomain
from sqllocks_spindle.inference import DataProfiler, SchemaBuilder


@pytest.fixture(scope="module")
def retail_tables():
    result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
    return result.tables


# ---------------------------------------------------------------------------
# DataProfiler
# ---------------------------------------------------------------------------

class TestDataProfiler:
    def test_profile_single_table(self, retail_tables):
        profiler = DataProfiler()
        profile = profiler.profile_dataframe(retail_tables["customer"])
        assert profile is not None
        assert len(profile.columns) > 0

    def test_profile_dataset_detects_types(self, retail_tables):
        profiler = DataProfiler()
        profile = profiler.profile_dataset(retail_tables)
        assert len(profile.tables) > 0
        # Should detect column dtypes
        all_dtypes = set()
        for tp in profile.tables.values():
            for cp in tp.columns.values():
                all_dtypes.add(str(cp.dtype))
        assert len(all_dtypes) > 1, "Only one dtype detected across all columns"

    def test_profile_detects_enums(self, retail_tables):
        profiler = DataProfiler()
        profile = profiler.profile_dataset(retail_tables)
        # Retail has categorical columns like loyalty_tier
        found_enum = False
        for tp in profile.tables.values():
            for cp in tp.columns.values():
                if cp.is_enum:
                    found_enum = True
                    break
        assert found_enum, "No enum columns detected"

    def test_profile_detects_fk_relationships(self, retail_tables):
        profiler = DataProfiler()
        profile = profiler.profile_dataset(retail_tables)
        assert len(profile.relationships) > 0, "No FK relationships detected"


# ---------------------------------------------------------------------------
# SchemaBuilder — profile → schema → generate
# ---------------------------------------------------------------------------

class TestSchemaBuilder:
    def test_build_schema_from_profile(self, retail_tables):
        profiler = DataProfiler()
        profile = profiler.profile_dataset(retail_tables)
        builder = SchemaBuilder()
        schema = builder.build(profile, domain_name="inferred_retail")
        assert len(schema.tables) > 0
        assert "inferred_retail" in schema.model.name

    def test_inferred_schema_generates_data(self, retail_tables):
        profiler = DataProfiler()
        profile = profiler.profile_dataset(retail_tables)
        builder = SchemaBuilder()
        schema = builder.build(profile, domain_name="inferred_retail")
        result = Spindle().generate(schema=schema, scale="small", seed=42)
        assert len(result.tables) > 0
        for table_name, df in result.tables.items():
            assert len(df) > 0, f"Inferred {table_name} generated 0 rows"

    def test_inferred_schema_has_scales(self, retail_tables):
        profiler = DataProfiler()
        profile = profiler.profile_dataset(retail_tables)
        builder = SchemaBuilder()
        schema = builder.build(profile, domain_name="test")
        assert "small" in schema.generation.scales
        assert "medium" in schema.generation.scales


# ---------------------------------------------------------------------------
# Round-trip: real data → profile → schema → generate → compare
# ---------------------------------------------------------------------------

class TestInferenceRoundTrip:
    def test_round_trip_produces_similar_schema(self, retail_tables):
        """Profile real data, build schema, generate — tables should have same columns."""
        profiler = DataProfiler()
        profile = profiler.profile_dataset({"customer": retail_tables["customer"]})
        builder = SchemaBuilder()
        schema = builder.build(profile, domain_name="roundtrip")
        result = Spindle().generate(schema=schema, scale="small", seed=42)
        assert "customer" in result.tables
        orig_cols = set(retail_tables["customer"].columns)
        gen_cols = set(result.tables["customer"].columns)
        # Generated table should have most of the original columns
        overlap = orig_cols & gen_cols
        assert len(overlap) >= len(orig_cols) * 0.5, (
            f"Only {len(overlap)}/{len(orig_cols)} columns matched"
        )
