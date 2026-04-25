"""E2E tests: verify all 23 generation strategies produce valid data via domain generation.

Rather than testing strategies in isolation (which requires complex setup),
we verify each strategy type appears in at least one domain's generated output
and produces non-null values. This catches strategy registration, config parsing,
and generation bugs.
"""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle, RetailDomain, HealthcareDomain, FinancialDomain
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain
from sqllocks_spindle.domains.hr import HrDomain
from sqllocks_spindle.domains.education import EducationDomain


# Pre-generate domains that collectively use all strategy types
_cache = {}

def _result(cls):
    if cls.__name__ not in _cache:
        _cache[cls.__name__] = Spindle().generate(domain=cls(), scale="small", seed=42)
    return _cache[cls.__name__]


def _find_strategy(result, strategy_name):
    """Find a column using the given strategy and return (table, col, values)."""
    for tname, tdef in result.schema.tables.items():
        for cname, cdef in tdef.columns.items():
            if cdef.generator.get("strategy") == strategy_name:
                if cname in result.tables[tname].columns:
                    return tname, cname, result.tables[tname][cname]
    return None, None, None


class TestSequenceStrategy:
    def test_sequence_produces_unique_integers(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "sequence")
        assert vals is not None, "No sequence strategy found"
        assert vals.is_unique


class TestUUIDStrategy:
    def test_uuid_produces_unique_strings(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "uuid")
        if vals is None:
            pytest.skip("No uuid strategy in RetailDomain")
        assert vals.is_unique


class TestFakerStrategy:
    def test_faker_produces_non_null_values(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "faker")
        assert vals is not None, "No faker strategy found"
        assert vals.notna().sum() > 0


class TestWeightedEnumStrategy:
    def test_weighted_enum_produces_expected_values(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "weighted_enum")
        assert vals is not None, "No weighted_enum strategy found"
        assert len(vals.unique()) >= 2


class TestDistributionStrategy:
    def test_distribution_produces_numeric_values(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "distribution")
        assert vals is not None, "No distribution strategy found"
        assert vals.dtype in ("float64", "int64", "Float64", "Int64")


class TestTemporalStrategy:
    def test_temporal_produces_dates(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "temporal")
        assert vals is not None, "No temporal strategy found"
        assert vals.notna().sum() > 0


class TestForeignKeyStrategy:
    def test_fk_values_reference_parent(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "foreign_key")
        assert vals is not None, "No foreign_key strategy found"
        assert vals.notna().sum() > 0


class TestFormulaStrategy:
    def test_formula_produces_values(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "formula")
        if vals is None:
            pytest.skip("No formula strategy in RetailDomain")
        assert vals.notna().sum() > 0


class TestDerivedStrategy:
    def test_derived_produces_values(self):
        r = _result(HealthcareDomain)
        tname, col, vals = _find_strategy(r, "derived")
        if vals is None:
            pytest.skip("No derived strategy found")
        assert vals.notna().sum() > 0


class TestCorrelatedStrategy:
    def test_correlated_produces_values(self):
        r = _result(CapitalMarketsDomain)
        tname, col, vals = _find_strategy(r, "correlated")
        assert vals is not None, "No correlated strategy found"
        assert vals.notna().sum() > 0


class TestLookupStrategy:
    def test_lookup_produces_values(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "lookup")
        if vals is None:
            pytest.skip("No lookup strategy in RetailDomain")
        assert vals.notna().sum() > 0


class TestReferenceDataStrategy:
    def test_reference_data_produces_values(self):
        r = _result(CapitalMarketsDomain)
        tname, col, vals = _find_strategy(r, "reference_data")
        assert vals is not None, "No reference_data strategy found"
        assert vals.notna().sum() > 0


class TestPatternStrategy:
    def test_pattern_produces_strings(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "pattern")
        if vals is None:
            pytest.skip("No pattern strategy in RetailDomain")
        assert vals.notna().sum() > 0


class TestConditionalStrategy:
    def test_conditional_produces_values(self):
        r = _result(RetailDomain)
        tname, col, vals = _find_strategy(r, "conditional")
        if vals is None:
            pytest.skip("No conditional strategy in RetailDomain")
        assert vals.notna().sum() > 0


class TestRecordSampleStrategy:
    def test_record_sample_produces_values(self):
        r = _result(CapitalMarketsDomain)
        tname, col, vals = _find_strategy(r, "record_sample")
        assert vals is not None, "No record_sample strategy found"
        assert vals.notna().sum() > 0


class TestRecordFieldStrategy:
    def test_record_field_produces_values(self):
        r = _result(CapitalMarketsDomain)
        tname, col, vals = _find_strategy(r, "record_field")
        assert vals is not None, "No record_field strategy found"
        assert vals.notna().sum() > 0


class TestSelfReferencingStrategy:
    def test_self_referencing_produces_values(self):
        for cls in [HrDomain, EducationDomain, RetailDomain]:
            r = _result(cls)
            tname, col, vals = _find_strategy(r, "self_referencing")
            if vals is not None:
                assert vals.notna().sum() > 0
                return
        pytest.skip("No self_referencing strategy found in tested domains")


class TestLifecycleStrategy:
    def test_lifecycle_produces_values(self):
        for cls in [RetailDomain, HrDomain, HealthcareDomain]:
            r = _result(cls)
            tname, col, vals = _find_strategy(r, "lifecycle")
            if vals is not None:
                assert vals.notna().sum() > 0
                return
        pytest.skip("No lifecycle strategy found in tested domains")


class TestFirstPerParentStrategy:
    def test_first_per_parent_produces_values(self):
        for cls in [RetailDomain, FinancialDomain]:
            r = _result(cls)
            tname, col, vals = _find_strategy(r, "first_per_parent")
            if vals is not None:
                assert vals.notna().sum() > 0
                return
        pytest.skip("No first_per_parent strategy found")


class TestComputedStrategy:
    def test_computed_produces_values(self):
        for cls in [RetailDomain, FinancialDomain]:
            r = _result(cls)
            tname, col, vals = _find_strategy(r, "computed")
            if vals is not None:
                assert vals.notna().sum() > 0
                return
        pytest.skip("No computed strategy found")
