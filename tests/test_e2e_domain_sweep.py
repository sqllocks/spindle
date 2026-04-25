"""E2E tests: generate every domain, verify integrity, PK uniqueness, null constraints."""

from __future__ import annotations

import time

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.retail import RetailDomain
from sqllocks_spindle.domains.healthcare import HealthcareDomain
from sqllocks_spindle.domains.financial import FinancialDomain
from sqllocks_spindle.domains.supply_chain import SupplyChainDomain
from sqllocks_spindle.domains.iot import IoTDomain
from sqllocks_spindle.domains.hr import HrDomain
from sqllocks_spindle.domains.insurance import InsuranceDomain
from sqllocks_spindle.domains.marketing import MarketingDomain
from sqllocks_spindle.domains.education import EducationDomain
from sqllocks_spindle.domains.real_estate import RealEstateDomain
from sqllocks_spindle.domains.manufacturing import ManufacturingDomain
from sqllocks_spindle.domains.telecom import TelecomDomain
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain


ALL_DOMAINS = [
    RetailDomain,
    HealthcareDomain,
    FinancialDomain,
    SupplyChainDomain,
    IoTDomain,
    HrDomain,
    InsuranceDomain,
    MarketingDomain,
    EducationDomain,
    RealEstateDomain,
    ManufacturingDomain,
    TelecomDomain,
    CapitalMarketsDomain,
]

DOMAIN_IDS = [d.__name__.replace("Domain", "").lower() for d in ALL_DOMAINS]

_cache: dict[str, object] = {}


def _get_result(domain_cls):
    key = domain_cls.__name__
    if key not in _cache:
        _cache[key] = Spindle().generate(domain=domain_cls(), scale="small", seed=42)
    return _cache[key]


# ---------------------------------------------------------------------------
# Generate + basic structure
# ---------------------------------------------------------------------------

class TestDomainGeneration:
    @pytest.mark.parametrize("domain_cls", ALL_DOMAINS, ids=DOMAIN_IDS)
    def test_generates_without_error(self, domain_cls):
        result = _get_result(domain_cls)
        assert len(result.tables) > 0

    @pytest.mark.parametrize("domain_cls", ALL_DOMAINS, ids=DOMAIN_IDS)
    def test_no_empty_tables(self, domain_cls):
        result = _get_result(domain_cls)
        for name, df in result.tables.items():
            assert len(df) > 0, f"{domain_cls.__name__}.{name} is empty"

    @pytest.mark.parametrize("domain_cls", ALL_DOMAINS, ids=DOMAIN_IDS)
    def test_all_expected_tables_present(self, domain_cls):
        result = _get_result(domain_cls)
        schema = result.schema
        for table_name in schema.tables:
            assert table_name in result.tables, f"Missing table: {table_name}"


# ---------------------------------------------------------------------------
# FK integrity
# ---------------------------------------------------------------------------

class TestDomainIntegrity:
    @pytest.mark.parametrize("domain_cls", ALL_DOMAINS, ids=DOMAIN_IDS)
    def test_fk_integrity(self, domain_cls):
        result = _get_result(domain_cls)
        errors = result.verify_integrity()
        assert errors == [], f"FK integrity errors in {domain_cls.__name__}: {errors}"


# ---------------------------------------------------------------------------
# PK uniqueness
# ---------------------------------------------------------------------------

class TestDomainPKUniqueness:
    @pytest.mark.parametrize("domain_cls", ALL_DOMAINS, ids=DOMAIN_IDS)
    def test_pk_uniqueness(self, domain_cls):
        result = _get_result(domain_cls)
        schema = result.schema
        for table_name, table_def in schema.tables.items():
            if not table_def.primary_key:
                continue
            df = result.tables[table_name]
            pk_cols = table_def.primary_key
            if len(pk_cols) == 1:
                assert df[pk_cols[0]].is_unique, (
                    f"{domain_cls.__name__}.{table_name}.{pk_cols[0]} has duplicates"
                )
            else:
                dupes = df.duplicated(subset=pk_cols, keep=False)
                assert not dupes.any(), (
                    f"{domain_cls.__name__}.{table_name} composite PK has duplicates"
                )


# ---------------------------------------------------------------------------
# Non-nullable columns have no nulls
# ---------------------------------------------------------------------------

class TestDomainNullConstraints:
    @pytest.mark.parametrize("domain_cls", ALL_DOMAINS, ids=DOMAIN_IDS)
    def test_non_nullable_columns(self, domain_cls):
        result = _get_result(domain_cls)
        schema = result.schema
        violations = []
        for table_name, table_def in schema.tables.items():
            df = result.tables[table_name]
            for col_name, col_def in table_def.columns.items():
                if col_name not in df.columns:
                    continue
                if not col_def.nullable and df[col_name].isna().any():
                    null_count = df[col_name].isna().sum()
                    violations.append(f"{table_name}.{col_name}: {null_count} nulls")
        assert violations == [], f"Null violations in {domain_cls.__name__}: {violations}"


# ---------------------------------------------------------------------------
# Reproducibility (seed determinism)
# ---------------------------------------------------------------------------

class TestDomainReproducibility:
    @pytest.mark.parametrize("domain_cls", [RetailDomain, HealthcareDomain, FinancialDomain],
                             ids=["retail", "healthcare", "financial"])
    def test_same_seed_same_result(self, domain_cls):
        s = Spindle()
        r1 = s.generate(domain=domain_cls(), scale="small", seed=99)
        r2 = s.generate(domain=domain_cls(), scale="small", seed=99)
        for table_name in r1.tables:
            assert r1[table_name].equals(r2[table_name]), (
                f"{domain_cls.__name__}.{table_name} not reproducible with same seed"
            )
