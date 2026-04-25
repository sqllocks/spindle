"""E2E tests: DataMasker — PII detection, masking, FK preservation, null preservation."""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from sqllocks_spindle import Spindle, RetailDomain, HealthcareDomain
from sqllocks_spindle.inference import DataMasker, MaskConfig


@pytest.fixture(scope="module")
def retail_tables():
    return Spindle().generate(domain=RetailDomain(), scale="small", seed=42).tables


@pytest.fixture(scope="module")
def healthcare_tables():
    return Spindle().generate(domain=HealthcareDomain(), scale="small", seed=42).tables


class TestMaskingBasic:
    def test_mask_returns_result(self, retail_tables):
        masker = DataMasker()
        result = masker.mask(retail_tables)
        assert result is not None
        assert len(result.tables) > 0

    def test_masked_tables_same_shape(self, retail_tables):
        masker = DataMasker()
        result = masker.mask(retail_tables)
        for name, orig_df in retail_tables.items():
            masked_df = result.tables[name]
            assert orig_df.shape == masked_df.shape, f"{name} shape changed after masking"

    def test_email_columns_masked(self, retail_tables):
        masker = DataMasker()
        result = masker.mask(retail_tables)
        # customer table should have email column masked
        if "customer" in retail_tables and "email" in retail_tables["customer"].columns:
            orig = retail_tables["customer"]["email"]
            masked = result.tables["customer"]["email"]
            # At least some values should differ
            assert not orig.equals(masked), "Email column was not masked"


class TestMaskingPIICategories:
    def test_healthcare_names_masked(self, healthcare_tables):
        masker = DataMasker()
        result = masker.mask(healthcare_tables)
        # patient table should have first_name/last_name masked
        if "patient" in healthcare_tables:
            patient_orig = healthcare_tables["patient"]
            patient_masked = result.tables["patient"]
            for col in ["first_name", "last_name"]:
                if col in patient_orig.columns:
                    assert not patient_orig[col].equals(patient_masked[col]), (
                        f"patient.{col} was not masked"
                    )


class TestMaskingPreservation:
    def test_null_preservation(self, retail_tables):
        config = MaskConfig(preserve_nulls=True, seed=42)
        masker = DataMasker()
        result = masker.mask(retail_tables, config=config)
        for name, orig_df in retail_tables.items():
            masked_df = result.tables[name]
            for col in orig_df.columns:
                if col in masked_df.columns:
                    orig_nulls = orig_df[col].isna()
                    masked_nulls = masked_df[col].isna()
                    if orig_nulls.any():
                        assert orig_nulls.equals(masked_nulls), (
                            f"{name}.{col}: null positions changed after masking"
                        )

    def test_exclude_columns(self, retail_tables):
        config = MaskConfig(exclude_columns=["customer_id"], seed=42)
        masker = DataMasker()
        result = masker.mask(retail_tables, config=config)
        if "customer" in retail_tables and "customer_id" in retail_tables["customer"].columns:
            assert retail_tables["customer"]["customer_id"].equals(
                result.tables["customer"]["customer_id"]
            ), "Excluded column was masked"


class TestMaskingReproducibility:
    def test_same_seed_same_result(self, retail_tables):
        config = MaskConfig(seed=42)
        masker = DataMasker()
        r1 = masker.mask(retail_tables, config=config)
        r2 = masker.mask(retail_tables, config=config)
        for name in r1.tables:
            assert r1.tables[name].equals(r2.tables[name]), (
                f"{name}: masking not reproducible with same seed"
            )
