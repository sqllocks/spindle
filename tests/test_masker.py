"""Tests for the DataMasker PII masking module."""

import pytest
import numpy as np
import pandas as pd

from sqllocks_spindle.inference.masker import DataMasker, MaskConfig


class TestDataMasker:
    def test_email_column_masked(self):
        df = pd.DataFrame({
            "id": range(10),
            "email": [f"user{i}@test.com" for i in range(10)],
        })
        masker = DataMasker()
        result = masker.mask({"users": df})
        masked = result.tables["users"]
        # Emails should be replaced
        assert not any(masked["email"].str.contains("test.com", na=False))
        assert "email" in result.columns_masked["users"]

    def test_phone_column_masked(self):
        df = pd.DataFrame({
            "id": range(10),
            "phone_number": ["555-0100"] * 10,
        })
        masker = DataMasker()
        result = masker.mask({"contacts": df})
        assert "phone_number" in result.columns_masked["contacts"]

    def test_name_columns_masked(self):
        df = pd.DataFrame({
            "id": range(10),
            "first_name": ["Alice"] * 10,
            "last_name": ["Smith"] * 10,
        })
        masker = DataMasker()
        result = masker.mask({"people": df})
        assert "first_name" in result.columns_masked["people"]
        assert "last_name" in result.columns_masked["people"]

    def test_nulls_preserved(self):
        df = pd.DataFrame({
            "id": range(10),
            "email": [
                "a@b.com", None, "c@d.com", None, "e@f.com",
                None, "g@h.com", None, "i@j.com", None,
            ],
        })
        masker = DataMasker()
        config = MaskConfig(preserve_nulls=True)
        result = masker.mask({"t": df}, config=config)
        masked = result.tables["t"]
        # Null positions should be preserved
        original_nulls = df["email"].isna()
        masked_nulls = masked["email"].isna()
        assert original_nulls.equals(masked_nulls)

    def test_exclude_columns(self):
        df = pd.DataFrame({
            "id": range(10),
            "email": ["test@test.com"] * 10,
            "name": ["Alice"] * 10,
        })
        config = MaskConfig(exclude_columns=["email"])
        masker = DataMasker()
        result = masker.mask({"t": df}, config=config)
        # email should NOT be masked
        assert "email" not in result.columns_masked["t"]
        # name should be masked
        assert "name" in result.columns_masked["t"]

    def test_explicit_pii_columns(self):
        df = pd.DataFrame({
            "id": range(10),
            "custom_field": ["sensitive"] * 10,
        })
        config = MaskConfig(pii_columns={"custom_field": "name"})
        masker = DataMasker()
        result = masker.mask({"t": df}, config=config)
        assert "custom_field" in result.columns_masked["t"]

    def test_id_columns_not_masked(self):
        df = pd.DataFrame({
            "customer_id": range(10),
            "order_id": range(10),
        })
        masker = DataMasker()
        result = masker.mask({"t": df})
        assert "customer_id" not in result.columns_masked["t"]

    def test_row_count_preserved(self):
        df = pd.DataFrame({
            "id": range(100),
            "email": [f"u{i}@test.com" for i in range(100)],
        })
        masker = DataMasker()
        result = masker.mask({"t": df})
        assert len(result.tables["t"]) == 100

    def test_summary(self):
        df = pd.DataFrame({"id": range(5), "email": ["a@b.com"] * 5})
        masker = DataMasker()
        result = masker.mask({"t": df})
        assert "Masking" in result.summary()

    def test_seed_reproducibility(self):
        df = pd.DataFrame({"id": range(10), "email": ["a@b.com"] * 10})
        m1 = DataMasker().mask({"t": df}, MaskConfig(seed=42))
        m2 = DataMasker().mask({"t": df}, MaskConfig(seed=42))
        assert m1.tables["t"]["email"].tolist() == m2.tables["t"]["email"].tolist()

    def test_multi_table(self):
        t1 = pd.DataFrame({"id": range(5), "name": ["Alice"] * 5})
        t2 = pd.DataFrame({"id": range(5), "email": ["a@b.com"] * 5})
        masker = DataMasker()
        result = masker.mask({"people": t1, "contacts": t2})
        assert len(result.tables) == 2
