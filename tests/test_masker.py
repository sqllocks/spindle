"""Tests for the DataMasker PII masking module."""

import re

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

    def test_mask_bare_dataframe_raises_type_error(self):
        m = DataMasker()
        df = pd.DataFrame({"name": ["Alice"], "email": ["a@b.com"]})
        with pytest.raises(TypeError, match=r"dict\[str, DataFrame\]"):
            m.mask(df)

    def test_multi_table(self):
        t1 = pd.DataFrame({"id": range(5), "name": ["Alice"] * 5})
        t2 = pd.DataFrame({"id": range(5), "email": ["a@b.com"] * 5})
        masker = DataMasker()
        result = masker.mask({"people": t1, "contacts": t2})
        assert len(result.tables) == 2

    # ------------------------------------------------------------------
    # STORY-013 / ADR-008: name-independent value-pattern masking
    # ------------------------------------------------------------------

    def test_ssn_in_misnamed_column_masked(self):
        """SSN value-pattern in a column named 'notes' is masked (ADR-008)."""
        ssns = [f"{100 + i:03d}-{10 + i:02d}-{1000 + i:04d}" for i in range(20)]
        df = pd.DataFrame({"id": range(20), "notes": ssns})
        result = DataMasker().mask({"t": df})
        assert "notes" in result.columns_masked["t"]
        masked = result.tables["t"]["notes"]
        # Originals gone; replacements are valid SSN-format strings.
        assert not set(masked) & set(ssns)
        assert all(re.fullmatch(r"\d{3}-\d{2}-\d{4}", str(v)) for v in masked)

    def test_ip_in_misnamed_column_masked(self):
        """IPv4 value-pattern in 'c_47' is masked; output is valid IPv4."""
        ips = [f"10.0.{i}.{i + 1}" for i in range(20)]
        df = pd.DataFrame({"id": range(20), "c_47": ips})
        result = DataMasker().mask({"t": df})
        assert "c_47" in result.columns_masked["t"]
        masked = result.tables["t"]["c_47"]
        assert not set(masked) & set(ips)
        ip_re = r"(?:25[0-5]|2[0-4]\d|[01]?\d\d?)"
        assert all(
            re.fullmatch(rf"{ip_re}\.{ip_re}\.{ip_re}\.{ip_re}", str(v))
            for v in masked
        )

    def test_iban_in_misnamed_column_masked_locale_aware(self):
        """IBAN in a misnamed column is masked with a locale-aware generator."""
        ibans = [f"GB{(10 + i):02d}ABCD{(60000000 + i):020d}"[:22] for i in range(20)]
        df = pd.DataFrame({"id": range(20), "ref": ibans})
        result = DataMasker().mask({"t": df}, MaskConfig(locale="de_DE"))
        assert "ref" in result.columns_masked["t"]
        masked = result.tables["t"]["ref"]
        assert not set(masked) & set(ibans)
        # de_DE locale yields German IBANs (start "DE"), proving locale awareness.
        assert all(str(v).startswith("DE") for v in masked)

    def test_postal_in_misnamed_column_masked_locale_aware(self):
        """US ZIP pattern in a misnamed column is masked locale-aware.

        PROFILER GAP (discovered STORY-013): the profiler coerces pure 5-digit
        ZIP strings to spindle dtype 'integer', so it never computes a
        'postal_code' pattern for them and they are unreachable from
        value-pattern detection (only the ZIP+4 form stays 'string' and yields
        the pattern). This mirrors the 'cc' gap. The masker registry + the
        locale-aware fake.postcode() generator are correct; the gap is upstream
        in the profiler's numeric-string coercion. Test uses the ZIP+4 form,
        which the profiler DOES classify as postal_code.
        """
        zips = [f"{90000 + i:05d}-{1000 + i:04d}" for i in range(20)]
        df = pd.DataFrame({"id": range(20), "loc": zips})
        result = DataMasker().mask({"t": df}, MaskConfig(locale="en_US"))
        assert "loc" in result.columns_masked["t"]
        masked = result.tables["t"]["loc"]
        assert not set(masked) & set(zips)
        # en_US postcodes are 5-digit (optionally ZIP+4).
        assert all(re.fullmatch(r"\d{5}(-\d{4})?", str(v)) for v in masked)

    def test_pattern_registry_covers_every_profiler_pattern(self):
        """The registry maps each profiler PII pattern to a masker type.

        PROFILER GAP: the profiler has no credit-card ('cc') value detector,
        so 'cc' is wired into the registry as future-proofing but is currently
        unreachable from value-pattern detection (name-based masking only).
        """
        reg = DataMasker.PATTERN_TO_PII_TYPE
        for p in ("email", "phone", "ssn", "ip_address", "iban", "postal_code"):
            assert reg.get(p), f"profiler pattern {p!r} missing from registry"
        assert reg["uuid"] is None  # UUID is not PII
        assert reg["cc"] == "credit_card"  # PROFILER GAP: future-proofing only
