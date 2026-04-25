"""Tests for ProfileIO — profile export, import, listing, and inference."""

from __future__ import annotations

import json

import pandas as pd
import pytest
from pathlib import Path

from sqllocks_spindle.inference.profile_io import ExportedProfile, ProfileIO


class TestProfileIO:
    """Core ProfileIO round-trip and inference tests."""

    def test_export_profile(self, tmp_path: Path) -> None:
        from sqllocks_spindle import RetailDomain

        domain = RetailDomain()
        io = ProfileIO()
        output = tmp_path / "retail_profile.json"
        result = io.export_profile(domain, output)

        assert result.exists()
        with open(result) as f:
            data = json.load(f)
        assert data["source_domain"] == "retail"
        assert "distributions" in data

    def test_import_profile(self, tmp_path: Path) -> None:
        from sqllocks_spindle import RetailDomain

        io = ProfileIO()
        # First export
        export_path = tmp_path / "export.json"
        io.export_profile(RetailDomain(), export_path)

        # Then import into same domain
        domain = RetailDomain()
        name = io.import_profile(export_path, domain, save_as="imported_test")
        assert name == "imported_test"

        # Verify the file was created
        imported_file = domain.domain_path / "profiles" / "imported_test.json"
        assert imported_file.exists()

        # Clean up
        imported_file.unlink()

    def test_list_profiles(self) -> None:
        from sqllocks_spindle import RetailDomain

        io = ProfileIO()
        profiles = io.list_profiles(RetailDomain())
        assert len(profiles) >= 1
        assert any(p["name"] == "default" for p in profiles)

    def test_from_dataframe(self) -> None:
        df = pd.DataFrame(
            {
                "id": range(100),
                "status": ["active"] * 70 + ["inactive"] * 30,
                "value": range(100),
            }
        )
        io = ProfileIO()
        profile = io.from_dataframe(df, table_name="users", name="test_profile")

        assert profile.name == "test_profile"
        assert "users.status" in profile.distributions

        # status should have ~70/30 split
        dist = profile.distributions["users.status"]
        assert abs(dist["active"] - 0.7) < 0.01

    def test_export_import_roundtrip(self, tmp_path: Path) -> None:
        from sqllocks_spindle import RetailDomain

        io = ProfileIO()
        domain = RetailDomain()
        export_path = tmp_path / "roundtrip.json"
        io.export_profile(domain, export_path)

        # Verify we can read it back
        with open(export_path) as f:
            data = json.load(f)
        assert data["name"] == domain.profile_name
        assert isinstance(data["distributions"], dict)

    def test_from_dataframe_high_cardinality_excluded(self) -> None:
        # Column with too many unique values should not be in distributions
        df = pd.DataFrame(
            {
                "id": range(1000),
                "name": [f"user_{i}" for i in range(1000)],
            }
        )
        io = ProfileIO()
        profile = io.from_dataframe(df, table_name="users")
        assert "users.name" not in profile.distributions

    def test_exported_profile_fields(self) -> None:
        profile = ExportedProfile(
            name="test",
            description="desc",
            source_domain="retail",
            distributions={"a": {"x": 0.5}},
            ratios={"r": 1.0},
            metadata={"key": "val"},
        )
        assert profile.name == "test"
        assert profile.source_domain == "retail"
