"""Acceptance tests AT-1..AT-4 — end-to-end publish to Fabric targets (E15).

These tests validate the full generate → publish pipeline against real
Fabric endpoints.  They are skipped by default unless the corresponding
environment variables are set.

Environment variables:
    SPINDLE_LAKEHOUSE_PATH   — abfss:// path for AT-1 (Lakehouse Files)
    SPINDLE_SQL_CONNECTION   — connection string for AT-2 (SQL Database)
    SPINDLE_EVENTHOUSE_URI   — Eventhouse cluster URI for AT-3
    SPINDLE_EVENTHOUSE_DB    — KQL database name for AT-3
    SPINDLE_WORKSPACE_ID     — Fabric workspace ID for AT-4 (manifest)
    SPINDLE_LAKEHOUSE_ID     — Fabric lakehouse ID for AT-4 (manifest)

Run with:
    pytest tests/test_acceptance.py -v --tb=short

To run against real Fabric:
    export SPINDLE_LAKEHOUSE_PATH="abfss://ws@onelake.dfs.fabric.microsoft.com/lh.Lakehouse"
    pytest tests/test_acceptance.py -v -k "at1"
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

from sqllocks_spindle.engine.generator import Spindle


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def small_retail_result():
    """Generate a small retail dataset for acceptance tests."""
    from sqllocks_spindle.domains.retail import RetailDomain
    domain = RetailDomain(schema_mode="3nf")
    spindle = Spindle()
    return spindle.generate(domain=domain, scale="small", seed=42)


# ---------------------------------------------------------------------------
# AT-1: Lakehouse Files publish
# ---------------------------------------------------------------------------

LAKEHOUSE_PATH = os.environ.get("SPINDLE_LAKEHOUSE_PATH")

@pytest.mark.skipif(
    not LAKEHOUSE_PATH,
    reason="SPINDLE_LAKEHOUSE_PATH not set — skipping Lakehouse acceptance test",
)
class TestAT1LakehousePublish:
    """AT-1: Generate → write to Lakehouse Files → verify files exist."""

    def test_publish_to_lakehouse(self, small_retail_result):
        from sqllocks_spindle.fabric import LakehouseFilesWriter

        writer = LakehouseFilesWriter(base_path=LAKEHOUSE_PATH, default_format="parquet")

        for table_name, df in small_retail_result.tables.items():
            path = writer.paths.landing_zone_path("retail", table_name, "acceptance_test")
            file_path = writer.write_partition(df, path, format="parquet")
            assert file_path is not None


# ---------------------------------------------------------------------------
# AT-1 Local: Lakehouse Files publish (always runs with tmp_path)
# ---------------------------------------------------------------------------

class TestAT1LocalLakehousePublish:
    """AT-1 Local: Generate → write to local Lakehouse Files → verify."""

    def test_publish_parquet_to_local(self, small_retail_result, tmp_path):
        from sqllocks_spindle.fabric import LakehouseFilesWriter

        writer = LakehouseFilesWriter(base_path=str(tmp_path), default_format="parquet")

        written_paths = []
        for table_name, df in small_retail_result.tables.items():
            path = writer.paths.landing_zone_path("retail", table_name, "test")
            file_path = writer.write_partition(df, path, format="parquet")
            written_paths.append(file_path)
            assert file_path.exists()
            # Verify parquet is readable
            loaded = pd.read_parquet(file_path)
            assert len(loaded) == len(df)

        assert len(written_paths) > 0

    def test_publish_csv_to_local(self, small_retail_result, tmp_path):
        from sqllocks_spindle.fabric import LakehouseFilesWriter

        writer = LakehouseFilesWriter(base_path=str(tmp_path), default_format="csv")
        table_name = list(small_retail_result.tables.keys())[0]
        df = small_retail_result.tables[table_name]

        path = writer.paths.landing_zone_path("retail", table_name, "test")
        file_path = writer.write_partition(df, path, format="csv")
        assert file_path.exists()

    def test_manifest_written(self, small_retail_result, tmp_path):
        from sqllocks_spindle.fabric import LakehouseFilesWriter
        from sqllocks_spindle.manifests import ManifestBuilder

        writer = LakehouseFilesWriter(base_path=str(tmp_path))
        builder = ManifestBuilder()
        builder.start(spec=None, pack=None, domain_name="retail", scale="small", seed=42)
        builder.set_fabric_ids(workspace_id="test-ws", lakehouse_id="test-lh")

        for table_name, df in small_retail_result.tables.items():
            path = writer.paths.landing_zone_path("retail", table_name, "test")
            writer.write_partition(df, path)
            builder.record_output(table_name, rows=len(df), columns=len(df.columns))

        manifest = builder.finish()
        manifest_path = writer.paths.control_path("retail", "manifest") / "run_manifest.json"
        ManifestBuilder.to_file(manifest, manifest_path)

        assert manifest_path.exists()
        loaded = ManifestBuilder.from_file(manifest_path)
        assert loaded.domain == "retail"
        assert loaded.workspace_id == "test-ws"
        assert loaded.lakehouse_id == "test-lh"
        assert len(loaded.tables) == len(small_retail_result.tables)
        assert loaded.sbom  # SBOM should be populated


# ---------------------------------------------------------------------------
# AT-2: SQL Database publish
# ---------------------------------------------------------------------------

SQL_CONNECTION = os.environ.get("SPINDLE_SQL_CONNECTION")

@pytest.mark.skipif(
    not SQL_CONNECTION,
    reason="SPINDLE_SQL_CONNECTION not set — skipping SQL Database acceptance test",
)
class TestAT2SqlDatabasePublish:
    """AT-2: Generate → write to Fabric SQL Database → verify row counts."""

    def test_publish_to_sql_database(self, small_retail_result):
        from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

        writer = FabricSqlDatabaseWriter(
            connection_string=SQL_CONNECTION,
            auth_method="cli",
        )
        write_result = writer.write(
            small_retail_result,
            schema_name="spindle_at2",
            mode="create_insert",
        )
        assert write_result.tables_written > 0
        assert not write_result.errors


# ---------------------------------------------------------------------------
# AT-3: Eventhouse publish
# ---------------------------------------------------------------------------

EVENTHOUSE_URI = os.environ.get("SPINDLE_EVENTHOUSE_URI")
EVENTHOUSE_DB = os.environ.get("SPINDLE_EVENTHOUSE_DB")

@pytest.mark.skipif(
    not EVENTHOUSE_URI or not EVENTHOUSE_DB,
    reason="SPINDLE_EVENTHOUSE_URI/DB not set — skipping Eventhouse acceptance test",
)
class TestAT3EventhousePublish:
    """AT-3: Generate → write to Eventhouse/KQL → verify."""

    def test_publish_to_eventhouse(self, small_retail_result):
        from sqllocks_spindle.fabric.eventhouse_writer import EventhouseWriter

        writer = EventhouseWriter(
            cluster_uri=EVENTHOUSE_URI,
            database=EVENTHOUSE_DB,
            auth_method="cli",
        )
        write_result = writer.write(small_retail_result, table_prefix="at3_")
        assert write_result.tables_written > 0
        assert not write_result.errors


# ---------------------------------------------------------------------------
# AT-4: Credential resolver integration
# ---------------------------------------------------------------------------

class TestAT4CredentialResolver:
    """AT-4: Verify credential resolver works in publish pipeline."""

    def test_env_credential_resolves(self, monkeypatch):
        from sqllocks_spindle.fabric.credentials import CredentialResolver

        monkeypatch.setenv("SPINDLE_AT4_SECRET", "test_connection_string")
        resolver = CredentialResolver()
        result = resolver.resolve("env://SPINDLE_AT4_SECRET")
        assert result == "test_connection_string"

    def test_file_credential_resolves(self, tmp_path):
        from sqllocks_spindle.fabric.credentials import CredentialResolver

        secret_file = tmp_path / "connection.txt"
        secret_file.write_text("Server=test;Database=db\n", encoding="utf-8")

        resolver = CredentialResolver()
        result = resolver.resolve(f"file://{secret_file}")
        assert result == "Server=test;Database=db"

    def test_raw_passthrough(self):
        from sqllocks_spindle.fabric.credentials import CredentialResolver

        resolver = CredentialResolver()
        result = resolver.resolve("Server=localhost;Database=spindle")
        assert result == "Server=localhost;Database=spindle"
