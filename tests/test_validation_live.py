"""Live validation suite — ~30 combinations against real Fabric sinks.

Auth: InteractiveBrowserCredential fires once per session, token cached.

Requires:
  - Sound BI tenant credentials (browser prompt on first run)
  - Fabric_Lakehouse_Demo workspace accessible
  - Optional env vars for other sinks (tests skip gracefully if unset):
      SPINDLE_TEST_WH_CONN      Fabric Warehouse ODBC connection string
      SPINDLE_TEST_EH_CONN      Eventhouse cluster URI
      SPINDLE_TEST_SQL_CONN     Fabric SQL Database ODBC connection string
      SPINDLE_TEST_ONPREM_CONN  On-prem SQL Server ODBC connection string

Run:
    .venv-mac/bin/python -m pytest tests/test_validation_live.py -m live -v
"""
from __future__ import annotations

import importlib
import os

import pytest

from sqllocks_spindle.cli import _get_domain_registry
from sqllocks_spindle.engine.generator import Spindle
from sqllocks_spindle.fabric.sql_database_writer import FabricSqlDatabaseWriter

_SOUND_BI_TENANT = "2536810f-20e1-4911-a453-4409fd96db8a"
_WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
_LAKEHOUSE_ID = "ec851642-fa89-42bc-aebf-2742845d36fe"

_browser_cred = None  # cached across tests


def _get_storage_token():
    global _browser_cred
    try:
        from azure.identity import InteractiveBrowserCredential
        if _browser_cred is None:
            _browser_cred = InteractiveBrowserCredential(tenant_id=_SOUND_BI_TENANT)
        tok = _browser_cred.get_token("https://storage.azure.com/.default")
        return tok.token if tok else None
    except Exception:
        return None


def _load_domain(domain_name):
    registry = _get_domain_registry()
    module_path, class_name, _ = registry[domain_name]
    module = importlib.import_module(module_path)
    return getattr(module, class_name)(schema_mode="3nf")


def _write_to_lakehouse(result, token, table_prefix="spindle_live"):
    from deltalake import write_deltalake
    for table_name, df in result.tables.items():
        path = (
            f"abfss://{_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
            f"/{_LAKEHOUSE_ID}/Tables/{table_prefix}_{table_name}"
        )
        write_deltalake(
            path, df, mode="overwrite",
            storage_options={"bearer_token": token, "use_emulator": "false"},
            schema_mode="overwrite",
        )


# Group A: All 13 domains x lakehouse x small x seeding

ALL_DOMAINS = [
    "capital_markets", "education", "financial", "healthcare", "hr",
    "insurance", "iot", "manufacturing", "marketing", "real_estate",
    "retail", "supply_chain", "telecom",
]


@pytest.mark.live
@pytest.mark.parametrize("domain", ALL_DOMAINS)
def test_domain_to_lakehouse_small(domain):
    """All 13 domains write successfully to lakehouse at small scale."""
    token = _get_storage_token()
    assert token, "Could not acquire storage token — browser auth required"

    domain_obj = _load_domain(domain)
    result = Spindle().generate(domain=domain_obj, scale="small", seed=42)

    assert result.tables, f"{domain}: no tables generated"
    errors = result.verify_integrity()
    assert errors == [], f"{domain} FK integrity: {errors}"

    _write_to_lakehouse(result, token, table_prefix=f"spindle_live_{domain}")


# Group B: retail x all 5 sinks x fabric_demo x seeding

SINKS_FOR_GROUP_B = [
    ("lakehouse", None),
    ("warehouse", os.getenv("SPINDLE_TEST_WH_CONN")),
    ("eventhouse", os.getenv("SPINDLE_TEST_EH_CONN")),
    ("sql-database", os.getenv("SPINDLE_TEST_SQL_CONN")),
    ("sql-server", os.getenv("SPINDLE_TEST_ONPREM_CONN")),
]


@pytest.mark.live
@pytest.mark.parametrize("sink_type,conn", SINKS_FOR_GROUP_B)
def test_retail_all_sinks_fabric_demo(sink_type, conn):
    """Retail domain at fabric_demo scale against every sink type."""
    domain_obj = _load_domain("retail")
    size = "medium" if sink_type == "sql-server" else "fabric_demo"
    result = Spindle().generate(domain=domain_obj, scale=size, seed=42)
    assert result.tables

    if sink_type == "lakehouse":
        token = _get_storage_token()
        assert token
        _write_to_lakehouse(result, token, table_prefix="spindle_groupb_retail")

    elif sink_type in ("warehouse", "sql-database", "sql-server"):
        if not conn:
            pytest.skip(f"SPINDLE_TEST_{sink_type.upper().replace('-','_')}_CONN not set")
        auth = "sql" if sink_type == "sql-server" else "cli"
        writer = FabricSqlDatabaseWriter(conn, auth_method=auth)
        writer.write(result, schema_name="dbo", mode="create_insert")

    elif sink_type == "eventhouse":
        if not conn:
            pytest.skip("SPINDLE_TEST_EH_CONN not set")
        pytest.skip("Eventhouse live write not yet implemented in Phase 5")


# Group C: retail x lakehouse x all 4 sizes x streaming

@pytest.mark.live
@pytest.mark.parametrize("size", ["small", "medium", "large", "fabric_demo"])
def test_retail_lakehouse_streaming(size):
    """Retail streaming mode at every scale size writes to lakehouse."""
    token = _get_storage_token()
    assert token

    domain_obj = _load_domain("retail")
    from deltalake import write_deltalake

    tables_written = []
    for table_name, df in Spindle().generate_stream(domain=domain_obj, scale=size, seed=42):
        assert len(df) > 0, f"retail/{table_name}: 0 rows at size={size} in stream"
        path = (
            f"abfss://{_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
            f"/{_LAKEHOUSE_ID}/Tables/spindle_stream_{size}_{table_name}"
        )
        write_deltalake(
            path, df, mode="overwrite",
            storage_options={"bearer_token": token, "use_emulator": "false"},
            schema_mode="overwrite",
        )
        tables_written.append(table_name)

    assert tables_written, "No tables yielded in streaming mode"


# Group D: retail x warehouse x all sizes x seeding

@pytest.mark.live
@pytest.mark.parametrize("size", ["small", "medium", "large", "fabric_demo"])
def test_retail_warehouse_all_sizes(size):
    """Retail seeding at all sizes against Fabric Warehouse."""
    conn = os.getenv("SPINDLE_TEST_WH_CONN")
    if not conn:
        pytest.skip("SPINDLE_TEST_WH_CONN not set")

    domain_obj = _load_domain("retail")
    result = Spindle().generate(domain=domain_obj, scale=size, seed=42)
    assert result.tables

    writer = FabricSqlDatabaseWriter(conn, auth_method="cli")
    writer.write(result, schema_name="dbo", mode="create_insert")
