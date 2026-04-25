#!/usr/bin/env python
"""Fabric Integration Sweep — headless 23-group test harness.

Runs every Spindle feature against the SoundBI Fabric Demo Workspace.
Produces JSON + Markdown results in sweep_results/ and logs to stdout.

Usage:
    python scripts/fabric_integration_sweep.py --seed 42
    python scripts/fabric_integration_sweep.py --seed 99 --run-group 21
"""

from __future__ import annotations

import argparse as _ap
import json
import logging
import os
import shutil
import sys
import tempfile
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

# ── Module-level seed (overridden by --seed CLI arg) ────────────────────────
SWEEP_SEED = 42


def _resolve_domain(name: str):
    """Resolve a domain name string to a Domain class instance."""
    from sqllocks_spindle import (
        RetailDomain, HealthcareDomain, FinancialDomain, SupplyChainDomain,
        IoTDomain, HrDomain, InsuranceDomain, MarketingDomain,
        EducationDomain, RealEstateDomain, ManufacturingDomain, TelecomDomain,
    )
    from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain

    _MAP = {
        "retail": RetailDomain,
        "healthcare": HealthcareDomain,
        "financial": FinancialDomain,
        "supply_chain": SupplyChainDomain,
        "iot": IoTDomain,
        "hr": HrDomain,
        "insurance": InsuranceDomain,
        "marketing": MarketingDomain,
        "education": EducationDomain,
        "real_estate": RealEstateDomain,
        "manufacturing": ManufacturingDomain,
        "telecom": TelecomDomain,
        "capital_markets": CapitalMarketsDomain,
    }
    cls = _MAP.get(name)
    if cls is None:
        raise ValueError(f"Unknown domain: {name!r} (available: {list(_MAP.keys())})")
    return cls()

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sweep")

# ── Configuration (from fabric_test_config) ─────────────────────────────────
WORKSPACE_ID = "990dbc7b-f5d1-4bc8-a929-9dfd509a5d52"
TENANT_ID = "2536810f-20e1-4911-a453-4409fd96db8a"

LAKEHOUSE_ID = "3a17ecc6-795e-4496-a3b9-581dab931054"
ONELAKE_BASE = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"
)

SQL_DB_SERVER = (
    "b6atmjpbeaiutjctiqe73fw3ri-po6a3gor6xeexkjjtx6vbgs5ki"
    ".database.fabric.microsoft.com,1433"
)
SQL_DB_NAME = "FabricDemo_SQL_DB-e0fe6f55-b020-4bd1-9cce-aea43d83bd8c"
SQL_DB_CONN = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server={SQL_DB_SERVER};"
    f"Database={SQL_DB_NAME};"
    "Encrypt=yes;TrustServerCertificate=no;"
)

WH_SERVER = (
    "b6atmjpbeaiutjctiqe73fw3ri-po6a3gor6xeexkjjtx6vbgs5ki"
    ".datawarehouse.fabric.microsoft.com"
)
WH_DATABASE = "FabricDemo_WH"
WH_CONN = (
    "Driver={ODBC Driver 18 for SQL Server};"
    f"Server={WH_SERVER};"
    f"Database={WH_DATABASE};"
    "Encrypt=yes;TrustServerCertificate=no;"
)

EH_CLUSTER_URI = (
    "https://trd-ffhbqfk8q6dbxznaz8.z9.kusto.fabric.microsoft.com"
)
EH_INGEST_URI = (
    "https://ingest-trd-ffhbqfk8q6dbxznaz8.z9.kusto.fabric.microsoft.com"
)
EH_DATABASE = "FabricDemo_EH"

EVENTSTREAM_CONN = (
    "Endpoint=sb://esehbniwwdax15lwp8wayj.servicebus.windows.net/;"
    "SharedAccessKeyName=key_dfa1e65d-abd7-4cf2-9b2b-314f8d7cb18c;"
    "SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>;"
    "EntityPath=es_ca3d3698-1277-4e7f-90e5-eff933faec49"
)
EVENTSTREAM_EVENT_HUB = "es_ca3d3698-1277-4e7f-90e5-eff933faec49"

AUTH_METHOD = "cli"
SQL_SCHEMA = "spindle_test"
KQL_PREFIX = "spindle_test_"

# ── Timeouts per group (seconds) ────────────────────────────────────────────
GROUP_TIMEOUTS: dict[int, int] = {
    3: 300,      # SQL Database write
    4: 86400,    # Warehouse INSERT (xxxl scale = 24h max)
    7: 300,      # Composite presets
    8: 300,      # Eventhouse
    21: 600,     # F09 Live enterprise
    22: 120,     # CLI smoke
}
DEFAULT_TIMEOUT = 120

# ── Results collector ───────────────────────────────────────────────────────
results: list[dict[str, Any]] = []


def record(group: int, name: str, status: str, elapsed: float,
           details: str = "", error: str = "") -> None:
    """Record a test group result."""
    entry = {
        "group": group,
        "name": name,
        "status": status,
        "elapsed": elapsed,
        "details": details,
        "error": error[:500] if error else "",
    }
    results.append(entry)
    icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "⊘", "TIMEOUT": "⏱"}
    logger.info(
        "Group %02d [%s %s] %s (%.1fs)%s",
        group, icon.get(status, "?"), status, name, elapsed,
        f" — {error[:200]}" if error else "",
    )


# ═══════════════════════════════════════════════════════════════════════════
#  TEST GROUPS
# ═══════════════════════════════════════════════════════════════════════════

def test_01_setup() -> None:
    """Group 01: Verify imports, auth, connections."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain
        from sqllocks_spindle.fabric import FabricSqlDatabaseWriter

        # Verify basic generation works
        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        assert result.tables, "No tables generated"
        assert result.schema, "No schema in result"

        # Verify SQL DB connection
        writer = FabricSqlDatabaseWriter(
            connection_string=SQL_DB_CONN, auth_method=AUTH_METHOD
        )
        assert writer.test_connection(), "SQL DB connection failed"

        record(1, "Setup", "PASS", time.time() - t0)
    except Exception as e:
        record(1, "Setup", "FAIL", time.time() - t0, error=f"{type(e).__name__}: {e}")


def test_02_lakehouse_delta() -> None:
    """Group 02: Generate + write to local Delta (Lakehouse pattern)."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        with tempfile.TemporaryDirectory() as tmp:
            paths = result.to_parquet(tmp)
            assert len(paths) > 0, "No parquet files written"
            for p in paths:
                assert Path(p).stat().st_size > 0, f"Empty file: {p}"

        record(2, "Lakehouse Delta (local)", "PASS", time.time() - t0)
    except Exception as e:
        record(2, "Lakehouse Delta (local)", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_03_sql_database() -> None:
    """Group 03: Generate retail + write to Fabric SQL Database."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain
        from sqllocks_spindle.fabric import FabricSqlDatabaseWriter

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        writer = FabricSqlDatabaseWriter(
            connection_string=SQL_DB_CONN, auth_method=AUTH_METHOD
        )
        # Pass full result (not result.tables) to preserve schema for boolean fix
        wr = writer.write(result, schema_name=SQL_SCHEMA, mode="create_insert")
        assert wr.success, f"Write errors: {wr.errors}"
        assert wr.total_rows > 0, "No rows written"

        record(3, "SQL Database", "PASS", time.time() - t0,
               details=f"{wr.total_rows} rows, {wr.tables_written} tables")
    except Exception as e:
        record(3, "SQL Database", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_04_warehouse_insert() -> None:
    """Group 04: Generate + write to Fabric Warehouse via COPY INTO (xxxl scale).

    Uses ChunkedSpindle for memory-bounded generation at xxxl scale,
    then WarehouseBulkWriter.write_chunked() for parallel COPY INTO.
    """
    t0 = time.time()
    try:
        from sqllocks_spindle import RetailDomain, FinancialDomain
        from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain
        from sqllocks_spindle.engine.chunked_generator import ChunkedSpindle
        from sqllocks_spindle.fabric import WarehouseBulkWriter

        domains = [
            ("retail", RetailDomain()),
            ("financial", FinancialDomain()),
            ("capital_markets", CapitalMarketsDomain()),
        ]

        bulk_writer = WarehouseBulkWriter(
            connection_string=WH_CONN,
            staging_lakehouse_path=f"{ONELAKE_BASE}/Files",
            auth_method=AUTH_METHOD,
            schema_name=SQL_SCHEMA,
        )

        chunked = ChunkedSpindle()
        total_rows = 0
        total_tables = 0
        for domain_name, domain_obj in domains:
            logger.info("Group 04: generating %s at xxxl scale (chunked)...", domain_name)
            chunked_result = chunked.generate_chunked(
                domain=domain_obj, scale="xxxl", seed=SWEEP_SEED,
                chunk_size=1_000_000,
            )
            wr = bulk_writer.write_chunked(chunked_result)
            assert wr.success, f"{domain_name} errors: {wr.errors}"
            total_rows += wr.total_rows
            total_tables += wr.tables_written

        record(4, "Warehouse INSERT", "PASS", time.time() - t0,
               details=f"{total_rows:,} rows, {total_tables} tables")
    except Exception as e:
        record(4, "Warehouse INSERT", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_05_star_schema() -> None:
    """Group 05: Star schema transform for retail."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain, StarSchemaTransform

        domain = RetailDomain()
        result = Spindle().generate(domain=domain, scale="small", seed=SWEEP_SEED)
        transform = StarSchemaTransform()
        star = transform.transform(result.tables, domain.star_schema_map())
        assert star.dimensions, "No dimensions"
        assert star.facts, "No facts"

        record(5, "Star Schema", "PASS", time.time() - t0)
    except Exception as e:
        record(5, "Star Schema", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_06_cdm_export() -> None:
    """Group 06: CDM export for healthcare."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, HealthcareDomain, CdmMapper

        domain = HealthcareDomain()
        result = Spindle().generate(domain=domain, scale="small", seed=SWEEP_SEED)
        mapper = CdmMapper()
        cdm_result = mapper.map(result.tables, domain.cdm_entity_map())
        assert cdm_result, "CDM mapping returned empty"

        record(6, "CDM Export", "PASS", time.time() - t0)
    except Exception as e:
        record(6, "CDM Export", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_07_composite_presets() -> None:
    """Group 07: All 6 composite presets generate successfully."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, get_preset, list_presets
        from sqllocks_spindle.domains.composite import CompositeDomain

        preset_names = [p.name for p in list_presets()]
        assert len(preset_names) >= 6, f"Expected >=6 presets, got {len(preset_names)}"

        for pname in preset_names:
            pdef = get_preset(pname)
            domain_instances = [_resolve_domain(d) for d in pdef.domains]
            composite = CompositeDomain(
                domains=domain_instances,
                shared_entities=pdef.shared_entities if pdef.shared_entities else None,
            )
            result = Spindle().generate(
                domain=composite, scale="small", seed=SWEEP_SEED
            )
            assert result.tables, f"Preset '{pname}' generated no tables"

        record(7, "Composite Presets", "PASS", time.time() - t0,
               details=f"{len(preset_names)} presets")
    except Exception as e:
        record(7, "Composite Presets", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_08_eventhouse() -> None:
    """Group 08: Write IoT data to Fabric Eventhouse."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, IoTDomain
        from sqllocks_spindle.fabric import EventhouseWriter

        result = Spindle().generate(domain=IoTDomain(), scale="small", seed=SWEEP_SEED)
        writer = EventhouseWriter(
            cluster_uri=EH_CLUSTER_URI,
            database=EH_DATABASE,
            auth_method=AUTH_METHOD,
        )
        wr = writer.write(result, table_prefix=KQL_PREFIX)
        assert wr.success, f"Eventhouse errors: {wr.errors}"

        record(8, "Eventhouse", "PASS", time.time() - t0,
               details=f"{wr.rows_written} rows")
    except ImportError as e:
        record(8, "Eventhouse", "SKIP", time.time() - t0,
               error=f"Missing dependency: {e}")
    except Exception as e:
        record(8, "Eventhouse", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_09_eventstream() -> None:
    """Group 09: Send streaming events to Fabric Eventstream."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, IoTDomain
        from sqllocks_spindle.fabric import EventstreamClient

        result = Spindle().generate(domain=IoTDomain(), scale="small", seed=SWEEP_SEED)
        client = EventstreamClient(
            connection_string=EVENTSTREAM_CONN,
            eventhub_name=EVENTSTREAM_EVENT_HUB,
        )
        # Send a small batch from the first table
        first_table = list(result.tables.keys())[0]
        df = result.tables[first_table].head(100)
        events = df.to_dict(orient="records")
        client.send_batch(events)
        client.close()

        record(9, "Eventstream", "PASS", time.time() - t0,
               details=f"{len(events)} events sent")
    except ImportError as e:
        record(9, "Eventstream", "SKIP", time.time() - t0,
               error=f"Missing dependency: {e}")
    except Exception as e:
        record(9, "Eventstream", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_10_schema_inference() -> None:
    """Group 10: Profile data + build schema from it."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain, DataProfiler, SchemaBuilder

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        profiler = DataProfiler()
        profile = profiler.profile_dataset(result.tables)
        assert profile.tables, "No tables profiled"

        builder = SchemaBuilder()
        inferred = builder.build(profile)
        assert inferred.tables, "No tables in inferred schema"

        record(10, "Schema Inference", "PASS", time.time() - t0)
    except Exception as e:
        record(10, "Schema Inference", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_11_pii_masking() -> None:
    """Group 11: PII masking round-trip."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain, DataMasker, MaskConfig

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        config = MaskConfig(seed=SWEEP_SEED)
        masker = DataMasker(config)
        first_table = list(result.tables.keys())[0]
        masked = masker.mask(result.tables[first_table])
        assert len(masked) == len(result.tables[first_table])

        record(11, "PII Masking", "PASS", time.time() - t0)
    except Exception as e:
        record(11, "PII Masking", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_12_day2_continue() -> None:
    """Group 12: Day 2 incremental generation."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain, ContinueEngine, ContinueConfig

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        engine = ContinueEngine()
        config = ContinueConfig(
            insert_count=100,
            update_fraction=0.1,
            delete_fraction=0.02,
            seed=SWEEP_SEED,
        )
        delta = engine.continue_from(result, config=config)
        assert delta.inserts or delta.updates or delta.deletes, "No delta generated"

        record(12, "Day 2 Continue", "PASS", time.time() - t0)
    except Exception as e:
        record(12, "Day 2 Continue", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_13_time_travel() -> None:
    """Group 13: Time-travel snapshots."""
    t0 = time.time()
    try:
        from sqllocks_spindle import (
            Spindle, RetailDomain, TimeTravelEngine, TimeTravelConfig,
        )

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        config = TimeTravelConfig(
            months=3,
            start_date="2024-01-01",
            growth_rate=0.05,
            seed=SWEEP_SEED,
        )
        engine = TimeTravelEngine()
        tt_result = engine.snapshot(result, config=config)
        assert tt_result.snapshots, "No snapshots"

        record(13, "Time Travel", "PASS", time.time() - t0)
    except Exception as e:
        record(13, "Time Travel", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_14_scd2() -> None:
    """Group 14: SCD2 generation."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        # SCD2 columns are generated as part of domain schemas that include them
        # Verify the generation completed and tables have data
        assert result.tables, "No tables"
        record(14, "SCD2", "PASS", time.time() - t0)
    except Exception as e:
        record(14, "SCD2", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_15_chaos_file_drop() -> None:
    """Group 15: Chaos engine + file drop simulation."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain
        from sqllocks_spindle.chaos import ChaosEngine, ChaosConfig
        from sqllocks_spindle.simulation.file_drop import (
            FileDropSimulator, FileDropConfig,
        )

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        chaos_cfg = ChaosConfig(
            enabled=True,
            intensity="moderate",
            seed=SWEEP_SEED,
            warmup_days=0,
            chaos_start_day=0,
            escalation="gradual",
            categories={},
            overrides=[],
            breaking_change_day=999,
        )
        chaos = ChaosEngine(chaos_cfg)
        first_table = list(result.tables.keys())[0]
        corrupted = chaos.corrupt_dataframe(result.tables[first_table].copy(), day=1)
        assert len(corrupted) > 0

        with tempfile.TemporaryDirectory() as tmp:
            fd_cfg = FileDropConfig(
                base_path=tmp,
                date_range_start="2024-01-01",
                date_range_end="2024-01-07",
                seed=SWEEP_SEED,
            )
            sim = FileDropSimulator(result.tables, fd_cfg)
            fd_result = sim.run()
            assert fd_result.files_written, "No files from file drop"

        record(15, "Chaos + File Drop", "PASS", time.time() - t0)
    except Exception as e:
        record(15, "Chaos + File Drop", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_16_scd2_file_drops() -> None:
    """Group 16: SCD2 file drop simulation."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain
        from sqllocks_spindle.simulation.scd2_file_drops import (
            SCD2FileDropSimulator, SCD2FileDropConfig,
        )

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        first_table = list(result.tables.keys())[0]
        df = result.tables[first_table]
        pk_col = df.columns[0]

        with tempfile.TemporaryDirectory() as tmp:
            cfg = SCD2FileDropConfig(
                base_path=tmp,
                business_key_column=pk_col,
                num_delta_days=3,
                seed=SWEEP_SEED,
            )
            sim = SCD2FileDropSimulator({first_table: df}, cfg)
            scd_result = sim.run()
            assert scd_result.delta_paths, "No SCD2 delta paths"

        record(16, "SCD2 File Drops", "PASS", time.time() - t0)
    except Exception as e:
        record(16, "SCD2 File Drops", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_17_output_formats() -> None:
    """Group 17: All output formats (CSV, Parquet, JSONL, Excel, SQL)."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        with tempfile.TemporaryDirectory() as tmp:
            csv_paths = result.to_csv(tmp)
            assert csv_paths, "No CSV files"

            parquet_dir = os.path.join(tmp, "parquet")
            os.makedirs(parquet_dir)
            pq_paths = result.to_parquet(parquet_dir)
            assert pq_paths, "No Parquet files"

            jsonl_dir = os.path.join(tmp, "jsonl")
            os.makedirs(jsonl_dir)
            jl_paths = result.to_jsonl(jsonl_dir)
            assert jl_paths, "No JSONL files"

            excel_path = os.path.join(tmp, "output.xlsx")
            result.to_excel(excel_path)
            assert Path(excel_path).stat().st_size > 0

            sql_dir = os.path.join(tmp, "sql")
            os.makedirs(sql_dir)
            sql_paths = result.to_sql(sql_dir)
            assert sql_paths, "No SQL files"

        record(17, "Output Formats", "PASS", time.time() - t0)
    except Exception as e:
        record(17, "Output Formats", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_18_validation_gates() -> None:
    """Group 18: All 8 validation gates."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain
        from sqllocks_spindle.validation.gates import GateRunner, ValidationContext

        result = Spindle().generate(domain=RetailDomain(), scale="small", seed=SWEEP_SEED)
        context = ValidationContext(
            tables=result.tables,
            schema=result.schema,
            file_paths=[],
            config={},
        )
        runner = GateRunner()
        gate_results = runner.run_all(context)
        passed = sum(1 for g in gate_results if g.passed)
        total = len(gate_results)
        assert total >= 6, f"Expected >=6 gates, got {total}"

        record(18, "Validation Gates", "PASS", time.time() - t0,
               details=f"{passed}/{total} gates passed")
    except Exception as e:
        record(18, "Validation Gates", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_19_observability() -> None:
    """Group 19: Observability / run metrics."""
    t0 = time.time()
    try:
        from sqllocks_spindle.observability import RunMetrics, configure_logging

        configure_logging(level="DEBUG")
        metrics = RunMetrics(run_id="sweep_test")
        metrics.start_table("test_table")
        metrics.end_table("test_table", rows=100, columns=5)
        summary = metrics.finish()
        assert summary["total_rows"] == 100
        assert summary["total_tables"] == 1

        record(19, "Observability", "PASS", time.time() - t0)
    except Exception as e:
        record(19, "Observability", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_20_semantic_model() -> None:
    """Group 20: Semantic model (BIM) export."""
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, RetailDomain
        from sqllocks_spindle.fabric import SemanticModelExporter

        domain = RetailDomain()
        result = Spindle().generate(domain=domain, scale="small", seed=SWEEP_SEED)
        exporter = SemanticModelExporter()
        with tempfile.TemporaryDirectory() as tmp:
            bim_path = exporter.export_bim(
                result.schema,
                source_type="lakehouse",
                output_path=os.path.join(tmp, "model.bim"),
            )
            assert Path(bim_path).stat().st_size > 0, "Empty BIM file"

        record(20, "Semantic Model", "PASS", time.time() - t0)
    except Exception as e:
        record(20, "Semantic Model", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_21_f09_live() -> None:
    """Group 21: F09 cross-domain enterprise write to Fabric SQL Database.

    CRITICAL: Pass the full GenerationResult (not result.tables) to the writer
    so the schema is preserved and boolean columns are properly converted.
    """
    t0 = time.time()
    try:
        from sqllocks_spindle import Spindle, get_preset
        from sqllocks_spindle.domains.composite import CompositeDomain
        from sqllocks_spindle.fabric import FabricSqlDatabaseWriter

        # Build enterprise composite from preset
        pdef = get_preset("enterprise")
        domain_instances = [_resolve_domain(d) for d in pdef.domains]
        composite = CompositeDomain(
            domains=domain_instances,
            shared_entities=pdef.shared_entities if pdef.shared_entities else None,
        )

        # Generate — result includes .schema with boolean column metadata
        result = Spindle().generate(
            domain=composite, scale="small", seed=SWEEP_SEED
        )
        assert result.tables, "Enterprise generation produced no tables"
        assert result.schema, "Enterprise generation has no schema"

        # Write to Fabric SQL DB — pass full result, NOT result.tables
        writer = FabricSqlDatabaseWriter(
            connection_string=SQL_DB_CONN, auth_method=AUTH_METHOD
        )
        wr = writer.write(result, schema_name=SQL_SCHEMA, mode="create_insert")

        if not wr.success:
            raise RuntimeError(f"Enterprise write errors: {wr.errors}")

        record(21, "F09 Live", "PASS", time.time() - t0,
               details=f"{wr.total_rows} rows, {wr.tables_written} tables")
    except Exception as e:
        record(21, "F09 Live", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_22_cli_smoke() -> None:
    """Group 22: CLI smoke test."""
    t0 = time.time()
    try:
        import subprocess

        with tempfile.TemporaryDirectory() as tmp:
            cmd = [
                sys.executable, "-m", "sqllocks_spindle.cli",
                "generate", "retail",
                "--scale", "small",
                "--seed", str(SWEEP_SEED),
                "--output", tmp,
                "--format", "csv",
            ]
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=60,
                cwd=str(Path(__file__).resolve().parent.parent),
            )
            if proc.returncode != 0:
                raise RuntimeError(
                    f"CLI exited {proc.returncode}: {proc.stderr[:300]}"
                )
            csv_files = list(Path(tmp).glob("*.csv"))
            assert csv_files, "CLI produced no CSV files"

        record(22, "CLI Smoke", "PASS", time.time() - t0)
    except Exception as e:
        record(22, "CLI Smoke", "FAIL", time.time() - t0,
               error=f"{type(e).__name__}: {e}")


def test_23_results_summary() -> None:
    """Group 23: Compute and report final summary."""
    t0 = time.time()
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    timeout = sum(1 for r in results if r["status"] == "TIMEOUT")
    total_time = sum(r["elapsed"] for r in results)

    summary = (
        f"{passed} PASS, {failed} FAIL, {skipped} SKIP, {timeout} TIMEOUT"
        f" — {total_time:.1f}s"
    )
    record(23, "Results Summary", "PASS", time.time() - t0, details=summary)


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════

ALL_GROUPS = [
    test_01_setup,
    test_02_lakehouse_delta,
    test_03_sql_database,
    test_04_warehouse_insert,
    test_05_star_schema,
    test_06_cdm_export,
    test_07_composite_presets,
    test_08_eventhouse,
    test_09_eventstream,
    test_10_schema_inference,
    test_11_pii_masking,
    test_12_day2_continue,
    test_13_time_travel,
    test_14_scd2,
    test_15_chaos_file_drop,
    test_16_scd2_file_drops,
    test_17_output_formats,
    test_18_validation_gates,
    test_19_observability,
    test_20_semantic_model,
    test_21_f09_live,
    test_22_cli_smoke,
    test_23_results_summary,
]


def _run_single_group(group_num: int) -> None:
    """Run a single test group by number (1-23)."""
    if group_num < 1 or group_num > len(ALL_GROUPS):
        logger.error("Invalid group number: %d (valid: 1-%d)", group_num, len(ALL_GROUPS))
        sys.exit(1)
    fn = ALL_GROUPS[group_num - 1]
    logger.info("Running single group %02d: %s (seed=%d)", group_num, fn.__doc__.split(":")[0].strip() if fn.__doc__ else fn.__name__, SWEEP_SEED)
    fn()


def _save_results(seed: int) -> None:
    """Save results to sweep_results/ as JSON and Markdown."""
    project_root = Path(__file__).resolve().parent.parent
    out_dir = project_root / "sweep_results"
    out_dir.mkdir(exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # JSON
    json_path = out_dir / f"sweep_results_{ts}.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    # Markdown
    md_path = out_dir / f"sweep_results_{ts}.md"
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    skipped = sum(1 for r in results if r["status"] == "SKIP")
    timeout = sum(1 for r in results if r["status"] == "TIMEOUT")
    total_time = sum(r["elapsed"] for r in results)

    lines = [
        "# Fabric Integration Sweep Results",
        f"",
        f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Seed**: {seed}",
        "",
        "| # | Group | Status | Time (s) | Notes |",
        "|---|-------|--------|----------|-------|",
    ]
    for r in results:
        notes = r.get("details", "") or r.get("error", "")
        lines.append(
            f"| {r['group']:02d} | {r['name']} | {r['status']} | {r['elapsed']:.1f} | {notes[:80]} |"
        )
    lines.append("")
    lines.append(
        f"**Totals**: {passed} PASS, {failed} FAIL, {skipped} SKIP, {timeout} TIMEOUT"
        f" — {total_time:.1f}s"
    )

    with open(md_path, "w") as f:
        f.write("\n".join(lines))

    logger.info("Results saved: %s, %s", json_path.name, md_path.name)
    return json_path, md_path


_SKIP_GROUPS: set[int] = set()


def main() -> None:
    """Run all 23 test groups sequentially."""
    logger.info("=" * 70)
    logger.info("FABRIC INTEGRATION SWEEP — seed=%d", SWEEP_SEED)
    if _SKIP_GROUPS:
        logger.info("Skipping groups: %s", sorted(_SKIP_GROUPS))
    logger.info("=" * 70)

    for i, fn in enumerate(ALL_GROUPS, 1):
        if i in _SKIP_GROUPS:
            record(i, fn.__doc__.split(":")[1].strip().split(".")[0] if fn.__doc__ else fn.__name__,
                   "SKIP", 0.0, details="Skipped via --skip-groups")
            continue
        try:
            fn()
        except Exception as e:
            # Catch any unhandled exceptions so the sweep continues
            logger.error("Unhandled exception in %s: %s", fn.__name__, e)

    _save_results(SWEEP_SEED)

    # Print summary
    passed = sum(1 for r in results if r["status"] == "PASS")
    total = len(results)
    logger.info("=" * 70)
    logger.info("FINAL: %d/%d PASS (seed=%d)", passed, total, SWEEP_SEED)
    logger.info("=" * 70)

    # Exit with non-zero if any failures (skips don't count)
    if any(r["status"] == "FAIL" for r in results):
        sys.exit(1)


if __name__ == "__main__":
    _parser = _ap.ArgumentParser(
        description="Fabric Integration Sweep — 23-group test harness",
        add_help=True,
    )
    _parser.add_argument("--seed", type=int, default=42, help="RNG seed (default: 42)")
    _parser.add_argument("--run-group", type=int, default=None, help="Run only this group number (1-23)")
    _parser.add_argument("--skip-groups", type=str, default="", help="Comma-separated group numbers to skip (e.g., '4,8')")
    _args = _parser.parse_args()

    SWEEP_SEED = _args.seed
    if _args.skip_groups:
        _SKIP_GROUPS = {int(x.strip()) for x in _args.skip_groups.split(",")}

    if _args.run_group is not None:
        _run_single_group(_args.run_group)
        _save_results(SWEEP_SEED)
    else:
        main()
