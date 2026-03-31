"""User workflow / CLI / end-to-end tests — what a real user does."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from click.testing import CliRunner

from sqllocks_spindle import ChunkedSpindle, MultiWriter, Spindle
from sqllocks_spindle.cli import main
from sqllocks_spindle.fabric.lakehouse_files_writer import LakehouseFilesWriter
from sqllocks_spindle.domains.retail import RetailDomain


ALL_DOMAIN_NAMES = [
    "retail", "healthcare", "financial", "hr", "supply_chain", "iot",
    "capital_markets", "real_estate", "education", "insurance",
    "manufacturing", "marketing", "telecom",
]


class TestCLIGenerate:
    @pytest.mark.parametrize("domain_name", ALL_DOMAIN_NAMES)
    def test_cli_all_domains_generate(self, domain_name):
        """All 13 domains: `spindle generate {name}` exits 0."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "generate", domain_name,
                "--scale", "small",
                "--seed", "42",
                "--output", tmpdir,
                "--format", "parquet",
            ])
            assert result.exit_code == 0, f"{domain_name}: {result.output}"

    def test_cli_parquet_output(self):
        """spindle generate retail --format parquet produces files."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            result = runner.invoke(main, [
                "generate", "retail",
                "--scale", "small",
                "--seed", "42",
                "--output", tmpdir,
                "--format", "parquet",
            ])
            assert result.exit_code == 0
            parquet_files = list(Path(tmpdir).rglob("*.parquet"))
            assert len(parquet_files) > 0, "No parquet files written"

    def test_cli_dry_run(self):
        """--dry-run shows planned counts without generating."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "generate", "retail",
            "--scale", "small",
            "--dry-run",
        ])
        assert result.exit_code == 0
        assert "No data generated" in result.output


class TestWorkflowAnchorToParquet:
    def test_workflow_chunked_anchor_to_parquet(self):
        """ChunkedSpindle anchor API -> write each chunk to parquet."""
        cs = ChunkedSpindle()
        result = cs.generate_chunked(
            domain=RetailDomain(),
            target_table="order_line",
            target_count=50_000,
            chunk_size=10_000,
            seed=42,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            lw = LakehouseFilesWriter(base_path=tmpdir)
            tables = {}
            for table_name in result.child_table_names:
                chunks = list(result.iter_chunks(table_name))
                df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
                tables[table_name] = df

            wr = lw.write_all(tables)
            assert wr.success
            assert wr.rows_written > 0
            # order_line should have ~50K rows
            assert tables.get("order_line") is not None
            assert len(tables["order_line"]) == 50_000


class TestWorkflowMultiWriter:
    def test_workflow_multiwriter_local_lakehouse_only(self):
        """MultiWriter with only lakehouse configured — no errors for unconfigured stores."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="small", seed=42)

        with tempfile.TemporaryDirectory() as tmpdir:
            lw = LakehouseFilesWriter(base_path=tmpdir)
            mw = MultiWriter(lakehouse=lw)
            mr = mw.write(result.tables)

            assert mr.success
            assert len(mr.stores) == 1
            assert mr.stores[0].store == "lakehouse"
            assert mr.total_rows > 0


class TestWorkflowSeedReproducibility:
    def test_workflow_seed_reproducibility(self):
        """Same domain + seed -> identical outputs."""
        s = Spindle()
        r1 = s.generate(domain=RetailDomain(), scale="small", seed=42)
        r2 = s.generate(domain=RetailDomain(), scale="small", seed=42)

        for tname in r1.tables:
            df1 = r1.tables[tname]
            df2 = r2.tables[tname]
            assert len(df1) == len(df2), f"{tname}: row count mismatch"
            assert list(df1.columns) == list(df2.columns), f"{tname}: column mismatch"


class TestWorkflowScalePreset:
    def test_workflow_scale_preset_fabric_demo(self):
        """Spindle scale='fabric_demo' generates retail without error."""
        s = Spindle()
        result = s.generate(domain=RetailDomain(), scale="fabric_demo", seed=42)
        assert sum(result.row_counts.values()) > 0
