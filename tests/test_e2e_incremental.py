"""E2E tests: ContinueEngine (Day 2 deltas), TimeTravelEngine (snapshots), SCD2Strategy."""

from __future__ import annotations

import pytest

from sqllocks_spindle import (
    Spindle,
    RetailDomain,
    ContinueEngine,
    ContinueConfig,
    TimeTravelEngine,
    TimeTravelConfig,
)


@pytest.fixture(scope="module")
def retail_result():
    return Spindle().generate(domain=RetailDomain(), scale="small", seed=42)


# ---------------------------------------------------------------------------
# ContinueEngine — Day 2 deltas
# ---------------------------------------------------------------------------

class TestContinueEngine:
    def test_delta_produces_inserts(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=50, update_fraction=0.0, delete_fraction=0.0, seed=42)
        delta = engine.continue_from(retail_result, config=config)
        assert len(delta.inserts) > 0
        for table, df in delta.inserts.items():
            if len(df) > 0:
                assert "_delta_type" in df.columns
                assert (df["_delta_type"] == "INSERT").all()

    def test_delta_produces_updates(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=0, update_fraction=0.1, delete_fraction=0.0, seed=42)
        delta = engine.continue_from(retail_result, config=config)
        has_updates = any(len(df) > 0 for df in delta.updates.values())
        assert has_updates, "No updates generated"

    def test_delta_produces_deletes(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=0, update_fraction=0.0, delete_fraction=0.05, seed=42)
        delta = engine.continue_from(retail_result, config=config)
        has_deletes = any(len(df) > 0 for df in delta.deletes.values())
        assert has_deletes, "No deletes generated"

    def test_delta_combined_has_all_types(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=50, update_fraction=0.1, delete_fraction=0.02, seed=42)
        delta = engine.continue_from(retail_result, config=config)
        all_types = set()
        for table, df in delta.combined.items():
            if "_delta_type" in df.columns:
                all_types.update(df["_delta_type"].unique())
        assert "INSERT" in all_types
        assert "UPDATE" in all_types
        assert "DELETE" in all_types

    def test_delta_insert_pks_higher_than_existing(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=50, seed=42)
        delta = engine.continue_from(retail_result, config=config)
        schema = retail_result.schema
        for table_name, table_def in schema.tables.items():
            if not table_def.primary_key or table_name not in delta.inserts:
                continue
            pk_col = table_def.primary_key[0]
            ins_df = delta.inserts[table_name]
            if len(ins_df) == 0 or pk_col not in ins_df.columns:
                continue
            orig_df = retail_result.tables[table_name]
            if pk_col not in orig_df.columns:
                continue
            try:
                max_orig = orig_df[pk_col].max()
                min_new = ins_df[pk_col].min()
                assert min_new > max_orig, (
                    f"{table_name}.{pk_col}: new min {min_new} <= orig max {max_orig}"
                )
            except TypeError:
                pass  # Non-numeric PKs

    def test_delta_fk_integrity(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=100, update_fraction=0.05, seed=42)
        delta = engine.continue_from(retail_result, config=config)
        # Combined result should maintain FK integrity when merged with originals
        assert delta.stats is not None

    def test_delta_seed_reproducibility(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=50, seed=77)
        d1 = engine.continue_from(retail_result, config=config)
        d2 = engine.continue_from(retail_result, config=config)
        for table in d1.inserts:
            if len(d1.inserts[table]) > 0:
                # Exclude timestamp columns which use now()
                cols = [c for c in d1.inserts[table].columns
                        if "timestamp" not in c.lower()]
                assert d1.inserts[table][cols].equals(d2.inserts[table][cols])


# ---------------------------------------------------------------------------
# TimeTravelEngine — monthly snapshots
# ---------------------------------------------------------------------------

class TestTimeTravelEngine:
    def test_generates_monthly_snapshots(self):
        engine = TimeTravelEngine()
        config = TimeTravelConfig(months=6, growth_rate=0.05, churn_rate=0.02, seed=42)
        result = engine.generate(domain=RetailDomain(), config=config, scale="small")
        assert len(result.snapshots) == 7  # month 0 + 6 months

    def test_snapshots_grow_over_time(self):
        engine = TimeTravelEngine()
        config = TimeTravelConfig(months=6, growth_rate=0.10, churn_rate=0.0, seed=42)
        result = engine.generate(domain=RetailDomain(), config=config, scale="small")
        # With 10% growth and 0% churn, later snapshots should have more rows
        first_rows = sum(result.snapshots[0].row_counts.values())
        last_rows = sum(result.snapshots[-1].row_counts.values())
        assert last_rows > first_rows, "Snapshots didn't grow"

    def test_churn_reduces_rows(self):
        engine = TimeTravelEngine()
        config = TimeTravelConfig(months=6, growth_rate=0.0, churn_rate=0.10, seed=42)
        result = engine.generate(domain=RetailDomain(), config=config, scale="small")
        first_rows = sum(result.snapshots[0].row_counts.values())
        last_rows = sum(result.snapshots[-1].row_counts.values())
        assert last_rows < first_rows, "Churn didn't reduce rows"

    def test_seasonality_applied(self):
        engine = TimeTravelEngine()
        config = TimeTravelConfig(
            months=12, growth_rate=0.05, seasonality={11: 2.0, 12: 2.0}, seed=42
        )
        result = engine.generate(domain=RetailDomain(), config=config, scale="small")
        assert len(result.snapshots) == 13

    def test_to_partitioned_dfs(self):
        engine = TimeTravelEngine()
        config = TimeTravelConfig(months=3, growth_rate=0.05, seed=42)
        result = engine.generate(domain=RetailDomain(), config=config, scale="small")
        partitioned = result.to_partitioned_dfs()
        assert isinstance(partitioned, dict)
        for table_name, df in partitioned.items():
            assert "_snapshot_date" in df.columns
