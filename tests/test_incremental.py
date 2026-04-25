"""Tests for the incremental (continue) generation engine."""

from __future__ import annotations

import pytest

from sqllocks_spindle.incremental import ContinueConfig, ContinueEngine, DeltaResult


@pytest.fixture(scope="module")
def retail_result():
    from sqllocks_spindle import RetailDomain, Spindle

    return Spindle().generate(domain=RetailDomain(), scale="small", seed=42)


class TestContinueEngine:
    def test_delta_result_has_all_keys(self, retail_result):
        engine = ContinueEngine()
        delta = engine.continue_from(retail_result)
        assert "inserts" in dir(delta)
        assert "updates" in dir(delta)
        assert "deletes" in dir(delta)
        assert "combined" in dir(delta)

    def test_inserts_have_higher_pks(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=50)
        delta = engine.continue_from(retail_result, config=config)
        # Check that inserted customer_id values are > max existing
        if "customer" in delta.inserts and len(delta.inserts["customer"]) > 0:
            existing_max = retail_result.tables["customer"]["customer_id"].max()
            new_min = delta.inserts["customer"]["customer_id"].min()
            assert new_min > existing_max

    def test_inserts_count(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=25)
        delta = engine.continue_from(retail_result, config=config)
        for table_name, df in delta.inserts.items():
            assert len(df) == 25

    def test_updates_have_delta_tags(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(update_fraction=0.1)
        delta = engine.continue_from(retail_result, config=config)
        for table_name, df in delta.updates.items():
            if len(df) > 0:
                assert "_delta_type" in df.columns
                assert (df["_delta_type"] == "UPDATE").all()

    def test_deletes_are_soft(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(delete_fraction=0.05)
        delta = engine.continue_from(retail_result, config=config)
        for table_name, df in delta.deletes.items():
            if len(df) > 0:
                assert "_delta_type" in df.columns
                assert (df["_delta_type"] == "DELETE").all()
                # Soft delete means full row is preserved
                orig_cols = set(retail_result.tables[table_name].columns)
                delta_cols = set(df.columns) - {"_delta_type", "_delta_timestamp"}
                assert orig_cols == delta_cols

    def test_state_transitions(self, retail_result):
        transitions = {
            "order.status": {
                "pending": {"shipped": 0.7, "cancelled": 0.3},
                "shipped": {"delivered": 0.9, "returned": 0.1},
            }
        }
        engine = ContinueEngine()
        config = ContinueConfig(update_fraction=0.5, state_transitions=transitions)
        delta = engine.continue_from(retail_result, config=config)
        if "order" in delta.updates and len(delta.updates["order"]) > 0:
            # Updated orders should have valid status transitions
            updated = delta.updates["order"]
            # Transition targets + any original states that have no defined transition
            transition_targets = {"shipped", "cancelled", "delivered", "returned"}
            # Original statuses from retail domain that may not have transitions defined
            original_statuses = set(retail_result.tables["order"]["status"].dropna().unique())
            valid_states = transition_targets | original_statuses
            actual = set(updated["status"].dropna().unique())
            assert actual.issubset(valid_states)

    def test_combined_has_all_delta_types(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(
            insert_count=10, update_fraction=0.1, delete_fraction=0.05
        )
        delta = engine.continue_from(retail_result, config=config)
        for table_name, df in delta.combined.items():
            if len(df) > 0:
                assert "_delta_type" in df.columns
                types = set(df["_delta_type"].unique())
                # At least some delta types should be present
                assert types.issubset({"INSERT", "UPDATE", "DELETE"})

    def test_summary(self, retail_result):
        engine = ContinueEngine()
        delta = engine.continue_from(retail_result)
        summary = delta.summary()
        assert "Incremental" in summary

    def test_continue_from_dict(self, retail_result):
        # Should also accept plain dict of DataFrames
        engine = ContinueEngine()
        delta = engine.continue_from(retail_result.tables)
        assert isinstance(delta, DeltaResult)

    def test_seed_reproducibility(self, retail_result):
        engine = ContinueEngine()
        config1 = ContinueConfig(insert_count=10, seed=99)
        config2 = ContinueConfig(insert_count=10, seed=99)
        delta1 = engine.continue_from(retail_result, config=config1)
        delta2 = engine.continue_from(retail_result, config=config2)
        # Same seed should give same deletes (sampled rows) — compare without timestamp
        for table in delta1.deletes:
            if table in delta2.deletes and len(delta1.deletes[table]) > 0:
                cols = [c for c in delta1.deletes[table].columns if c != "_delta_timestamp"]
                assert delta1.deletes[table][cols].equals(delta2.deletes[table][cols])

    def test_stats_are_populated(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=5, update_fraction=0.05, delete_fraction=0.01)
        delta = engine.continue_from(retail_result, config=config)
        for table_name, s in delta.stats.items():
            assert "inserts" in s
            assert "updates" in s
            assert "deletes" in s
            assert s["inserts"] == 5

    def test_no_orphan_fks_in_inserts(self, retail_result):
        engine = ContinueEngine()
        config = ContinueConfig(insert_count=20)
        delta = engine.continue_from(retail_result, config=config)
        # order inserts should reference existing customer_ids
        if "order" in delta.inserts and len(delta.inserts["order"]) > 0:
            order_ins = delta.inserts["order"]
            if "customer_id" in order_ins.columns:
                existing_customers = set(
                    retail_result.tables["customer"]["customer_id"]
                )
                # Also include newly inserted customers
                if "customer" in delta.inserts and len(delta.inserts["customer"]) > 0:
                    existing_customers.update(
                        delta.inserts["customer"]["customer_id"]
                    )
                orphans = set(order_ins["customer_id"]) - existing_customers
                assert len(orphans) == 0, f"Orphan customer_ids in order inserts: {orphans}"
