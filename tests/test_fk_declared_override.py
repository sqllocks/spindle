"""STORY-017: declared FKs override detected advisory FKs (ADR-009)."""

from sqllocks_spindle.inference.lakehouse_profiler import LakehouseProfiler


def test_declared_overrides_detected_and_reports_it():
    detected = {
        "orders": {"customer_id": {"parent_table": "customer", "overlap": 0.95,
                                   "advisory": True, "full_scan": False}}
    }
    declared = [("orders", "customer_id", "account")]  # authoritative, conflicts
    out = LakehouseProfiler.reconcile_declared_foreign_keys(detected, declared)
    fk = out["foreign_keys"]["orders"]["customer_id"]
    # declared wins
    assert fk["parent_table"] == "account"
    assert fk["declared"] is True and fk["advisory"] is False
    # overridden detected FK is reported, not silently dropped
    assert len(out["overridden"]) == 1
    rep = out["overridden"][0]
    assert rep["detected_parent"] == "customer" and rep["declared_parent"] == "account"
    assert rep["detected_overlap"] == 0.95


def test_declared_adds_new_and_nonconflicting_passthrough():
    detected = {"orders": {"customer_id": {"parent_table": "customer", "overlap": 1.0,
                                           "advisory": True, "full_scan": False}}}
    # a declaration for a different column (no conflict) + agreeing declaration
    declared = [{"child_table": "orders", "child_col": "store_id", "parent_table": "store"},
                {"child_table": "orders", "child_col": "customer_id", "parent_table": "customer"}]
    out = LakehouseProfiler.reconcile_declared_foreign_keys(detected, declared)
    assert out["foreign_keys"]["orders"]["store_id"]["parent_table"] == "store"
    assert out["foreign_keys"]["orders"]["customer_id"]["parent_table"] == "customer"
    assert out["overridden"] == []  # agreeing declaration is not an override


def test_no_declarations_is_passthrough():
    detected = {"orders": {"customer_id": {"parent_table": "customer", "overlap": 0.9,
                                           "advisory": True, "full_scan": False}}}
    out = LakehouseProfiler.reconcile_declared_foreign_keys(detected, [])
    assert out["foreign_keys"] == detected
    assert out["overridden"] == []
