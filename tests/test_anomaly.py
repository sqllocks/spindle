"""Tests for the Spindle anomaly injection system (Phase 2)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.streaming.anomaly import (
    AnomalyRegistry,
    CollectiveAnomaly,
    ContextualAnomaly,
    PointAnomaly,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rng():
    return np.random.default_rng(99)


@pytest.fixture
def order_df():
    """Small synthetic order table for anomaly testing."""
    n = 200
    rng = np.random.default_rng(7)
    return pd.DataFrame({
        "order_id": range(1, n + 1),
        "customer_id": rng.integers(1, 20, size=n),
        "total_amount": rng.uniform(10.0, 150.0, size=n).round(2),
        "order_month": rng.integers(1, 13, size=n),
        "product_category": rng.choice(["Electronics", "Clothing", "Books"], size=n),
        "order_date": pd.date_range("2024-01-01", periods=n, freq="h"),
    })


# ---------------------------------------------------------------------------
# PointAnomaly
# ---------------------------------------------------------------------------

class TestPointAnomaly:
    def test_inject_marks_correct_fraction(self, order_df, rng):
        anomaly = PointAnomaly("big_order", column="total_amount", fraction=0.05)
        result = anomaly.inject(order_df, rng)
        n_flagged = int(result["_spindle_is_anomaly"].sum())
        expected = max(1, int(len(order_df) * 0.05))
        assert n_flagged == expected

    def test_inject_values_are_extreme(self, order_df, rng):
        anomaly = PointAnomaly("big_order", column="total_amount", multiplier_range=(50, 100), fraction=0.05)
        result = anomaly.inject(order_df, rng)
        baseline_mean = order_df["total_amount"].mean()
        flagged = result.loc[result["_spindle_is_anomaly"] == True, "total_amount"]
        assert (flagged > baseline_mean * 10).all()

    def test_anomaly_type_label(self, order_df, rng):
        anomaly = PointAnomaly("big_order", column="total_amount", fraction=0.05)
        result = anomaly.inject(order_df, rng)
        flagged_types = result.loc[result["_spindle_is_anomaly"] == True, "_spindle_anomaly_type"]
        assert (flagged_types == "point:big_order").all()

    def test_missing_column_returns_unchanged(self, order_df, rng):
        anomaly = PointAnomaly("x", column="nonexistent_col", fraction=0.1)
        result = anomaly.inject(order_df, rng)
        assert "_spindle_is_anomaly" not in result.columns

    def test_empty_df_returns_unchanged(self, rng):
        anomaly = PointAnomaly("x", column="val", fraction=0.1)
        df = pd.DataFrame({"val": pd.Series([], dtype=float)})
        result = anomaly.inject(df, rng)
        assert result.empty

    def test_anomaly_type_property(self):
        anomaly = PointAnomaly("my_anomaly", column="col")
        assert anomaly.anomaly_type == "point:my_anomaly"

    def test_fraction_property(self):
        anomaly = PointAnomaly("x", column="col", fraction=0.03)
        assert anomaly.fraction == pytest.approx(0.03)


# ---------------------------------------------------------------------------
# ContextualAnomaly
# ---------------------------------------------------------------------------

class TestContextualAnomaly:
    def test_inject_changes_column_in_eligible_rows(self, order_df, rng):
        anomaly = ContextualAnomaly(
            name="winter_in_summer",
            column="product_category",
            condition_column="order_month",
            normal_values=[6, 7, 8],
            anomalous_values=["Winter Coats"],
            fraction=0.5,
        )
        result = anomaly.inject(order_df, rng)
        flagged = result[result["_spindle_is_anomaly"] == True]
        assert (flagged["product_category"] == "Winter Coats").all()

    def test_only_eligible_rows_affected(self, order_df, rng):
        """Rows where condition_column is NOT in normal_values must be untouched."""
        anomaly = ContextualAnomaly(
            name="test",
            column="product_category",
            condition_column="order_month",
            normal_values=[6, 7, 8],
            anomalous_values=["Winter Coats"],
            fraction=0.5,
        )
        result = anomaly.inject(order_df, rng)
        # Non-summer rows must not appear in the flagged set.
        # (Label column may be absent for unset rows when calling inject() directly.)
        if "_spindle_is_anomaly" in result.columns:
            flagged_idx = set(result.index[result["_spindle_is_anomaly"] == True])
            non_summer_idx = set(result.index[~result["order_month"].isin([6, 7, 8])])
            assert len(flagged_idx & non_summer_idx) == 0

    def test_missing_column_skips_gracefully(self, order_df, rng):
        anomaly = ContextualAnomaly(
            name="x",
            column="missing_col",
            condition_column="order_month",
            normal_values=[6],
            anomalous_values=["val"],
        )
        result = anomaly.inject(order_df, rng)
        assert "_spindle_is_anomaly" not in result.columns

    def test_missing_condition_column_skips_gracefully(self, order_df, rng):
        anomaly = ContextualAnomaly(
            name="x",
            column="product_category",
            condition_column="missing_condition",
            normal_values=["A"],
            anomalous_values=["B"],
        )
        result = anomaly.inject(order_df, rng)
        assert "_spindle_is_anomaly" not in result.columns

    def test_no_eligible_rows_returns_unchanged(self, order_df, rng):
        anomaly = ContextualAnomaly(
            name="x",
            column="product_category",
            condition_column="order_month",
            normal_values=[99],  # no rows match
            anomalous_values=["X"],
        )
        result = anomaly.inject(order_df, rng)
        assert "_spindle_is_anomaly" not in result.columns

    def test_anomaly_type_property(self):
        anomaly = ContextualAnomaly(
            "my_ctx", column="c", condition_column="d",
            normal_values=[], anomalous_values=[],
        )
        assert anomaly.anomaly_type == "contextual:my_ctx"


# ---------------------------------------------------------------------------
# CollectiveAnomaly
# ---------------------------------------------------------------------------

class TestCollectiveAnomaly:
    def test_inject_clusters_timestamps(self, order_df, rng):
        anomaly = CollectiveAnomaly(
            name="velocity_fraud",
            group_column="customer_id",
            timestamp_column="order_date",
            window_seconds=600.0,
            fraction=0.1,
        )
        result = anomaly.inject(order_df, rng)
        flagged = result[result["_spindle_is_anomaly"] == True]
        assert len(flagged) > 0

    def test_flagged_rows_labelled_correctly(self, order_df, rng):
        anomaly = CollectiveAnomaly(
            name="burst",
            group_column="customer_id",
            timestamp_column="order_date",
            fraction=0.2,
        )
        result = anomaly.inject(order_df, rng)
        flagged = result[result["_spindle_is_anomaly"] == True]
        assert (flagged["_spindle_anomaly_type"] == "collective:burst").all()

    def test_missing_group_column_skips(self, order_df, rng):
        anomaly = CollectiveAnomaly(
            name="x", group_column="missing", timestamp_column="order_date"
        )
        result = anomaly.inject(order_df, rng)
        assert "_spindle_is_anomaly" not in result.columns

    def test_missing_timestamp_column_skips(self, order_df, rng):
        anomaly = CollectiveAnomaly(
            name="x", group_column="customer_id", timestamp_column="missing_ts"
        )
        result = anomaly.inject(order_df, rng)
        assert "_spindle_is_anomaly" not in result.columns

    def test_anomaly_type_property(self):
        anomaly = CollectiveAnomaly("velocity", "cust_id", "ts")
        assert anomaly.anomaly_type == "collective:velocity"


# ---------------------------------------------------------------------------
# AnomalyRegistry
# ---------------------------------------------------------------------------

class TestAnomalyRegistry:
    def test_empty_registry_returns_df_unchanged(self, order_df, rng):
        registry = AnomalyRegistry()
        result = registry.inject(order_df, rng)
        # Should add label columns but no rows flagged
        assert "_spindle_is_anomaly" in result.columns
        assert int(result["_spindle_is_anomaly"].sum()) == 0

    def test_empty_df_returns_unchanged(self, rng):
        registry = AnomalyRegistry([PointAnomaly("x", "col")])
        df = pd.DataFrame({"col": pd.Series([], dtype=float)})
        result = registry.inject(df, rng)
        assert result.empty

    def test_single_anomaly(self, order_df, rng):
        registry = AnomalyRegistry([PointAnomaly("big", column="total_amount", fraction=0.05)])
        result = registry.inject(order_df, rng)
        assert int(result["_spindle_is_anomaly"].sum()) > 0

    def test_multiple_anomalies_stacked(self, order_df, rng):
        registry = AnomalyRegistry([
            PointAnomaly("big", column="total_amount", fraction=0.05),
            ContextualAnomaly(
                "ctx", column="product_category",
                condition_column="order_month", normal_values=[6, 7, 8],
                anomalous_values=["Winter Coats"], fraction=0.5,
            ),
        ])
        result = registry.inject(order_df, rng)
        assert int(result["_spindle_is_anomaly"].sum()) > 0

    def test_label_columns_always_present(self, order_df, rng):
        registry = AnomalyRegistry([PointAnomaly("x", column="total_amount", fraction=0.05)])
        result = registry.inject(order_df, rng)
        assert "_spindle_is_anomaly" in result.columns
        assert "_spindle_anomaly_type" in result.columns

    def test_add_method_chains(self, order_df, rng):
        registry = (
            AnomalyRegistry()
            .add(PointAnomaly("a", column="total_amount", fraction=0.02))
            .add(PointAnomaly("b", column="total_amount", fraction=0.02))
        )
        assert len(registry) == 2

    def test_len(self):
        registry = AnomalyRegistry([
            PointAnomaly("x", "col"),
            PointAnomaly("y", "col"),
        ])
        assert len(registry) == 2

    def test_repr(self):
        registry = AnomalyRegistry([PointAnomaly("my_anomaly", "col")])
        assert "point:my_anomaly" in repr(registry)
