"""Tests for financial transaction stream patterns (E8)."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sqllocks_spindle.simulation.financial_patterns import (
    FinancialStreamConfig,
    FinancialStreamResult,
    FinancialStreamSimulator,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def transactions_df():
    n = 200
    rng = np.random.default_rng(1)
    return pd.DataFrame({
        "transaction_id": range(1, n + 1),
        "account_id": rng.integers(1, 21, size=n),
        "amount": rng.uniform(10.0, 500.0, size=n).round(2),
        "transaction_date": pd.date_range("2024-01-01", periods=n, freq="h"),
        "transaction_type": rng.choice(["debit", "credit", "transfer"], size=n),
    })


@pytest.fixture
def accounts_df():
    return pd.DataFrame({
        "account_id": range(1, 21),
        "customer_id": range(101, 121),
        "account_type": ["checking"] * 10 + ["savings"] * 10,
        "balance": np.random.default_rng(1).uniform(1000, 50000, size=20).round(2),
    })


@pytest.fixture
def config():
    return FinancialStreamConfig(duration_hours=24.0, seed=42)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestFinancialStreamConfig:
    def test_defaults(self):
        cfg = FinancialStreamConfig()
        assert cfg.reversal_enabled is True
        assert cfg.fraud_burst_enabled is True
        assert cfg.settlement_enabled is True
        assert cfg.seed == 42


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class TestFinancialStreamSimulator:
    def test_run_returns_result(self, transactions_df, accounts_df, config):
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=config,
        )
        result = sim.run()
        assert isinstance(result, FinancialStreamResult)

    def test_transactions_returned(self, transactions_df, accounts_df, config):
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=config,
        )
        result = sim.run()
        assert isinstance(result.transactions, pd.DataFrame)
        assert len(result.transactions) >= len(transactions_df)

    def test_stats_populated(self, transactions_df, accounts_df, config):
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=config,
        )
        result = sim.run()
        assert isinstance(result.stats, dict)


# ---------------------------------------------------------------------------
# Reversals
# ---------------------------------------------------------------------------

class TestReversals:
    def test_reversals_generated(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=True,
            reversal_probability=0.5,  # High for test
            fraud_burst_enabled=False,
            settlement_enabled=False,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        assert len(result.reversals) > 0

    def test_reversal_has_required_columns(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=True,
            reversal_probability=0.5,
            fraud_burst_enabled=False,
            settlement_enabled=False,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        if len(result.reversals) > 0:
            assert "reversal_id" in result.reversals.columns
            assert "original_transaction_id" in result.reversals.columns
            assert "amount" in result.reversals.columns
            assert "reversal_reason" in result.reversals.columns

    def test_reversal_amounts_are_negative(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=True,
            reversal_probability=0.5,
            fraud_burst_enabled=False,
            settlement_enabled=False,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        if len(result.reversals) > 0:
            assert (result.reversals["amount"] < 0).all()

    def test_reversal_reasons_are_valid(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=True,
            reversal_probability=0.5,
            fraud_burst_enabled=False,
            settlement_enabled=False,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        valid_reasons = {"customer_dispute", "duplicate", "fraud_confirmed", "error"}
        if len(result.reversals) > 0:
            assert set(result.reversals["reversal_reason"]).issubset(valid_reasons)


# ---------------------------------------------------------------------------
# Fraud bursts
# ---------------------------------------------------------------------------

class TestFraudBursts:
    def test_fraud_events_generated(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=False,
            fraud_burst_enabled=True,
            fraud_burst_probability=1.0,  # Force burst
            fraud_burst_count=10,
            settlement_enabled=False,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        assert len(result.fraud_events) > 0

    def test_fraud_has_required_columns(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=False,
            fraud_burst_enabled=True,
            fraud_burst_probability=1.0,
            settlement_enabled=False,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        if len(result.fraud_events) > 0:
            assert "fraud_tx_id" in result.fraud_events.columns
            assert "account_id" in result.fraud_events.columns
            assert "amount" in result.fraud_events.columns
            assert "is_fraud" in result.fraud_events.columns

    def test_fraud_marked_as_fraud(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=False,
            fraud_burst_enabled=True,
            fraud_burst_probability=1.0,
            settlement_enabled=False,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        if len(result.fraud_events) > 0:
            assert result.fraud_events["is_fraud"].all()


# ---------------------------------------------------------------------------
# Settlements
# ---------------------------------------------------------------------------

class TestSettlements:
    def test_settlements_generated(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=False,
            fraud_burst_enabled=False,
            settlement_enabled=True,
            settlement_batch_hours=4.0,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        assert len(result.settlements) > 0

    def test_settlement_has_required_columns(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=False,
            fraud_burst_enabled=False,
            settlement_enabled=True,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        if len(result.settlements) > 0:
            assert "batch_id" in result.settlements.columns
            assert "settled_at" in result.settlements.columns
            assert "transaction_count" in result.settlements.columns
            assert "total_amount" in result.settlements.columns
            assert "status" in result.settlements.columns

    def test_settlement_statuses_valid(self, transactions_df, accounts_df):
        cfg = FinancialStreamConfig(
            reversal_enabled=False,
            fraud_burst_enabled=False,
            settlement_enabled=True,
            seed=42,
        )
        sim = FinancialStreamSimulator(
            transactions_df=transactions_df,
            accounts_df=accounts_df,
            config=cfg,
        )
        result = sim.run()
        valid_statuses = {"settled", "failed", "partial"}
        if len(result.settlements) > 0:
            assert set(result.settlements["status"]).issubset(valid_statuses)
