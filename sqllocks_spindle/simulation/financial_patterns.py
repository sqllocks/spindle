"""Financial transaction stream patterns — reversals, fraud bursts, settlement events.

Generates realistic financial streaming data anomalies that can be layered
on top of base Financial domain data produced by the generator.

Usage::

    from sqllocks_spindle.simulation.financial_patterns import (
        FinancialStreamSimulator, FinancialStreamConfig,
    )

    cfg = FinancialStreamConfig(duration_hours=24)
    result = FinancialStreamSimulator(transactions_df=txns, accounts_df=accounts, config=cfg).run()
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Reversal reason weights
# ---------------------------------------------------------------------------

_REVERSAL_REASONS: list[tuple[str, float]] = [
    ("customer_dispute", 0.40),
    ("duplicate", 0.25),
    ("fraud_confirmed", 0.20),
    ("error", 0.15),
]

# Merchant categories used for synthetic fraud bursts
_FRAUD_MERCHANT_CATEGORIES: list[str] = [
    "electronics",
    "jewelry",
    "gift_cards",
    "cryptocurrency",
    "wire_transfer",
    "online_gambling",
    "luxury_goods",
]

# Settlement failure reasons
_SETTLEMENT_FAILURE_REASONS: list[str] = [
    "insufficient_funds",
    "account_closed",
    "compliance_hold",
    "network_timeout",
    "duplicate_batch",
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class FinancialStreamConfig:
    """Configuration for :class:`FinancialStreamSimulator`.

    Args:
        duration_hours: Total simulation window in hours.
        reversal_enabled: Whether to generate transaction reversals.
        reversal_probability: Fraction of transactions that get reversed.
        reversal_delay_hours_max: Maximum delay between original transaction
            and its reversal.
        fraud_burst_enabled: Whether to generate fraud burst events.
        fraud_burst_probability: Per-hour probability of a fraud burst
            occurring.
        fraud_burst_count: Number of transactions per fraud burst.
        fraud_burst_amount_range: ``(min, max)`` dollar range for fraud
            transactions.
        settlement_enabled: Whether to generate settlement batch records.
        settlement_batch_hours: Settlement runs every N hours.
        settlement_success_rate: Fraction of settlements that succeed.
        seed: Random seed for reproducibility.
    """

    duration_hours: float = 24.0
    reversal_enabled: bool = True
    reversal_probability: float = 0.03
    reversal_delay_hours_max: float = 48.0
    fraud_burst_enabled: bool = True
    fraud_burst_probability: float = 0.01
    fraud_burst_count: int = 15
    fraud_burst_amount_range: tuple[float, float] = (500.0, 10000.0)
    settlement_enabled: bool = True
    settlement_batch_hours: float = 4.0
    settlement_success_rate: float = 0.98
    seed: int = 42


# ---------------------------------------------------------------------------
# Result
# ---------------------------------------------------------------------------

@dataclass
class FinancialStreamResult:
    """Result of a :meth:`FinancialStreamSimulator.run` execution.

    Attributes:
        transactions: Combined DataFrame — original transactions plus any
            generated reversals and fraud events.
        reversals: DataFrame containing only reversal records.
        fraud_events: DataFrame containing only fraud burst records.
        settlements: DataFrame containing settlement batch results.
        stats: Summary statistics for the simulation run.
    """

    transactions: pd.DataFrame
    reversals: pd.DataFrame
    fraud_events: pd.DataFrame
    settlements: pd.DataFrame
    stats: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"FinancialStreamResult("
            f"transactions={len(self.transactions)}, "
            f"reversals={len(self.reversals)}, "
            f"fraud_events={len(self.fraud_events)}, "
            f"settlements={len(self.settlements)})"
        )


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class FinancialStreamSimulator:
    """Generate financial transaction stream anomalies.

    Layers reversals, fraud bursts, and settlement events on top of
    pre-generated transaction and account DataFrames.

    Args:
        transactions_df: Base transactions DataFrame.  Expected to contain
            at least ``transaction_id``, ``account_id``, ``amount``, and
            ``transaction_time`` columns.
        accounts_df: Accounts DataFrame.  Expected to contain at least
            ``account_id``.
        config: :class:`FinancialStreamConfig` (uses defaults if ``None``).

    Example::

        cfg = FinancialStreamConfig(duration_hours=24, seed=99)
        sim = FinancialStreamSimulator(txns, accounts, cfg)
        result = sim.run()
        print(result.stats)
    """

    def __init__(
        self,
        transactions_df: pd.DataFrame,
        accounts_df: pd.DataFrame,
        config: FinancialStreamConfig | None = None,
    ) -> None:
        self._transactions = transactions_df.copy()
        self._accounts = accounts_df.copy()
        self._config = config or FinancialStreamConfig()
        self._rng = np.random.default_rng(self._config.seed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> FinancialStreamResult:
        """Execute the simulation and return a :class:`FinancialStreamResult`."""
        cfg = self._config

        reversals_df = self._generate_reversals() if cfg.reversal_enabled else _empty_reversals()
        fraud_df = self._generate_fraud_bursts() if cfg.fraud_burst_enabled else _empty_fraud()
        settlements_df = self._generate_settlements() if cfg.settlement_enabled else _empty_settlements()

        # Combine original transactions with reversals and fraud events
        combined = self._transactions.copy()
        if not reversals_df.empty:
            combined = pd.concat([combined, reversals_df], ignore_index=True)
        if not fraud_df.empty:
            combined = pd.concat([combined, fraud_df], ignore_index=True)

        stats: dict[str, Any] = {
            "original_transaction_count": len(self._transactions),
            "reversal_count": len(reversals_df),
            "fraud_event_count": len(fraud_df),
            "settlement_batch_count": len(settlements_df),
            "combined_transaction_count": len(combined),
            "duration_hours": cfg.duration_hours,
            "seed": cfg.seed,
        }

        return FinancialStreamResult(
            transactions=combined,
            reversals=reversals_df,
            fraud_events=fraud_df,
            settlements=settlements_df,
            stats=stats,
        )

    # ------------------------------------------------------------------
    # Reversals
    # ------------------------------------------------------------------

    def _generate_reversals(self) -> pd.DataFrame:
        """Select transactions for reversal and create reversal records.

        Each reversal mirrors the original transaction with a negative amount
        and is linked via ``original_transaction_id``.
        """
        cfg = self._config
        n_txns = len(self._transactions)
        if n_txns == 0:
            return _empty_reversals()

        # Determine which transactions get reversed
        mask = self._rng.random(n_txns) < cfg.reversal_probability
        selected = self._transactions[mask]
        if selected.empty:
            return _empty_reversals()

        # Build weighted reason choices
        reasons, weights = zip(*_REVERSAL_REASONS)
        weights_arr = np.array(weights, dtype=np.float64)
        weights_arr /= weights_arr.sum()

        records: list[dict[str, Any]] = []
        for _, row in selected.iterrows():
            delay_hours = self._rng.uniform(0.1, cfg.reversal_delay_hours_max)
            original_time = pd.Timestamp(row.get("transaction_time", datetime.now(timezone.utc)))
            reversed_at = original_time + timedelta(hours=delay_hours)

            reason = self._rng.choice(reasons, p=weights_arr)

            records.append({
                "reversal_id": str(uuid.uuid4()),
                "original_transaction_id": row.get("transaction_id", str(uuid.uuid4())),
                "account_id": row["account_id"],
                "amount": -abs(float(row["amount"])),
                "reversal_reason": str(reason),
                "reversed_at": reversed_at,
            })

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Fraud bursts
    # ------------------------------------------------------------------

    def _generate_fraud_bursts(self) -> pd.DataFrame:
        """Generate rapid-fire fraud transactions from compromised accounts.

        Each hour of the simulation window has an independent chance of
        triggering a burst.  During a burst, ``fraud_burst_count``
        transactions are emitted from a single randomly-chosen account.
        """
        cfg = self._config
        if self._accounts.empty:
            return _empty_fraud()

        account_ids = self._accounts["account_id"].values
        n_hours = int(np.ceil(cfg.duration_hours))

        # Determine the simulation start time from the earliest transaction
        if not self._transactions.empty and "transaction_time" in self._transactions.columns:
            sim_start = pd.Timestamp(self._transactions["transaction_time"].min())
        else:
            sim_start = pd.Timestamp(datetime.now(timezone.utc))

        records: list[dict[str, Any]] = []

        for hour in range(n_hours):
            if self._rng.random() >= cfg.fraud_burst_probability:
                continue

            # Pick a compromised account
            compromised = self._rng.choice(account_ids)
            burst_start = sim_start + timedelta(hours=hour)
            lo, hi = cfg.fraud_burst_amount_range

            for i in range(cfg.fraud_burst_count):
                # Rapid-fire: transactions within seconds of each other
                offset_secs = self._rng.uniform(0, 120)
                tx_time = burst_start + timedelta(seconds=offset_secs)
                amount = float(self._rng.uniform(lo, hi))
                category = str(self._rng.choice(_FRAUD_MERCHANT_CATEGORIES))

                records.append({
                    "fraud_tx_id": str(uuid.uuid4()),
                    "account_id": compromised,
                    "amount": round(amount, 2),
                    "merchant_category": category,
                    "transaction_time": tx_time,
                    "is_fraud": True,
                })

        if not records:
            return _empty_fraud()

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Settlements
    # ------------------------------------------------------------------

    def _generate_settlements(self) -> pd.DataFrame:
        """Create periodic settlement batch records.

        Settlement runs at fixed intervals (``settlement_batch_hours``).
        Each batch summarises how many transactions were settled, their
        total amount, and whether the batch succeeded, failed, or partially
        settled.
        """
        cfg = self._config
        n_batches = max(1, int(np.ceil(cfg.duration_hours / cfg.settlement_batch_hours)))

        # Determine simulation start
        if not self._transactions.empty and "transaction_time" in self._transactions.columns:
            sim_start = pd.Timestamp(self._transactions["transaction_time"].min())
        else:
            sim_start = pd.Timestamp(datetime.now(timezone.utc))

        # Pre-compute per-batch transaction slices if possible
        has_time = (
            not self._transactions.empty
            and "transaction_time" in self._transactions.columns
        )
        txns_sorted = (
            self._transactions.sort_values("transaction_time")
            if has_time
            else self._transactions
        )

        records: list[dict[str, Any]] = []

        for batch_idx in range(n_batches):
            batch_start = sim_start + timedelta(hours=batch_idx * cfg.settlement_batch_hours)
            batch_end = batch_start + timedelta(hours=cfg.settlement_batch_hours)

            # Slice transactions in this window
            if has_time:
                window = txns_sorted[
                    (txns_sorted["transaction_time"] >= batch_start)
                    & (txns_sorted["transaction_time"] < batch_end)
                ]
                tx_count = len(window)
                total_amount = float(window["amount"].sum()) if tx_count > 0 else 0.0
            else:
                # Distribute evenly when timestamps are unavailable
                chunk = max(1, len(self._transactions) // n_batches)
                start_i = batch_idx * chunk
                end_i = min(start_i + chunk, len(self._transactions))
                window = self._transactions.iloc[start_i:end_i]
                tx_count = len(window)
                total_amount = float(window["amount"].sum()) if tx_count > 0 else 0.0

            # Determine settlement status
            roll = self._rng.random()
            if roll < cfg.settlement_success_rate:
                status = "settled"
                failure_reason = None
            elif roll < cfg.settlement_success_rate + (1 - cfg.settlement_success_rate) * 0.5:
                status = "partial"
                failure_reason = str(self._rng.choice(_SETTLEMENT_FAILURE_REASONS))
                # Partial: only a fraction of the amount settles
                total_amount = round(total_amount * self._rng.uniform(0.3, 0.9), 2)
            else:
                status = "failed"
                failure_reason = str(self._rng.choice(_SETTLEMENT_FAILURE_REASONS))
                total_amount = 0.0

            records.append({
                "batch_id": str(uuid.uuid4()),
                "settled_at": batch_end,
                "transaction_count": tx_count,
                "total_amount": round(total_amount, 2),
                "status": status,
                "failure_reason": failure_reason,
            })

        return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Empty DataFrame factories (keep column schemas consistent)
# ---------------------------------------------------------------------------

def _empty_reversals() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "reversal_id", "original_transaction_id", "account_id",
        "amount", "reversal_reason", "reversed_at",
    ])


def _empty_fraud() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "fraud_tx_id", "account_id", "amount",
        "merchant_category", "transaction_time", "is_fraud",
    ])


def _empty_settlements() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "batch_id", "settled_at", "transaction_count",
        "total_amount", "status", "failure_reason",
    ])
