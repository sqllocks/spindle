"""Integration tests for the financial domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.financial import FinancialDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=FinancialDomain(), scale="small", seed=42)


class TestFinancialStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "branch", "customer", "account", "transaction",
            "transaction_category", "loan", "loan_payment",
            "card", "fraud_flag", "statement",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["customer"]) == 1000
        assert len(r["branch"]) == 200
        assert len(r["transaction_category"]) == 40
        assert len(r["account"]) == 2200
        assert len(r["transaction"]) == 10000
        assert len(r["loan"]) == 400
        assert len(r["card"]) == 1760
        assert len(r["statement"]) == 13200

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("branch") < order.index("customer")
        assert order.index("customer") < order.index("account")
        assert order.index("account") < order.index("transaction")
        assert order.index("account") < order.index("card")
        assert order.index("customer") < order.index("loan")
        assert order.index("loan") < order.index("loan_payment")
        assert order.index("transaction") < order.index("fraud_flag")


class TestFinancialIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_customer_id_is_unique(self, result_small):
        assert result_small["customer"]["customer_id"].is_unique

    def test_account_id_is_unique(self, result_small):
        assert result_small["account"]["account_id"].is_unique

    def test_account_customer_fk_valid(self, result_small):
        customer_ids = set(result_small["customer"]["customer_id"])
        account_cust_ids = set(result_small["account"]["customer_id"])
        assert account_cust_ids.issubset(customer_ids)

    def test_transaction_account_fk_valid(self, result_small):
        account_ids = set(result_small["account"]["account_id"])
        txn_acct_ids = set(result_small["transaction"]["account_id"])
        assert txn_acct_ids.issubset(account_ids)

    def test_loan_customer_fk_valid(self, result_small):
        customer_ids = set(result_small["customer"]["customer_id"])
        loan_cust_ids = set(result_small["loan"]["customer_id"])
        assert loan_cust_ids.issubset(customer_ids)

    def test_fraud_flag_transaction_fk_valid(self, result_small):
        txn_ids = set(result_small["transaction"]["transaction_id"])
        flag_txn_ids = set(result_small["fraud_flag"]["transaction_id"])
        assert flag_txn_ids.issubset(txn_ids)


class TestFinancialDistributions:
    def test_credit_tier_distribution(self, result_small):
        tiers = result_small["customer"]["credit_tier"].value_counts(normalize=True)
        assert 0.15 <= tiers.get("Excellent", 0) <= 0.30
        assert 0.28 <= tiers.get("Good", 0) <= 0.42

    def test_account_type_distribution(self, result_small):
        types = result_small["account"]["account_type"].value_counts(normalize=True)
        assert 0.38 <= types.get("Checking", 0) <= 0.52

    def test_transaction_type_in_set(self, result_small):
        types = set(result_small["transaction"]["transaction_type"].unique())
        valid = {"Deposit", "Withdrawal", "Transfer", "Payment", "Fee", "Interest", "Refund"}
        assert types.issubset(valid)

    def test_card_network_distribution(self, result_small):
        networks = result_small["card"]["card_network"].value_counts(normalize=True)
        assert 0.45 <= networks.get("Visa", 0) <= 0.60

    def test_loan_type_distribution(self, result_small):
        types = result_small["loan"]["loan_type"].value_counts(normalize=True)
        assert 0.25 <= types.get("Mortgage", 0) <= 0.45

    def test_branch_has_coordinates(self, result_small):
        branch = result_small["branch"]
        assert branch["lat"].between(17.0, 72.0).all()
        assert branch["lng"].between(-180.0, -65.0).all()


class TestFinancialBusinessRules:
    def test_outstanding_leq_principal(self, result_small):
        loans = result_small["loan"]
        violations = (loans["outstanding_balance"] > loans["principal_amount"] + 0.01).sum()
        assert violations == 0, f"{violations} loans have outstanding > principal"

    def test_interest_portion_nonnegative(self, result_small):
        payments = result_small["loan_payment"]
        # interest_portion = payment_amount - principal_portion, should be >= 0
        negative_rate = (payments["interest_portion"] < -0.01).mean()
        assert negative_rate < 0.05, f"{negative_rate:.1%} of payments have negative interest"

    def test_fraud_risk_score_range(self, result_small):
        flags = result_small["fraud_flag"]
        assert (flags["risk_score"] >= 0.0).all()
        assert (flags["risk_score"] <= 1.01).all()


class TestFinancialReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=FinancialDomain(), scale="small", seed=99)
        r2 = s.generate(domain=FinancialDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
