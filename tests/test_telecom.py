"""Integration tests for the telecom domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.telecom import TelecomDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=TelecomDomain(), scale="small", seed=42)


class TestTelecomStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "plan", "device_model", "subscriber", "service_line",
            "usage_record", "billing", "payment",
            "network_event", "churn_indicator",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["plan"]) == 20
        assert len(r["device_model"]) == 40
        assert len(r["subscriber"]) == 2000
        assert len(r["service_line"]) >= 3000
        assert len(r["billing"]) >= 10000
        assert len(r["payment"]) >= 10000
        assert len(r["churn_indicator"]) == 2000  # per_parent 1.0

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("plan") < order.index("service_line")
        assert order.index("subscriber") < order.index("service_line")
        assert order.index("service_line") < order.index("usage_record")
        assert order.index("subscriber") < order.index("billing")
        assert order.index("billing") < order.index("payment")
        assert order.index("service_line") < order.index("network_event")
        assert order.index("subscriber") < order.index("churn_indicator")


class TestTelecomIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_subscriber_id_is_unique(self, result_small):
        assert result_small["subscriber"]["subscriber_id"].is_unique

    def test_plan_id_is_unique(self, result_small):
        assert result_small["plan"]["plan_id"].is_unique

    def test_line_id_is_unique(self, result_small):
        assert result_small["service_line"]["line_id"].is_unique

    def test_bill_id_is_unique(self, result_small):
        assert result_small["billing"]["bill_id"].is_unique

    def test_service_line_subscriber_fk_valid(self, result_small):
        sub_ids = set(result_small["subscriber"]["subscriber_id"])
        line_sub_ids = set(result_small["service_line"]["subscriber_id"])
        assert line_sub_ids.issubset(sub_ids)

    def test_service_line_plan_fk_valid(self, result_small):
        plan_ids = set(result_small["plan"]["plan_id"])
        line_plan_ids = set(result_small["service_line"]["plan_id"])
        assert line_plan_ids.issubset(plan_ids)

    def test_usage_record_line_fk_valid(self, result_small):
        line_ids = set(result_small["service_line"]["line_id"])
        usage_line_ids = set(result_small["usage_record"]["line_id"])
        assert usage_line_ids.issubset(line_ids)

    def test_billing_subscriber_fk_valid(self, result_small):
        sub_ids = set(result_small["subscriber"]["subscriber_id"])
        bill_sub_ids = set(result_small["billing"]["subscriber_id"])
        assert bill_sub_ids.issubset(sub_ids)

    def test_payment_bill_fk_valid(self, result_small):
        bill_ids = set(result_small["billing"]["bill_id"])
        pay_bill_ids = set(result_small["payment"]["bill_id"])
        assert pay_bill_ids.issubset(bill_ids)

    def test_network_event_line_fk_valid(self, result_small):
        line_ids = set(result_small["service_line"]["line_id"])
        ne_line_ids = set(result_small["network_event"]["line_id"])
        assert ne_line_ids.issubset(line_ids)

    def test_churn_indicator_subscriber_fk_valid(self, result_small):
        sub_ids = set(result_small["subscriber"]["subscriber_id"])
        churn_sub_ids = set(result_small["churn_indicator"]["subscriber_id"])
        assert churn_sub_ids.issubset(sub_ids)


class TestTelecomDistributions:
    def test_subscriber_account_status_distribution(self, result_small):
        statuses = result_small["subscriber"]["account_status"].value_counts(normalize=True)
        assert 0.70 <= statuses.get("Active", 0) <= 0.95

    def test_payment_method_distribution(self, result_small):
        methods = result_small["payment"]["payment_method"].value_counts(normalize=True)
        assert 0.25 <= methods.get("Auto-Pay", 0) <= 0.55

    def test_usage_type_distribution(self, result_small):
        types = result_small["usage_record"]["record_type"].value_counts(normalize=True)
        assert 0.35 <= types.get("Data", 0) <= 0.65
        assert 0.15 <= types.get("Voice", 0) <= 0.40

    def test_billing_status_distribution(self, result_small):
        statuses = result_small["billing"]["payment_status"].value_counts(normalize=True)
        assert 0.65 <= statuses.get("Paid", 0) <= 0.88

    def test_churn_risk_level_in_set(self, result_small):
        valid = {"Low", "Medium", "High", "Very High"}
        actual = set(result_small["churn_indicator"]["risk_level"].unique())
        assert actual.issubset(valid)

    def test_service_line_status_in_set(self, result_small):
        valid = {"Active", "Suspended", "Disconnected"}
        actual = set(result_small["service_line"]["status"].unique())
        assert actual.issubset(valid)

    def test_plan_type_in_set(self, result_small):
        valid = {"Prepaid", "Postpaid", "Family", "Business"}
        actual = set(result_small["plan"]["plan_type"].unique())
        assert actual.issubset(valid)

    def test_subscriber_credit_class_distribution(self, result_small):
        classes = result_small["subscriber"]["credit_class"].value_counts(normalize=True)
        # "A" and "B" combined should be majority
        a_and_b = classes.get("A", 0) + classes.get("B", 0)
        assert 0.45 <= a_and_b <= 0.75


class TestTelecomBusinessRules:
    def test_billing_total_positive(self, result_small):
        assert (result_small["billing"]["total_amount"] > 0).all()

    def test_payment_amount_positive(self, result_small):
        assert (result_small["payment"]["amount"] > 0).all()

    def test_plan_monthly_rate_positive(self, result_small):
        assert (result_small["plan"]["monthly_rate"] > 0).all()

    def test_churn_score_range(self, result_small):
        score = result_small["churn_indicator"]["churn_score"]
        assert (score >= 0).all() and (score <= 1).all()

    def test_usage_duration_nonnegative(self, result_small):
        dur = result_small["usage_record"]["duration_seconds"].dropna()
        assert (dur >= 0).all()


class TestTelecomReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=TelecomDomain(), scale="small", seed=99)
        r2 = s.generate(domain=TelecomDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
