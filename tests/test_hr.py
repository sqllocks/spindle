"""Integration tests for the HR domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.hr import HrDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=HrDomain(), scale="small", seed=42)


class TestHrStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "department", "position", "employee", "compensation",
            "performance_review", "time_off_request", "training",
            "training_enrollment", "termination",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["department"]) == 30
        assert len(r["position"]) == 80
        assert len(r["employee"]) == 500
        assert len(r["compensation"]) == 1500
        assert len(r["performance_review"]) == 1250
        assert len(r["time_off_request"]) == 2500
        assert len(r["training"]) == 100
        assert len(r["training_enrollment"]) == 2000
        assert len(r["termination"]) == 75

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("department") < order.index("employee")
        assert order.index("position") < order.index("employee")
        assert order.index("employee") < order.index("compensation")
        assert order.index("employee") < order.index("performance_review")
        assert order.index("employee") < order.index("time_off_request")
        assert order.index("training") < order.index("training_enrollment")
        assert order.index("employee") < order.index("training_enrollment")
        assert order.index("employee") < order.index("termination")


class TestHrIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_employee_id_is_unique(self, result_small):
        assert result_small["employee"]["employee_id"].is_unique

    def test_department_id_is_unique(self, result_small):
        assert result_small["department"]["department_id"].is_unique

    def test_position_id_is_unique(self, result_small):
        assert result_small["position"]["position_id"].is_unique

    def test_training_id_is_unique(self, result_small):
        assert result_small["training"]["training_id"].is_unique

    def test_employee_department_fk_valid(self, result_small):
        dept_ids = set(result_small["department"]["department_id"])
        emp_dept_ids = set(result_small["employee"]["department_id"])
        assert emp_dept_ids.issubset(dept_ids)

    def test_employee_position_fk_valid(self, result_small):
        pos_ids = set(result_small["position"]["position_id"])
        emp_pos_ids = set(result_small["employee"]["position_id"])
        assert emp_pos_ids.issubset(pos_ids)

    def test_compensation_employee_fk_valid(self, result_small):
        emp_ids = set(result_small["employee"]["employee_id"])
        comp_emp_ids = set(result_small["compensation"]["employee_id"])
        assert comp_emp_ids.issubset(emp_ids)

    def test_performance_review_employee_fk_valid(self, result_small):
        emp_ids = set(result_small["employee"]["employee_id"])
        review_emp_ids = set(result_small["performance_review"]["employee_id"])
        assert review_emp_ids.issubset(emp_ids)

    def test_training_enrollment_training_fk_valid(self, result_small):
        training_ids = set(result_small["training"]["training_id"])
        enroll_training_ids = set(result_small["training_enrollment"]["training_id"])
        assert enroll_training_ids.issubset(training_ids)

    def test_training_enrollment_employee_fk_valid(self, result_small):
        emp_ids = set(result_small["employee"]["employee_id"])
        enroll_emp_ids = set(result_small["training_enrollment"]["employee_id"])
        assert enroll_emp_ids.issubset(emp_ids)

    def test_termination_employee_fk_valid(self, result_small):
        emp_ids = set(result_small["employee"]["employee_id"])
        term_emp_ids = set(result_small["termination"]["employee_id"])
        assert term_emp_ids.issubset(emp_ids)


class TestHrDistributions:
    def test_employment_status_distribution(self, result_small):
        statuses = result_small["employee"]["employment_status"].value_counts(normalize=True)
        assert 0.72 <= statuses.get("Active", 0) <= 0.92

    def test_employment_status_in_set(self, result_small):
        statuses = set(result_small["employee"]["employment_status"].unique())
        valid = {"Active", "On Leave", "Terminated", "Retired"}
        assert statuses.issubset(valid)

    def test_employment_type_distribution(self, result_small):
        types = result_small["employee"]["employment_type"].value_counts(normalize=True)
        assert 0.65 <= types.get("Full-Time", 0) <= 0.85

    def test_employment_type_in_set(self, result_small):
        types = set(result_small["employee"]["employment_type"].unique())
        valid = {"Full-Time", "Part-Time", "Contract", "Temporary"}
        assert types.issubset(valid)

    def test_compensation_change_reason_distribution(self, result_small):
        reasons = result_small["compensation"]["change_reason"].value_counts(normalize=True)
        assert 0.30 <= reasons.get("Annual Review", 0) <= 0.50

    def test_compensation_change_reason_in_set(self, result_small):
        reasons = set(result_small["compensation"]["change_reason"].unique())
        valid = {"Annual Review", "Promotion", "Market Adjustment", "New Hire", "Transfer"}
        assert reasons.issubset(valid)

    def test_leave_type_in_set(self, result_small):
        types = set(result_small["time_off_request"]["leave_type"].unique())
        valid = {"PTO", "Sick", "Personal", "Bereavement", "Jury Duty", "Parental", "Unpaid"}
        assert types.issubset(valid)

    def test_termination_type_distribution(self, result_small):
        # 75 rows -- wide tolerance for small sample
        types = result_small["termination"]["termination_type"].value_counts(normalize=True)
        assert 0.35 <= types.get("Voluntary", 0) <= 0.70

    def test_termination_type_in_set(self, result_small):
        types = set(result_small["termination"]["termination_type"].unique())
        valid = {"Voluntary", "Involuntary", "Layoff", "Retirement", "End of Contract"}
        assert types.issubset(valid)


class TestHrBusinessRules:
    def test_max_salary_geq_min_salary(self, result_small):
        positions = result_small["position"]
        violations = (positions["max_salary"] < positions["min_salary"] - 0.01).sum()
        assert violations == 0, f"{violations} positions have max_salary < min_salary"

    def test_hours_requested_positive(self, result_small):
        reqs = result_small["time_off_request"]
        assert (reqs["hours_requested"] > 0).all()

    def test_base_salary_reasonable(self, result_small):
        comp = result_small["compensation"]
        assert (comp["base_salary"] >= 28000.0).all()
        assert (comp["base_salary"] <= 350001.0).all()

    def test_bonus_pct_range(self, result_small):
        comp = result_small["compensation"]
        assert (comp["bonus_pct"] >= 0.0).all()
        assert (comp["bonus_pct"] <= 25.01).all()


class TestHrReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=HrDomain(), scale="small", seed=99)
        r2 = s.generate(domain=HrDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
