"""Integration tests for the education domain."""

from __future__ import annotations

import pytest

from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.education import EducationDomain


@pytest.fixture(scope="module")
def result_small():
    s = Spindle()
    return s.generate(domain=EducationDomain(), scale="small", seed=42)


class TestEducationStructure:
    def test_expected_tables_present(self, result_small):
        expected = {
            "department", "instructor", "course", "student",
            "enrollment", "financial_aid", "course_section",
            "grade_appeal", "academic_standing",
        }
        assert expected == set(result_small.tables.keys())

    def test_row_counts_small_scale(self, result_small):
        r = result_small
        assert len(r["department"]) == 25
        assert len(r["course"]) == 300
        assert len(r["instructor"]) == 150
        assert len(r["course_section"]) == 600
        assert len(r["student"]) == 2000
        assert len(r["academic_standing"]) == 4000
        assert len(r["enrollment"]) == 16000
        assert len(r["financial_aid"]) == 1400
        assert len(r["grade_appeal"]) == 320

    def test_generation_order_respects_dependencies(self, result_small):
        order = result_small.generation_order
        assert order.index("department") < order.index("course")
        assert order.index("department") < order.index("instructor")
        assert order.index("course") < order.index("course_section")
        assert order.index("instructor") < order.index("course_section")
        assert order.index("department") < order.index("student")
        assert order.index("student") < order.index("enrollment")
        assert order.index("student") < order.index("financial_aid")
        assert order.index("enrollment") < order.index("grade_appeal")
        assert order.index("student") < order.index("academic_standing")


class TestEducationIntegrity:
    def test_fk_integrity_passes(self, result_small):
        errors = result_small.verify_integrity()
        assert errors == [], f"FK integrity errors: {errors}"

    def test_student_id_is_unique(self, result_small):
        assert result_small["student"]["student_id"].is_unique

    def test_department_id_is_unique(self, result_small):
        assert result_small["department"]["department_id"].is_unique

    def test_course_id_is_unique(self, result_small):
        assert result_small["course"]["course_id"].is_unique

    def test_instructor_id_is_unique(self, result_small):
        assert result_small["instructor"]["instructor_id"].is_unique

    def test_enrollment_id_is_unique(self, result_small):
        assert result_small["enrollment"]["enrollment_id"].is_unique

    def test_course_department_fk_valid(self, result_small):
        dept_ids = set(result_small["department"]["department_id"])
        course_dept_ids = set(result_small["course"]["department_id"])
        assert course_dept_ids.issubset(dept_ids)

    def test_instructor_department_fk_valid(self, result_small):
        dept_ids = set(result_small["department"]["department_id"])
        instr_dept_ids = set(result_small["instructor"]["department_id"])
        assert instr_dept_ids.issubset(dept_ids)

    def test_student_major_department_fk_valid(self, result_small):
        dept_ids = set(result_small["department"]["department_id"])
        student_dept_ids = set(result_small["student"]["major_department_id"])
        assert student_dept_ids.issubset(dept_ids)

    def test_enrollment_student_fk_valid(self, result_small):
        student_ids = set(result_small["student"]["student_id"])
        enroll_student_ids = set(result_small["enrollment"]["student_id"])
        assert enroll_student_ids.issubset(student_ids)

    def test_enrollment_course_fk_valid(self, result_small):
        course_ids = set(result_small["course"]["course_id"])
        enroll_course_ids = set(result_small["enrollment"]["course_id"])
        assert enroll_course_ids.issubset(course_ids)

    def test_enrollment_instructor_fk_valid(self, result_small):
        instr_ids = set(result_small["instructor"]["instructor_id"])
        enroll_instr_ids = set(result_small["enrollment"]["instructor_id"])
        assert enroll_instr_ids.issubset(instr_ids)

    def test_financial_aid_student_fk_valid(self, result_small):
        student_ids = set(result_small["student"]["student_id"])
        aid_student_ids = set(result_small["financial_aid"]["student_id"])
        assert aid_student_ids.issubset(student_ids)

    def test_course_section_course_fk_valid(self, result_small):
        course_ids = set(result_small["course"]["course_id"])
        section_course_ids = set(result_small["course_section"]["course_id"])
        assert section_course_ids.issubset(course_ids)

    def test_course_section_instructor_fk_valid(self, result_small):
        instr_ids = set(result_small["instructor"]["instructor_id"])
        section_instr_ids = set(result_small["course_section"]["instructor_id"])
        assert section_instr_ids.issubset(instr_ids)

    def test_grade_appeal_enrollment_fk_valid(self, result_small):
        enroll_ids = set(result_small["enrollment"]["enrollment_id"])
        appeal_enroll_ids = set(result_small["grade_appeal"]["enrollment_id"])
        assert appeal_enroll_ids.issubset(enroll_ids)

    def test_academic_standing_student_fk_valid(self, result_small):
        student_ids = set(result_small["student"]["student_id"])
        standing_student_ids = set(result_small["academic_standing"]["student_id"])
        assert standing_student_ids.issubset(student_ids)


class TestEducationDistributions:
    def test_student_classification_distribution(self, result_small):
        clf = result_small["student"]["classification"].value_counts(normalize=True)
        assert 0.15 <= clf.get("Freshman", 0) <= 0.30
        assert 0.13 <= clf.get("Sophomore", 0) <= 0.28
        assert 0.13 <= clf.get("Junior", 0) <= 0.28
        assert 0.13 <= clf.get("Senior", 0) <= 0.28
        assert 0.11 <= clf.get("Graduate", 0) <= 0.26

    def test_student_status_distribution(self, result_small):
        status = result_small["student"]["status"].value_counts(normalize=True)
        assert 0.72 <= status.get("Active", 0) <= 0.88

    def test_grade_values_in_set(self, result_small):
        grades = set(result_small["enrollment"]["grade"].dropna().unique())
        valid = {"A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F", "W", "I"}
        assert grades.issubset(valid)

    def test_grade_distribution(self, result_small):
        grades = result_small["enrollment"]["grade"].dropna().value_counts(normalize=True)
        # A should be ~18%
        assert 0.10 <= grades.get("A", 0) <= 0.26

    def test_college_distribution(self, result_small):
        # department table is small (25 rows), use very wide tolerances
        colleges = result_small["department"]["college"].value_counts(normalize=True)
        assert 0.05 <= colleges.get("Arts & Sciences", 0) <= 0.70
        assert 0.05 <= colleges.get("Engineering", 0) <= 0.55

    def test_instructor_rank_distribution(self, result_small):
        ranks = result_small["instructor"]["rank"].value_counts(normalize=True)
        assert 0.12 <= ranks.get("Professor", 0) <= 0.30
        assert 0.17 <= ranks.get("Assistant Professor", 0) <= 0.35

    def test_financial_aid_status_distribution(self, result_small):
        status = result_small["financial_aid"]["status"].value_counts(normalize=True)
        assert 0.30 <= status.get("Awarded", 0) <= 0.52
        assert 0.28 <= status.get("Disbursed", 0) <= 0.48

    def test_course_credits_distribution(self, result_small):
        credits = result_small["course"]["credits"].value_counts(normalize=True)
        # 3-credit courses should dominate (~60%)
        three_credit = credits.get("3", credits.get(3, 0))
        assert three_credit >= 0.45


class TestEducationBusinessRules:
    def test_gpa_range(self, result_small):
        gpa = result_small["student"]["gpa"]
        assert (gpa >= 0.0).all()
        assert (gpa <= 4.0).all()

    def test_cumulative_gpa_range(self, result_small):
        gpa = result_small["academic_standing"]["cumulative_gpa"]
        assert (gpa >= 0.0).all()
        assert (gpa <= 4.0).all()

    def test_semester_gpa_range(self, result_small):
        gpa = result_small["academic_standing"]["semester_gpa"]
        assert (gpa >= 0.0).all()
        assert (gpa <= 4.0).all()

    def test_enrolled_count_leq_capacity(self, result_small):
        sections = result_small["course_section"]
        violations = (sections["enrolled_count"] > sections["capacity"]).sum()
        assert violations == 0, f"{violations} sections have enrolled > capacity"

    def test_credits_positive(self, result_small):
        credits = result_small["course"]["credits"].astype(int)
        assert (credits > 0).all()

    def test_aid_amount_positive(self, result_small):
        amounts = result_small["financial_aid"]["amount"]
        assert (amounts > 0).all()

    def test_department_budget_positive(self, result_small):
        budgets = result_small["department"]["budget"]
        assert (budgets > 0).all()


class TestEducationReproducibility:
    def test_same_seed_same_output(self):
        s = Spindle()
        r1 = s.generate(domain=EducationDomain(), scale="small", seed=99)
        r2 = s.generate(domain=EducationDomain(), scale="small", seed=99)
        import pandas as pd
        for table in r1.tables:
            pd.testing.assert_frame_equal(r1[table], r2[table])
