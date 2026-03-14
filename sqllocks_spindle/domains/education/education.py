"""Education domain implementation."""

from __future__ import annotations

from pathlib import Path

from sqllocks_spindle.domains.base import Domain
from sqllocks_spindle.schema.parser import SpindleSchema


class EducationDomain(Domain):
    """Education domain — academic institutions, students, courses, and financial aid.

    Available schema modes:
        - 3nf: Normalized schema with proper 3NF relationships

    Tables (3NF mode):
        - department: Academic departments
        - instructor: Faculty members
        - course: Course catalog
        - student: Enrolled students
        - enrollment: Course enrollments
        - financial_aid: Aid awards
        - course_section: Scheduled sections
        - grade_appeal: Grade appeals
        - academic_standing: Standing history
    """

    @property
    def name(self) -> str:
        return "education"

    @property
    def description(self) -> str:
        return "Education domain with students, courses, enrollments, grades, and financial aid"

    @property
    def domain_path(self) -> Path:
        return Path(__file__).parent

    def _build_schema(self) -> SpindleSchema:
        """Build education 3NF schema programmatically."""
        from sqllocks_spindle.schema.parser import SchemaParser

        schema_dict = {
            "model": {
                "name": f"education_{self._schema_mode}",
                "description": f"Education domain — {self._schema_mode} schema",
                "domain": "education",
                "schema_mode": self._schema_mode,
                "locale": "en_US",
                "seed": 42,
                "date_range": {"start": "2020-01-01", "end": "2025-12-31"},
            },
            "tables": {
                # ── department ────────────────────────────────────
                "department": {
                    "description": "Academic departments",
                    "primary_key": ["department_id"],
                    "columns": {
                        "department_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "department_name": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "department_names",
                            },
                        },
                        "college": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("department.college", {
                                    "Arts & Sciences": 0.30,
                                    "Engineering": 0.20,
                                    "Business": 0.18,
                                    "Education": 0.12,
                                    "Health Sciences": 0.12,
                                    "Law": 0.08,
                                }),
                            },
                        },
                        "budget": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 13.5,
                                "sigma": 0.8,
                                "min": 500000.0,
                                "max": 20000000.0,
                                "round": 2,
                            },
                        },
                    },
                },

                # ── instructor ────────────────────────────────────
                "instructor": {
                    "description": "Faculty members",
                    "primary_key": ["instructor_id"],
                    "columns": {
                        "instructor_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "first_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "first_name"},
                        },
                        "last_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "last_name"},
                        },
                        "email": {
                            "type": "string",
                            "max_length": 255,
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "department_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "department.department_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "rank": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("instructor.rank", {
                                    "Professor": 0.20,
                                    "Associate Professor": 0.20,
                                    "Assistant Professor": 0.25,
                                    "Lecturer": 0.15,
                                    "Adjunct": 0.20,
                                }),
                            },
                        },
                        "tenure_status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("instructor.tenure_status", {
                                    "Tenured": 0.35,
                                    "Tenure-Track": 0.25,
                                    "Non-Tenure": 0.40,
                                }),
                            },
                        },
                        "hire_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2000-01-01",
                                "end": "2025-06-30",
                                "pattern": "uniform",
                            },
                        },
                    },
                },

                # ── course ────────────────────────────────────────
                "course": {
                    "description": "Course catalog",
                    "primary_key": ["course_id"],
                    "columns": {
                        "course_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "course_code": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "pattern",
                                "format": "CRS-{seq:4}",
                            },
                        },
                        "course_name": {
                            "type": "string",
                            "max_length": 200,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "course_catalog",
                            },
                        },
                        "department_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "department.department_id",
                            },
                        },
                        "credits": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("course.credits", {
                                    "1": 0.05,
                                    "2": 0.10,
                                    "3": 0.60,
                                    "4": 0.25,
                                }),
                            },
                        },
                        "level": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("course.level", {
                                    "100": 0.25,
                                    "200": 0.22,
                                    "300": 0.20,
                                    "400": 0.15,
                                    "500": 0.10,
                                    "600": 0.08,
                                }),
                            },
                        },
                    },
                },

                # ── student ───────────────────────────────────────
                "student": {
                    "description": "Enrolled students",
                    "primary_key": ["student_id"],
                    "columns": {
                        "student_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "first_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "first_name"},
                        },
                        "last_name": {
                            "type": "string",
                            "max_length": 50,
                            "generator": {"strategy": "faker", "provider": "last_name"},
                        },
                        "email": {
                            "type": "string",
                            "max_length": 255,
                            "generator": {"strategy": "faker", "provider": "email"},
                        },
                        "date_of_birth": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "1990-01-01",
                                "end": "2006-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "enrollment_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2018-08-01",
                                "end": "2025-08-31",
                                "pattern": "uniform",
                            },
                        },
                        "major_department_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "department.department_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "gpa": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "normal",
                                "mean": 3.0,
                                "sigma": 0.5,
                                "min": 0.0,
                                "max": 4.0,
                                "round": 2,
                            },
                        },
                        "classification": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("student.classification", {
                                    "Freshman": 0.22,
                                    "Sophomore": 0.20,
                                    "Junior": 0.20,
                                    "Senior": 0.20,
                                    "Graduate": 0.18,
                                }),
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("student.status", {
                                    "Active": 0.80,
                                    "Graduated": 0.12,
                                    "Withdrawn": 0.05,
                                    "Suspended": 0.03,
                                }),
                            },
                        },
                    },
                },

                # ── enrollment ────────────────────────────────────
                "enrollment": {
                    "description": "Course enrollments linking students to courses",
                    "primary_key": ["enrollment_id"],
                    "columns": {
                        "enrollment_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "student_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "student.student_id",
                            },
                        },
                        "course_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "course.course_id",
                                "distribution": "zipf",
                                "alpha": 1.3,
                            },
                        },
                        "instructor_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "instructor.instructor_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "semester": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("enrollment.semester", {
                                    "Fall 2023": 0.20,
                                    "Spring 2024": 0.22,
                                    "Summer 2024": 0.12,
                                    "Fall 2024": 0.22,
                                    "Spring 2025": 0.24,
                                }),
                            },
                        },
                        "grade": {
                            "type": "string",
                            "max_length": 2,
                            "nullable": True,
                            "null_rate": 0.10,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("enrollment.grade", {
                                    "A": 0.18,
                                    "A-": 0.10,
                                    "B+": 0.12,
                                    "B": 0.14,
                                    "B-": 0.10,
                                    "C+": 0.08,
                                    "C": 0.08,
                                    "C-": 0.05,
                                    "D": 0.05,
                                    "F": 0.04,
                                    "W": 0.04,
                                    "I": 0.02,
                                }),
                            },
                        },
                        "credits_attempted": {
                            "type": "integer",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("enrollment.credits_attempted", {
                                    "1": 0.05,
                                    "2": 0.10,
                                    "3": 0.60,
                                    "4": 0.25,
                                }),
                            },
                        },
                    },
                },

                # ── financial_aid ─────────────────────────────────
                "financial_aid": {
                    "description": "Student financial aid awards",
                    "primary_key": ["aid_id"],
                    "columns": {
                        "aid_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "student_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "student.student_id",
                            },
                        },
                        "aid_type": {
                            "type": "string",
                            "max_length": 100,
                            "generator": {
                                "strategy": "reference_data",
                                "dataset": "aid_types",
                            },
                        },
                        "academic_year": {
                            "type": "string",
                            "max_length": 9,
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("financial_aid.academic_year", {
                                    "2021-2022": 0.12,
                                    "2022-2023": 0.18,
                                    "2023-2024": 0.25,
                                    "2024-2025": 0.30,
                                    "2025-2026": 0.15,
                                }),
                            },
                        },
                        "amount": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "log_normal",
                                "mean": 8.5,
                                "sigma": 0.8,
                                "min": 500.0,
                                "max": 50000.0,
                                "round": 2,
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("financial_aid.status", {
                                    "Awarded": 0.40,
                                    "Disbursed": 0.38,
                                    "Pending": 0.15,
                                    "Cancelled": 0.07,
                                }),
                            },
                        },
                    },
                },

                # ── course_section ────────────────────────────────
                "course_section": {
                    "description": "Scheduled course sections",
                    "primary_key": ["section_id"],
                    "columns": {
                        "section_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "course_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "course.course_id",
                            },
                        },
                        "instructor_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "instructor.instructor_id",
                                "distribution": "zipf",
                                "alpha": 1.2,
                            },
                        },
                        "semester": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("course_section.semester", {
                                    "Fall 2023": 0.20,
                                    "Spring 2024": 0.22,
                                    "Summer 2024": 0.12,
                                    "Fall 2024": 0.22,
                                    "Spring 2025": 0.24,
                                }),
                            },
                        },
                        "room_number": {
                            "type": "string",
                            "max_length": 10,
                            "generator": {
                                "strategy": "pattern",
                                "format": "RM-{seq:3}",
                            },
                        },
                        "capacity": {
                            "type": "integer",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "uniform",
                                "min": 20,
                                "max": 300,
                            },
                        },
                        "enrolled_count": {
                            "type": "integer",
                            "generator": {
                                "strategy": "correlated",
                                "source_column": "capacity",
                                "operation": "multiply",
                                "params": {"factor_min": 0.50, "factor_max": 0.95},
                            },
                        },
                    },
                },

                # ── grade_appeal ──────────────────────────────────
                "grade_appeal": {
                    "description": "Grade appeals filed by students",
                    "primary_key": ["appeal_id"],
                    "columns": {
                        "appeal_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "enrollment_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "enrollment.enrollment_id",
                            },
                        },
                        "filed_date": {
                            "type": "date",
                            "generator": {
                                "strategy": "temporal",
                                "start": "2023-01-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                        "reason": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("grade_appeal.reason", {
                                    "Calculation Error": 0.30,
                                    "Grading Criteria": 0.25,
                                    "Incomplete Grade": 0.20,
                                    "Medical": 0.15,
                                    "Other": 0.10,
                                }),
                            },
                        },
                        "status": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("grade_appeal.status", {
                                    "Pending": 0.20,
                                    "Under Review": 0.25,
                                    "Approved": 0.30,
                                    "Denied": 0.25,
                                }),
                            },
                        },
                        "resolution_date": {
                            "type": "date",
                            "nullable": True,
                            "null_rate": 0.30,
                            "generator": {
                                "strategy": "temporal",
                                "start": "2023-02-01",
                                "end": "2025-12-31",
                                "pattern": "uniform",
                            },
                        },
                    },
                },

                # ── academic_standing ─────────────────────────────
                "academic_standing": {
                    "description": "Student academic standing history",
                    "primary_key": ["standing_id"],
                    "columns": {
                        "standing_id": {
                            "type": "integer",
                            "generator": {"strategy": "sequence", "start": 1},
                        },
                        "student_id": {
                            "type": "integer",
                            "generator": {
                                "strategy": "foreign_key",
                                "ref": "student.student_id",
                            },
                        },
                        "semester": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("academic_standing.semester", {
                                    "Fall 2023": 0.20,
                                    "Spring 2024": 0.22,
                                    "Summer 2024": 0.12,
                                    "Fall 2024": 0.22,
                                    "Spring 2025": 0.24,
                                }),
                            },
                        },
                        "standing": {
                            "type": "string",
                            "generator": {
                                "strategy": "weighted_enum",
                                "values": self._dist("academic_standing.standing", {
                                    "Good Standing": 0.70,
                                    "Dean's List": 0.15,
                                    "Probation": 0.10,
                                    "Suspension": 0.05,
                                }),
                            },
                        },
                        "cumulative_gpa": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "normal",
                                "mean": 3.0,
                                "sigma": 0.5,
                                "min": 0.0,
                                "max": 4.0,
                                "round": 2,
                            },
                        },
                        "semester_gpa": {
                            "type": "float",
                            "generator": {
                                "strategy": "distribution",
                                "distribution": "normal",
                                "mean": 3.0,
                                "sigma": 0.6,
                                "min": 0.0,
                                "max": 4.0,
                                "round": 2,
                            },
                        },
                    },
                },
            },

            # ── relationships ────────────────────────────────────
            "relationships": [
                {
                    "name": "instructor_department",
                    "parent": "department",
                    "child": "instructor",
                    "parent_key": "department_id",
                    "child_key": "department_id",
                },
                {
                    "name": "course_department",
                    "parent": "department",
                    "child": "course",
                    "parent_key": "department_id",
                    "child_key": "department_id",
                },
                {
                    "name": "student_major",
                    "parent": "department",
                    "child": "student",
                    "parent_key": "department_id",
                    "child_key": "major_department_id",
                },
                {
                    "name": "enrollment_student",
                    "parent": "student",
                    "child": "enrollment",
                    "parent_key": "student_id",
                    "child_key": "student_id",
                },
                {
                    "name": "enrollment_course",
                    "parent": "course",
                    "child": "enrollment",
                    "parent_key": "course_id",
                    "child_key": "course_id",
                },
                {
                    "name": "enrollment_instructor",
                    "parent": "instructor",
                    "child": "enrollment",
                    "parent_key": "instructor_id",
                    "child_key": "instructor_id",
                },
                {
                    "name": "financial_aid_student",
                    "parent": "student",
                    "child": "financial_aid",
                    "parent_key": "student_id",
                    "child_key": "student_id",
                },
                {
                    "name": "course_section_course",
                    "parent": "course",
                    "child": "course_section",
                    "parent_key": "course_id",
                    "child_key": "course_id",
                },
                {
                    "name": "course_section_instructor",
                    "parent": "instructor",
                    "child": "course_section",
                    "parent_key": "instructor_id",
                    "child_key": "instructor_id",
                },
                {
                    "name": "grade_appeal_enrollment",
                    "parent": "enrollment",
                    "child": "grade_appeal",
                    "parent_key": "enrollment_id",
                    "child_key": "enrollment_id",
                },
                {
                    "name": "academic_standing_student",
                    "parent": "student",
                    "child": "academic_standing",
                    "parent_key": "student_id",
                    "child_key": "student_id",
                },
            ],

            # ── business rules ───────────────────────────────────
            "business_rules": [
                {
                    "name": "gpa_range",
                    "type": "constraint",
                    "table": "student",
                    "rule": "gpa >= 0.0 AND gpa <= 4.0",
                },
                {
                    "name": "aid_amount_positive",
                    "type": "constraint",
                    "table": "financial_aid",
                    "rule": "amount > 0",
                },
                {
                    "name": "enrolled_leq_capacity",
                    "description": "Enrolled count must be <= section capacity",
                    "type": "column_comparison",
                    "table": "course_section",
                    "left": "enrolled_count",
                    "operator": "<=",
                    "right": "capacity",
                },
            ],

            # ── generation config ────────────────────────────────
            "generation": {
                "scales": {
                    "fabric_demo": {"student": 200},
                    "small": {"student": 2000},
                    "medium": {"student": 20000},
                    "large": {"student": 200000},
                    "warehouse": {"student": 2000000},
                },
                "derived_counts": {
                    "department": {"fixed": 25},
                    "instructor": {"fixed": 150},
                    "course": {"fixed": 300},
                    "enrollment": {"per_parent": "student", "ratio": self._ratio("enrollment_per_student", 8.0)},
                    "financial_aid": {"per_parent": "student", "ratio": self._ratio("financial_aid_per_student", 0.7)},
                    "course_section": {"per_parent": "course", "ratio": self._ratio("course_section_per_course", 2.0)},
                    "grade_appeal": {"per_parent": "enrollment", "ratio": self._ratio("grade_appeal_per_enrollment", 0.02)},
                    "academic_standing": {"per_parent": "student", "ratio": self._ratio("academic_standing_per_student", 2.0)},
                },
            },
        }

        parser = SchemaParser()
        return parser.parse_dict(schema_dict)

    def star_schema_map(self):
        """Return the star schema mapping for the Education domain.

        Produces:
          - dim_student    (from student)
          - dim_course     (from course)
          - dim_instructor (from instructor)
          - dim_department (from department)
          - fact_enrollment    (from enrollment)
          - fact_financial_aid (from financial_aid)
        """
        from sqllocks_spindle.transform.star_schema import DimSpec, FactSpec, StarSchemaMap

        return StarSchemaMap(
            dims={
                "dim_student": DimSpec(
                    source="student",
                    sk="sk_student",
                    nk="student_id",
                ),
                "dim_course": DimSpec(
                    source="course",
                    sk="sk_course",
                    nk="course_id",
                ),
                "dim_instructor": DimSpec(
                    source="instructor",
                    sk="sk_instructor",
                    nk="instructor_id",
                ),
                "dim_department": DimSpec(
                    source="department",
                    sk="sk_department",
                    nk="department_id",
                ),
            },
            facts={
                "fact_enrollment": FactSpec(
                    primary="enrollment",
                    fk_map={
                        "student_id": "dim_student",
                        "course_id": "dim_course",
                        "instructor_id": "dim_instructor",
                    },
                    date_cols=["enrollment_date"],
                ),
                "fact_financial_aid": FactSpec(
                    primary="financial_aid",
                    fk_map={"student_id": "dim_student"},
                    date_cols=["award_date"],
                ),
            },
        )

    def cdm_map(self):
        """Return the CDM entity map for the Education domain.

        Maps source table names to Microsoft Common Data Model entity names.
        """
        from sqllocks_spindle.transform.cdm_mapper import CdmEntityMap

        return CdmEntityMap({
            "department": "BusinessUnit",
            "instructor": "Worker",
            "course": "Course",
            "student": "Contact",
            "enrollment": "CourseEnrollment",
            "financial_aid": "Award",
            "course_section": "CourseSection",
            "grade_appeal": "Case",
            "academic_standing": "Assessment",
        })
