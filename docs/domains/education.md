# Education Domain

Education domain with students, courses, enrollments, grades, and financial aid.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `department` | 25 | Academic departments |
| `instructor` | 150 | Faculty members |
| `course` | 300 | Course catalog |
| `student` | 2,000 | Enrolled students |
| `enrollment` | 16,000 | Course enrollments linking students to courses |
| `financial_aid` | 1,400 | Student financial aid awards |
| `course_section` | 600 | Scheduled course sections |
| `grade_appeal` | 320 | Grade appeals filed by students |
| `academic_standing` | 4,000 | Student academic standing history |

## Quick Start

```python
from sqllocks_spindle import Spindle, EducationDomain

result = Spindle().generate(domain=EducationDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Realistic grade distribution (A 18%, B+ 12%, B 14%, C 8%, F 4%, W 4%)
- Normal-distributed GPA (mean 3.0, sigma 0.5, range 0.0-4.0)
- Student classification progression (Freshman through Graduate)
- Semester-based enrollment with instructor assignments
- Financial aid awards with academic year tracking
- Course section capacity with correlated enrolled counts (50-95% fill)

## Scale Presets

| Preset | `student` |
| --- | --- |
| `fabric_demo` | 200 |
| `small` | 2,000 |
| `medium` | 20,000 |
| `large` | 200,000 |
| `warehouse` | 2,000,000 |
