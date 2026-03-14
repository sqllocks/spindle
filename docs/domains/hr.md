# HR Domain

Human resources domain with employees, departments, compensation, and performance.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `department` | 30 | Organizational departments |
| `position` | 80 | Job titles and levels |
| `employee` | 500 | Employees with department and position assignments |
| `compensation` | 1,500 | Salary history with effective dates |
| `performance_review` | 1,250 | Annual performance reviews with ratings |
| `time_off_request` | 2,500 | PTO, sick, and personal leave requests |
| `training` | 100 | Training course catalog |
| `training_enrollment` | 2,000 | Employee training enrollments |
| `termination` | 75 | Employee terminations |

## Quick Start

```python
from sqllocks_spindle import Spindle, HrDomain

result = Spindle().generate(domain=HrDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Self-referencing manager hierarchy with configurable depth (max 4 levels, 5 roots)
- Log-normal salary distribution ($28K-$350K) with pay grade bands (G1-G7)
- Performance ratings following a bell curve (1-5 scale, mode at 3)
- Leave type distribution (PTO 45%, Sick 25%, Personal 12%, Parental 6%)
- Termination reasons with rehire eligibility tracking
- Training enrollment lifecycle with completion scores

## Scale Presets

| Preset | `employee` |
| --- | --- |
| `fabric_demo` | 100 |
| `small` | 500 |
| `medium` | 5,000 |
| `large` | 50,000 |
| `xlarge` | 500,000 |
| `warehouse` | 5,000,000 |
