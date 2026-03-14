# Healthcare Domain

Healthcare domain with patients, encounters, diagnoses, procedures, and claims.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `provider` | 100 | Doctors, nurses, and healthcare professionals |
| `facility` | 50 | Hospitals, clinics, and care sites |
| `patient` | 500 | Patient demographics and insurance |
| `encounter` | 2,500 | Patient visits/admissions |
| `diagnosis` | 4,500 | ICD-10 diagnoses linked to encounters |
| `procedure` | 3,000 | CPT procedures linked to encounters |
| `medication` | 2,250 | Prescriptions linked to encounters |
| `claim` | 2,375 | Insurance claims linked to encounters |
| `claim_line` | 5,938 | Line items on claims linked to procedures |

## Quick Start

```python
from sqllocks_spindle import Spindle, HealthcareDomain

result = Spindle().generate(domain=HealthcareDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Realistic encounter type distribution (Outpatient 55%, Inpatient 15%, ER 15%, Telehealth 10%)
- ICD-10 diagnosis codes from reference data with Zipf-distributed frequency
- CPT procedure codes with log-normal charge amounts
- Claims with computed totals from child claim_line rows
- Allowed/paid amount waterfall: charge > allowed > paid
- Seasonal encounter patterns with weekday/hour-of-day profiles

## Scale Presets

| Preset | `patient` | `provider` | `encounter` |
| --- | --- | --- | --- |
| `fabric_demo` | 100 | 20 | 500 |
| `small` | 500 | 100 | 2,500 |
| `medium` | 25,000 | 2,000 | 150,000 |
| `large` | 250,000 | 15,000 | 1,500,000 |
| `xlarge` | 2,500,000 | 100,000 | 20,000,000 |
| `warehouse` | 500,000 | 30,000 | 5,000,000 |
