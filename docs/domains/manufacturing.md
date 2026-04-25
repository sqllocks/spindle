# Manufacturing Domain

Manufacturing domain with production lines, work orders, quality control, and equipment.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `production_line` | 20 | Manufacturing production lines |
| `product` | 100 | Manufactured products |
| `bom` | 500 | Bill of materials -- components per product |
| `work_order` | 500 | Production work orders |
| `quality_check` | 1,500 | Quality control inspections |
| `defect` | 450 | Defect records from quality checks |
| `equipment` | 80 | Machines, tools, and equipment |
| `downtime_event` | 320 | Equipment downtime tracking |
| `production_metric` | 2,500 | Production KPIs per work order |

## Quick Start

```python
from sqllocks_spindle import Spindle, ManufacturingDomain

result = Spindle().generate(domain=ManufacturingDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Multi-facility production lines (Plant A-D) with capacity tracking
- Bill of materials with critical component flagging
- Correlated produced vs. planned quantities (85-100% yield)
- QC inspection results (Pass 82%, Fail 8%, Rework 10%)
- Defect root cause analysis (Material, Process, Equipment, Human, Design)
- OEE scores, yield rates, cycle times, and scrap rates as production KPIs

## Scale Presets

| Preset | `work_order` |
| --- | --- |
| `fabric_demo` | 50 |
| `small` | 500 |
| `medium` | 5,000 |
| `large` | 50,000 |
| `warehouse` | 5,000,000 |
