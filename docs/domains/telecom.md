# Telecom Domain

Telecom domain with subscribers, service lines, usage records, billing, and churn.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `plan` | 20 | Telecom service plans |
| `device_model` | 40 | Phone and device models |
| `subscriber` | 2,000 | Telecom customers |
| `service_line` | 3,600 | Phone/data lines linked to subscribers |
| `usage_record` | 108,000 | Call detail records and usage data |
| `billing` | 12,000 | Monthly subscriber bills |
| `payment` | 12,000 | Bill payments |
| `network_event` | 7,200 | Network events and incidents |
| `churn_indicator` | 2,000 | Churn prediction indicators per subscriber |

## Quick Start

```python
from sqllocks_spindle import Spindle, TelecomDomain

result = Spindle().generate(domain=TelecomDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Service plan tiers (Prepaid 25%, Postpaid 35%, Family 25%, Business 15%)
- Device manufacturer distribution matching market share (Apple 35%, Samsung 28%)
- Usage records by type (Data 45%, Voice 25%, SMS 25%, MMS 5%)
- Billing lifecycle with payment status tracking (Paid, Pending, Overdue, Partial)
- Network events with cell tower IDs and signal strength measurements
- Churn prediction scores with risk levels and primary churn factors

## Scale Presets

| Preset | `subscriber` |
| --- | --- |
| `fabric_demo` | 200 |
| `small` | 2,000 |
| `medium` | 20,000 |
| `large` | 200,000 |
| `warehouse` | 2,000,000 |
