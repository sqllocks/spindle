# IoT Domain

IoT domain with devices, sensors, readings, alerts, and maintenance.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `device_type` | 20 | IoT device type catalog |
| `location` | 100 | Deployment locations |
| `device` | 500 | Deployed IoT devices |
| `sensor` | 1,250 | Device sensors |
| `reading` | 25,000 | Sensor readings |
| `alert` | 250 | Device alerts |
| `maintenance_log` | 750 | Device maintenance records |
| `command` | 1,500 | Device commands |

## Quick Start

```python
from sqllocks_spindle import Spindle, IoTDomain

result = Spindle().generate(domain=IoTDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Multi-protocol devices (MQTT 35%, HTTP 25%, CoAP 15%, AMQP 15%, Modbus 10%)
- Sensor types with matching units (Temperature/C, Humidity/%RH, Pressure/Pa)
- Normal-distributed reading values with quality flags (Good 90%, Suspect 7%, Bad 3%)
- Alert severity levels with resolution tracking and acknowledgment status
- Maintenance types including preventive, corrective, calibration, and firmware updates
- Device lifecycle statuses (Active, Inactive, Maintenance, Decommissioned)

## Scale Presets

| Preset | `device` |
| --- | --- |
| `fabric_demo` | 50 |
| `small` | 500 |
| `medium` | 5,000 |
| `large` | 50,000 |
| `xlarge` | 500,000 |
| `warehouse` | 5,000,000 |
