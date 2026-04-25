# Supply Chain Domain

Supply chain domain with warehouses, purchasing, inventory, and logistics.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `warehouse` | 50 | Distribution centers with locations |
| `supplier` | 200 | Material and product suppliers |
| `material` | 300 | Raw materials and components |
| `purchase_order` | 2,000 | Purchase orders to suppliers |
| `purchase_order_line` | 6,000 | PO line items |
| `inventory` | 900 | Stock levels by warehouse and material |
| `shipment` | 2,400 | Inbound/outbound shipments |
| `shipment_event` | 9,600 | Shipment tracking events |
| `quality_inspection` | 720 | QA inspection results |
| `demand_forecast` | 1,800 | Forecasted demand by period |

## Quick Start

```python
from sqllocks_spindle import Spindle, SupplyChainDomain

result = Spindle().generate(domain=SupplyChainDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Multi-country supplier distribution (US 35%, China 20%, Germany 10%, etc.)
- Supplier reliability scores and lead time tracking
- Correlated received vs. ordered quantities (85-100% fill rate)
- Shipment lifecycle with tracking events (Picked Up through Delivered)
- Quality inspection pass/fail/conditional results with defect counts
- Demand forecasting with multiple methods (ARIMA, ML, Exponential Smoothing)

## Scale Presets

| Preset | `purchase_order` |
| --- | --- |
| `fabric_demo` | 200 |
| `small` | 2,000 |
| `medium` | 20,000 |
| `large` | 200,000 |
| `xlarge` | 2,000,000 |
| `warehouse` | 20,000,000 |
