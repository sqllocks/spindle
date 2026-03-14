# Retail Domain

Retail / E-Commerce domain with customers, products, orders, and returns.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `customer` | 1,000 | Individual customers |
| `address` | 1,500 | Customer addresses (1:N) |
| `product_category` | 50 | 3-level category hierarchy |
| `product` | 500 | Individual SKUs |
| `store` | 150 | Physical and online stores |
| `promotion` | ~200 | Marketing promotions |
| `order` | 5,000 | Customer orders (header) |
| `order_line` | 12,500 | Individual line items within an order |
| `return` | 850 | Return transactions |

## Quick Start

```python
from sqllocks_spindle import Spindle, RetailDomain

result = Spindle().generate(domain=RetailDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Seasonal order patterns with November/December peaks and bimodal hour-of-day distribution
- Pareto-distributed customer orders (power-law repeat buyers)
- Zipf-distributed product popularity on order lines
- Log-normal price distributions with correlated cost (30-70% of price)
- Geometric quantity distribution on line items
- Promotion linkage with conditional discount calculation

## Scale Presets

| Preset | `customer` | `product` | `order` |
| --- | --- | --- | --- |
| `fabric_demo` | 200 | 100 | 1,000 |
| `small` | 1,000 | 500 | 5,000 |
| `medium` | 50,000 | 5,000 | 500,000 |
| `large` | 500,000 | 25,000 | 5,000,000 |
| `xlarge` | 5,000,000 | 100,000 | 100,000,000 |
| `warehouse` | 1,000,000 | 50,000 | 10,000,000 |
