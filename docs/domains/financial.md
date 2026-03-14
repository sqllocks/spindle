# Financial Domain

Banking domain with accounts, transactions, loans, and fraud detection.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `branch` | 200 | Bank branches with locations |
| `transaction_category` | 40 | Transaction category hierarchy |
| `customer` | 1,000 | Bank customers with credit tiers |
| `account` | 2,200 | Checking, savings, investment accounts |
| `card` | 1,760 | Credit/debit cards linked to accounts |
| `transaction` | 10,000 | Deposits, withdrawals, transfers, payments |
| `loan` | 400 | Mortgages, auto, personal, student loans |
| `loan_payment` | 4,800 | Monthly loan payments |
| `fraud_flag` | 200 | Suspicious transaction flags |
| `statement` | 13,200 | Monthly account statements |

## Quick Start

```python
from sqllocks_spindle import Spindle, FinancialDomain

result = Spindle().generate(domain=FinancialDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Pareto-distributed transaction frequency per account (heavy-tail spending)
- Log-normal balance and amount distributions matching real banking data
- Credit tier distribution (Excellent 22%, Good 35%, Fair 28%, Poor 15%)
- Multi-channel transactions (Online, Mobile, ATM, Branch, Phone, Wire)
- Fraud flag generation with risk scores and resolution tracking
- Loan payment split into principal and interest portions via formula

## Scale Presets

| Preset | `customer` | `transaction` |
| --- | --- | --- |
| `fabric_demo` | 100 | 1,000 |
| `small` | 1,000 | 10,000 |
| `medium` | 50,000 | 500,000 |
| `large` | 500,000 | 5,000,000 |
| `xlarge` | 5,000,000 | 100,000,000 |
| `warehouse` | 10,000,000 | 2,000,000,000 |
