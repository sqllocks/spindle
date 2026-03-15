# Capital Markets Domain

Public equities market data with real S&P 500 tickers, daily OHLCV pricing via Geometric Brownian Motion, corporate actions, earnings, and insider transactions.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `exchange` | 3 | Stock exchanges (NYSE, NASDAQ, AMEX) |
| `sector` | 11 | GICS sectors |
| `industry` | 61 | GICS industries with sector FK |
| `company` | 100 | Public companies with real S&P 500 tickers and SEC EDGAR CIKs |
| `daily_price` | ~75,600 | Daily OHLCV bars (252 trading days/year x 3 years x 100 companies) |
| `trade` | ~300,000 | Tick-level trades for streaming demos |
| `dividend` | ~450 | Dividend payments with ex-date, pay-date, frequency |
| `split` | ~15 | Stock splits with ratio (2:1, 3:1, etc.) |
| `earnings` | ~1,200 | Quarterly earnings with EPS estimate, actual, and surprise % |
| `insider_transaction` | ~600 | SEC Form 4 insider trades (BUY/SELL/GRANT) |

## Quick Start

```python
from sqllocks_spindle import Spindle, CapitalMarketsDomain

result = Spindle().generate(domain=CapitalMarketsDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- **Real tickers** — 110 S&P 500 companies with actual CIK numbers, GICS sectors/industries, and exchanges
- **GBM pricing** — daily OHLCV via Geometric Brownian Motion with enforced OHLC business rules (high >= low, close within range)
- **Log-normal volumes** — realistic trading volume distributions (mean ~33K shares/day)
- **Zipf ticker distribution** — tick-level trades concentrated in mega-cap names (alpha=1.5)
- **Dividend frequency** — 75% quarterly, 20% annual, 5% special
- **Earnings surprise** — EPS actual correlated to estimate (85-120%), surprise % normal(0%, 2.5%)
- **Insider titles** — weighted distribution (CEO 15%, CFO 12%, Director 25%, VP 15%, etc.)
- **Star schema map** — 4 dimensions (company, exchange, sector, industry) + 4 facts (daily_price, dividend, earnings, insider_txn)

## Scale Presets

| Preset | `company` | Years | Approx `daily_price` Rows |
| --- | --- | --- | --- |
| `fabric_demo` | 30 | 1 | ~7,500 |
| `small` | 100 | 3 | ~75,600 |
| `medium` | 500 | 5 | ~630,000 |
| `large` | 1,000 | 10 | ~2,520,000 |
| `xlarge` | 4,000 | 20 | ~20,160,000 |
