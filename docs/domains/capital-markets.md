# Capital Markets Domain

Securities, market data, corporate actions, and trading for S&P 500-scale datasets.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `exchange` | 3 | Stock exchanges (NYSE, NASDAQ, CBOE) |
| `sector` | 11 | GICS sectors |
| `industry` | 69 | GICS industries within sectors |
| `company` | 100 | Public companies with ticker, market cap, sector/industry |
| `daily_price` | ~25,000 | OHLCV daily bars via geometric Brownian motion (GBM) |
| `dividend` | ~400 | Quarterly dividend payments |
| `split` | ~20 | Stock split events |
| `earnings` | ~400 | Quarterly earnings (EPS, revenue, beat/miss) |
| `insider_transaction` | ~1,000 | SEC Form 4 insider buys/sells |
| `trade` | ~50,000 | Tick-level trade execution records |

## Quick Start

```python
from sqllocks_spindle import Spindle
from sqllocks_spindle.domains.capital_markets import CapitalMarketsDomain

result = Spindle().generate(domain=CapitalMarketsDomain(), scale="small", seed=42)
print(result.summary())

# Access market data
prices = result["daily_price"]
print(prices[["ticker", "trade_date", "open", "high", "low", "close", "volume"]].head())
```

## Key Features

- **Geometric Brownian Motion (GBM)** for daily price generation — produces realistic random walks with drift and volatility
- **S&P 500 reference data** for company tickers, sectors, and industries
- **GICS sector/industry hierarchy** with proper FK relationships
- **Corporate actions** — dividends, splits, and earnings tied to company calendars
- **Insider transactions** modeled on SEC Form 4 patterns
- **Tick-level trades** with realistic price/volume distributions

## Scale Presets

| Preset | `company` | `daily_price` | `trade` |
| --- | --- | --- | --- |
| `fabric_demo` | 30 | ~7,500 | ~15,000 |
| `small` | 100 | ~25,000 | ~50,000 |
| `medium` | 500 | ~125,000 | ~500,000 |
| `large` | 1,000 | ~250,000 | ~2,000,000 |
| `xlarge` | 4,000 | ~1,000,000 | ~10,000,000 |

## Star Schema

4 dimensions + 4 fact tables:

| Type | Table | Description |
| --- | --- | --- |
| Dimension | `dim_company` | Company master with sector/industry denormalized |
| Dimension | `dim_exchange` | Exchange reference |
| Dimension | `dim_sector` | GICS sector reference |
| Dimension | `dim_date` | Standard date dimension |
| Fact | `fact_daily_price` | Daily OHLCV bars |
| Fact | `fact_dividend` | Dividend events |
| Fact | `fact_earnings` | Quarterly earnings |
| Fact | `fact_insider_transaction` | Insider trades |
