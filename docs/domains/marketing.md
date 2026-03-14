# Marketing Domain

Marketing domain with campaigns, contacts, leads, opportunities, and conversions.

## Tables

| Table | Rows (small) | Description |
| --- | --- | --- |
| `campaign_type` | 15 | Campaign categories |
| `industry` | 25 | Target industries |
| `campaign` | 200 | Marketing campaigns |
| `lead_source` | 20 | Lead origins |
| `contact` | 5,000 | Marketing contacts |
| `lead` | 2,000 | Qualified leads from contacts |
| `opportunity` | 1,000 | Sales opportunities from leads |
| `email_send` | 10,000 | Email send events per campaign |
| `web_visit` | 25,000 | Website visits per contact |
| `conversion` | 600 | Conversion events from leads |

## Quick Start

```python
from sqllocks_spindle import Spindle, MarketingDomain

result = Spindle().generate(domain=MarketingDomain(), scale="small", seed=42)
print(result.summary())
```

## Key Features

- Full marketing funnel: Contact -> Lead -> Opportunity -> Conversion
- Multi-channel campaigns (Email 25%, Social 20%, PPC 18%, Content 15%)
- Lead scoring (0-100) with status progression (New, Contacted, Qualified, Converted)
- Email engagement metrics (25% open rate, 8% click rate, 3% bounce rate)
- Web visit tracking with referrer attribution (Organic, Paid, Social, Direct, Email)
- Opportunity pipeline stages with probability and deal amounts

## Scale Presets

| Preset | `campaign` |
| --- | --- |
| `fabric_demo` | 20 |
| `small` | 200 |
| `medium` | 2,000 |
| `large` | 20,000 |
| `warehouse` | 200,000 |
