# Spindle Data Generation Methodology

This document describes how Spindle's distribution weights are calibrated against real-world data
and the sources used. All defaults in the `profiles/default.json` for each domain reflect these
calibrated values.

## Overview

Spindle generates synthetic data that mirrors real-world statistical distributions. Each domain
(Retail, Healthcare) has a default profile with weights derived from authoritative public data.
Users can override any weight via named profiles or the `overrides` parameter.

---

## Retail Domain

### Customer Demographics

| Parameter | Value | Source |
|-----------|-------|--------|
| Gender (M/F) | 49% / 51% | CapitalOne Shopping Research (2024); derived from online shopping participation rates × Census population |
| Loyalty tiers (Basic/Silver/Gold/Platinum) | 55% / 25% / 13% / 7% | Antavo, Mastercard, Voucherify tiered program design guides (2024); Bond Brand Loyalty Report 2024 |
| Active rate | 85% active | Industry standard estimate |
| Addresses per customer | 1.5 avg | Industry convention (65-70% have 1 address, 20-25% have 2) |

### Order Patterns

| Parameter | Value | Source |
|-----------|-------|--------|
| Customer order frequency | Pareto α=1.16 | Classic 80/20 rule; Shopify, Unific, SmartKarrot (2024) |
| Order status (completed/shipped/processing/cancelled/returned) | 77% / 8% / 2% / 4% / 9% | NRF/Happy Returns Report 2024 (16.9% return rate); Opensend cancellation stats (2024) |
| Items per order | 2.5 avg (geometric p=0.6) | Statista (2024): 4.95 units/transaction overall; adjusted down excluding grocery |
| Product popularity | Zipf α=1.5 | 80/20 SKU concentration rule; multiple industry analyses |

### Seasonality

| Parameter | Value | Source |
|-----------|-------|--------|
| Monthly distribution | Dec 10.6%, Nov 9.6%, Aug 8.8%, Feb 6.8% (lowest) | US Census Monthly Retail Trade (FRED); NRF holiday sales reports |
| Day of week | Mon 15.5% (peak), Sat 11.5% (lowest) | Doofinder, SaleCycle, ECDB (2024) — US-specific weekday pattern |
| Hour of day | Bimodal peaks at 12pm and 8pm | SureBright, ECDB (2024) — lunch peak + larger evening peak |

### Returns

| Parameter | Value | Source |
|-----------|-------|--------|
| Return rate | 17% of orders | NRF/Happy Returns (2024): 16.9% overall, up to 20.4% for e-commerce |
| Return reasons | wrong_size 25%, changed_mind 22%, not_as_described 20%, defective 15%, damaged 10%, late 5%, other 3% | Channelwill (2025), NRF (2024), Shopify return reason surveys |

### Promotions

| Parameter | Value | Source |
|-----------|-------|--------|
| Discount levels | 20% most common at 25%; 10% at 25% | Statista US e-shoppers survey (2024); Opensend average discount 19% |
| Promo types | percent_off 40%, free_shipping 15%, fixed_amount 20%, bogo 15%, bundle 10% | RetailMeNot, CouponFollow (2024), DemandSage coupon stats |
| Orders with any promo | ~35-40% | CapitalOne Shopping (2024): 39.9% of carts matchable to working code |

### Product Lifecycle

| Parameter | Value | Source |
|-----------|-------|--------|
| Active / Introduced / Discontinued | 75% / 8% / 17% | Shopify inventory turnover guidance (2024); Onramp Funds benchmarks (2025) |

---

## Healthcare Domain

### Provider Workforce

| Parameter | Value | Source |
|-----------|-------|--------|
| Credentials (MD/DO/NP/PA/RN) | 55% / 11% / 20% / 10% / 4% | AAMC 2024 Key Findings (~1M active physicians, 85% MD/15% DO); BLS OES May 2023 (280K NP, 146K PA, 3.2M RN); billing-eligible provider weighting |
| Active rate | 90% | AMA Physician Masterfile; estimated |
| Specialties | 32 specialties with weights | AAMC specialty distribution data |

### Facility Distribution

| Parameter | Value | Source |
|-----------|-------|--------|
| Facility types | Hospital 25%, Clinic 35%, Urgent Care 15%, Surgery Center 10%, Rehab 8%, Psych 4%, LTC 3% | AHA Hospital Statistics (2023); CMS Provider Enrollment data |

### Patient Demographics

| Parameter | Value | Source |
|-----------|-------|--------|
| Gender (M/F/NB) | 49% / 50% / 1% | US Census Bureau (2020): 50.5% F / 49.5% M; nonbinary estimate from Williams Institute |
| Race/Ethnicity | White 57.8%, Hispanic 18.7%, Black 12.4%, Asian 6.0%, Other 5.1% | US Census Bureau (2020 Decennial Census) |
| Insurance payer mix | Weighted across 23 plans | KFF Health Insurance Coverage (2023); CMS enrollment data |
| Active rate | 88% | Estimated based on patient churn research |

### Encounter Patterns

| Parameter | Value | Source |
|-----------|-------|--------|
| Encounter types | Outpatient 70%, Emergency 10%, Inpatient 8%, Telehealth 8%, Observation 4% | CDC NAMCS (~860M office visits, 2018 — most recent NAMCS physician survey); CDC NHAMCS (155M ED visits, 2022); AHA (33.7M inpatient, 2022); CMS/Epic telehealth at ~5% |
| Patient frequency | Pareto α=1.05, max 80 | MEPS Statistical Brief #560: top 5% of patients = 51.2% of spending; top 1% = 24% |
| Provider concentration | Zipf α=1.2 | Provider panel size variation |
| Facility concentration | Zipf α=1.4 | Hospital market concentration (HHI data) |

### Encounter Seasonality

| Parameter | Value | Source |
|-----------|-------|--------|
| Monthly distribution | Jan 9.6% (flu peak), Jun-Jul 7.5% (summer low) | CDC MMWR influenza surveillance; HCUP seasonal patterns; ~66% of conditions show 12-month periodicity (PLOS ONE 2017) |
| Day of week | Mon 19.5% (peak), Sat 6.5%, Sun 5.0% | PMC9068998 weekday/seasonal ED study; outpatient scheduling heavily Mon-Fri |
| Hour of day | Bimodal peaks at 9am and 2pm | Clinic scheduling patterns; ED arrival time studies |

### Diagnoses

| Parameter | Value | Source |
|-----------|-------|--------|
| ICD-10 codes | 48 codes with weights | CMS top diagnosis codes from Medicare FFS claims (2022) |
| Diagnosis types | Primary 35%, Secondary 40%, Admitting 15%, External Cause 10% | CMS claims coding guidelines; average 1.8 diagnoses per encounter |
| Chronic rate | 30% chronic | CDC National Health Interview Survey (2022) |

### Procedures

| Parameter | Value | Source |
|-----------|-------|--------|
| CPT codes | 49 codes with charges | CMS Medicare Physician Fee Schedule (2024); top E/M, lab, imaging, surgery codes |
| Modifiers | 26 (30%), TC (25%), 59 (20%), 25 (15%), 76 (10%) | CMS modifier usage statistics |
| Avg procedures per encounter | 1.2 | CMS claims data average |

### Medications

| Parameter | Value | Source |
|-----------|-------|--------|
| Top medications | 40 drugs with weights | CMS Part D Prescriber data (2022); IQVIA top 200 drugs |
| Frequency | Once daily 35%, Twice daily 25%, As needed 15%, etc. | Clinical prescribing guidelines |
| Days supply | 30-day 50%, 90-day 25%, 14-day 10%, 7-day 8%, 60-day 7% | CMS Part D days supply distribution |
| Avg prescriptions per encounter | 0.9 | CMS claims utilization |

### Claims & Billing

| Parameter | Value | Source |
|-----------|-------|--------|
| Claim types | Professional (CMS-1500) 55%, Institutional (UB-04) 30%, Pharmacy 15% | CMS claims volume by form type |
| Claim status | Paid 72%, Denied 15%, Pending 5%, Partially Paid 5%, Appealed 3% | KFF ACA Marketplace denial data (2023): 19-20% denial rate; Health Affairs MA study (2024): 15.7%; Aptarro industry-wide 11.8% (2024) |
| Allowed-to-charge ratio | 55-90% of billed charges | CMS Medicare fee schedule vs. billed charges; FAIR Health data |
| Paid-to-allowed ratio | 70-100% of allowed amount | CMS reimbursement rates; commercial payer averages |
| Patient copays | $0-$100 range, $25 most common at 20% | KFF Employer Health Benefits Survey (2024) |
| Avg claim lines per claim | 2.5 | CMS claims line count averages |

---

## Calibration Confidence Levels

| Level | Meaning | Examples |
|-------|---------|---------|
| **High** | Based on authoritative published statistics | Census demographics, NRF return rates, CMS claims data |
| **Medium** | Derived from multiple consistent industry sources | Loyalty tiers, promotion types, seasonal patterns |
| **Low** | Informed estimates without direct public benchmarks | Addresses per customer, product lifecycle phases |

## Updating Distributions

To override any distribution, create a named profile JSON or pass overrides:

```python
# Named profile
domain = RetailDomain(profile="holiday_heavy")

# Inline overrides
domain = HealthcareDomain(overrides={
    "encounter.encounter_type": {"Outpatient": 0.45, "Inpatient": 0.20, "Emergency": 0.15, "Observation": 0.10, "Telehealth": 0.10},
})
```

Profile files live in `domains/<name>/profiles/` and follow the same JSON schema as `default.json`.

---

## Sources

### Retail
- [NRF/Happy Returns: 2024 Retail Returns Report](https://nrf.com/research/2024-consumer-returns-retail-industry) — $890B in returns, 16.9% return rate
- [NRF: 2025 Retail Returns Report](https://nrf.com/media-center/press-releases/consumers-expected-to-return-nearly-850-billion-in-merchandise-in-2025)
- [US Census Bureau: Monthly Retail Trade Survey](https://www.census.gov/retail/index.html) ([FRED series](https://fred.stlouisfed.org/series/MRTSSM44000USS))
- [Statista: Monthly Average Units Per E-commerce Transaction (2024)](https://www.statista.com/statistics/1363180/monthly-average-units-per-e-commerce-transaction/)
- [Shopify: 80/20 Rule in E-commerce](https://www.shopify.com/blog/80-20-rule); [Inventory Turnover Ratio](https://www.shopify.com/blog/inventory-turnover-ratio?country=us&lang=en)
- [CapitalOne Shopping: Coupon Statistics (2026)](https://capitaloneshopping.com/research/coupon-statistics/)
- [Opensend: Average Discount Rate Statistics](https://www.opensend.com/post/average-discount-rate-statistics-ecommerce); [Order Cancellation Rate Statistics](https://www.opensend.com/post/order-cancellation-rate-statistics)
- [Doofinder: Busiest Online Shopping Day](https://www.doofinder.com/en/statistics/busiest-online-shopping-day)
- [SaleCycle: When Are People Most Likely to Buy Online?](https://www.salecycle.com/blog/stats/when-are-people-most-likely-to-buy-online/)
- [ECDB: Golden Hours of eCommerce](https://ecdb.com/blog/online-shopping-habits-the-golden-hours-of-ecommerce/4462)
- [SureBright: 24-Hour E-Commerce Trends](https://www.surebright.com/research/hour-by-hour-e-commerce-data-when-americans-really-buy-big-ticket-goods)
- [CouponFollow: Coupon Statistics (2025)](https://couponfollow.com/research/coupon-statistics)
- [DemandSage: 74 Coupon Statistics (2026)](https://www.demandsage.com/coupon-statistics/)
- [Channelwill: Ecommerce Return Rates by Category (2025)](https://www.channelwill.com/blogs/ecommerce-return-rates/)
- [Antavo: Tiered Loyalty Programs](https://antavo.com/loyalty-program-types/tiered-loyalty-programs/)
- [Bond Brand Loyalty Report (2024, with Visa)](https://info.bondbrandloyalty.com/the-loyalty-report-2024-press-release)
- [Onramp Funds: Inventory Turnover Benchmarks (2025)](https://www.onrampfunds.com/resources/inventory-turnover-benchmarks-by-industry-2025)

### Healthcare
- [CMS: Medicare Part D Prescriber Data](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers)
- [CMS: Medicare Physician Fee Schedule (2024)](https://www.cms.gov/medicare/payment/fee-schedules/physician) ([PFS Lookup](https://www.cms.gov/medicare/physician-fee-schedule/search))
- [CDC: Influenza Surveillance (FluView)](https://www.cdc.gov/fluview/overview/index.html)
- [CDC NAMCS: National Ambulatory Medical Care Survey](https://www.cdc.gov/nchs/namcs/about/) ([2019 Summary Tables](https://www.cdc.gov/nchs/data/ahcd/namcs_summary/2019-namcs-web-tables-508.pdf))
- [CDC NHAMCS: Emergency Department Visit Rates, 2022](https://www.cdc.gov/nchs/products/databriefs/db503.htm) ([Data Brief #503](https://www.cdc.gov/nchs/data/databriefs/db503.pdf))
- [AHRQ MEPS: Statistical Brief #560 — Concentration of Health Expenditures (2018-2022)](https://meps.ahrq.gov/data_files/publications/st560/stat560.shtml)
- [AAMC: 2024 Key Findings — Physician Workforce](https://www.aamc.org/data-reports/data/2024-key-findings-and-definitions) ([Workforce Dashboard](https://www.aamc.org/data-reports/report/us-physician-workforce-data-dashboard))
- [KFF: Claims Denials and Appeals in ACA Marketplace Plans (2023)](https://www.kff.org/private-insurance/claims-denials-and-appeals-in-aca-marketplace-plans-in-2023/)
- [KFF: Employer Health Benefits Survey (2024)](https://www.kff.org/health-costs/2024-employer-health-benefits-survey/) ([Full PDF](https://files.kff.org/attachment/Employer-Health-Benefits-Survey-2024-Annual-Survey.pdf))
- [Health Affairs: Medicare Advantage Denies 17% of Initial Claims (2024)](https://www.healthaffairs.org/doi/10.1377/hlthaff.2024.01485)
- [AHA: Hospital Statistics](https://www.ahadata.com/aha-hospital-statistics) ([Fast Facts 2024](https://www.aha.org/system/files/media/file/2024/01/fast-facts-on-us-hospitals-2024-20240112.pdf))
- [BLS: Nurse Practitioners — OES May 2023](https://www.bls.gov/oes/2023/may/oes291171.htm); [Physician Assistants](https://www.bls.gov/oes/2023/may/oes291071.htm)
- [Aptarro: US Healthcare Denial Rates & Reimbursement Statistics (2026)](https://www.aptarro.com/insights/us-healthcare-denial-rates-reimbursement-statistics)
- [FAIR Health: Benchmark Data Products — Charge & Allowed](https://www.fairhealth.org/benchmark-data-products)
- [ClinCalc/IQVIA: Top 200 Drugs (2022)](https://clincalc.com/DrugStats/Top200Drugs.aspx)
- [Williams Institute: Nonbinary LGBTQ Adults in the US](https://williamsinstitute.law.ucla.edu/publications/nonbinary-lgbtq-adults-us/)
- [PLOS ONE: Seasonal Patterns in Hospital Utilization (2017)](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0172049)
- [PMC9068998: Weekday & Seasonal ED Trends](https://pmc.ncbi.nlm.nih.gov/articles/PMC9068998/)
