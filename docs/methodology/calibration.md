# Methodology & Calibration

Spindle's distribution weights are calibrated against real-world data from authoritative public sources. All defaults in each domain's `profiles/default.json` reflect these calibrated values.

## Calibration Approach

Every domain has a default profile with distribution weights derived from published statistics. Users can override any weight via named profiles or the `overrides` parameter (see [Distribution Profiles](../guides/profiles.md)).

## Confidence Levels

| Level | Meaning | Examples |
| --- | --- | --- |
| **High** | Based on authoritative published statistics | Census demographics, NRF return rates, CMS claims data |
| **Medium** | Derived from multiple consistent industry sources | Loyalty tiers, promotion types, seasonal patterns |
| **Low** | Informed estimates without direct public benchmarks | Addresses per customer, product lifecycle phases |

## Domain Calibration Status

All 13 domains have distribution parameters sourced from published data. The depth varies by domain:

| Domain | Sources | Key References |
| --- | --- | --- |
| **Retail** | 20+ | NRF, Census, Statista, Shopify, CapitalOne Shopping |
| **Healthcare** | 20+ | CDC, CMS, AAMC, KFF, AHA, BLS, MEPS |
| **HR** | 10+ | BLS JOLTS, BLS OEWS, SHRM, ATD, Mercer |
| **Education** | 10+ | NCES IPEDS, AAUP, GradeInflation.com |
| **Insurance** | 10+ | NAIC, AM Best, III, KFF, LOMA |
| **Telecom** | 8+ | CTIA, FCC, Counterpoint, JD Power |
| **Financial** | 8+ | FDIC, Federal Reserve, FICO, Fed Payments Study |
| **Capital Markets** | 8+ | SEC EDGAR, S&P/MSCI GICS, FactSet, CRSP |
| **Marketing** | 6+ | HubSpot, Salesforce, Belkins, Mailchimp |
| **Real Estate** | 5+ | NAR, Census Bureau |
| **Manufacturing** | 5+ | LNS Research, Gartner, ASQ, OEE.com |
| **Supply Chain** | 5+ | ISM, APQC, APICS/ASCM |
| **IoT** | 5+ | Gartner, McKinsey, Frost & Sullivan, IEEE |

All domains produce relationally correct data with proper FK integrity. Users can override any distribution via profiles or the `overrides` parameter. Full per-parameter citations are in [METHODOLOGY.md](https://github.com/sqllocks/spindle/blob/main/METHODOLOGY.md).

---

## Retail Domain Sources

**Customer Demographics:** CapitalOne Shopping Research, US Census Bureau, Antavo/Mastercard tiered program guides, Bond Brand Loyalty Report 2024.

**Order Patterns:** Pareto alpha=1.16 (80/20 rule, Shopify/Unific/SmartKarrot), NRF/Happy Returns 2024 (16.9% return rate), Statista (4.95 units/transaction), Zipf alpha=1.5 (SKU concentration).

**Seasonality:** US Census Monthly Retail Trade (FRED), NRF holiday sales reports, Doofinder/SaleCycle/ECDB weekday patterns, SureBright hour-of-day data.

**Returns:** NRF/Happy Returns 2024, Channelwill 2025, Shopify return reason surveys.

## Healthcare Domain Sources

**Provider Workforce:** AAMC 2024 Key Findings, BLS OES May 2023, AMA Physician Masterfile.

**Patient Demographics:** US Census Bureau 2020, Williams Institute (nonbinary estimates), KFF Health Insurance Coverage 2023.

**Encounters:** CDC NAMCS (~860M office visits), CDC NHAMCS (155M ED visits), AHA (33.7M inpatient), CMS/Epic telehealth data. Patient frequency: MEPS Statistical Brief #560 (top 5% = 51.2% of spending).

**Diagnoses & Procedures:** CMS top diagnosis codes from Medicare FFS claims 2022, CMS Medicare Physician Fee Schedule 2024, IQVIA top 200 drugs.

**Claims:** KFF ACA Marketplace denial data 2023 (19-20% denial rate), Health Affairs MA study 2024, Aptarro industry-wide statistics, FAIR Health benchmark data.

## Full Source List

### Retail
- [NRF/Happy Returns: 2024 Retail Returns Report](https://nrf.com/research/2024-consumer-returns-retail-industry)
- [US Census Bureau: Monthly Retail Trade Survey](https://www.census.gov/retail/index.html)
- [Statista: Monthly Average Units Per E-commerce Transaction](https://www.statista.com/statistics/1363180/monthly-average-units-per-e-commerce-transaction/)
- [Shopify: 80/20 Rule in E-commerce](https://www.shopify.com/blog/80-20-rule)
- [CapitalOne Shopping: Coupon Statistics](https://capitaloneshopping.com/research/coupon-statistics/)
- [Bond Brand Loyalty Report 2024](https://info.bondbrandloyalty.com/the-loyalty-report-2024-press-release)

### Healthcare
- [CMS: Medicare Part D Prescriber Data](https://data.cms.gov/provider-summary-by-type-of-service/medicare-part-d-prescribers)
- [CDC: Influenza Surveillance (FluView)](https://www.cdc.gov/fluview/overview/index.html)
- [CDC NAMCS: National Ambulatory Medical Care Survey](https://www.cdc.gov/nchs/namcs/about/)
- [AHRQ MEPS: Statistical Brief #560](https://meps.ahrq.gov/data_files/publications/st560/stat560.shtml)
- [AAMC: 2024 Key Findings](https://www.aamc.org/data-reports/data/2024-key-findings-and-definitions)
- [KFF: Claims Denials in ACA Marketplace Plans 2023](https://www.kff.org/private-insurance/claims-denials-and-appeals-in-aca-marketplace-plans-in-2023/)
- [KFF: Employer Health Benefits Survey 2024](https://www.kff.org/health-costs/2024-employer-health-benefits-survey/)
- [AHA: Hospital Statistics](https://www.ahadata.com/aha-hospital-statistics)

For the complete per-parameter citation trail, see [METHODOLOGY.md](https://github.com/sqllocks/spindle/blob/main/METHODOLOGY.md) in the repository.
