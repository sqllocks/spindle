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

## HR Domain Sources

**Employee Demographics:** BLS Current Population Survey 2024 (83% full-time), BLS OEWS May 2023 (median wage $48,060), Payscale Compensation Best Practices Report 2024 (G1-G7 grade structure mirrors Mercer/Hay Group).

**Turnover & Separations:** BLS JOLTS 2024 (quits 3.2M vs layoffs 1.7M monthly, ~65% voluntary), Work Institute Retention Report 2024, Gallup State of the Global Workplace 2024. SHRM data: 65-75% of separations are rehire-eligible.

**Performance Reviews:** SHRM/Mercer performance management research (bell curve with positive skew), Betterworks 2024 Performance Enablement Report. 65% of organizations use annual reviews.

**Leave & Time Off:** BLS Employee Benefits Survey 2024, SHRM Benefits Report. BLS absence data: mean duration 4.4 days.

**Training & Development:** ATD State of the Industry Report 2024 (70-85% completion for mandatory, 40-60% voluntary), LinkedIn Learning Workplace Learning Report.

## Education Domain Sources

**Faculty Composition:** AAUP Data Snapshot 2023 (32% tenured/tenure-track, 68% contingent), NCES IPEDS Human Resources Survey.

**Student Demographics:** NCES Condition of Education 2024 (~17M undergraduate, ~3.8M graduate), NCES 6-year graduation rate ~64% at 4-year institutions, 32.9% dropout rate nationally. GradeCalcTools 2024: average GPA rose 0.43 points since 1990.

**Grades:** Stuart Rojstaczer's GradeInflation.com (2024), NCES grade inflation trends. Standard accreditation: 3-credit courses dominate (60%).

**Financial Aid:** NCES 2024: average federal grant aid ~$6,700; 86% of first-time full-time undergrads received some aid (2021-22).

## Insurance Domain Sources

**Policy Portfolio:** NAIC 2024 Market Share Report (auto largest at ~35% of P&C premiums, $344B), III industry composition. Experian 2024: average FICO 715, 71.2% score 670+.

**Claims:** NAIC claims data; homeowners denial rate ~37% nationally, health denial rate ~16% (KFF 2023), life denial rate ~10-20%. AM Best 2023: US individual life lapse ratio 5.1.

**Deductibles & Coverage:** III consumer preference data, state-level deductible requirements. NAIC coverage attachment rates.

**Premium Payments:** III digital adoption survey 2024, JD Power Insurance Digital Experience Study. Industry delinquency benchmarks.

## Telecom Domain Sources

**Subscriber Base:** CTIA 2024 Annual Survey (postpaid ~59%, prepaid ~15%), FCC 2024 Communications Marketplace Report. Industry churn rate ~1.6% monthly.

**Device Mix:** Counterpoint/BankMyCell 2024 (Apple 61%, Samsung 23% in US sales; adjusted for subscriber base including older devices). CTIA device mix data, IDC connected device forecast.

**Usage Patterns:** CTIA Annual Survey (100+ trillion MB data traffic 2023, average 17.5 GB/month per smartphone). FCC voice usage statistics.

**Billing & Churn:** JD Power Wireless Customer Satisfaction Study 2024. Carrier financial reports (bad debt rate ~1-3%).

## Financial Domain Sources

**Account Types:** FDIC Summary of Deposits 2024 (76,000+ branch offices, $14.5T in deposits), Federal Reserve Survey of Consumer Finances. FICO 2024: average US score 717.

**Cards:** Nilson Report card market share (Visa 52%, Mastercard 24%), Federal Reserve debit card survey 2024.

**Transactions:** Federal Reserve Payments Study 2022 (204B card payments, 30B ACH, 3.4B checks). Log-normal amount distribution (median debit ~$40, credit ~$50).

**Loans:** Federal Reserve Consumer Credit G.19 2024 ($5.1T total consumer credit). FDIC Quarterly Banking Profile Q4 2024 delinquency/charge-off rates.

**Fraud:** Federal Reserve Bank of Kansas City 2024 (card-present fraud 5.1-14.2 basis points, card-not-present 41.6 bps). Pulse debit issuer study 2023.

## Capital Markets Domain Sources

**Company Reference Data:** SEC EDGAR CIK database (110 real S&P 500 tickers), MSCI/S&P Global Industry Classification Standard (11 sectors, 61 industries).

**Pricing (GBM Model):** Geometric Brownian Motion calibrated to S&P 500 price range per Hull's "Options, Futures, and Other Derivatives." Four market cap tiers with tier-specific drift (6-12%), volatility (18-35%), and price ranges calibrated from CRSP historical returns and VIX baseline volatility.

**Sector Correlations:** MSCI/FactSet sector correlation matrices (Info Tech / Comm Services 0.75 highest, 0.40 default baseline).

**Corporate Actions:** S&P Dow Jones (~80% of S&P 500 pay quarterly dividends), FactSet Earnings Insight 2024 (beat estimates by ~4% on average), SEC Form 4 filing data (insider transactions).

## Supply Chain Domain Sources

**Warehouse & Supplier:** CSCMP State of Logistics Report, NAIOP industrial real estate data. US Census Bureau Foreign Trade Statistics (US 35%, China 20%, Germany 10%). ISM Manufacturing PMI Supplier Deliveries Index; APQC benchmarks (79-day global average lead time, April 2024).

**Purchasing:** ISM Report on Business 2024, APICS/ASCM procurement priority conventions. ERP transaction lifecycle analytics.

**Shipment & Quality:** ANSI/ASQ AQL (Acceptable Quality Level) standards for incoming inspection benchmarks. Carrier performance data for delivery statistics.

**Inventory:** ABC inventory analysis conventions, S&OP practice surveys for demand planning technology adoption.

## IoT Domain Sources

**Device & Sensor:** Gartner 2025 IoT installed base forecasts, IDC IoT device taxonomy. Azure IoT Hub protocol statistics (MQTT 35%, HTTP 25%, CoAP 15%).

**Alerts & Maintenance:** ISA-18.2 alarm management standard for alert severity/type distributions. Gartner 2025: 22% MTTR reduction with IoT monitoring. McKinsey 2024: IoT reduces unplanned downtime 35%.

**Sensor Quality:** Sensor error/anomaly rates and transmission loss data (Good 90%, Suspect 7%, Bad 3%). Frost & Sullivan: 38% MTBF improvement with IoT 2024.

## Manufacturing Domain Sources

**Production Lines & Equipment:** ISA-95 manufacturing operations management standard for line types and work order lifecycle. Equipment uptime benchmarks from maintenance scheduling data.

**Work Orders & Quality:** ASQ quality benchmarks (AQL standards). Defect root causes follow Ishikawa/fishbone analysis conventions and 8D report patterns. QC pass/rework/fail (82/10/8%) from ASQ industry benchmarks.

**Production Metrics:** LNS Research OEE benchmarks (global average 55-60%, world-class 85%). Six Sigma first-pass yield benchmarks.

**Downtime:** Gartner 2025: $260K/hr average large-plant downtime cost. TPM loss categories for cause distribution.

## Marketing Domain Sources

**Campaign & Channel Mix:** HubSpot State of Marketing 2024, MarketingProfs channel mix benchmarks. Salesforce/HubSpot CRM analytics for campaign lifecycle.

**Lead Generation & Funnel:** Belkins/HubSpot 2024 (31% lead-to-MQL), HubSpot 2024 Sales Trends Report (20% MQL-to-SQL), Salesforce 2025 (40-60% SQL-to-Opportunity). First Page Sage B2B conversion rates. Overall win rate 15-25%; qualified opportunity close ~29%.

**Email & Engagement:** Mailchimp Email Marketing Benchmarks 2024 (20-25% open rate), Campaign Monitor B2B averages. Google Analytics traffic attribution data.

## Real Estate Domain Sources

**Property Characteristics:** US Census Bureau American Housing Survey (bedroom/bathroom distributions, housing stock age), Zillow Home Value Index.

**Listings & Transactions:** NAR April 2024 (average 26 days on market, ~33% sold above list price, median existing-home price ~$400K with 5.7% YoY increase). MLS historical status transitions.

**Agent & Market:** NAR member profile survey (commission rates, agent demographics). Post-2024 NAR settlement adjustments for commission rates (2.5-6.0%).

**Seasonality:** NAR monthly existing home sales (Mar-Jun peak ~40% of annual, Dec-Jan trough ~12%).

---

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

### HR
- [BLS JOLTS: Job Openings and Labor Turnover Survey](https://www.bls.gov/jlt/)
- [BLS OEWS: Occupational Employment and Wage Statistics](https://www.bls.gov/oes/)
- [BLS CPS: Current Population Survey — Full/Part-Time Employment](https://www.bls.gov/cps/cpsaat08.htm)
- [BLS Employee Benefits Survey](https://www.bls.gov/ncs/ebs/)
- [SHRM: Performance Management Benchmarks](https://www.shrm.org/)
- [ATD: State of the Industry Report](https://www.td.org/research-reports)
- [Betterworks: 2024 Performance Enablement Report](https://www.betterworks.com/)
- [Work Institute: Retention Report](https://workinstitute.com/)
- [Gallup: State of the Global Workplace](https://www.gallup.com/workplace/349484/state-of-the-global-workplace.aspx)
- [Mercer/Hay Group: Job Evaluation & Compensation Surveys](https://www.mercer.com/)

### Education
- [NCES: Condition of Education (2024)](https://nces.ed.gov/programs/coe/)
- [NCES IPEDS: Enrollment, Completions, Human Resources](https://nces.ed.gov/ipeds/)
- [NCES: Fast Facts — Most Popular Majors](https://nces.ed.gov/fastfacts/display.asp?id=37)
- [NCES: Undergraduate Retention and Graduation Rates](https://nces.ed.gov/programs/coe/indicator/ctr)
- [AAUP: Data Snapshot — Tenure and Contingency (2023)](https://www.aaup.org/academe/issues/spring-2023/data-snapshot-tenure-and-contingency-us-higher-education)
- [GradeInflation.com: Stuart Rojstaczer Grade Trend Data](https://www.gradeinflation.com/)
- [GradeCalcTools: GPA Inflation Statistics 1990-2024](https://gradecalculatortools.com/blog/gpa-inflation-statistics-1990-2024/)
- [Research.com: College Dropout Rates (2026)](https://research.com/universities-colleges/college-dropout-rates)

### Insurance
- [NAIC: 2024 Market Share Data](https://content.naic.org/article/naic-releases-2024-market-share-data)
- [NAIC: Property/Casualty Market Share Report (2024)](https://content.naic.org/sites/default/files/research-actuarial-property-casualty-market-share.pdf)
- [AM Best: US Individual Life Lapse Ratio (2023)](https://news.ambest.com/newscontent.aspx?refnum=259009)
- [III: Insurance Information Institute](https://www.iii.org/)
- [KFF: Claims Denials in ACA Marketplace Plans (2023)](https://www.kff.org/private-insurance/claims-denials-and-appeals-in-aca-marketplace-plans-in-2023/)
- [Experian: 2024 Consumer Credit Review](https://www.experian.com/blogs/ask-experian/consumer-credit-review/)
- [FICO: Average US Score 717 (2024)](https://www.fico.com/blogs/average-u-s-fico-score-stays-717)
- [LOMA: Persistency Benchmarks](https://www.loma.org/)
- [JD Power: Insurance Digital Experience Study](https://www.jdpower.com/)

### Telecom
- [CTIA: 2024 Annual Survey Highlights](https://www.ctia.org/news/2024-annual-survey-highlights)
- [FCC: 2024 Communications Marketplace Report](https://www.fcc.gov/document/fcc-releases-2024-communications-marketplace-report)
- [Counterpoint/BankMyCell: US Smartphone Market Share (2024)](https://www.bankmycell.com/blog/us-smartphone-market-share)
- [Statcounter: Mobile Vendor Market Share US](https://gs.statcounter.com/vendor-market-share/mobile/united-states-of-america)
- [JD Power: Wireless Customer Satisfaction Study (2024)](https://www.jdpower.com/)

### Financial
- [FDIC: Quarterly Banking Profile Q4 2024](https://www.fdic.gov/quarterly-banking-profile/quarterly-banking-profile-q4-2024)
- [FDIC: Summary of Deposits (2024)](https://www.fdic.gov/news/press-releases/2024/fdic-releases-results-summary-deposits-annual-survey)
- [Federal Reserve: Payments Study (2022)](https://www.federalreserve.gov/paymentsystems/fr-payments-study.htm)
- [Federal Reserve Bank of Kansas City: Card Fraud Rates](https://www.kansascityfed.org/research/payments-system-research-briefings/)
- [FICO: Average US Score 717 (2024)](https://www.fico.com/blogs/average-u-s-fico-score-stays-717)
- [Federal Reserve: Consumer Credit G.19](https://www.federalreserve.gov/releases/g19/current/)
- [Federal Reserve Bank of New York: Consumer Credit Panel](https://www.newyorkfed.org/microeconomics/hhdc)
- [Experian: 2024 Consumer Credit Review](https://www.experian.com/blogs/ask-experian/consumer-credit-review/)

### Capital Markets
- [SEC EDGAR: Company Search & CIK Database](https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany)
- [MSCI/S&P: Global Industry Classification Standard (GICS)](https://www.msci.com/our-solutions/indexes/gics)
- [S&P Dow Jones Indices](https://www.spglobal.com/spdji/)
- [FactSet: Earnings Insight Reports](https://www.factset.com/earningsinsight)
- [Hull, J.C.: Options, Futures, and Other Derivatives](https://www.pearson.com/)
- [SEC Form 4 Filings](https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=4)

### Supply Chain
- [ISM: Report on Business (Manufacturing PMI)](https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/)
- [APQC: Supply Chain Benchmarking](https://www.apqc.org/)
- [APICS/ASCM: Supply Chain Operations Reference Model](https://www.ascm.org/)
- [Census Bureau: Annual Survey of Manufactures](https://www.census.gov/programs-surveys/asm.html)

### IoT
- [Gartner: IoT Market Insights (2025)](https://www.gartner.com/en/information-technology/insights/internet-of-things)
- [McKinsey: IoT Value Creation (2024)](https://www.mckinsey.com/)
- [Frost & Sullivan: MTBF Benchmarks (2024)](https://www.frost.com/)
- [IEEE: IoT Standards](https://standards.ieee.org/)
- [ISA-95/ISA-18.2: Alarm Management Standard](https://www.isa.org/)

### Manufacturing
- [LNS Research: OEE Benchmarks by Industry](https://blog.lnsresearch.com/bid/155988/Overall-Equipment-Effectiveness-Benchmark-Data-by-Industry)
- [OEE.com: World-Class OEE Benchmarks](https://www.oee.com/)
- [ASQ: Quality Benchmarks](https://asq.org/)
- [Gartner (2025): IoT boosts OEE 15-20%](https://www.gartner.com/)
- [ISA-95: Manufacturing Operations Management](https://www.isa.org/)

### Marketing
- [HubSpot: 2024 Sales Trends Report](https://www.hubspot.com/)
- [Salesforce: State of Sales (2025)](https://www.salesforce.com/)
- [Belkins: B2B Conversion Rate Benchmarks](https://belkins.io/blog/lead-generation-conversion)
- [First Page Sage: B2B Conversion Rates by Industry](https://firstpagesage.com/reports/b2b-conversion-rates-by-industry-fc/)
- [Mailchimp: Email Marketing Benchmarks (2024)](https://mailchimp.com/resources/email-marketing-benchmarks/)
- [Gradient Works: 2024 B2B Sales Benchmarks](https://www.gradient.works/blog/2024-b2b-sales-benchmarks)

### Real Estate
- [NAR: Research and Statistics](https://www.nar.realtor/research-and-statistics)
- [NAR: Existing-Home Sales Data](https://www.nar.realtor/research-and-statistics/housing-statistics/existing-home-sales)
- [Census Bureau: New Residential Construction](https://www.census.gov/construction/nrc/)
- [NAR: Member Profile Survey](https://www.nar.realtor/)

For the complete per-parameter citation trail, see [METHODOLOGY.md](https://github.com/sqllocks/spindle/blob/main/METHODOLOGY.md) in the repository.
