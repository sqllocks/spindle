# Spindle Data Generation Methodology

This document describes how Spindle's distribution weights are calibrated against real-world data
and the sources used. All defaults in the `profiles/default.json` for each domain reflect these
calibrated values.

## Overview

Spindle generates synthetic data that mirrors real-world statistical distributions. All 13 domains
have distribution parameters sourced from published data — government statistics (BLS, Census, FDIC,
Federal Reserve, SEC, NCES, FCC), industry bodies (NAIC, NAR, CTIA, ISM, AAUP), and research firms
(Gartner, McKinsey, HubSpot, Experian, FactSet).

Retail and Healthcare have the deepest calibration (20+ sources each, every parameter individually
cited). Other domains are sourced at the key-parameter level with authoritative references for the
most impactful distributions. All 13 domains produce relationally correct data with proper FK
integrity. Users can override any weight via named profiles or the `overrides` parameter.

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

## HR Domain

### Employee Demographics

| Parameter | Value | Source |
|-----------|-------|--------|
| Employment type (FT/PT/Contract/Temp) | 75% / 12% / 8% / 5% | BLS Current Population Survey (2024): 83% of employed workers are full-time; BLS Contingent Worker Survey (2023) |
| Employment status (Active/On Leave/Terminated/Retired) | 82% / 5% / 10% / 3% | BLS JOLTS (2024): total separations rate ~3.4% monthly; SHRM workforce benchmarks |
| Salary range (base) | Log-normal, mean=$65K, range $28K-$350K | BLS Occupational Employment and Wage Statistics (OEWS), May 2023: median annual wage $48,060 all occupations; 90th percentile varies $80K-$200K+ |
| Pay grade distribution (G1-G7) | G1: 15%, G2: 20%, G3: 25%, G4: 20%, G5: 12%, G6: 5%, G7: 3% | Payscale Compensation Best Practices Report (2024); typical 7-grade structure mirrors Mercer/Hay Group job evaluation |
| FLSA exempt vs non-exempt | 60% exempt / 40% non-exempt | BLS (2024): ~55-60% of salaried workers are exempt under FLSA; varies by industry |

### Turnover & Separations

| Parameter | Value | Source |
|-----------|-------|--------|
| Termination type (Voluntary/Involuntary/Layoff/Retirement/Contract End) | 55% / 25% / 10% / 7% / 3% | BLS JOLTS (2024): quits 3.2M vs layoffs/discharges 1.7M monthly (~65% voluntary of total separations); SHRM 2024 benchmarks |
| Termination reasons | Better Opp 30%, Performance 15%, Relocation 12%, Restructuring 10%, Personal 10%, Conduct 8%, Retirement 7%, Other 8% | Work Institute Retention Report (2024); Gallup State of the Global Workplace (2024) |
| Eligible for rehire | 70% | SHRM survey data: ~65-75% of separations are rehire-eligible (excludes misconduct/performance) |

### Performance Reviews

| Parameter | Value | Source |
|-----------|-------|--------|
| Rating distribution (1-5 scale) | 1: 5%, 2: 12%, 3: 45%, 4: 28%, 5: 10% | SHRM/Mercer performance management research: typical bell curve with slight positive skew; Betterworks 2024 Performance Enablement Report |
| Review cycle | Annual, concentrated Q1/Q4 | SHRM Performance Management Survey (2024): 65% of organizations use annual reviews |

### Leave & Time Off

| Parameter | Value | Source |
|-----------|-------|--------|
| Leave type distribution | PTO 45%, Sick 25%, Personal 12%, Parental 6%, Bereavement 4%, Jury Duty 3%, Unpaid 5% | BLS Employee Benefits Survey (2024); SHRM Benefits Report |
| Leave approval rate | Approved 75%, Pending 10%, Denied 8%, Cancelled 7% | FMLA compliance data; internal HR benchmarks |
| Average leave duration | 1-14 days (uniform) | BLS absence data: mean absence duration 4.4 days (2024) |

### Training & Development

| Parameter | Value | Source |
|-----------|-------|--------|
| Training categories | Compliance 25%, Technical 22%, Leadership 15%, Safety 13%, Soft Skills 12%, Onboarding 8%, DEI 5% | ATD (Association for Talent Development) State of the Industry Report (2024); LinkedIn Learning Workplace Learning Report |
| Course completion rate | Completed 65%, In Progress 15%, Enrolled 10%, Dropped 7%, Failed 3% | ATD benchmark: 70-85% completion for mandatory; 40-60% for voluntary |
| Mandatory training percentage | 35% mandatory | OSHA/compliance requirements; ATD data |

---

## Education Domain

### Faculty Composition

| Parameter | Value | Source |
|-----------|-------|--------|
| Faculty rank (Prof/Assoc/Asst/Lecturer/Adjunct) | 20% / 20% / 25% / 15% / 20% | AAUP Data Snapshot (2023): 32% tenured/tenure-track, 68% contingent; NCES IPEDS Human Resources Survey |
| Tenure status (Tenured/Track/Non-Tenure) | 35% / 25% / 40% | AAUP (2021): 32% tenured/tenure-track, 68% contingent. At R1 institutions: 50/50 split |

### Student Demographics

| Parameter | Value | Source |
|-----------|-------|--------|
| Classification (Fresh/Soph/Junior/Senior/Grad) | 22% / 20% / 20% / 20% / 18% | NCES Condition of Education (2024): undergraduate enrollment ~17M, graduate ~3.8M |
| Student status (Active/Graduated/Withdrawn/Suspended) | 80% / 12% / 5% / 3% | NCES (2024): 6-year graduation rate ~64% at 4-year institutions; 32.9% dropout rate nationally |
| GPA distribution | Normal, mean=3.0, sigma=0.5 | GradeCalcTools analysis (2024): average GPA rose 0.43 points since 1990; current average ~3.1-3.2 |
| Major concentration | Zipf alpha=1.3 | NCES IPEDS: Business and health professions are top 2 fields at both associate's and bachelor's levels |

### Grades

| Parameter | Value | Source |
|-----------|-------|--------|
| Grade distribution (A through F/W/I) | A: 18%, A-: 10%, B+: 12%, B: 14%, B-: 10%, C+: 8%, C: 8%, C-: 5%, D: 5%, F: 4%, W: 4%, I: 2% | Stuart Rojstaczer's GradeInflation.com (2024); NCES grade inflation trends; average 0.1 GPA increase 2018-2021 |
| Course credits | 1 credit: 5%, 2: 10%, 3: 60%, 4: 25% | Standard accreditation: 3-credit courses dominate; AACU/HLC credit hour requirements |

### Financial Aid

| Parameter | Value | Source |
|-----------|-------|--------|
| Aid amount distribution | Log-normal, mean ~$4,500, range $500-$50K | NCES (2024): average federal grant aid ~$6,700; average institutional aid varies widely |
| Aid participation rate | ~70% of students | NCES: 86% of first-time, full-time undergrads received some financial aid (2021-22) |

### Retention & Graduation

| Parameter | Value | Source |
|-----------|-------|--------|
| Semester distribution | Fall/Spring ~22% each, Summer ~12% | NCES enrollment data: fall/spring semesters have ~4x summer enrollment |
| Grade appeal rate | 2% of enrollments | Institutional data conventions; low-frequency event |
| Academic standing | Good 70%, Dean's List 15%, Probation 10%, Suspension 5% | NCES retention data; typical institutional thresholds (2.0 GPA for good standing) |

---

## Insurance Domain

### Policy Portfolio

| Parameter | Value | Source |
|-----------|-------|--------|
| Policy category (Auto/Home/Life/Health/Commercial/Umbrella) | 25% / 20% / 18% / 15% / 15% / 7% | NAIC 2024 Market Share Report: auto insurance largest at ~35% of P&C premiums ($344B); III (Insurance Information Institute) industry composition |
| Agent specialization | P&C 30%, Life 20%, Health 20%, Commercial 15%, Multi-Line 15% | NAIC agent licensing data; III industry workforce composition |
| Policyholder credit score | Normal, mean=700, sigma=80, range 300-850 | Experian 2024: average FICO score 715; 71.2% of consumers score 670+ |

### Claims

| Parameter | Value | Source |
|-----------|-------|--------|
| Claim status (Open/Review/Approved/Denied/Closed) | 10% / 15% / 40% / 10% / 25% | NAIC claims data; homeowners denial rate ~37% nationally (NAIC); health denial rate ~16% (KFF 2023); life denial rate ~10-20% (industry estimates) |
| Claim amount distribution | Log-normal, mean ~$4,900, range $100-$1M | NAIC loss severity data by line; III average claim costs by type |
| Policy lapse/cancellation | Active 72%, Cancelled 8%, Expired 15%, Lapsed 5% | AM Best (2023): US individual life lapse ratio 5.1; LOMA persistency benchmarks; auto/home cancellation ~5-8% |

### Deductibles & Coverage

| Parameter | Value | Source |
|-----------|-------|--------|
| Deductible tiers | $500: 30%, $1000: 35%, $2000: 20%, $5000: 15% | III consumer preference data; state-level deductible requirements; industry standard tier structure |
| Coverage type mix | Liability 25%, Collision 18%, Comprehensive 17%, Medical 15%, Property 15%, Uninsured 10% | NAIC coverage attachment rates; state minimum coverage requirements |

### Premium Payments

| Parameter | Value | Source |
|-----------|-------|--------|
| Payment method | Auto-Pay 45%, Online 30%, Mail 15%, Agent 10% | III digital adoption survey (2024); JD Power insurance digital experience study |
| Payment status | Paid 82%, Pending 8%, Late 7%, Returned 3% | Industry delinquency benchmarks; carrier payment success rates |
| Policies per policyholder | 1.8 average | III cross-sell metrics; bundling statistics (home+auto) |

---

## Telecom Domain

### Subscriber Base

| Parameter | Value | Source |
|-----------|-------|--------|
| Plan type (Prepaid/Postpaid/Family/Business) | 25% / 35% / 25% / 15% | CTIA 2024 Annual Survey: postpaid ~59% of connections, prepaid ~15% (2019 baseline); prepaid share growing in 2024 |
| Account status (Active/Suspended/Cancelled) | 85% / 5% / 10% | FCC Communications Marketplace Report (2024); industry churn rate ~1.6% monthly (CTIA 2019) |
| Device manufacturer | Samsung 28%, Apple 35%, Google 15%, OnePlus 10%, Motorola 12% | Counterpoint/BankMyCell (2024): Apple 61%, Samsung 23% in US. Spindle adjusts for subscriber base (not sales) which includes older devices |
| Device type | Smartphone 70%, Tablet 12%, Hotspot 8%, Watch 10% | CTIA device mix data; IDC US connected device forecast |

### Usage Patterns

| Parameter | Value | Source |
|-----------|-------|--------|
| Record type (Voice/Data/SMS/MMS) | 25% / 45% / 25% / 5% | CTIA Annual Survey: data traffic 100+ trillion MB (2023); voice declining; SMS stable |
| Call duration | Log-normal, mean ~90s, range 1-7200s | FCC voice usage statistics; average call duration ~3-4 minutes (declining trend) |
| Data usage per session | Log-normal, mean ~20MB, range 0.01-5000MB | CTIA: average 17.5 GB/month per smartphone (2023); session-level varies widely |

### Billing & Churn

| Parameter | Value | Source |
|-----------|-------|--------|
| Payment status | Paid 75%, Pending 10%, Overdue 10%, Partial 5% | Carrier financial reports; industry bad debt rate ~1-3% |
| Payment method | Auto-Pay 40%, Online 25%, In-Store 15%, Phone 10%, Mail 10% | JD Power wireless customer care study (2024); digital payment adoption trends |
| Churn drivers | Price 25%, Service Quality 20%, Competition 20%, Coverage 15%, Life Event 20% | JD Power Wireless Customer Satisfaction Study (2024); carrier win/loss analysis |
| Churn risk distribution | Low 40%, Medium 30%, High 20%, Very High 10% | Industry churn modeling benchmarks; carrier retention analytics |

---

## Financial Domain

### Account Types & Demographics

| Parameter | Value | Source |
|-----------|-------|--------|
| Account type | Checking 45%, Savings 30%, Money Market 10%, CD 8%, Investment 7% | FDIC Summary of Deposits (2024): 76,000+ branch offices, $14.5T in deposits; Federal Reserve Survey of Consumer Finances |
| Credit tier | Excellent 22%, Good 35%, Fair 28%, Poor 15% | FICO (2024): average US score 717; Experian (2024): 71.2% score 670+; CFPB credit distribution data |
| Account status | Active 85%, Dormant 8%, Closed 5%, Frozen 2% | FDIC deposit insurance data; OCC account status reporting |
| Customer churn | 12% inactive | Federal Reserve Survey of Consumer Finances; ABA member bank retention data |
| Accounts per customer | 2.2 average | Federal Reserve: average accounts per household across all product types |

### Cards

| Parameter | Value | Source |
|-----------|-------|--------|
| Card type | Debit 55%, Credit 35%, Prepaid 10% | Nilson Report; Federal Reserve debit card survey (2024) |
| Card network | Visa 52%, Mastercard 24%, Amex 15%, Discover 9% | Nilson Report card market share; Visa/Mastercard earnings reports |

### Transactions

| Parameter | Value | Source |
|-----------|-------|--------|
| Transaction type | Deposit 25%, Withdrawal 20%, Transfer 18%, Payment 22%, Fee 5%, Interest 5%, Refund 5% | Federal Reserve Payments Study (2022): 204B card payments, 30B ACH transactions, 3.4B check payments |
| Channel | Online 35%, Mobile 30%, ATM 15%, Branch 12%, Phone 5%, Wire 3% | Federal Reserve digital banking adoption surveys; ABA channel preference data |
| Transaction status | Completed 92%, Pending 4%, Failed 2%, Reversed 2% | Visa/Mastercard settlement failure rates; ACH return code statistics |
| Amount distribution | Log-normal, mean ~$67, range $0.01-$100K | Federal Reserve Payments Study: median debit ~$40, median credit ~$50 |
| Fraud flag rate | 2% of transactions flagged | FinCEN SAR data; Nilson Report fraud detection volumes |
| Fraud resolution | False Positive 55%, Under Review 20%, Confirmed Fraud 15%, Customer Verified 10% | Card association chargeback data; industry fraud confirmation rates |
| Monthly seasonality | Dec 9.4% (peak), Feb 7.2% (low) | Federal Reserve payment volume seasonality; retail sales cycles |
| Day-of-week | Mon 16.5% (peak), Sun 10.0% (low) | ACH processing schedules; banking weekend/weekday transaction ratios |

### Loans

| Parameter | Value | Source |
|-----------|-------|--------|
| Loan type | Mortgage 35%, Auto 25%, Personal 20%, Student 12%, Business 8% | Federal Reserve Consumer Credit G.19 (2024): $5.1T total consumer credit outstanding |
| Loan status | Current 78%, Paid Off 10%, Delinquent 6%, Default 3%, Forbearance 3% | Federal Reserve Delinquency/Charge-off Rates; FDIC Quarterly Banking Profile Q4 2024 |
| Loan term | 360mo (30yr mortgage) 20%, 60mo 15%, 36mo 12%, 240mo 10%, 180mo 10% | FHFA mortgage data; Federal Reserve auto loan term distributions |
| Interest rate | Uniform 2.5%-18.0% | Freddie Mac Primary Mortgage Market Survey; Federal Reserve prime rate |
| Loans per customer | 0.4 average | Federal Reserve Survey of Consumer Finances: lending penetration |

### Fraud

| Parameter | Value | Source |
|-----------|-------|--------|
| Card-present fraud rate | 5.1-14.2 basis points | Federal Reserve Bank of Kansas City (2024): dual-message vs single-message networks |
| Card-not-present fraud rate | 41.6 basis points | Pulse debit issuer study (2023); Federal Reserve Bank of Kansas City |
| Debit card fraud share | 39% of total fraud losses | Federal Reserve Financial Services survey (2024): 73% of FIs experienced debit fraud attempts |

---

## Capital Markets Domain

### Company Reference Data

| Parameter | Value | Source |
|-----------|-------|--------|
| Company tickers | 110 real S&P 500 companies | SEC EDGAR CIK database; S&P Dow Jones Indices |
| GICS sectors | 11 standard sectors | MSCI/S&P Global Industry Classification Standard (GICS) |
| GICS industries | 61 industries | MSCI/S&P GICS hierarchy (current as of 2024) |
| Exchange distribution | NYSE, NASDAQ, AMEX | SEC market data; exchange listing statistics |

### Pricing (GBM Model)

| Parameter | Value | Source |
|-----------|-------|--------|
| Initial price distribution | Log-normal, mean=$55, range $0.01-$10,000 | Standard quantitative finance (Hull, "Options, Futures, and Other Derivatives"); calibrated to S&P 500 price range |
| OHLC business rules | High >= Low, Close within [Low, High] | Market microstructure theory; exchange data validation rules |
| Volume distribution | Log-normal, mean ~33K shares/day, range 100-1B | SEC market data; average daily volume across S&P 500 components |

### GBM Parameters by Market Cap Tier

| Tier | Drift (annual) | Volatility (annual) | Price Range | Source |
|------|---------------|---------------------|-------------|--------|
| Mega-cap | 12% | 18% | $100-$500 | CRSP mega-cap historical returns; VIX baseline volatility |
| Large-cap | 10% | 22% | $50-$200 | CRSP large-cap returns; S&P 500 historical volatility |
| Mid-cap | 8% | 28% | $20-$100 | CRSP mid-cap returns; S&P MidCap 400 data |
| Small-cap | 6% | 35% | $5-$50 | CRSP small-cap returns; Russell 2000 historical volatility |

### Sector Correlations

| Sector Pair | Correlation | Source |
|-------------|-------------|--------|
| Info Tech / Communication Services | 0.75 | MSCI/FactSet sector correlation matrices |
| Financials / Real Estate | 0.65 | MSCI/FactSet |
| Energy / Materials | 0.60 | MSCI/FactSet |
| Consumer Disc / Communication Services | 0.55 | MSCI/FactSet |
| Industrials / Materials | 0.50 | MSCI/FactSet |
| Consumer Staples / Health Care | 0.45 | MSCI/FactSet |
| Default (unspecified pairs) | 0.40 | General market correlation baseline |

### Corporate Actions

| Parameter | Value | Source |
|-----------|-------|--------|
| Dividend frequency | Quarterly 75%, Annual 20%, Special 5% | S&P Dow Jones: ~80% of S&P 500 companies pay quarterly dividends; S&P Global Market Intelligence |
| Dividend amount | Log-normal, range $0.01-$25/share | CRSP/FactSet dividend data; typical yield ~2-3% |
| Ex-to-pay date delta | Uniform 14-45 days | SEC dividend settlement rules; regulatory T+3 minimum |
| Stock split ratios | 2:1 (55%), 3:1 (15%), 4:1 (10%), 5:1 (5%), 10:1 (5%), Reverse (5%) | Historical S&P 500 split data; CRSP database |
| Split frequency | ~0.05 per company per year | S&P 500: stock splits are rare (~1 per 20 companies/year) |
| EPS estimate | Normal, mean=$2.50, sigma=$1.50, range -$5 to $20 | S&P 500 average EPS; FactSet Earnings Insight (2024) |
| EPS actual vs estimate | 85%-120% of estimate | FactSet: S&P 500 companies beat EPS estimates by ~4% on average |
| EPS surprise % | Normal, mean=0%, sigma=2.5%, range -25% to +25% | I/B/E/S; FactSet Earnings Insight surprise distribution |
| Insider transaction types | Buy 45%, Sell 50%, Grant 5% | SEC Form 4 filing data; InsiderMonkey/OpenInsider aggregated statistics |
| Insider titles | Director 25%, CEO 15%, VP 15%, CFO 12%, SVP 10%, EVP 10%, COO 8%, CTO 5% | SEC Form 4 data; typical public company officer/director composition |
| Insider trade size | Log-normal, mean ~3,000 shares, range 100-5M | SEC Form 4 transaction size distributions |
| Ticker concentration (trades) | Zipf alpha=1.5 | Market microstructure: mega-cap stocks dominate trading volume |

### Derived Counts

| Parameter | Value | Source |
|-----------|-------|--------|
| Trading days per year | 252 | NYSE/NASDAQ calendar (2024-2025) |
| Dividends per company/year | 1.5 average | S&P 500 dividend frequency (~1-4x/year) |
| Earnings per company/year | 4.0 | SEC quarterly reporting mandate |
| Insider txns per company/year | 2.0 | SEC Form 4 filing frequency (highly variable: 1-50+) |

---

## Supply Chain Domain

### Warehouse & Supplier

| Parameter | Value | Source |
|-----------|-------|--------|
| Warehouse type | Distribution Center 40%, Regional Hub 25%, Cross-Dock 15%, Cold Storage 12%, Fulfillment 8% | CSCMP State of Logistics Report; NAIOP industrial real estate data |
| Warehouse capacity | Log-normal, range 10K-500K sqft | NAIOP/CoreLogic warehouse size distribution data |
| Supplier country | US 35%, China 20%, Germany 10%, Mexico 10%, Japan 8%, others 17% | US Census Bureau Foreign Trade Statistics; WTO trade partner data |
| Supplier lead time | Log-normal, mean ~20 days, range 1-120 days | ISM Manufacturing PMI Supplier Deliveries Index; APQC benchmarks: 79-day global average (April 2024) |
| Supplier reliability | Uniform 60%-99% | APQC on-time delivery benchmarks; supplier scorecard conventions |

### Purchasing

| Parameter | Value | Source |
|-----------|-------|--------|
| PO status | Delivered 35%, Approved 30%, Shipped 20%, Pending 10%, Cancelled 5% | ISM Report on Business (2024); ERP transaction lifecycle analytics |
| PO priority | Standard 60%, Expedited 25%, Critical 10%, Low 5% | APICS/ASCM procurement priority conventions |
| PO amount | Log-normal, range $100-$5M | Supplier spend analysis; Pareto 80/20 rule typically applies |
| Receipt variance | 85%-100% of ordered quantity | Supply reliability metrics; damaged-in-transit rates |

### Shipment & Quality

| Parameter | Value | Source |
|-----------|-------|--------|
| Shipment status | Delivered 55%, In Transit 20%, Pending Pickup 10%, Delayed 8%, Returned 4%, Lost 3% | Carrier performance data; last-mile delivery statistics |
| Quality inspection result | Pass 75%, Conditional 10%, Fail 10%, Retest 5% | ANSI/ASQ AQL (Acceptable Quality Level) standards; incoming inspection benchmarks |

### Inventory

| Parameter | Value | Source |
|-----------|-------|--------|
| Inventory levels | Log-normal, range 0-100K units | ABC inventory analysis conventions; turnover rate benchmarks |
| Reservation rate | 0%-40% of on-hand | Allocation/backorder rates; demand planning conventions |
| Forecast methods | Moving Avg 30%, Exp Smoothing 25%, ARIMA 20%, ML 15%, Manual 10% | S&OP practice surveys; demand planning technology adoption |

---

## IoT Domain

### Device & Sensor

| Parameter | Value | Source |
|-----------|-------|--------|
| Device category | Industrial 30%, Consumer 25%, Automotive 15%, Medical 15%, Smart Home 15% | Gartner (2025): IoT installed base forecasts; IDC IoT device taxonomy |
| Device status | Active 72%, Inactive 10%, Maintenance 10%, Decommissioned 8% | IoT device lifecycle benchmarks; hardware failure/replacement cycle data |
| Communication protocol | MQTT 35%, HTTP 25%, CoAP 15%, AMQP 15%, Modbus 10% | Azure IoT Hub protocol statistics; IoT connectivity adoption surveys |
| Sensor types | Temperature 18%, Humidity 15%, Pressure 14%, Vibration 12%, Flow 10%, Level 10%, Proximity 11%, Light 10% | Industrial sensor deployment data; common IoT use case analysis |
| Reading quality | Good 90%, Suspect 7%, Bad 3% | Sensor error/anomaly rates; transmission loss data; data quality benchmarks |

### Alerts & Maintenance

| Parameter | Value | Source |
|-----------|-------|--------|
| Alert type | Threshold 30%, Anomaly 25%, Connectivity 20%, Battery 15%, Maintenance 10% | ISA-18.2 alarm management standard; IoT monitoring best practices |
| Alert severity | Medium 40%, Low 30%, High 20%, Critical 10% | ISA-95/ISA-18.2 alarm rationalization benchmarks; industry SLA targets |
| Alert acknowledgment | 70% acknowledged | Alert response SLA compliance data; IoT operations benchmarks |
| Alert resolution time | Uniform 0-7 days | MTTR benchmarks: Gartner (2025) reports 22% MTTR reduction with IoT monitoring |
| Maintenance type | Preventive 35%, Corrective 25%, Calibration 20%, Firmware Update 20% | TPM (Total Productive Maintenance) strategy ratios; McKinsey (2024): IoT reduces unplanned downtime 35% |
| Command success rate | Executed 55%, Acknowledged 20%, Sent 15%, Failed 10% | IoT connectivity reliability; device management platform data |

### Cardinality

| Parameter | Value | Source |
|-----------|-------|--------|
| Sensors per device | 2.5 | Device architecture; sensor density in industrial IoT |
| Readings per sensor | 20.0 | ~5-minute sampling intervals for continuous sensing |
| Alerts per device | 0.5 | Alarm rationalization targets; alert fatigue prevention |
| Maintenance per device | 1.5 | Preventive maintenance schedules |

---

## Manufacturing Domain

### Production Lines & Equipment

| Parameter | Value | Source |
|-----------|-------|--------|
| Line type | Assembly 30%, Machining 25%, Packaging 20%, Testing 15%, Finishing 10% | Manufacturing process mix; ISA-95 production operations |
| Equipment type | CNC 25%, Press 18%, Conveyor 15%, Robot 15%, Oven 12%, Tester 15% | Manufacturing asset portfolio; technology adoption surveys |
| Equipment status | Operational 80%, Under Maintenance 15%, Decommissioned 5% | Equipment uptime benchmarks; maintenance scheduling data |

### Work Orders & Quality

| Parameter | Value | Source |
|-----------|-------|--------|
| Work order status | Completed 55%, In Progress 15%, Open 10%, On Hold 10%, Cancelled 10% | ISA-95 manufacturing operations management standard; MES workflow data |
| Work order priority | Medium 55%, Low 25%, High 20% | Order prioritization policies; customer demand patterns |
| Quantity yield | 85%-100% of planned quantity | First-pass yield benchmarks; scrap/rework rates |
| Quality check result | Pass 82%, Rework 10%, Fail 8% | ASQ quality benchmarks; AQL (Acceptable Quality Level) standards |
| Defect severity | Minor 50%, Cosmetic 30%, Major 15%, Critical 5% | Defect classification standards; customer impact assessment |
| Defect root cause | Material 25%, Process 25%, Equipment 20%, Human 20%, Design 10% | Ishikawa/fishbone analysis conventions; 8D report patterns |
| Defect disposition | Rework 35%, Scrap 25%, Use As-Is 25%, Return to Supplier 15% | NCR (Non-Conformance Report) disposition benchmarks; cost of poor quality |

### Production Metrics

| Parameter | Value | Source |
|-----------|-------|--------|
| OEE score | Uniform 50%-99% (avg 60-75%) | LNS Research OEE benchmarks; global average 55-60%; 85% world-class |
| Yield rate | Uniform 85%-99% | First-pass yield industry data; Six Sigma benchmarks |
| Cycle time | Log-normal, mean ~55s, range 5s-1hr | Manufacturing cycle time benchmarks by process type |
| Scrap rate | Uniform 0.1%-10% | Waste rate benchmarks; defect-to-scrap conversion |

### Downtime

| Parameter | Value | Source |
|-----------|-------|--------|
| Downtime cause | Breakdown 25%, Planned Maintenance 25%, Material Shortage 15%, Changeover 15%, Power Outage 10%, Quality Issue 10% | TPM loss categories; MES/OEE data; Gartner (2025): $260K/hr average large-plant downtime cost |
| Downtime duration | Log-normal, mean ~33 min, range 5 min-24 hr | MTTR benchmarks; maintenance duration data |
| Impact level | Medium 50%, Low 30%, High 20% | Production loss assessment; financial impact classification |

---

## Marketing Domain

### Campaign & Channel Mix

| Parameter | Value | Source |
|-----------|-------|--------|
| Campaign channel | Email 25%, Social 20%, PPC 18%, Content 15%, Event 12%, Direct Mail 10% | HubSpot State of Marketing (2024); MarketingProfs channel mix benchmarks |
| Campaign status | Completed 45%, Active 25%, Paused 15%, Draft 15% | Marketing automation platform benchmarks; Salesforce/HubSpot CRM analytics |
| Lead source type | Inbound 35%, Outbound 25%, Referral 22%, Partner 18% | HubSpot 2024 Sales Trends Report; industry lead source surveys |

### Lead Generation & Funnel

| Parameter | Value | Source |
|-----------|-------|--------|
| Lead status | Contacted 25%, New 20%, Qualified 20%, Converted 20%, Unqualified 15% | Belkins/HubSpot 2024 funnel benchmarks; First Page Sage B2B conversion rates |
| Lead-to-MQL conversion | ~31% of leads become MQLs | Belkins (2024); HubSpot funnel benchmarks |
| MQL-to-SQL conversion | ~20% | HubSpot 2024 Sales Trends Report; Gradient Works 2024 B2B benchmarks |
| SQL-to-Opportunity conversion | ~40-60% | Salesforce (2025): mature organizations convert 40-60% of SQLs |
| Opportunity stages | Proposal 20%, Qualification 18%, Closed Won 18%, Negotiation 17%, Prospecting 15%, Closed Lost 12% | Salesforce pipeline analytics; Gong B2B deal stage benchmarks |
| Opportunity close rate | ~22-30% | HubSpot/Salesforce: overall win rates 15-25%; qualified opportunity close ~29% |
| Conversion types | Purchase 30%, Trial 25%, Demo 25%, Signup 20% | SaaS product funnel data; CRM marketing automation analytics |

### Email & Engagement

| Parameter | Value | Source |
|-----------|-------|--------|
| Email open rate | 25% (true: 0.25, false: 0.75) | Mailchimp Email Marketing Benchmarks (2024); Campaign Monitor B2B averages ~20-25% |
| Email click-through rate | 8% (true: 0.08, false: 0.92) | Mailchimp/HubSpot email benchmarks; ~2-3% of all recipients, ~8% of openers |
| Email bounce rate | 3% (true: 0.03, false: 0.97) | Email deliverability benchmarks; industry list quality metrics |
| Web traffic referrer | Organic 30%, Paid 22%, Social 18%, Direct 18%, Email 12% | Google Analytics traffic attribution data; web analytics platform benchmarks |

### Cardinality Ratios

| Parameter | Value | Source |
|-----------|-------|--------|
| Contacts per campaign | 25 | Contact management ratio; contacts acquired per campaign |
| Leads per contact | 0.4 | Conversion rate from contact to qualified lead |
| Opportunities per lead | 0.5 | Sales pipeline ratio |
| Email sends per campaign | 50 | Email volume per campaign; list size and send patterns |
| Web visits per contact | 5 | Repeat visitor metrics; session count per contact |
| Conversions per lead | 0.3 | Lead-to-conversion rate; sales effectiveness |

---

## Real Estate Domain

### Property Characteristics

| Parameter | Value | Source |
|-----------|-------|--------|
| Bedrooms | 3BR: 35%, 4BR: 25%, 2BR: 18%, 1BR: 8%, 5BR: 10%, 6BR: 4% | US Census Bureau: American Housing Survey; housing stock distributions |
| Bathrooms | 2.0: 30%, 2.5: 22%, 1.5: 15%, 1.0: 12%, 3.0: 12%, 3.5: 5%, 4.0: 4% | US Census Bureau housing characteristics; MLS inventory data |
| Square footage | Log-normal, mean ~1,500 sqft, range 500-10,000 | US Census Bureau: median new home size ~2,200 sqft (2024); existing stock lower |
| Year built | Uniform 1950-2025 | US Census Bureau: housing stock age distribution; post-WWII construction boom |
| Assessed value | Log-normal, range $50K-$5M | Tax assessor records; Zillow Home Value Index |

### Listings & Transactions

| Parameter | Value | Source |
|-----------|-------|--------|
| Listing status | Sold 45%, Active 20%, Expired 15%, Withdrawn 10%, Pending 10% | NAR existing home sales data; MLS historical status transitions |
| Days on market | Log-normal, mean ~26 days | NAR April 2024: homes spent average 26 days on market before sale |
| Sold above list price | ~33% of homes | NAR (2024): nearly one-third of homes sold above listing price |
| Median sale price | ~$400K (2024) | NAR (2024): median existing-home price showed 5.7% YoY increase |
| Offer status | Accepted 30%, Rejected 25%, Countered 20%, Pending 15%, Withdrawn 10% | MLS offer data; NAR market competitiveness metrics |
| Offer contingencies | Inspection 30%, Financing 28%, Appraisal 22%, None 20% | NAR transaction data; contingency frequency varies with market conditions |
| Inspection result | Pass 60%, Conditional 30%, Fail 10% | Home inspection industry data; ASHI (American Society of Home Inspectors) |

### Agent & Market

| Parameter | Value | Source |
|-----------|-------|--------|
| Agent specialization | Residential 45%, Commercial 20%, Luxury 15%, Investment 12%, New Construction 8% | NAR member profile survey; agent specialization demographics |
| Commission rate | 2.5%-6.0% (uniform) | NAR member profile survey; post-2024 NAR settlement adjustments |
| Listing seasonality | Mar-Jun peak (~40% of annual), Dec-Jan trough (~12%) | NAR monthly existing home sales data: spring/summer peak, winter trough |
| Buyer interest level | Medium 35%, Low 30%, High 20%, None 15% | Real estate showing feedback data; market demand indicators |

### Cardinality Ratios

| Parameter | Value | Source |
|-----------|-------|--------|
| Listings per property | 1.5 | Relisting frequency; average times listed before sale |
| Showings per listing | 5.0 | NAR showing frequency data; varies with market tightness |
| Offers per listing | 1.5 | NAR: multiple offer frequency depends on market conditions |
| Sales per listing | 0.4 | Sale closure rate; accounts for withdrawn/expired listings |

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

### HR
- [BLS JOLTS: Job Openings and Labor Turnover Survey](https://www.bls.gov/jlt/) — quit rates, layoffs, separations
- [BLS OEWS: Occupational Employment and Wage Statistics](https://www.bls.gov/oes/) — median wages by occupation
- [BLS CPS: Current Population Survey — Full/Part-Time Employment](https://www.bls.gov/cps/cpsaat08.htm)
- [BLS Employee Benefits Survey](https://www.bls.gov/ncs/ebs/) — leave, benefits participation
- [SHRM: Performance Management Benchmarks](https://www.shrm.org/) — review cycles, rating distributions
- [ATD: State of the Industry Report](https://www.td.org/research-reports) — training spend, completion rates
- [Betterworks: 2024 Performance Enablement Report](https://www.betterworks.com/) — performance review trends
- [Work Institute: Retention Report](https://workinstitute.com/) — turnover reasons
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
- [NAIC: 2024 Market Share Data](https://content.naic.org/article/naic-releases-2024-market-share-data) — P&C, Life, Health market composition
- [NAIC: Property/Casualty Market Share Report (2024)](https://content.naic.org/sites/default/files/research-actuarial-property-casualty-market-share.pdf)
- [AM Best: US Individual Life Lapse Ratio (2023)](https://news.ambest.com/newscontent.aspx?refnum=259009) — 5.1 lapse ratio
- [III: Insurance Information Institute](https://www.iii.org/) — industry composition, claim statistics
- [KFF: Claims Denials in ACA Marketplace Plans (2023)](https://www.kff.org/private-insurance/claims-denials-and-appeals-in-aca-marketplace-plans-in-2023/)
- [Experian: 2024 Consumer Credit Review](https://www.experian.com/blogs/ask-experian/consumer-credit-review/) — FICO score distribution
- [FICO: Average US Score 717 (2024)](https://www.fico.com/blogs/average-u-s-fico-score-stays-717)
- [LOMA: Persistency Benchmarks](https://www.loma.org/) — policy retention rates
- [JD Power: Insurance Digital Experience Study](https://www.jdpower.com/)

### Telecom
- [CTIA: 2024 Annual Survey Highlights](https://www.ctia.org/news/2024-annual-survey-highlights) — subscriber counts, data traffic
- [FCC: 2024 Communications Marketplace Report](https://www.fcc.gov/document/fcc-releases-2024-communications-marketplace-report)
- [Counterpoint/BankMyCell: US Smartphone Market Share (2024)](https://www.bankmycell.com/blog/us-smartphone-market-share) — Apple 61%, Samsung 23%
- [Statcounter: Mobile Vendor Market Share US](https://gs.statcounter.com/vendor-market-share/mobile/united-states-of-america)
- [JD Power: Wireless Customer Satisfaction Study (2024)](https://www.jdpower.com/)

### Financial
- [FDIC: Quarterly Banking Profile Q4 2024](https://www.fdic.gov/quarterly-banking-profile/quarterly-banking-profile-q4-2024)
- [FDIC: Summary of Deposits (2024)](https://www.fdic.gov/news/press-releases/2024/fdic-releases-results-summary-deposits-annual-survey)
- [Federal Reserve: Payments Study (2022)](https://www.federalreserve.gov/paymentsystems/fr-payments-study.htm) — 204B card payments
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
- [Hull, J.C.: Options, Futures, and Other Derivatives](https://www.pearson.com/) — GBM pricing theory
- [SEC Form 4 Filings](https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=4) — insider transactions

### Supply Chain
- [ISM: Report on Business (Manufacturing PMI)](https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/)
- [APQC: Supply Chain Benchmarking](https://www.apqc.org/) — lead times, supplier metrics
- [APICS/ASCM: Supply Chain Operations Reference Model](https://www.ascm.org/)
- [Census Bureau: Annual Survey of Manufactures](https://www.census.gov/programs-surveys/asm.html)

### IoT
- [Gartner: IoT Market Insights (2025)](https://www.gartner.com/en/information-technology/insights/internet-of-things)
- [McKinsey: IoT Value Creation (2024)](https://www.mckinsey.com/) — downtime reduction 35%
- [Frost & Sullivan: MTBF Benchmarks (2024)](https://www.frost.com/) — 38% improvement with IoT
- [IEEE: IoT Standards](https://standards.ieee.org/)
- [ISA-95/ISA-18.2: Alarm Management Standard](https://www.isa.org/)

### Manufacturing
- [LNS Research: OEE Benchmarks by Industry](https://blog.lnsresearch.com/bid/155988/Overall-Equipment-Effectiveness-Benchmark-Data-by-Industry)
- [OEE.com: World-Class OEE Benchmarks](https://www.oee.com/) — 85% world-class, 60% average
- [ASQ: Quality Benchmarks](https://asq.org/) — defect rates, Six Sigma
- [Gartner (2025): IoT boosts OEE 15-20%](https://www.gartner.com/)
- [ISA-95: Manufacturing Operations Management](https://www.isa.org/)

### Marketing
- [HubSpot: 2024 Sales Trends Report](https://www.hubspot.com/) — funnel benchmarks, lead quality
- [Salesforce: State of Sales (2025)](https://www.salesforce.com/) — SQL-to-opportunity conversion
- [Belkins: B2B Conversion Rate Benchmarks](https://belkins.io/blog/lead-generation-conversion)
- [First Page Sage: B2B Conversion Rates by Industry](https://firstpagesage.com/reports/b2b-conversion-rates-by-industry-fc/)
- [Mailchimp: Email Marketing Benchmarks (2024)](https://mailchimp.com/resources/email-marketing-benchmarks/)
- [Gradient Works: 2024 B2B Sales Benchmarks](https://www.gradient.works/blog/2024-b2b-sales-benchmarks)

### Real Estate
- [NAR: Research and Statistics](https://www.nar.realtor/research-and-statistics) — median prices, days on market
- [NAR: Existing-Home Sales Data](https://www.nar.realtor/research-and-statistics/housing-statistics/existing-home-sales)
- [Census Bureau: New Residential Construction](https://www.census.gov/construction/nrc/)
- [NAR: Member Profile Survey](https://www.nar.realtor/) — commission rates, agent demographics
