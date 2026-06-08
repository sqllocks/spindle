# Argentum Financial — Spindle Demo/Test Source Database (Design Spec v1)

**Date:** 2026-06-06
**Status:** DRAFT (brainstorm output — awaiting confirmation of the 3 forks)
**Purpose:** A verbose, realistic, *live* transactional OLTP database — full of synthetic PII, deep FK
topology, and deliberately adversarial distributions — built so that **every Spindle subsystem can be
demoed and regression-tested against one coherent source.**

> **Not a Spindle domain pack.** Domain packs are calibrated from published stats and need no input.
> This is a *fixture to profile against* — the thing Spindle ingests in `profile → safe-profile → regenerate`.

## Recommended defaults (override any)
- **Universe:** Neobank — *Argentum Financial* (densest PII + transaction volume + correlation + free-text)
- **Delivery:** Fabric SQL Database in Sound BI tenant (`2536810f-...`) — live, queryable, native Fabric round-trip
- **Scale:** Demo — ~5k customers, ~50k transactions, 3 yrs history (profiles in seconds; conference-ready)
- All values synthetic-but-realistic → the DB itself is publishable; **no real PII ever**.

## Design principle
**No column exists unless it tortures a specific Spindle subsystem.** "Extreme" = deliberately adversarial
against the profiler, not merely large.

## Spindle subsystem → coverage matrix
| Subsystem | What the schema feeds it |
|---|---|
| `profiles/` DataProfiler | Varied dtypes / cardinalities / distribution shapes |
| `inference/safe_profile.py` PII gate (ADR-004/008) | SSN, email, phone, PAN, DOB, ZIP, account#, names |
| safe_profile content scanner (STORY-010) | Free-text `support_ticket.notes` leaking PII in prose |
| safe profile k-anon (STORY-007) | Quasi-identifiers + singleton rare categoricals |
| `engine` GaussianCopula | income → balance → spend → credit_limit correlation chain |
| `engine` EmpiricalStrategy + tail anchors (STORY-019) | Heavy-tail tx amounts (p99.5 whales), bimodal income |
| `schema/` FK ordering | Deep chains, composite keys, self-ref FKs, M:N junctions |
| `chaos/` | Clean baseline to A/B corrupt |
| `streaming/` + `incremental/` | Timestamped `transaction` table w/ seasonality |
| `validation/` DistributionGate | Ground-truth distributions to score fidelity |
| `fabric/` sinks | Real SQL types that round-trip to Lakehouse/Warehouse |

## Schema (≈18 tables, 3NF)
**Core entities:** `customer`, `address` (SCD2 history), `account`, `account_holder` (M:N junction,
composite PK), `card`, `merchant`, `merchant_category`, `transaction` (fact, high-volume), `dispute`,
`chargeback`, `loan`, `loan_payment`, `beneficiary` (self-ref → customer), `employee` (self-ref → manager),
`branch`, `support_ticket` (free-text notes), `kyc_document`, `audit_log`.

### PII inventory (per column)
- `customer`: full_name, ssn (valid + intentionally malformed), dob, email (with `+tags`), phone (e.164 + sloppy)
- `address`: street, city, state, zip (zero-padded **string** `"07090"` — tests profiler-gap id=3654)
  - **Street line (DECIDED 2026-06-06, option 1):** fabricated street *name* + random house *number* (faker-style),
    bound to a REAL ZIP/city/state/lat-lng sampled from `us_zip_locations.json`. Looks real, maps correctly,
    corresponds to **no real residence**. Verified-real street datasets (OpenAddresses/TIGER) explicitly
    REJECTED — a real number+street+ZIP could land on a real person's home (re-identification risk).
  - Ethics line to ship with the artifact: *"addresses are real geographic locations (ZIP/city/lat-lng) with
    fabricated street lines, corresponding to no real residence."*
- `card`: pan (test BINs), cvv_hash, expiry
- `employee`: ssn, salary (HR PII)
- `support_ticket.notes`: free-text traps — *"called Jane at 206-555-0142, verified SSN ending 4821"*

### FK topology zoo
- Self-ref: `beneficiary→customer`, `employee→manager`
- Composite-key junction: `account_holder(account_id, customer_id)`
- Optional/nullable FKs; one deliberately **orphan-able** child for referential-repair tests

### Distribution zoo
- Pareto: merchant transaction volume
- Bimodal: income (salaried vs hourly)
- Heavy tail: `transaction.amount` with p99.5 whales
- Zero-inflated: chargeback counts
- Seasonal + trend: daily transaction volume over 3 yrs
- Benford-conformant amounts (testable assertion)

### Correlation chain (for copula fidelity scoring)
`customer.income → account.balance → monthly_spend → card.credit_limit`

### Temporal / SCD
`created_at` / `updated_at` / soft-delete `is_active`; `address` kept as SCD2 history table.

## Scenarios this one DB unlocks
1. profile → safe-profile → regenerate (fidelity + DistributionGate)
2. PII masking demo (`profile validate --safe` proves zero PII survives)
3. Free-text leak interception (support-ticket content scanner)
4. Chaos run (corrupt clean baseline → pipeline breakage)
5. Streaming/incremental replay of `transaction` into Eventhouse
6. FK-integrity regeneration (15+ tables, zero orphans)
7. Copula correlation preservation demo
8. Fabric round-trip → Lakehouse/Warehouse → Power BI
9. k-anon / rare-cell suppression

## Scope boundaries (anti-features)
- Not a calibrated domain pack.
- Never seeded with real PII.
- No app/UI — database + seed only.

## Open forks (the 3 that change the build)
1. **Universe** — Neobank (rec.) / Healthcare clinic / Omni-channel retail
2. **Delivery** — Fabric SQL DB (rec.) / local SQL Server via sql-server MCP / SQLite+SQL files / CSV-Parquet pack
3. **Scale** — Demo ~50k tx (rec.) / Realistic 1–2M / Extreme 10M+

## Next step
On confirmation: (a) generate DDL + FK constraints, (b) write a deterministic Python seeder using
`sqllocks-spindle` domain packs where they fit + Faker/custom for the adversarial columns, (c) load to the
chosen target, (d) verify FK integrity + row counts + distribution shapes before declaring ready.
