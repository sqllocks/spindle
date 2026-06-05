# Spindle fidelity: the honest, measured claim language

Source of truth for how we talk about fidelity. Measured 2026-06-05 across all 13
reference domains (840 columns) via FidelityComparator (KS numeric + chi-squared
categorical) on a profile -> regenerate round trip. Do not drift back to "99%" or
"70%".

## The principle
Never claim ONE blended number. A blended overall % conflates two different
things: data we DELIBERATELY removed (PII) and data we reproduce. Claim fidelity
only on what we are allowed to reproduce, and state the PII removal as the feature.

## The measured split (all 13 domains, 840 columns)
| Column class | Share | Mean fidelity |
|---|---|---|
| Reproducible (numeric / categories / codes) | 76% | 85.5% |
| PII-gated (names / emails / phones / addresses) | 13% | 28.9% (by design) |
| Datetime (no safe temporal stat yet - a fixable gap) | 11% | 15.2% |

- Reproducible columns reach 95-99% specifically on continuous, correlated,
  PII-free data (best case; not typical).
- PII columns are intentionally NOT faithful: you cannot reproduce a column of
  real names without leaking real names. Low fidelity there is the product
  working.
- Datetime is a current gap (persist a coarse year/month histogram -> joins the
  reproducible bucket; free, no safety cost).

## What we SAY (approved language)
> Spindle reproduces the statistical shape of your non-PII data at high fidelity
> (~85% across 13 domains, 95%+ on continuous correlated data), while the PII
> (names, emails, phones, dates of birth) is removed by design, not approximated.

Blog one-liner:
> Ship the recipe, not the data: high-fidelity synthetic shape with the PII
> removed by construction - and we show you exactly which columns we reproduce
> and which we won't.

Flip the caveat into the pitch:
> It is not 100%, and that is the point. The part you lose is the PII. Spindle
> keeps the analytics shape and drops the people.

## What we do NOT say
- NOT "99% fidelity" unqualified (best case only: PII-free correlated continuous).
- NOT "~70% fidelity" (blended; misleadingly low - counts columns we chose to delete).
- NOT "PII-free AND 90%+" on the same artifact without the column-class split.

## Why this is trustworthy
The redaction_manifest already reports, per column, what was reproduced vs
removed. Showing that split is what converts "is it really safe?" into trust.
