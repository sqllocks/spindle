# STORY-019 - Generator overhaul: empirical inverse-CDF marginals + capped copula (the real 99%)

**Epic:** E8 - High-fidelity generation
**ADR:** ADR-002 / ADR-010
**Status:** backlog (the real path to high fidelity; copula WIRING already landed)

## Story
As a data engineer, I want the production generator to reproduce numeric marginals
and joint correlation faithfully so that reproducible columns regenerate at the
fidelity already PROVEN achievable from the safe profile (~95-99% on correlated
continuous data), not the current ~52%.

## Proven context (measured 2026-06-05)
- A standalone reconstructor using empirical inverse-CDF marginals + a
  Cholesky-Gaussian copula scores ~99% on correlated continuous data from the SAME
  safe profile. So the safe stats are SUFFICIENT; the production generator is the
  bottleneck.
- The production generator: (a) over-correlates derived/clipped numeric columns to
  ~1.0 (independent of the copula), and (b) reproduces marginals poorly (~52% on
  the correlated fixture). See tests/test_copula_production.py (xfail documents it).
- The copula WIRING is already done + safe (SafeProfileAdapter emits
  correlated_columns from correlation_matrix; GaussianCopula post-pass fires).
  This story fixes what the wiring exposes.

## Acceptance Criteria
- [ ] Numeric generation uses the persisted empirical quantile fingerprint
      (p1..p99) via piecewise-linear inverse-CDF sampling, NOT normal(mean,std),
      for columns that have quantiles (clipped to bounds).
- [ ] The correlation post-pass tracks the TARGET pairwise r (e.g. ~0.80) and does
      NOT collapse to 1.0; root-cause + fix the over-correlation (likely shared
      rank/uniform draws across columns, or a degenerate Cholesky fallback).
- [ ] tests/test_copula_production.py::...recovers_correlation_target passes
      (0.6 < gen_corr < 0.95) - remove the xfail.
- [ ] Round-trip fidelity on a correlated continuous fixture >= 90% (KS+chi2).
- [ ] No raw value persisted (safety unchanged); the per-domain reference
      benchmark does not regress (those domains are uncorrelated/recent-dated).
- [ ] MUST pass the loop's adversarial security verify before commit.

## Notes
This is a generator-internal build (engine/strategies + the copula apply), larger
than the safe-profile work. Scope it on its own; validate against a correlated
fixture (the reference domains cannot validate it - they have ~0 correlation).
