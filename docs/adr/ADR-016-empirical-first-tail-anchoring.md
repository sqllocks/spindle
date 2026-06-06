# ADR-016 — Empirical-first numeric generation, reconciled with winsorization-widening via tail anchors

**Status:** Accepted (2026-06-06)
**Epic:** E8 — High-fidelity generation
**Stories:** STORY-019 (generator overhaul); reconciles STORY-006 (winsorized bounds)
**Supersedes/extends:** ADR-002 (winsorized bounds)

## Context

STORY-019 required the production generator to reproduce numeric marginals and
joint correlation faithfully (the safe profile was already proven sufficient —
~95–99% achievable from it via a standalone reconstructor — but the production
generator scored ~52% and over-correlated derived columns toward r≈1.0).

Root cause (confirmed empirically): `SafeProfileAdapter._numeric_generator`
preferred the profiler's **fitted distribution** over the empirical quantile
fingerprint. On tightly-coupled columns (e.g. `income ≈ f(age)`) the profiler
fits near-zero-sigma log-normals, which degenerate the regenerated column to a
near-constant (std ~1e-14). A constant column has undefined (NaN) pairwise
correlation, so the `GaussianCopula` post-pass — which was always wired
correctly — had nothing to reorder. The **same** bad parametric fit explained
both the poor marginal fidelity and the correlation collapse.

The fix (route quantile-bearing numeric columns through the **empirical
inverse-CDF** strategy first) directly conflicts with STORY-006 / ADR-002, whose
**winsorization-widening** lever recovers heavy-tail fidelity by widening the
clip window from p1/p99 to p0.5/p99.5. The empirical strategy's interpolation
table only spanned p1..p99, so it clamped both tails at p1/p99 regardless of the
configured bounds — widening became a no-op through the empirical path, and two
STORY-006 tests (`test_distribution_path_clips_to_bounds`,
`test_widening_recovers_fidelity_on_heavy_tail_nonliteral`) broke.

## Decision

1. **Empirical-first.** When a numeric column carries a full p1..p99 quantile
   fingerprint, `_numeric_generator` routes it to the `empirical` strategy
   (piecewise-linear inverse-CDF) **before** any fitted distribution. Fitted
   distributions remain the fallback for columns without a full fingerprint.

2. **Tail anchors reconcile with winsorization-widening (Option C).** The
   `EmpiricalStrategy` includes the persisted widening endpoints **p0_5 / p99_5**
   as *outer* interpolation anchors (at CDF 0.005 / 0.995) when present and
   monotonic. The post-interpolation min/max clip (the winsorized `bounds`) then
   selects the effective window:
   - **Default bounds** (lo=p1, hi=p99): values beyond p1/p99 are clipped back —
     behaviour unchanged, narrow window.
   - **Widened bounds** (lo=p0_5, hi=p99_5): the heavy-tail mass between p99 and
     p99_5 passes through — widening recovers fidelity **through** the empirical
     path, exactly as ADR-002 intends.

   No raw extreme is ever persisted or read: p0_5/p99_5 are aggregate quantiles,
   safe by the same argument as p1/p99 (ADR-001/ADR-002).

## Consequences

- STORY-019 AC#1–AC#4 pass by behaviour: empirical strategy on numeric columns;
  generated `age`/`income` correlation tracks the target (~0.80, no collapse);
  the previously-xfailed `test_production_generate_recovers_correlation_target`
  passes (0.6 < r < 0.95); round-trip fidelity on a correlated continuous fixture
  ≥ 90% (measured ~98–99%).
- STORY-006 stays green. `test_distribution_path_clips_to_bounds` is updated to
  drop its quantile fingerprint (so it genuinely exercises the fitted-distribution
  branch — the empirical-path clip is covered by `test_empirical_path_clips_to_bounds`).
  `test_widening_recovers_fidelity_on_heavy_tail_nonliteral` passes unchanged once
  tail anchors are present.
- Columns without a full p1..p99 fingerprint are unaffected (fitted distribution
  / mean-std / uniform fallbacks unchanged).
- Safety posture unchanged (ADR-001/002/003/007): no raw min/max/enum leaves prod.

## Alternatives considered

- **A — empirical-first wins, rewrite STORY-006 tests to the new contract.**
  Simpler, but weakens STORY-006's distributional guarantees and rewrites another
  story's assertions more than necessary.
- **B — narrow AC#1: prefer empirical only when no faithful fitted distribution
  exists (or only for copula-correlated columns).** Preserves STORY-006 untouched
  but makes the high-fidelity path conditional and fragile (the degenerate-fit
  detection is exactly what we're trying to avoid relying on).
- **C — empirical-first + tail anchors (chosen).** Reconciles both stories with
  no loss to either; the widening lever keeps working, empirical-first is
  unconditional for quantile-bearing columns.
