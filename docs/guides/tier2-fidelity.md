# Tier 2 Fidelity

Tier 2 adds format preservation, string similarity, cardinality constraints, and anomaly rate checks.

## Quick Start

```python
from sqllocks_spindle.inference.tier2_profiler import run_tier2

report = run_tier2(real_df, synth_df)
print(report.summary())
print(f"Pass rate: {report.passing_rate():.1%}")
```

## Format Preservation

Detects structured string formats (email, UUID, phone, URL, IP, ZIP, SSN, date) in the real data and verifies the synthetic data preserves them.

```python
from sqllocks_spindle.inference.tier2_profiler import FormatPreservationAnalyzer

results = FormatPreservationAnalyzer(threshold=0.10).analyze(real_df, synth_df)
for col, r in results.items():
    print(f"{col}: {r.detected_format}  real={r.real_format_rate:.1%}  synth={r.synth_format_rate:.1%}")
```

If `real_format_rate >= 0.5` and `|real - synth| > threshold`, the check fails.

## String Similarity

Character trigram cosine similarity between real and synthetic string value distributions. Score near 1.0 means identical vocabularies; near 0 means completely different.

```python
from sqllocks_spindle.inference.tier2_profiler import StringSimilarityAnalyzer

results = StringSimilarityAnalyzer(ngram_n=3).analyze(real_df, synth_df)
for col, r in results.items():
    print(f"{col}: {r.cosine_similarity:.4f} ({r.score:.1f}/100)")
```

## Cardinality Constraints

Checks that `synth_unique / real_unique` stays within ±20% of 1.0.

```python
from sqllocks_spindle.inference.tier2_profiler import CardinalityConstraintChecker

results = CardinalityConstraintChecker(max_deviation=0.20).analyze(real_df, synth_df)
for col, r in results.items():
    status = "PASS" if r.passed else "FAIL"
    print(f"[{status}] {col}: ratio={r.ratio:.3f}")
```

## Anomaly Rate Checker

When using `AnomalyRegistry`, verify the injected anomaly fraction matches expectations.

```python
from sqllocks_spindle.inference.tier2_profiler import check_anomaly_rates

result = check_anomaly_rates(df, expected_fractions={"point": 0.01}, tolerance=0.05)
print(f"Actual anomaly rate: {result.actual_fraction:.2%}")
print(f"Passed: {result.passed}")
```
