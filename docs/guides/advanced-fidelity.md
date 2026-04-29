# Advanced Fidelity (Tier 1)

The `AdvancedProfiler` adds five Tier 1 fidelity checks on top of the base KS/Chi-squared scoring.

## Quick Start

```python
from sqllocks_spindle.inference import AdvancedProfiler

profiler = AdvancedProfiler()
adv = profiler.profile_pair(real_df, synth_df, table_name="orders")

# GMM fits
for col, fit in adv.gmm_fits.items():
    print(f"{col}: {fit.n_components} Gaussian components  BIC={fit.bic:.1f}")

# Adversarial distinguishability
if adv.adversarial:
    print(f"AUC-ROC: {adv.adversarial.auc_roc:.3f}")  # 0.5 = perfect
    print(f"Passed: {adv.adversarial.passed}")
```

## Features

### GMM (Gaussian Mixture Models)

Fits a mixture of 1–5 Gaussians to each numeric column. BIC selects the best number of components. This detects bimodal or multi-modal distributions that a single Gaussian would miss.

Requires `scikit-learn`. Install: `pip install sqllocks-spindle[advanced]`

### Adversarial Validator

Trains a GradientBoostingClassifier to distinguish real from synthetic rows. If the classifier can't do better than chance (AUC ≈ 0.5), the data is indistinguishable. AUC > 0.75 is flagged as a concern.

### Conditional Profiles

Profiles how numeric columns behave conditioned on categorical values. For example: `mean(revenue | segment=Enterprise)`. Captures the joint structure that marginal distributions miss.

### Temporal Profiler

For datetime columns, computes gap statistics (mean/std/min/max gap seconds), lag-1 and lag-7 autocorrelation, and fits the gap distribution (exponential vs normal).

### FFT Periodicity

Uses the Fast Fourier Transform to detect recurring patterns (daily, weekly, monthly). A dominant power > 5× median power indicates strong periodicity.

## Requirements

```
pip install sqllocks-spindle[advanced]
# Installs: scikit-learn>=1.3, scipy>=1.11
```
