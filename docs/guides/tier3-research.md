# Tier 3 Research Features

Tier 3 provides research-grade fidelity features: Bayesian structure learning, differential privacy, drift monitoring, and bootstrap generation.

## Chow-Liu Bayesian Network

Learns the dependency tree structure between columns via the Chow-Liu algorithm. The resulting tree is the maximum spanning tree of pairwise mutual information.

```python
from sqllocks_spindle.inference.tier3_research import ChowLiuNetwork

net = ChowLiuNetwork(n_bins=10)
result = net.fit(df)
for edge in sorted(result.edges, key=lambda e: -e.mutual_information):
    print(f"  {edge.parent} → {edge.child}  MI={edge.mutual_information:.4f}")
```

The MI matrix shows all pairwise dependencies — useful for understanding which features are redundant or correlated.

## Differential Privacy

Apply Laplace or Gaussian noise calibrated to `L1_sensitivity / ε`.

```python
from sqllocks_spindle.inference.tier3_research import DifferentialPrivacy

dp = DifferentialPrivacy(epsilon=1.0, mechanism="laplace")
noised_df, result = dp.apply(df)
print(f"Columns noised: {result.columns_noised}")
```

**Privacy budget guidance:**

| ε | Privacy level | Utility impact |
|---|--------------|----------------|
| 0.1 | Very strong | High noise |
| 1.0 | Standard | Moderate noise |
| 5.0 | Weak | Low noise |

## Drift Monitor

Detect when a dataset has shifted away from a reference.

```python
from sqllocks_spindle.inference.tier3_research import DriftMonitor

monitor = DriftMonitor(pvalue_threshold=0.05, psi_threshold=0.2)
report = monitor.compare(reference_df, current_df)
print(f"Drifted columns: {report.drifted_columns}")
print(f"Overall drift: {report.overall_drift_score:.4f}")
```

KS test is used for numeric columns; Chi-squared for categorical. PSI > 0.2 also flags drift.

## Bootstrap Mode

Generate synthetic data by sampling with replacement from real data. Preserves exact distributions but does not generalize.

```python
from sqllocks_spindle.inference.tier3_research import BootstrapMode

bm = BootstrapMode(add_jitter=True, jitter_std_fraction=0.01)
synth_df, result = bm.generate(real_df, n_rows=5000, seed=42)
```

Useful as a **fidelity ceiling baseline** — comparing Spindle's score against bootstrap's score shows how much parametric generation costs vs exact resampling.

## CTGAN (Optional Deep Learning)

```python
from sqllocks_spindle.inference.tier3_research import CTGANWrapper

if CTGANWrapper.is_available():
    wrapper = CTGANWrapper(epochs=300)
    wrapper.fit(real_df)
    synth_df = wrapper.sample(5000)
```

Install: `pip install sqllocks-spindle[deep]` (adds `ctgan` dependency).
