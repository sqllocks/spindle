"""Advanced fidelity profiling — Tier 1 improvements.

Adds multi-variate conditional profiling, GMM distribution fitting,
adversarial distinguishability scoring, temporal/sequence profiling,
and FFT periodicity detection on top of the base DataProfiler.

All features degrade gracefully when optional dependencies are absent:
- sklearn: GMM fitting + adversarial scoring
- scipy: periodicity (FFT) + KS test for sequence gaps
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Optional dependency guards
# ---------------------------------------------------------------------------

try:
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import cross_val_score
    from sklearn.preprocessing import LabelEncoder
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False

try:
    from sklearn.mixture import GaussianMixture
    HAS_GMM = True
except ImportError:
    HAS_GMM = False

try:
    from scipy import stats as sp_stats
    from scipy.fft import rfft, rfftfreq
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class GMMFit:
    """Gaussian Mixture Model fit for a numeric column."""

    column: str
    n_components: int
    means: list[float]
    weights: list[float]
    stds: list[float]
    bic: float  # lower is better
    aic: float


@dataclass
class ConditionalProfile:
    """Conditional statistics for col_a given values of col_b."""

    primary_col: str
    conditioned_on: str
    stats_by_value: dict[str, dict[str, float]]  # value -> {mean, std, count}


@dataclass
class AdversarialResult:
    """Result of the adversarial (distinguishability) test."""

    auc_roc: float  # 0.5 = indistinguishable, 1.0 = perfectly distinguishable
    accuracy: float
    top_features: list[tuple[str, float]]  # (feature, importance)
    n_samples: int
    passed: bool  # True if AUC < threshold (default 0.75)

    @property
    def distinguishability_score(self) -> float:
        """0 = perfectly indistinguishable, 100 = perfectly distinguishable."""
        return round((self.auc_roc - 0.5) * 200, 2)


@dataclass
class TemporalProfile:
    """Temporal / sequence analysis for a datetime or sorted numeric column."""

    column: str
    mean_gap_seconds: float | None
    std_gap_seconds: float | None
    min_gap_seconds: float | None
    max_gap_seconds: float | None
    autocorrelation_lag1: float | None  # lag-1 autocorrelation of values
    autocorrelation_lag7: float | None  # lag-7 (weekly if daily data)
    gap_distribution: str | None  # best-fit name (exponential, normal, …)


@dataclass
class PeriodicityResult:
    """FFT-based periodicity detection result."""

    column: str
    dominant_period: float | None  # in samples
    dominant_frequency: float | None
    dominant_power: float | None
    top_periods: list[tuple[float, float]]  # (period_samples, power)
    is_periodic: bool  # True if dominant power > threshold


@dataclass
class AdvancedTableProfile:
    """Extended profile combining base stats with Tier 1 fidelity features."""

    table_name: str
    row_count: int

    gmm_fits: dict[str, GMMFit] = field(default_factory=dict)
    conditional_profiles: list[ConditionalProfile] = field(default_factory=list)
    adversarial: AdversarialResult | None = None
    temporal_profiles: dict[str, TemporalProfile] = field(default_factory=dict)
    periodicity: dict[str, PeriodicityResult] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# AdvancedProfiler
# ---------------------------------------------------------------------------


class AdvancedProfiler:
    """Runs Tier 1 fidelity profiling on a pair of DataFrames (real + synthetic).

    Usage::

        profiler = AdvancedProfiler()
        adv = profiler.profile_pair(real_df, synth_df, table_name="orders")
        print(f"AUC: {adv.adversarial.auc_roc:.3f}")
    """

    def __init__(
        self,
        max_gmm_components: int = 5,
        adversarial_threshold: float = 0.75,
        max_categorical_for_conditional: int = 20,
        max_rows_adversarial: int = 5000,
    ) -> None:
        self.max_gmm_components = max_gmm_components
        self.adversarial_threshold = adversarial_threshold
        self.max_categorical_for_conditional = max_categorical_for_conditional
        self.max_rows_adversarial = max_rows_adversarial

    # ---------------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------------

    def profile_pair(
        self,
        real: pd.DataFrame,
        synthetic: pd.DataFrame,
        table_name: str = "table",
    ) -> AdvancedTableProfile:
        """Profile real + synthetic DataFrames and return AdvancedTableProfile."""
        adv = AdvancedTableProfile(
            table_name=table_name,
            row_count=len(real),
        )
        adv.gmm_fits = self._fit_gmms(real)
        adv.conditional_profiles = self._compute_conditional_profiles(real)
        adv.adversarial = self._adversarial_test(real, synthetic)
        adv.temporal_profiles = self._temporal_profiles(real)
        adv.periodicity = self._periodicity_analysis(real)
        return adv

    def profile_single(
        self,
        df: pd.DataFrame,
        table_name: str = "table",
    ) -> AdvancedTableProfile:
        """Profile a single DataFrame (no adversarial test — needs both real+synth)."""
        adv = AdvancedTableProfile(table_name=table_name, row_count=len(df))
        adv.gmm_fits = self._fit_gmms(df)
        adv.conditional_profiles = self._compute_conditional_profiles(df)
        adv.temporal_profiles = self._temporal_profiles(df)
        adv.periodicity = self._periodicity_analysis(df)
        return adv

    # ---------------------------------------------------------------------------
    # GMM fitting
    # ---------------------------------------------------------------------------

    def _fit_gmms(self, df: pd.DataFrame) -> dict[str, GMMFit]:
        if not HAS_GMM:
            return {}
        results: dict[str, GMMFit] = {}
        for col in df.select_dtypes(include=[np.number]).columns:
            series = df[col].dropna()
            if len(series) < 20:
                continue
            values = series.values.reshape(-1, 1)
            best_bic = np.inf
            best_fit: GMMFit | None = None
            for n in range(1, self.max_gmm_components + 1):
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        gm = GaussianMixture(n_components=n, random_state=0, max_iter=200)
                        gm.fit(values)
                    bic = gm.bic(values)
                    if bic < best_bic:
                        best_bic = bic
                        best_fit = GMMFit(
                            column=col,
                            n_components=n,
                            means=[float(m[0]) for m in gm.means_],
                            weights=[float(w) for w in gm.weights_],
                            stds=[float(np.sqrt(c[0][0])) for c in gm.covariances_],
                            bic=float(bic),
                            aic=float(gm.aic(values)),
                        )
                except Exception:
                    continue
            if best_fit is not None:
                results[col] = best_fit
        return results

    # ---------------------------------------------------------------------------
    # Conditional profiles
    # ---------------------------------------------------------------------------

    def _compute_conditional_profiles(
        self, df: pd.DataFrame
    ) -> list[ConditionalProfile]:
        profiles: list[ConditionalProfile] = []
        numeric_cols = list(df.select_dtypes(include=[np.number]).columns)
        # Pick categorical columns with low cardinality as conditioning variables
        cond_cols = [
            c for c in df.columns
            if (
                not pd.api.types.is_numeric_dtype(df[c])
                and not pd.api.types.is_datetime64_any_dtype(df[c])
            )
        ]
        cond_cols = [
            c for c in cond_cols
            if df[c].nunique() <= self.max_categorical_for_conditional
        ][:5]  # limit to 5 conditioning cols

        for cond_col in cond_cols:
            for num_col in numeric_cols[:10]:  # limit to 10 numeric cols
                if cond_col == num_col:
                    continue
                stats_by_value: dict[str, dict[str, float]] = {}
                for value, group in df.groupby(cond_col, observed=True):
                    numeric_group = group[num_col].dropna()
                    if len(numeric_group) < 5:
                        continue
                    stats_by_value[str(value)] = {
                        "mean": float(numeric_group.mean()),
                        "std": float(numeric_group.std()),
                        "count": float(len(numeric_group)),
                        "p25": float(numeric_group.quantile(0.25)),
                        "p75": float(numeric_group.quantile(0.75)),
                    }
                if stats_by_value:
                    profiles.append(ConditionalProfile(
                        primary_col=num_col,
                        conditioned_on=cond_col,
                        stats_by_value=stats_by_value,
                    ))
        return profiles

    # ---------------------------------------------------------------------------
    # Adversarial distinguishability test
    # ---------------------------------------------------------------------------

    def _adversarial_test(
        self, real: pd.DataFrame, synthetic: pd.DataFrame
    ) -> AdversarialResult | None:
        if not HAS_SKLEARN:
            return None

        # Prepare feature matrix
        try:
            real_features = self._encode_for_adversarial(real)
            synth_features = self._encode_for_adversarial(synthetic)

            # Align columns
            common_cols = list(set(real_features.columns) & set(synth_features.columns))
            if not common_cols:
                return None
            real_features = real_features[common_cols]
            synth_features = synth_features[common_cols]

            # Sample to cap size
            max_each = self.max_rows_adversarial // 2
            if len(real_features) > max_each:
                real_features = real_features.sample(max_each, random_state=0)
            if len(synth_features) > max_each:
                synth_features = synth_features.sample(max_each, random_state=0)

            X = pd.concat([real_features, synth_features], ignore_index=True).fillna(0)
            y = np.array([1] * len(real_features) + [0] * len(synth_features))

            if len(X) < 20:
                return None

            clf = GradientBoostingClassifier(n_estimators=50, max_depth=3, random_state=0)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                auc_scores = cross_val_score(clf, X, y, cv=3, scoring="roc_auc")
                acc_scores = cross_val_score(clf, X, y, cv=3, scoring="accuracy")

            clf.fit(X, y)
            importances = clf.feature_importances_
            top_features = sorted(
                zip(common_cols, importances), key=lambda x: x[1], reverse=True
            )[:10]

            mean_auc = float(np.mean(auc_scores))
            mean_acc = float(np.mean(acc_scores))

            return AdversarialResult(
                auc_roc=mean_auc,
                accuracy=mean_acc,
                top_features=[(str(f), float(i)) for f, i in top_features],
                n_samples=len(X),
                passed=mean_auc < self.adversarial_threshold,
            )
        except Exception:
            return None

    def _encode_for_adversarial(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode DataFrame for use as classifier features."""
        result: dict[str, Any] = {}
        for col in df.columns:
            series = df[col]
            if pd.api.types.is_numeric_dtype(series):
                result[col] = series.fillna(series.median() if not series.empty else 0)
            elif pd.api.types.is_datetime64_any_dtype(series):
                result[col] = series.astype(np.int64) // 10**9
            else:
                try:
                    le = LabelEncoder()
                    non_null = series.fillna("__NULL__").astype(str)
                    result[col] = le.fit_transform(non_null)
                except Exception:
                    continue
        return pd.DataFrame(result)

    # ---------------------------------------------------------------------------
    # Temporal profiles
    # ---------------------------------------------------------------------------

    def _temporal_profiles(self, df: pd.DataFrame) -> dict[str, TemporalProfile]:
        profiles: dict[str, TemporalProfile] = {}
        dt_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
        for col in dt_cols:
            series = df[col].dropna().sort_values()
            if len(series) < 10:
                continue
            gaps = series.diff().dropna().dt.total_seconds()
            if len(gaps) == 0:
                continue

            autocorr_1 = None
            autocorr_7 = None
            numeric_series = series.astype(np.int64) // 10**9  # epoch seconds
            if len(numeric_series) > 2:
                try:
                    autocorr_1 = float(numeric_series.autocorr(lag=1))
                except Exception:
                    pass
            if len(numeric_series) > 8:
                try:
                    autocorr_7 = float(numeric_series.autocorr(lag=7))
                except Exception:
                    pass

            gap_dist = None
            if HAS_SCIPY and len(gaps) >= 20:
                try:
                    _, p_exp = sp_stats.kstest(
                        gaps[gaps > 0], "expon",
                        args=(float(gaps[gaps > 0].min()), float(gaps[gaps > 0].mean())),
                    )
                    _, p_norm = sp_stats.kstest(
                        gaps, "norm",
                        args=(float(gaps.mean()), float(gaps.std())),
                    )
                    gap_dist = "exponential" if p_exp > p_norm else "normal"
                except Exception:
                    pass

            profiles[col] = TemporalProfile(
                column=col,
                mean_gap_seconds=float(gaps.mean()),
                std_gap_seconds=float(gaps.std()),
                min_gap_seconds=float(gaps.min()),
                max_gap_seconds=float(gaps.max()),
                autocorrelation_lag1=autocorr_1,
                autocorrelation_lag7=autocorr_7,
                gap_distribution=gap_dist,
            )
        return profiles

    # ---------------------------------------------------------------------------
    # FFT periodicity detection
    # ---------------------------------------------------------------------------

    def _periodicity_analysis(self, df: pd.DataFrame) -> dict[str, PeriodicityResult]:
        if not HAS_SCIPY:
            return {}
        results: dict[str, PeriodicityResult] = {}
        numeric_cols = list(df.select_dtypes(include=[np.number]).columns)
        for col in numeric_cols[:10]:
            series = df[col].dropna()
            if len(series) < 32:
                continue
            try:
                # Detrend by subtracting mean
                values = series.values - series.mean()
                yf = np.abs(rfft(values))
                xf = rfftfreq(len(values), d=1.0)

                # Exclude DC component (index 0)
                yf_no_dc = yf[1:]
                xf_no_dc = xf[1:]
                if len(yf_no_dc) == 0:
                    continue

                # Top 5 periods by power
                top_idx = np.argsort(yf_no_dc)[::-1][:5]
                top_periods = [
                    (float(1.0 / xf_no_dc[i]) if xf_no_dc[i] > 0 else 0.0,
                     float(yf_no_dc[i]))
                    for i in top_idx
                ]

                dominant_idx = top_idx[0]
                dominant_freq = float(xf_no_dc[dominant_idx])
                dominant_period = float(1.0 / dominant_freq) if dominant_freq > 0 else None
                dominant_power = float(yf_no_dc[dominant_idx])

                # Periodicity threshold: dominant power > 5× median power
                median_power = float(np.median(yf_no_dc))
                is_periodic = dominant_power > 5 * median_power if median_power > 0 else False

                results[col] = PeriodicityResult(
                    column=col,
                    dominant_period=dominant_period,
                    dominant_frequency=dominant_freq,
                    dominant_power=dominant_power,
                    top_periods=top_periods,
                    is_periodic=is_periodic,
                )
            except Exception:
                continue
        return results
