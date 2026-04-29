"""Tier 2 fidelity improvements.

Adds:
- FormatPreservationAnalyzer — detect and compare format patterns (email, phone, UUID, …)
- StringSimilarityAnalyzer   — n-gram cosine similarity between string value distributions
- CardinalityConstraintChecker — flag columns where synth cardinality diverges significantly
- AnomalyRateChecker          — verify _spindle_is_anomaly rates match expected fractions
- Tier2Report                 — composite result dataclass
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Format patterns
# ---------------------------------------------------------------------------

_FORMAT_PATTERNS: dict[str, re.Pattern] = {
    "email":    re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    "phone_us": re.compile(r"^\+?1?\s*[\-.]?\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}$"),
    "uuid":     re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I),
    "url":      re.compile(r"^https?://\S+"),
    "ipv4":     re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"),
    "zip_us":   re.compile(r"^\d{5}(-\d{4})?$"),
    "date_iso": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    "ssn_us":   re.compile(r"^\d{3}-\d{2}-\d{4}$"),
    "credit_card": re.compile(r"^\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}$"),
}


@dataclass
class FormatPreservationResult:
    """Format preservation metrics for a single string column."""

    column: str
    detected_format: str | None  # name of the dominant detected format
    real_format_rate: float  # fraction of real values matching detected format
    synth_format_rate: float  # fraction of synth values matching same format
    delta: float  # abs(real_format_rate - synth_format_rate)
    passed: bool  # True if delta < threshold


class FormatPreservationAnalyzer:
    """Detect format patterns in real data and check synth preserves them."""

    def __init__(self, threshold: float = 0.10, sample_size: int = 500) -> None:
        self.threshold = threshold
        self.sample_size = sample_size

    def analyze(
        self,
        real: pd.DataFrame,
        synthetic: pd.DataFrame,
    ) -> dict[str, FormatPreservationResult]:
        results: dict[str, FormatPreservationResult] = {}
        string_cols = [c for c in real.columns
                       if not pd.api.types.is_numeric_dtype(real[c])
                       and not pd.api.types.is_datetime64_any_dtype(real[c])
                       and c in synthetic.columns]
        for col in string_cols:
            real_sample = real[col].dropna().astype(str)
            synth_sample = synthetic[col].dropna().astype(str)
            if real_sample.empty or synth_sample.empty:
                continue
            if len(real_sample) > self.sample_size:
                real_sample = real_sample.sample(self.sample_size, random_state=0)
            if len(synth_sample) > self.sample_size:
                synth_sample = synth_sample.sample(self.sample_size, random_state=0)

            # Find dominant format in real data
            best_format: str | None = None
            best_rate: float = 0.0
            for fmt_name, pattern in _FORMAT_PATTERNS.items():
                rate = real_sample.str.match(pattern).mean()
                if rate > best_rate:
                    best_rate = float(rate)
                    best_format = fmt_name

            if best_format is None or best_rate < 0.5:
                continue  # no clear format detected

            synth_rate = float(synth_sample.str.match(_FORMAT_PATTERNS[best_format]).mean())
            delta = abs(best_rate - synth_rate)
            results[col] = FormatPreservationResult(
                column=col,
                detected_format=best_format,
                real_format_rate=best_rate,
                synth_format_rate=synth_rate,
                delta=delta,
                passed=delta <= self.threshold,
            )
        return results


# ---------------------------------------------------------------------------
# String similarity
# ---------------------------------------------------------------------------


@dataclass
class StringSimilarityResult:
    """Character n-gram cosine similarity between string column value distributions."""

    column: str
    ngram_n: int
    cosine_similarity: float  # 0-1, 1 = identical distributions
    score: float  # 0-100


def _char_ngrams(text: str, n: int) -> dict[str, int]:
    grams: dict[str, int] = {}
    for i in range(len(text) - n + 1):
        gram = text[i:i + n]
        grams[gram] = grams.get(gram, 0) + 1
    return grams


def _cosine(a: dict[str, int], b: dict[str, int]) -> float:
    keys = set(a) | set(b)
    if not keys:
        return 1.0
    va = np.array([a.get(k, 0) for k in keys], dtype=float)
    vb = np.array([b.get(k, 0) for k in keys], dtype=float)
    norm_a = np.linalg.norm(va)
    norm_b = np.linalg.norm(vb)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(va, vb) / (norm_a * norm_b))


class StringSimilarityAnalyzer:
    """Compute character n-gram cosine similarity between real and synth string columns."""

    def __init__(self, ngram_n: int = 3, sample_size: int = 1000) -> None:
        self.ngram_n = ngram_n
        self.sample_size = sample_size

    def analyze(
        self,
        real: pd.DataFrame,
        synthetic: pd.DataFrame,
    ) -> dict[str, StringSimilarityResult]:
        results: dict[str, StringSimilarityResult] = {}
        string_cols = [c for c in real.columns
                       if not pd.api.types.is_numeric_dtype(real[c])
                       and not pd.api.types.is_datetime64_any_dtype(real[c])
                       and c in synthetic.columns]
        for col in string_cols:
            real_vals = real[col].dropna().astype(str)
            synth_vals = synthetic[col].dropna().astype(str)
            if len(real_vals) < 10 or len(synth_vals) < 10:
                continue
            if len(real_vals) > self.sample_size:
                real_vals = real_vals.sample(self.sample_size, random_state=0)
            if len(synth_vals) > self.sample_size:
                synth_vals = synth_vals.sample(self.sample_size, random_state=0)

            real_text = " ".join(real_vals.values)
            synth_text = " ".join(synth_vals.values)
            real_grams = _char_ngrams(real_text, self.ngram_n)
            synth_grams = _char_ngrams(synth_text, self.ngram_n)
            sim = _cosine(real_grams, synth_grams)
            results[col] = StringSimilarityResult(
                column=col,
                ngram_n=self.ngram_n,
                cosine_similarity=sim,
                score=round(sim * 100, 2),
            )
        return results


# ---------------------------------------------------------------------------
# Cardinality constraint checker
# ---------------------------------------------------------------------------


@dataclass
class CardinalityConstraintResult:
    """Cardinality comparison for a single column."""

    column: str
    real_cardinality: int
    synth_cardinality: int
    ratio: float  # synth / real (1.0 = perfect)
    deviation: float  # abs(1 - ratio)
    passed: bool  # True if deviation < threshold


class CardinalityConstraintChecker:
    """Check that synthetic cardinality stays within tolerance of real cardinality."""

    def __init__(self, max_deviation: float = 0.20) -> None:
        self.max_deviation = max_deviation

    def analyze(
        self,
        real: pd.DataFrame,
        synthetic: pd.DataFrame,
    ) -> dict[str, CardinalityConstraintResult]:
        results: dict[str, CardinalityConstraintResult] = {}
        shared_cols = [c for c in real.columns if c in synthetic.columns
                       and not c.startswith("_spindle_")]
        for col in shared_cols:
            real_card = real[col].nunique()
            synth_card = synthetic[col].nunique()
            if real_card == 0:
                continue
            ratio = synth_card / real_card
            deviation = abs(1.0 - ratio)
            results[col] = CardinalityConstraintResult(
                column=col,
                real_cardinality=real_card,
                synth_cardinality=synth_card,
                ratio=round(ratio, 4),
                deviation=round(deviation, 4),
                passed=deviation <= self.max_deviation,
            )
        return results


# ---------------------------------------------------------------------------
# Anomaly rate checker
# ---------------------------------------------------------------------------


@dataclass
class AnomalyRateResult:
    """Checks whether the injected anomaly rate matches the registered anomaly fraction."""

    expected_fraction: float
    actual_fraction: float
    delta: float
    row_count: int
    anomaly_count: int
    passed: bool


def check_anomaly_rates(
    df: pd.DataFrame,
    expected_fractions: dict[str, float] | None = None,
    tolerance: float = 0.05,
) -> AnomalyRateResult | None:
    """Verify _spindle_is_anomaly rate in a DataFrame.

    Args:
        df: DataFrame produced by AnomalyRegistry.inject().
        expected_fractions: Optional mapping of anomaly_type -> expected fraction.
            If None, uses overall anomaly rate with expected = 0.0 (no anomalies).
        tolerance: Acceptable deviation from expected fraction.

    Returns:
        AnomalyRateResult or None if no anomaly columns present.
    """
    if "_spindle_is_anomaly" not in df.columns:
        return None

    row_count = len(df)
    anomaly_count = int(df["_spindle_is_anomaly"].sum())
    actual_fraction = anomaly_count / row_count if row_count > 0 else 0.0

    if expected_fractions:
        total_expected = sum(expected_fractions.values())
    else:
        total_expected = 0.0

    delta = abs(actual_fraction - total_expected)
    return AnomalyRateResult(
        expected_fraction=total_expected,
        actual_fraction=round(actual_fraction, 4),
        delta=round(delta, 4),
        row_count=row_count,
        anomaly_count=anomaly_count,
        passed=delta <= tolerance,
    )


# ---------------------------------------------------------------------------
# Composite Tier 2 Report
# ---------------------------------------------------------------------------


@dataclass
class Tier2Report:
    """Composite Tier 2 fidelity report."""

    format_preservation: dict[str, FormatPreservationResult] = field(default_factory=dict)
    string_similarity: dict[str, StringSimilarityResult] = field(default_factory=dict)
    cardinality: dict[str, CardinalityConstraintResult] = field(default_factory=dict)
    anomaly_rate: AnomalyRateResult | None = None

    def passing_rate(self) -> float:
        """Fraction of all checks that passed (0.0 - 1.0)."""
        checks: list[bool] = []
        for r in self.format_preservation.values():
            checks.append(r.passed)
        for r in self.cardinality.values():
            checks.append(r.passed)
        if self.anomaly_rate is not None:
            checks.append(self.anomaly_rate.passed)
        return sum(checks) / len(checks) if checks else 1.0

    def summary(self) -> str:
        lines = ["Tier 2 Fidelity Report", "=" * 50]
        lines.append(f"Passing rate: {self.passing_rate():.1%}")

        if self.format_preservation:
            lines.append("\nFormat Preservation:")
            for col, r in self.format_preservation.items():
                status = "PASS" if r.passed else "FAIL"
                lines.append(
                    f"  [{status}] {col}: {r.detected_format} "
                    f"real={r.real_format_rate:.2%} synth={r.synth_format_rate:.2%}"
                )
        if self.string_similarity:
            lines.append("\nString Similarity (trigram cosine):")
            for col, r in self.string_similarity.items():
                lines.append(f"  {col}: {r.cosine_similarity:.3f} ({r.score:.1f}/100)")

        if self.cardinality:
            lines.append("\nCardinality Constraints:")
            for col, r in self.cardinality.items():
                status = "PASS" if r.passed else "FAIL"
                lines.append(
                    f"  [{status}] {col}: real={r.real_cardinality} "
                    f"synth={r.synth_cardinality} ratio={r.ratio:.3f}"
                )
        if self.anomaly_rate:
            status = "PASS" if self.anomaly_rate.passed else "FAIL"
            lines.append(
                f"\nAnomaly Rate: [{status}] expected={self.anomaly_rate.expected_fraction:.2%} "
                f"actual={self.anomaly_rate.actual_fraction:.2%}"
            )
        return "\n".join(lines)


def run_tier2(
    real: pd.DataFrame,
    synthetic: pd.DataFrame,
    expected_anomaly_fractions: dict[str, float] | None = None,
) -> Tier2Report:
    """Run all Tier 2 checks and return a Tier2Report."""
    report = Tier2Report()
    report.format_preservation = FormatPreservationAnalyzer().analyze(real, synthetic)
    report.string_similarity = StringSimilarityAnalyzer().analyze(real, synthetic)
    report.cardinality = CardinalityConstraintChecker().analyze(real, synthetic)
    report.anomaly_rate = check_anomaly_rates(synthetic, expected_anomaly_fractions)
    return report
