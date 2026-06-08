# Spindle — Production-Confidence Engine Roadmap

> Backlog of **engine-layer** capabilities that convert Spindle's "PII-safe / high-fidelity"
> claims from marketing into **certifiable, per-run evidence**. Derived from the 2026-06-07
> production-confidence spiral (5 operator perspectives + engine archaeology). The
> control-plane **app** can mock the surfaces today; the items below are the real work that
> makes the surfaces *honest*.
>
> Source of truth for what the engine already computes: see "Engine ground truth" at the
> bottom. Memory: `spindle-production-confidence-feature-spiral-2026-06-07` (id 3704).

## Guiding principle
Confidence = **provable safety + provable fidelity + operational control**, where every claim
reduces to a per-run artifact. Do not surface a metric the engine cannot compute. The five
items below are exactly the gaps between what production operators demanded and what
`sqllocks_spindle` v2.14.0 actually produces.

---

## P0 — Output-vs-source leakage scan + DCR (makes "PII-safe" certifiable)
**Why:** The privacy officer's hard gate. Today the engine k-anonymizes the *profile*
(`safe_profile.py`, k=5/11, default-deny, SHA-256 hashing) but there is **no check that no real
record reproduces in the generated output**. Without it, "PII-safe" is not certifiable.

**Build:**
- `leakage_scan(real_df, synth_df, quasi_identifiers)` returning:
  - exact full-record match count (must be 0 on direct identifiers),
  - per-column exact-value overlap rate,
  - quasi-identifier *tuple* exact-match count (the {DOB, ZIP, sex} re-identification case),
  - **Distance-to-Closest-Record (DCR)** histogram: synth→real nearest-neighbour distances vs a real→real baseline (collapse to ~0 = memorization = leak),
  - output **k-anonymity** of the QI set: count of k=1 (unique) and k<5 (rare) combinations.
- Wire into a fail-closed **gate** (block run / quarantine on exact-match > 0 or k below floor).

**Plugs into:** new `inference/leakage.py`; called post-generation alongside `FidelityComparator`.
**Surfaces:** validation panel "No source PII in output" chip; certificate "Privacy Evidence".

## P0 — Auto-emit RunManifest from generate()
**Why:** The audit trail / exportable certificate the engineer AND privacy officer require.
`RunManifest` already exists (`manifests/run_manifest.py`: run_id, seed, engine_version,
per-table rows/cols/paths, validation gate map, timestamps, SBOM) — but it's **opt-in**, not
emitted by `Spindle.generate()`.

**Build:** have `generate()` (or a thin wrapper) always build + persist a `RunManifest`, extended
with the **per-column PII handling ledger** (synthesized / masked / k-anonymized / dropped,
from the SafeProfile redaction manifest) and the validation/leakage results. Add an immutable
write path (append-only, content-hash chained) for the audit use case.

**Plugs into:** `generator.py` (emit), `manifests/run_manifest.py` (extend schema).
**Surfaces:** Run Certificate export; Run History audit record.

## P1 — Constraint / business-rule violation check
**Why:** Consumer MUST-have. Synthetic rows that violate a CHECK the source never violated
(`ship_date >= order_date`, `status='cancelled' ⇒ amount=0`, `total = qty*price`, sum-to-100%)
are an automatic fail regardless of distributional fidelity. Not computed today.

**Build:** `constraint_check(synth_df, rules)` where rules are auto-inferred (from source:
monotonic pairs, functional dependencies, value-sum groups) + user-declared. Return per-rule
violation counts.
**Plugs into:** new `validation/constraints.py`; runs in the post-gen validation pass.
**Surfaces:** validation panel "Constraints" chip; fidelity failure-mode chips.

## P1 — TSTR (Train-Synthetic-Test-Real) utility score
**Why:** The data consumer's **#1 "I'll actually use it"** signal — measures the only thing that
matters (does a model learn the same thing from synthetic as from real). Distributional
fidelity is a proxy; TSTR is the outcome. Not computed today.

**Build:** `tstr(real_df, synth_df, target_col, task)` — train a quick model on synthetic,
evaluate on held-out real; compare to TRTR (train-real-test-real); report the gap. Plus the
privacy companion (nearest-neighbour / DCR from P0). Make it opt-in per dataset (it needs a
declared target/task).
**Plugs into:** new `inference/utility.py`.
**Surfaces:** Profile page "fitness for purpose" — "PASS for demo · WARN for ML (TSTR gap 9%)".

## P2 — Correlation-preservation scoring
**Why:** Correlations are *enforced* via `GaussianCopula.apply` (`engine/correlation.py`) but
**never scored** — there's no metric comparing real vs synthetic correlation matrices. The
consumer specifically wanted a correlation-diff heatmap. Until built, **do not display a
"correlation fidelity" number.**

**Build:** compare real vs synth Pearson (and add Spearman + a categorical-association measure)
matrices; emit a difference matrix + a scalar preservation score.
**Plugs into:** extend `inference/comparator.py` (`FidelityComparator`).
**Surfaces:** Profile "relationships" tab — source vs synth heatmaps + difference heatmap.

## P2 — Wasserstein / earth-mover distance (numeric)
**Why:** Consumer wanted it as the interpretable numeric headline (column's own units). **Not
implemented** (grep: zero matches). KS is present but weak on tails; Wasserstein complements it.
**Build:** add `scipy.stats.wasserstein_distance` per numeric column to `_compare_column`.
**Plugs into:** `inference/comparator.py:_compare_column`.
**Surfaces:** per-column fidelity drawer (secondary metric next to KS).

---

## Honesty guardrails (enforce in the app until the above ship)
- The inline `Spindle().generate(fidelity_profile=...)` score is **null_rate + cardinality only**
  (KS/chi² hardcoded `None`, `generator.py:534`). **Never** present it as "statistical fidelity."
  Use `FidelityComparator.compare(real, synth)` for real KS/chi-square/Jaccard.
- PII handling is **defense-in-depth, not a guarantee**; the masker has a documented
  credit-card-detection gap. Say "evidence-based PII reduction," never "guaranteed PII-free."
- Reproducibility ("same seed + schema → same data") **is** real and safe to claim
  (`generator.py:400`, per-table seed derivation).

## Suggested sequence
1. **P0 leakage scan + DCR** and **P0 auto-manifest** — together they unlock the
   certificate + the "PII-safe" gate (the two highest-trust artifacts).
2. **P1 constraint check** — cheap, high daily-use value, feeds the validation panel.
3. **P1 TSTR** — the ML-readiness verdict; biggest "will actually use" lever.
4. **P2 correlation scoring + Wasserstein** — deepen fidelity once the trust spine exists.

---

## Engine ground truth (verified 2026-06-07, `sqllocks_spindle` v2.14.0)
Already computes (app can surface honestly): per-column SafeProfile (dtype, null_rate,
cardinality, quantiles incl. p0.5/p99.5 tails, winsorized bounds, best-fit distribution,
k-anon category weights, hour/dow/temporal histograms, PII pattern, string-length, redaction
manifest + `unsafe` flag); per-table Pearson correlation matrix + PK + advisory FKs;
`GenerationResult` (row counts, elapsed, per-column lineage/strategy, `verify_integrity()` for
FK orphans); real `FidelityComparator` (KS+p numeric, chi-square+Jaccard categorical,
mean/std/null/cardinality deltas, 0-100 composite); `RunManifest` (run_id, seed, engine
version, SBOM, timestamps, validation gates — opt-in); seed-derived reproducibility.

Does **not** compute (do not claim): output-vs-source leakage scan, DCR, membership-inference,
output k-anonymity, constraint/business-rule checks, TSTR, correlation-preservation scoring,
Wasserstein distance.
