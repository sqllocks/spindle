# Safe Profile: threat model and the honest privacy claim (ADR-011)

## The claim (use this wording, not "PII-free")

> Spindle's Safe Profile is **PII-free in safe mode (the default)**: the persisted
> profile artifact carries no raw individual values. It is the *recipe* for your
> data's shape, not the data.

Do NOT say "PII-free" unconditionally. Say "PII-free in safe mode". The
`--unsafe-full-fidelity` opt-out deliberately persists raw values and stamps the
artifact `unsafe=true`; such artifacts are NOT safe to share and `profile
validate --safe` rejects them.

## What the safe artifact persists (no raw individual values)

Per column: dtype, null_rate, cardinality, mean, std; for HIGH-cardinality
numerics quantiles + winsorized bounds; for LOW-cardinality numerics/datetimes a
coarse `categorical_histogram` (rounded edges, quantiles/bounds suppressed);
categorical mass as either literal keys (ONLY for proven safe string LABELS) or
sha256-hashed keys; pattern + length_dist for PII-gated columns; temporal
histograms. A self-describing `redaction_manifest` records what was suppressed.

## What it defends against (in scope)

Direct value disclosure to an honest-but-curious recipient who reads the
artifact: no raw value, extreme, rare category, DOB, salary, account number,
SSN, phone, ZIP+4, MRN, or free-text string appears, regardless of column name or
value format. Enforced by default-deny key routing (007a/b) at the data layer and
the `validate --safe` structural scanner (010) as a fail-closed backstop.

## What it does NOT defend against (out of scope, named honestly)

- **Membership-inference / distributional-overlap attacks** (arXiv 2512.06062):
  a synthetic generator necessarily approximates the real distribution.
- **Linkage / attribute disclosure** on combinations of quasi-identifiers held
  in aggregate (e.g. a coarse age-band histogram crossed with a region label).
- **In-process access** to the rich in-memory profile (`vars()`/`__dict__`),
  which is intentionally allowed (ADR-007 AC4); the threat model is the *shared
  artifact*, not code running inside your process.

The designed answer to the first two is an opt-in epsilon-differential-privacy
profile mode, deferred to a post-v1.0 ADR.

## The safety / fidelity tradeoff (measured, honest)

Safety and per-value fidelity trade off, because for categorical and low-card
columns the literal values ARE the fidelity. Removing them costs accuracy:

Measured round-trip fidelity (FidelityComparator, KS + chi-squared) on the retail
reference, via `tests/test_safe_profile_fidelity.py` (STORY-011):

- Safe mode (default, no literals): ~73/100.
- Unsafe mode (`--unsafe-full-fidelity`, raw values, not shareable): ~71/100.

The two are roughly EQUAL: the default-deny safety transforms (k-anon, coarse
histogram bucketing, hashed high-card keys) round-trip about as well as raw
literals, so on this data **safety is roughly fidelity-neutral, not a large
tradeoff**. The honest claim:

**"Ship the recipe, not the data" gives you a safe, version-controllable,
prod-shaped dev dataset at roughly three-quarters of full statistical fidelity,
and turning safety ON costs almost nothing here.** Do NOT claim ">= 90% fidelity"
for the round-trip (the ~88-92 figure is the in-memory profile, not the
save/load/regenerate cycle, which measures ~73).
