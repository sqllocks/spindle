# Audit 3.0.0 Test Debt Inventory

Triage of hardcoded date ranges, sigma/std_dev key usage, reproducibility patterns,
and equals() comparisons found in tests/ before Release 3.0.0 lands.

## REBASELINE candidates (assertion needs adjustment under 3.0.0 semantics)

These tests assume default 2022-2025 dates because temporal start/end was previously
ignored (Tier 1 finding). Under 3.0.0 they MAY produce dates in the domain-declared
range. They still pass because they merely create test dataframes with literal date
ranges (no Spindle.generate call observed in the assertion path) OR they assert on
scoped subranges that domain ranges still satisfy.

- tests/test_safe_profile_temporal.py:27 — already CORRECT (asserts NOT 2022-25 era).
- tests/test_scd2_strategy.py:30 — uses explicit model_config date_range; the strategy
  honors the nested date_range key already; passes unchanged.
- tests/test_validation_gates.py:429, 433, 445, 467, 468, 484, 485 — fixture data,
  not generator output. Pass unchanged.
- All `pd.date_range("2024-...` literals in: test_anomaly.py, test_chaos_engine.py,
  test_e2e_chaos_integration.py, test_cdm_mapper.py, test_envelope.py, test_eventhouse_writer.py,
  test_financial_patterns.py, test_iot_patterns.py, test_multi_file_manifest.py, test_restatement.py,
  test_simulation_*.py, test_streaming.py — these are LITERAL fixtures, not generator output.
  Unaffected by B1. REBASELINE: none required.
- tests/test_e2e_simulation_systems.py:31-32 etc. — explicit date_range_start/end on simulation
  config (different surface area than B1). Unchanged.
- tests/test_fabric_paths.py, test_fabric_utils.py — path literals. Unchanged.
- tests/test_manifests.py:47 — run-id literal. Unchanged.

## REGRESSION candidates (existing behavior must keep working)

- tests/test_capital_markets.py:110 — same-seed reproducibility via .equals(). MUST still pass.
- tests/test_e2e_incremental.py:94, 104 — delta seed reproducibility. MUST still pass.
- tests/test_e2e_masking.py:96 — masker seed reproducibility. MUST still pass.
- tests/test_e2e_domain_sweep.py:147 — every domain same-seed reproducibility. MUST still pass.
- tests/test_correlation.py:59 — correlated seed reproducible. MUST still pass.
- tests/test_incremental.py:111, 121 — incremental seed reproducibility. MUST still pass.
- tests/test_masker.py:60, 110 — masker seed reproducibility. MUST still pass.
- tests/test_tier3_research.py:214, 227 — bootstrap reproducible. MUST still pass.
- tests/test_user_workflows.py:113 — workflow seed reproducibility. MUST still pass.
- tests/test_healthcare.py:232, test_retail.py:245 — masking seed change. MUST still pass.

## CONFLICT with B3 changes

- tests/test_time_travel.py:77 test_seed_reproducibility — compares snapshots[0] tables
  with .equals() across two independent generate() runs at same seed. Under B3, persisted
  per-table high-water-mark only affects the second-and-later snapshot. snapshot[0] is
  fresh (no prior state), so SAME SEED -> SAME OUTPUT. Test should pass as-is.
  ACTION: leave unchanged; if B3 makes it fail, add a single-run vs continue-run split.

## sigma/std_dev usage

- tests/test_audit_2_14_5.py:19,22,51 — asserts safe-profile-adapter emits sigma key
  for log_normal and std_dev for normal. These are FIXTURES of the desired schema and
  EXACTLY MATCH the B1 contract. PASS unchanged.
- tests/test_profile_store.py:45, test_strategies.py:106, test_smart_inference.py:507 —
  use nested params with sigma key. B1 changes are STRICTLY ADDITIVE (new aliases) so
  pre-existing nested params still win. PASS unchanged.
- tests/test_safe_profile_adapter.py:78 — asserts std_dev presence. B1 keeps std_dev as
  the canonical key in safe-profile-adapter; aliases are read in the strategy only.
  PASS unchanged.
- tests/test_safe_profile_winsorized_bounds.py:236 — numpy rng call. Unrelated.

## Reproducibility patterns

All `.equals()` comparisons of generator output across two same-seed runs in a single
process MUST keep passing. B2 strengthens single-process determinism (copula seed,
stable hash) so these become MORE reproducible, not less.

The new B2 test must verify CROSS-SUBPROCESS determinism, which is the actual finding.

## Verdict

No tests require code-level rebaselining ahead of B1-B4. The audit test-debt is
empirical: a single behavioral validation script (under tests/) exercises domain
output and confirms dates land in the declared range. That script is added in B5.
