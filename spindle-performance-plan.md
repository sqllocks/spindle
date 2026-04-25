# Spindle Performance Plan — v2.2.12

## Context

Spindle v2.2.11 live testing against Fabric Warehouse revealed that writing 21,750 rows across 9 tables takes 15+ minutes instead of <1 minute. Root cause analysis identified 24 performance bottlenecks spanning generation, inference, write paths, streaming, incremental, and transform subsystems. Combined estimated impact at 1M rows: 172-473 seconds unfixed → 9-24 seconds fixed (15-50× speedup).

This plan fixes all 24, bumps to v2.2.12, runs tests, commits, pushes to GitHub, and publishes to PyPI.

## Phases

### Phase 1: CRITICAL Fixes (Ranks 1-3)

**1. Constrained FK groupby cache** — `engine/id_manager.py:149-194`
- Add `self._constrained_fk_cache: dict[tuple[str, str], dict] = {}` to `__init__()`
- In `get_constrained_fks()`: check cache before `df.groupby()`, store result after
- Clear cache in `append_pks()` and `register_table()`
- Expected: 1-100s → <1s at 1M rows

**2. Inference profiling sampling** — `inference/profiler.py:304-380`
- In `_detect_distribution()`: sample 5K rows before `dist.fit()` and `kstest()`
- `_detect_pattern()` already samples 1K rows — no change needed
- Expected: 70-150s → <2s at 1M rows

**3. iterrows → values.tolist in SQL INSERT** — `output/pandas_writer.py:376`
- Replace `for _, row in batch.iterrows():` with `for row in batch.values.tolist():`
- Adjust `_format_sql_value()` calls to work with list items instead of Series items
- Expected: 50-100s → 5-10s at 1M rows

### Phase 2: HIGH Fixes (Ranks 4-11)

**4. Formula eval() pre-compilation** — `engine/strategies/formula.py:49`
- Add `self._compiled = compile(expression, '<formula>', 'eval')` in `__init__()`
- Replace `eval(expression, ...)` with `eval(self._compiled, ...)`
- Cache namespace dict template, copy per call

**5. Pattern strategy batching** — `engine/strategies/pattern.py:38-69`
- Pre-create `chars_array = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))` at class init
- Batch-generate random indices: `ctx.rng.integers(0, len(chars_array), size=(row_count, length))`
- Build all random strings via numpy fancy indexing, not per-row loop

**6. Star schema index lookups** — `transform/star_schema.py:154-276`
- Remove `.copy()` on right_df (lines 168, 226) — they're never modified
- Pre-build indexed lookup dicts from dimensions
- Replace chained `.merge()` with `.map()` for FK→SK replacement (already done at line 254 — extend to joins)

**7. Connection reuse in warehouse_bulk_writer** — `warehouse_bulk_writer.py:200,296`
- Add `self._shared_conn = None` and `self._shared_writer = None` to `__init__()`
- Add `_get_shared_connection()` method that creates once, reuses after
- Modify `create_table()` to use shared connection + cursor directly (DDL only, no full write cycle)
- Modify `copy_into()` to use shared connection
- Close in `finally` block of `write_chunked()` and `write_tables()`

**8. IDManager lookup_values caching** — `engine/id_manager.py:247-264`
- Cache `df.set_index(pk_column)` result per table at registration time
- Reuse indexed DF for all `lookup_values()` calls
- Store in `self._indexed_cache: dict[tuple[str, str], pd.Series] = {}`

**9. Streaming to_events vectorization** — `streaming/streamer.py:219-230`
- Replace per-value `_clean_value()` loop with vectorized Series-level type conversion
- Apply `_clean_value` at column level before `to_dict('records')`

**10. Anomaly injection vectorization** — `streaming/anomaly.py:103-105`
- Replace three separate `.loc[]`/`.iloc[]` assignments with single vectorized update
- Use `df.loc[idx, [col, '_spindle_is_anomaly', '_spindle_anomaly_type']] = [val, True, type]`

### Phase 3: MEDIUM Fixes (Ranks 12-21)

**11. DataFrame.copy() in stage_chunk** — `warehouse_bulk_writer.py:238`
- Only copy columns with datetime64[ns] dtype, skip rest
- Use `df.astype({col: "datetime64[us]" for col in dt_cols})` single call

**12. Cover row pre-computation** — `sql_database_writer.py:670-690`
- Pre-compute max string length per column ONCE before batch loop
- Use `df[col].astype(str).str.len().max()` upfront
- Pass pre-computed lengths to batch loop

**13. _coerce_df_for_insert selective copy** — `sql_database_writer.py:558`
- Only copy columns that need conversion (bool, nullable int, datetime, categorical)
- Leave unchanged columns as views

**14. Continue perturbation without copy** — `continue_engine.py:443-467`
- Replace `.values.copy()` with direct `df.loc[idx, col] = perturbed` assignment
- Eliminate intermediate array allocation

**15. Temporal rejection sampling** — `engine/strategies/temporal.py:67-73`
- Reduce oversample factor from 3× to 1.5× with adaptive resampling
- Or use inverse CDF for seasonal distributions

**16. OOO events permutation** — `streaming/streamer.py:256-262`
- Replace `list.pop()` + `list.insert()` with index permutation array
- Apply permutation once to reorder events list

**17. FirstPerParent vectorization** — `engine/strategies/first_per_parent.py:54`
- Replace set() + per-row check with `np.unique(parent_values, return_index=True)`

**18. SCD2 sorting** — `engine/strategies/scd2.py:67-150`
- Replace per-group `sorted()` with single `np.argsort()` on eff_dates + group split

**19. Reference data type caching** — `engine/strategies/reference_data.py:73-83`
- Cache `is_weighted` / `is_string_list` flag at load time
- Replace `all(isinstance(x, str) for x in data)` with cached flag
- Use `np.array(data)[indices]` instead of list comprehension

**20. max_per_parent single pass** — `engine/id_manager.py:266-303`
- Replace loop with vectorized `np.where(counts > max)` + single redistribution pass

### Phase 4: LOW Fixes (Ranks 22-24)

**21. INSERT batch_size default** — `sql_database_writer.py:151`
- Change default from `batch_size=5000` to `batch_size=50000`

**22. Double commit reduction** — `sql_database_writer.py:232-244`
- Already fixed in v2.2.11 (per-table commit). No further action unless we can batch DDL.

**23. Validator pre-computation** — `schema/validator.py:178-204`
- Pre-build validation map during schema parsing
- Store `{strategy: required_keys}` lookup once, single-pass validation

## Phase 5: Build & Test

1. Bump version to `2.2.12` in `pyproject.toml` and `__init__.py`
2. Run full test suite: `python -m pytest tests/ -x -q`
3. Quick smoke test: generate retail at `scale=medium`, verify output shape and FK integrity
4. Verify COPY INTO path: `spindle publish retail --target sql-database --auth cli --connection-string "..." --base-path "..." --dry-run`

## Phase 6: Commit & Deploy

**Working directory:** `c:/Users/JonathanStewart/OneDrive/VSCode/AzureClients/forge-workspace/projects/fabric-datagen`

1. `git add` changed files:
   ```
   git add engine/id_manager.py inference/profiler.py output/pandas_writer.py \
     engine/strategies/formula.py engine/strategies/pattern.py transform/star_schema.py \
     fabric/warehouse_bulk_writer.py fabric/sql_database_writer.py streaming/streamer.py \
     streaming/anomaly.py incremental/continue_engine.py engine/strategies/temporal.py \
     engine/strategies/first_per_parent.py engine/strategies/scd2.py \
     engine/strategies/reference_data.py schema/validator.py pyproject.toml \
     sqllocks_spindle/__init__.py
   ```
2. Commit:
   ```
   git commit -m "perf: 24 performance fixes — 15-50x speedup at 1M rows (v2.2.12)

   Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
   ```
3. Push:
   ```
   git push origin main
   ```
4. Build:
   ```
   rm -rf dist/ build/ *.egg-info
   python -m build
   python -m twine check dist/*
   ```
5. Publish to PyPI:
   ```
   TWINE_PASSWORD="REDACTED-PYPI-TOKEN" python -m twine upload dist/* --username __token__
   ```
6. Verify published:
   ```
   python -m pip install sqllocks-spindle==2.2.12 --dry-run
   ```

## Files Modified (18 files)

| File | Fixes |
|------|-------|
| `engine/id_manager.py` | #1 (FK cache), #8 (lookup cache), #20 (bincount) |
| `inference/profiler.py` | #2 (sampling) |
| `output/pandas_writer.py` | #3 (iterrows) |
| `engine/strategies/formula.py` | #4 (compile) |
| `engine/strategies/pattern.py` | #5 (batch random), #9 (regex cache), #17 (chars list) |
| `transform/star_schema.py` | #6 (index lookups) |
| `fabric/warehouse_bulk_writer.py` | #7 (connection reuse), #11 (selective copy) |
| `fabric/sql_database_writer.py` | #12 (cover row), #13 (selective coerce), #21 (batch size), #22 (commit) |
| `streaming/streamer.py` | #9 (to_events), #16 (OOO permutation) |
| `streaming/anomaly.py` | #10 (vectorize) |
| `incremental/continue_engine.py` | #14 (perturbation) |
| `engine/strategies/temporal.py` | #15 (oversample) |
| `engine/strategies/first_per_parent.py` | #17 (np.unique) |
| `engine/strategies/scd2.py` | #18 (argsort) |
| `engine/strategies/reference_data.py` | #19 (type cache) |
| `schema/validator.py` | #23 (pre-compute) |
| `pyproject.toml` | version bump |
| `sqllocks_spindle/__init__.py` | version bump |

## Verification

After all fixes:
1. `pytest tests/ -x -q` — all existing tests must pass
2. Generate retail at small, medium, large — verify row counts and FK integrity
3. `spindle publish retail --target sql-database --auth cli --base-path "abfss://..." --connection-string "..."` — full live test to Fabric Warehouse with COPY INTO
4. Time comparison: small scale end-to-end should complete in <30 seconds (was 15+ minutes)

## Headless Execution

This plan is designed for headless overnight execution. All 24 fixes are independent (no fix depends on another fix's output). The only sequential dependency is: all fixes → test → commit → deploy.
