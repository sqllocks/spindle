# Audit 3.0.0 - Cross-Domain Integrity Report

Run after the B3.5 relationship-parser fix (audit memory id=4788). Each
domain was generated at `scale="small"` with `seed=42` and exercised through
`GenerationResult.verify_integrity()`.

| Domain          | Orphan FK count | Notes |
|-----------------|-----------------|-------|
| retail          | 0               | clean |
| healthcare      | 0               | clean |
| financial       | 0               | clean |
| supply_chain    | 0               | clean |
| iot             | 0               | clean |
| hr              | 0               | clean |
| insurance       | 0               | clean |
| marketing       | 0               | clean |
| education       | 0               | clean |
| real_estate     | 0               | clean |
| manufacturing   | 0               | clean |
| telecom         | 0               | clean |
| capital_markets | 0               | clean |

Pre-3.0.0 the parser dropped `parent_key`/`child_key` scalars on the floor for
10 of 14 domains, so `verify_integrity()` had no checks to run and would
return `[]` regardless of whether the data was actually consistent. Post fix,
the parser converts scalar `parent_key`/`child_key` to single-element
`parent_columns`/`child_columns` lists; the table above shows that under the
new check the existing domain definitions still hold up.

No regressions surfaced. No pre-existing orphans found.
