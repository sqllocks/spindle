# MCP Server

Spindle includes an MCP (Model Context Protocol) bridge that exposes generation capabilities to AI assistants like Claude Code and Claude Desktop.

## Running the Bridge

```bash
python -m sqllocks_spindle.mcp_bridge
```

The bridge uses a JSON stdin/stdout protocol. Send a JSON object with `command` and `params`, receive a JSON response.

## Commands

### `list`

List all available domains with their profiles.

**Params:** none

```json
{"command": "list"}
```

**Returns:** `{version, domains: [{name, description, profiles}], count}`

### `describe`

Show a domain's full schema — tables, columns, relationships, generation order, and scale presets.

**Params:**

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `domain` | str | — | Domain name (required) |
| `mode` | str | `"3nf"` | Schema mode |

```json
{"command": "describe", "params": {"domain": "retail"}}
```

### `generate`

Generate synthetic data and return a summary with optional file output.

**Params:**

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `domain` | str | — | Domain name (required) |
| `scale` | str | `"small"` | Scale preset |
| `seed` | int | `42` | Random seed |
| `format` | str | `"summary"` | Output format |
| `output_dir` | str | — | Directory for file output |
| `mode` | str | `"3nf"` | Schema mode |
| `profile` | str | — | Named profile |

```json
{"command": "generate", "params": {"domain": "retail", "scale": "small", "format": "csv", "output_dir": "./output"}}
```

### `dry_run`

Preview what would be generated without actually generating.

**Params:** `domain`, `scale`, `mode`

```json
{"command": "dry_run", "params": {"domain": "healthcare", "scale": "medium"}}
```

**Returns:** `{domain, scale, generation_order, planned_rows, total_rows}`

### `validate`

Validate a `.spindle.json` schema file.

**Params:**

| Param | Type | Description |
| --- | --- | --- |
| `schema_path` | str | Path to schema file (required) |

```json
{"command": "validate", "params": {"schema_path": "my_schema.spindle.json"}}
```

### `preview`

Generate a small sample and return the first N rows per table.

**Params:**

| Param | Type | Default | Description |
| --- | --- | --- | --- |
| `domain` | str | — | Domain name (required) |
| `rows` | int | `5` | Rows to preview per table |
| `seed` | int | `42` | Random seed |
| `tables` | list | — | Specific tables (default: all) |
| `mode` | str | `"3nf"` | Schema mode |

```json
{"command": "preview", "params": {"domain": "retail", "rows": 3, "tables": ["customer", "order"]}}
```

### `profile_info`

Show a domain's distribution profile — all distribution keys, weights, and ratios.

**Params:** `domain`, `profile`, `mode`

```json
{"command": "profile_info", "params": {"domain": "healthcare"}}
```

**Returns:** `{domain, profile, available_profiles, distribution_keys, distributions, ratio_keys, ratios}`
