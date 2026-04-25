# Composite Presets

Pre-configured multi-domain combinations with shared entity mappings. Use a preset name instead of manually specifying domains and shared entities.

## Quick Start

```bash
# List available presets
spindle presets

# Generate from a preset
spindle composite enterprise --scale small --output ./data/ --format parquet
```

```python
from sqllocks_spindle.presets import get_preset, list_presets
from sqllocks_spindle.domains.composite import CompositeDomain
from sqllocks_spindle import Spindle, RetailDomain, HrDomain, FinancialDomain

# List all presets
for p in list_presets():
    print(f"{p.name}: {p.description} ({', '.join(p.domains)})")

# Load and use a preset
preset = get_preset("enterprise")
composite = CompositeDomain(
    domains=[RetailDomain(), HrDomain(), FinancialDomain()],
    shared_entities=preset.shared_entities,
)
result = Spindle().generate(domain=composite, scale="small", seed=42)
```

## Built-In Presets

| Preset | Domains | Shared Entities | Use Case |
| --- | --- | --- | --- |
| **enterprise** | retail, hr, financial | PERSON: hr.employee → retail.customer, financial.account | Full enterprise (sales + people + finance) |
| **healthcare_system** | healthcare, insurance, hr | PERSON: hr.employee primary | Hospital/clinic with insurance and payroll |
| **smart_factory** | manufacturing, iot, supply_chain | Auto-registry defaults | Factory automation and logistics |
| **digital_commerce** | retail, marketing, financial | Auto-registry defaults | E-commerce and digital marketing |
| **campus** | education, hr | Auto-registry defaults | University with HR |
| **telecom_bundle** | telecom, marketing, financial | Auto-registry defaults | Telecom provider with marketing and billing |

## CLI Reference

### List Presets

```bash
spindle presets
```

### Generate from Preset

```bash
spindle composite PRESET_NAME [OPTIONS]
```

| Option | Default | Description |
| --- | --- | --- |
| `PRESET_NAME` | — | Preset name or ad-hoc `domain+domain+domain` (required) |
| `--scale, -s` | `small` | Scale preset |
| `--seed` | `42` | Random seed |
| `--output, -o` | — | Output directory |
| `--format` | `summary` | Output format: `summary`, `csv`, `parquet`, `jsonl` |

### Ad-Hoc Combinations

Combine any domains without a preset:

```bash
# Ad-hoc: uses auto-registry for shared entity resolution
spindle composite retail+healthcare+iot --scale small --output ./data/
```

## How Shared Entities Work

When a preset specifies shared entities, the `SharedEntityRegistry` creates cross-domain FK relationships:

- The **primary** entity (e.g., `hr.employee`) is generated first
- **Linked** entities in other domains get FK columns referencing the primary's PK
- Tables are prefixed with domain name to avoid collisions: `hr_employee`, `retail_customer`, `financial_account`

Presets with "Auto-registry defaults" use the built-in mapping for 4 shared concepts: PERSON, LOCATION, ORGANIZATION, and CALENDAR.

## Custom Presets

Register your own preset programmatically:

```python
from sqllocks_spindle.presets import PresetDef
from sqllocks_spindle.presets.registry import PresetRegistry

registry = PresetRegistry()
registry.register(PresetDef(
    name="my_preset",
    description="Custom combo for testing",
    domains=["retail", "healthcare"],
    shared_entities={
        "person": {
            "primary": "healthcare.patient",
            "links": {"retail": "customer.patient_id"},
        },
    },
))
```
