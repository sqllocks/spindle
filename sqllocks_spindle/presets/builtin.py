"""Built-in composite presets shipped with Spindle."""

from __future__ import annotations

from sqllocks_spindle.presets.registry import PresetDef, PresetRegistry


def build_default_registry() -> PresetRegistry:
    """Build the default preset registry with built-in presets."""
    registry = PresetRegistry()

    # Enterprise preset: retail + hr + financial
    registry.register(PresetDef(
        name="enterprise",
        description="Enterprise dataset combining retail, HR, and financial domains",
        domains=["retail", "hr", "financial"],
        shared_entities={
            "person": {
                "primary": "hr.employee",
                "links": {
                    "retail": "customer.customer_id",
                    "financial": "account.account_id",
                },
            },
        },
    ))

    # Healthcare system: healthcare + insurance + hr
    registry.register(PresetDef(
        name="healthcare_system",
        description="Healthcare system with insurance and HR",
        domains=["healthcare", "insurance", "hr"],
        shared_entities={
            "person": {
                "primary": "hr.employee",
                "links": {
                    "healthcare": "patient.patient_id",
                    "insurance": "policy.policy_id",
                },
            },
        },
    ))

    # Smart factory: manufacturing + iot + supply_chain
    registry.register(PresetDef(
        name="smart_factory",
        description="Smart factory combining manufacturing, IoT, and supply chain",
        domains=["manufacturing", "iot", "supply_chain"],
    ))

    # Digital commerce: retail + marketing + financial
    registry.register(PresetDef(
        name="digital_commerce",
        description="Digital commerce with retail, marketing, and financial data",
        domains=["retail", "marketing", "financial"],
    ))

    # Campus: education + hr
    registry.register(PresetDef(
        name="campus",
        description="University campus combining education and HR",
        domains=["education", "hr"],
    ))

    # Telecom bundle: telecom + marketing + financial
    registry.register(PresetDef(
        name="telecom_bundle",
        description="Telecom provider with marketing and billing",
        domains=["telecom", "marketing", "financial"],
    ))

    return registry


# Module-level default registry
_default_registry = build_default_registry()


def get_preset(name: str) -> PresetDef:
    """Get a built-in preset by name."""
    return _default_registry.get(name)


def list_presets() -> list[PresetDef]:
    """List all built-in presets."""
    return _default_registry.list()
