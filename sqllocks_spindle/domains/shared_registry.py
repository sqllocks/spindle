"""Shared entity registry for cross-domain composition.

Defines canonical shared concepts (PERSON, LOCATION, ORGANIZATION, CALENDAR)
and knows which table in each domain represents each concept. Provides
methods to build cross-domain FK relationships and determine generation
order for composite schemas.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from sqllocks_spindle.schema.parser import RelationshipDef


class SharedConcept(str, Enum):
    """Canonical shared entity concepts that span multiple domains."""

    PERSON = "person"
    LOCATION = "location"
    ORGANIZATION = "organization"
    CALENDAR = "calendar"


@dataclass
class DomainEntityMapping:
    """Maps a shared concept to a specific table and PK column within a domain.

    Attributes:
        domain: Domain name (e.g., "retail", "hr").
        table: Table name within that domain (e.g., "customer", "employee").
        pk_column: Primary key column of that table (e.g., "customer_id").
        person_name_columns: Optional columns holding person name fields
            (for PERSON concept — enables identity correlation).
    """

    domain: str
    table: str
    pk_column: str
    person_name_columns: dict[str, str] = field(default_factory=dict)


# ─── Default mappings for all 12 domains ──────────────────────────────────

_DEFAULT_PERSON_MAPPINGS: list[DomainEntityMapping] = [
    DomainEntityMapping(
        domain="retail",
        table="customer",
        pk_column="customer_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="hr",
        table="employee",
        pk_column="employee_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="financial",
        table="customer",
        pk_column="customer_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="healthcare",
        table="patient",
        pk_column="patient_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="insurance",
        table="policyholder",
        pk_column="policyholder_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="education",
        table="student",
        pk_column="student_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="marketing",
        table="contact",
        pk_column="contact_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="telecom",
        table="subscriber",
        pk_column="subscriber_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
    DomainEntityMapping(
        domain="real_estate",
        table="agent",
        pk_column="agent_id",
        person_name_columns={"first_name": "first_name", "last_name": "last_name"},
    ),
]

_DEFAULT_LOCATION_MAPPINGS: list[DomainEntityMapping] = [
    DomainEntityMapping(domain="retail", table="store", pk_column="store_id"),
    DomainEntityMapping(domain="hr", table="department", pk_column="department_id"),
    DomainEntityMapping(domain="financial", table="branch", pk_column="branch_id"),
    DomainEntityMapping(domain="healthcare", table="facility", pk_column="facility_id"),
    DomainEntityMapping(domain="manufacturing", table="production_line", pk_column="production_line_id"),
    DomainEntityMapping(domain="iot", table="location", pk_column="location_id"),
    DomainEntityMapping(domain="supply_chain", table="warehouse", pk_column="warehouse_id"),
    DomainEntityMapping(domain="real_estate", table="neighborhood", pk_column="neighborhood_id"),
]

_DEFAULT_ORGANIZATION_MAPPINGS: list[DomainEntityMapping] = [
    DomainEntityMapping(domain="hr", table="department", pk_column="department_id"),
    DomainEntityMapping(domain="financial", table="branch", pk_column="branch_id"),
    DomainEntityMapping(domain="healthcare", table="provider", pk_column="provider_id"),
    DomainEntityMapping(domain="manufacturing", table="production_line", pk_column="production_line_id"),
    DomainEntityMapping(domain="education", table="department", pk_column="department_id"),
    DomainEntityMapping(domain="supply_chain", table="supplier", pk_column="supplier_id"),
    DomainEntityMapping(domain="insurance", table="agent", pk_column="agent_id"),
    DomainEntityMapping(domain="marketing", table="industry", pk_column="industry_id"),
    DomainEntityMapping(domain="capital_markets", table="company", pk_column="ticker"),
]

# Collect all defaults by concept
DEFAULT_MAPPINGS: dict[SharedConcept, list[DomainEntityMapping]] = {
    SharedConcept.PERSON: _DEFAULT_PERSON_MAPPINGS,
    SharedConcept.LOCATION: _DEFAULT_LOCATION_MAPPINGS,
    SharedConcept.ORGANIZATION: _DEFAULT_ORGANIZATION_MAPPINGS,
}


class SharedEntityRegistry:
    """Registry of shared entity concepts and their domain-specific mappings.

    Provides two key services for cross-domain composition:
    1. Building RelationshipDef objects that link shared entities across domains
    2. Computing the correct topological generation order (shared entities first)

    Example::

        registry = SharedEntityRegistry()

        # Get cross-domain relationships for retail + hr
        from sqllocks_spindle.domains.retail.retail import RetailDomain
        from sqllocks_spindle.domains.hr.hr import HrDomain
        domains = [RetailDomain(), HrDomain()]
        rels = registry.build_cross_domain_relationships(domains)

        # Get generation order
        order = registry.get_generation_order(domains)
    """

    def __init__(
        self,
        custom_mappings: dict[SharedConcept, list[DomainEntityMapping]] | None = None,
    ):
        self._mappings: dict[SharedConcept, list[DomainEntityMapping]] = {}
        # Start with defaults
        for concept, mappings in DEFAULT_MAPPINGS.items():
            self._mappings[concept] = list(mappings)
        # Override/extend with custom mappings
        if custom_mappings:
            for concept, mappings in custom_mappings.items():
                self._mappings[concept] = list(mappings)

    @property
    def concepts(self) -> list[SharedConcept]:
        """List all registered shared concepts."""
        return list(self._mappings.keys())

    def get_mappings(self, concept: SharedConcept) -> list[DomainEntityMapping]:
        """Get all domain mappings for a shared concept."""
        return self._mappings.get(concept, [])

    def get_mapping_for_domain(
        self,
        concept: SharedConcept,
        domain_name: str,
    ) -> DomainEntityMapping | None:
        """Get the mapping for a specific concept in a specific domain."""
        for mapping in self._mappings.get(concept, []):
            if mapping.domain == domain_name:
                return mapping
        return None

    def get_domains_for_concept(self, concept: SharedConcept) -> list[str]:
        """List all domain names that participate in a shared concept."""
        return [m.domain for m in self._mappings.get(concept, [])]

    def build_cross_domain_relationships(
        self,
        domains: list[Any],
        shared_entities: dict[str, dict[str, Any]] | None = None,
    ) -> list[RelationshipDef]:
        """Build FK RelationshipDef objects linking shared entities across domains.

        Args:
            domains: List of Domain instances participating in the composition.
            shared_entities: Optional explicit mapping overriding defaults.
                Format::

                    {
                        "person": {
                            "primary": "hr.employee",
                            "links": {
                                "retail": "customer.employee_id",
                                "financial": "account.holder_id",
                            },
                        },
                    }

        Returns:
            List of RelationshipDef objects for cross-domain FK references.
        """
        relationships: list[RelationshipDef] = []
        domain_names = {d.name for d in domains}

        if shared_entities:
            relationships.extend(
                self._build_from_explicit(shared_entities, domain_names)
            )
        else:
            relationships.extend(
                self._build_from_defaults(domain_names)
            )

        return relationships

    def _build_from_explicit(
        self,
        shared_entities: dict[str, dict[str, Any]],
        domain_names: set[str],
    ) -> list[RelationshipDef]:
        """Build relationships from explicit shared_entities config."""
        relationships: list[RelationshipDef] = []

        for concept_name, config in shared_entities.items():
            primary_ref = config.get("primary", "")
            links = config.get("links", {})

            if "." not in primary_ref:
                continue

            primary_domain, primary_table = primary_ref.split(".", 1)
            if primary_domain not in domain_names:
                continue

            # The primary table's PK is inferred as {table}_id
            primary_pk = f"{primary_table}_id"
            # Use prefixed name for the primary table in composite schema
            primary_prefixed = f"{primary_domain}_{primary_table}"

            for link_domain, link_spec in links.items():
                if link_domain not in domain_names:
                    continue

                if "." not in link_spec:
                    continue

                link_table, link_column = link_spec.split(".", 1)
                link_prefixed = f"{link_domain}_{link_table}"

                relationships.append(
                    RelationshipDef(
                        name=f"xdomain_{concept_name}_{primary_domain}_to_{link_domain}",
                        parent=primary_prefixed,
                        child=link_prefixed,
                        parent_columns=[primary_pk],
                        child_columns=[link_column],
                        type="one_to_many",
                    )
                )

        return relationships

    def _build_from_defaults(
        self,
        domain_names: set[str],
    ) -> list[RelationshipDef]:
        """Build relationships from default registry mappings.

        For each concept, the first domain in the participant list is treated
        as the primary (source of truth) and all others link to it.
        """
        relationships: list[RelationshipDef] = []

        for concept, mappings in self._mappings.items():
            # Filter to only domains participating in this composition
            active = [m for m in mappings if m.domain in domain_names]
            if len(active) < 2:
                continue

            primary = active[0]
            primary_prefixed = f"{primary.domain}_{primary.table}"

            for linked in active[1:]:
                linked_prefixed = f"{linked.domain}_{linked.table}"

                # Create a bridge FK column name
                bridge_fk = f"shared_{concept.value}_{primary.domain}_{primary.table}_id"

                relationships.append(
                    RelationshipDef(
                        name=f"xdomain_{concept.value}_{primary.domain}_to_{linked.domain}",
                        parent=primary_prefixed,
                        child=linked_prefixed,
                        parent_columns=[primary.pk_column],
                        child_columns=[bridge_fk],
                        type="one_to_many",
                        optional=True,
                    )
                )

        return relationships

    def get_generation_order(
        self,
        domains: list[Any],
        shared_entities: dict[str, dict[str, Any]] | None = None,
    ) -> list[str]:
        """Return a correct topological order: shared (primary) entities first.

        This provides a *hint* for ordering — the actual DependencyResolver in
        the Spindle engine will do the final sort based on FK references within
        the merged schema. This method ensures shared primary tables appear
        before their dependents.

        Args:
            domains: List of Domain instances.
            shared_entities: Optional explicit shared entity config.

        Returns:
            List of prefixed table names in recommended generation order.
        """
        domain_names = {d.name for d in domains}
        primary_tables: list[str] = []

        if shared_entities:
            for concept_name, config in shared_entities.items():
                primary_ref = config.get("primary", "")
                if "." in primary_ref:
                    domain, table = primary_ref.split(".", 1)
                    if domain in domain_names:
                        prefixed = f"{domain}_{table}"
                        if prefixed not in primary_tables:
                            primary_tables.append(prefixed)
        else:
            for concept, mappings in self._mappings.items():
                active = [m for m in mappings if m.domain in domain_names]
                if len(active) >= 2:
                    primary = active[0]
                    prefixed = f"{primary.domain}_{primary.table}"
                    if prefixed not in primary_tables:
                        primary_tables.append(prefixed)

        # Collect all remaining tables from each domain schema
        remaining: list[str] = []
        for domain in domains:
            schema = domain.get_schema()
            for table_name in schema.table_names:
                prefixed = f"{domain.name}_{table_name}"
                if prefixed not in primary_tables and prefixed not in remaining:
                    remaining.append(prefixed)

        return primary_tables + remaining
