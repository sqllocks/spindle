"""Smart Schema Inference Engine.

Takes a basic DDL-parsed SpindleSchema and enhances it with realistic
generation strategies inferred from schema structure: table roles,
column semantics, FK distributions, temporal patterns, and business rules.

Usage:
    from sqllocks_spindle.schema import DdlParser
    from sqllocks_spindle.schema.inference import SchemaInferenceEngine

    schema = DdlParser().parse_file("my_tables.sql")
    smart_schema = SchemaInferenceEngine().infer(schema)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Protocol

from sqllocks_spindle.schema.parser import SpindleSchema


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TableRole(Enum):
    """Semantic role of a table in the schema."""
    ENTITY = auto()              # Person, customer, employee — core business objects
    TRANSACTION = auto()         # Orders, claims, invoices — events/transactions
    TRANSACTION_DETAIL = auto()  # Order lines, claim lines — child of a transaction
    LOOKUP = auto()              # Status codes, categories, types — small reference
    HIERARCHY = auto()           # Self-referencing tree (org chart, category tree)
    BRIDGE = auto()              # Junction/bridge table (many-to-many)
    LOG = auto()                 # Audit log, event log, history
    DIMENSION = auto()           # Star schema dimension (dim_ prefix)
    FACT = auto()                # Star schema fact (fact_ prefix)
    UNKNOWN = auto()             # Could not classify


class ColumnSemantic(Enum):
    """Semantic role of a column."""
    PRIMARY_KEY = auto()
    FOREIGN_KEY = auto()
    MONETARY = auto()            # price, cost, amount, salary, revenue
    QUANTITY = auto()            # quantity, qty, count, units
    PERCENTAGE = auto()          # _pct, _percent, _rate, _ratio
    MEASUREMENT = auto()         # weight, height, width, length, size
    RATING = auto()              # rating, score, stars
    STATUS = auto()              # status, state (on transaction/entity)
    CATEGORICAL = auto()         # type, category, kind, class, level, tier
    BOOLEAN_FLAG = auto()        # is_*, has_*, flag
    TEMPORAL_TRANSACTION = auto() # order_date, purchase_date on a transaction table
    TEMPORAL_AUDIT = auto()      # created_at, modified_at, updated_at
    TEMPORAL_START = auto()      # start_date, begin_date, effective_date
    TEMPORAL_END = auto()        # end_date, expiry_date, close_date, due_date
    TEMPORAL_BIRTH = auto()      # birth_date, dob, date_of_birth
    TEMPORAL_GENERIC = auto()    # generic date column
    CODE = auto()                # _code, _number, _no, _num (short string)
    IDENTIFIER = auto()          # _key, _ref, _token
    TEXT_DESCRIPTION = auto()    # description, comment, note, reason
    NAME = auto()                # *name* (not FK)
    EMAIL = auto()
    PHONE = auto()
    ADDRESS = auto()
    CITY = auto()
    STATE_CODE = auto()
    POSTAL_CODE = auto()
    COUNTRY = auto()
    URL = auto()
    UNKNOWN = auto()


# ---------------------------------------------------------------------------
# InferenceContext — accumulated state passed through the pipeline
# ---------------------------------------------------------------------------

@dataclass
class InferenceAnnotation:
    """Explanation of a single inference decision."""
    table: str
    column: str | None
    rule_id: str
    description: str
    confidence: float  # 0.0 to 1.0


@dataclass
class InferenceContext:
    """Accumulated analysis state passed through the analyzer pipeline."""
    schema: SpindleSchema
    table_roles: dict[str, TableRole] = field(default_factory=dict)
    column_semantics: dict[str, dict[str, ColumnSemantic]] = field(default_factory=dict)
    # Graph: parent -> [children], child -> [parents]
    children_of: dict[str, list[str]] = field(default_factory=dict)
    parents_of: dict[str, list[str]] = field(default_factory=dict)
    annotations: list[InferenceAnnotation] = field(default_factory=list)

    def annotate(self, table: str, column: str | None, rule_id: str,
                 description: str, confidence: float = 0.8) -> None:
        self.annotations.append(InferenceAnnotation(
            table=table, column=column, rule_id=rule_id,
            description=description, confidence=confidence,
        ))

    def build_graphs(self) -> None:
        """Build parent/child relationship graphs from schema."""
        self.children_of.clear()
        self.parents_of.clear()
        for rel in self.schema.relationships:
            self.children_of.setdefault(rel.parent, []).append(rel.child)
            self.parents_of.setdefault(rel.child, []).append(rel.parent)


# ---------------------------------------------------------------------------
# Analyzer protocol
# ---------------------------------------------------------------------------

class Analyzer(Protocol):
    """Interface for inference pipeline stages."""
    def analyze(self, ctx: InferenceContext) -> None: ...


# ---------------------------------------------------------------------------
# SchemaInferenceEngine — the main entry point
# ---------------------------------------------------------------------------

class SchemaInferenceEngine:
    """Enhance a basic DDL-parsed schema with smart inferences.

    Usage:
        engine = SchemaInferenceEngine()
        smart_schema = engine.infer(basic_schema)
    """

    def __init__(self, confidence_threshold: float = 0.5) -> None:
        self._threshold = confidence_threshold
        self._analyzers: list[Analyzer] = self._build_pipeline()

    def _build_pipeline(self) -> list[Analyzer]:
        # Import here to avoid circular imports
        from sqllocks_spindle.schema.inference.table_classifier import TableClassifier
        from sqllocks_spindle.schema.inference.column_classifier import ColumnClassifier
        from sqllocks_spindle.schema.inference.fk_distribution import FKDistributionInferrer
        from sqllocks_spindle.schema.inference.cardinality_inference import CardinalityInferrer
        from sqllocks_spindle.schema.inference.numeric_inference import NumericDistributionInferrer
        from sqllocks_spindle.schema.inference.enum_inference import EnumInferrer
        from sqllocks_spindle.schema.inference.temporal_inference import TemporalPatternInferrer
        from sqllocks_spindle.schema.inference.correlation_inference import CorrelationInferrer
        from sqllocks_spindle.schema.inference.business_rule_inference import BusinessRuleInferrer
        return [
            TableClassifier(),
            ColumnClassifier(),
            FKDistributionInferrer(),
            CardinalityInferrer(),
            NumericDistributionInferrer(),
            EnumInferrer(),
            TemporalPatternInferrer(),
            CorrelationInferrer(),
            BusinessRuleInferrer(),
        ]

    def infer(self, schema: SpindleSchema) -> SpindleSchema:
        """Run the full inference pipeline and return an enhanced schema."""
        ctx = InferenceContext(schema=schema)
        ctx.build_graphs()

        for analyzer in self._analyzers:
            analyzer.analyze(ctx)

        return ctx.schema

    def infer_with_report(self, schema: SpindleSchema) -> tuple[SpindleSchema, list[InferenceAnnotation]]:
        """Run inference and return both the schema and the explanation report."""
        ctx = InferenceContext(schema=schema)
        ctx.build_graphs()

        for analyzer in self._analyzers:
            analyzer.analyze(ctx)

        return ctx.schema, ctx.annotations
