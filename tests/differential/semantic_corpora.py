"""Registration data shared by native capture and semantic replay consumers."""

from dataclasses import dataclass

from tests.differential.semantic_absence_consumer_cases import (
    SEED as ABSENCE_SEED,
    SEMANTIC_ABSENCE_CONSUMER_CASES,
)
from tests.differential.semantic_average_route_cases import (
    SEED as AVERAGE_ROUTE_SEED,
    SEMANTIC_AVERAGE_ROUTE_CASES,
)
from tests.differential.semantic_command_let_cases import (
    SEED as COMMAND_LET_SEED,
    SEMANTIC_COMMAND_LET_CASES,
)
from tests.differential.semantic_decimal_advanced_cases import (
    CONTROL_SEED as DECIMAL_CONTROL_SEED,
    EDGE_SEED as DECIMAL_ADVANCED_EDGE_SEED,
    GENERATED_SEED as DECIMAL_GENERATED_SEED,
    SEED as DECIMAL_ADVANCED_SEED,
    SEMANTIC_DECIMAL_ADVANCED_CASES,
    SEMANTIC_DECIMAL_ADVANCED_EDGE_CASES,
    SEMANTIC_DECIMAL_CONTROL_CASES,
    SEMANTIC_DECIMAL_GENERATED_CASES,
)
from tests.differential.semantic_decimal_cases import (
    EDGE_SEED as DECIMAL_EDGE_SEED,
    SEED as DECIMAL_SEED,
    SEMANTIC_DECIMAL_CASES,
    SEMANTIC_DECIMAL_EDGE_CASES,
)
from tests.differential.semantic_document_order_cases import (
    SEED as DOCUMENT_ORDER_SEED,
    SEMANTIC_DOCUMENT_ORDER_CASES,
)
from tests.differential.semantic_expression_grammar_cases import (
    SEED as EXPRESSION_GRAMMAR_SEED,
    SEMANTIC_EXPRESSION_GRAMMAR_CASES,
)
from tests.differential.semantic_group_cases import SEMANTIC_GROUP_CASES
from tests.differential.semantic_missing_cases import (
    SEED as MISSING_SEED,
    SEMANTIC_MISSING_CASES,
)
from tests.differential.semantic_numeric_cases import (
    SEED as NUMERIC_SEED,
    SEMANTIC_NUMERIC_CASES,
)
from tests.differential.semantic_numeric_predicate_cases import (
    SEED as NUMERIC_PREDICATE_SEED,
    SEMANTIC_NUMERIC_PREDICATE_CASES,
)
from tests.differential.semantic_object_grammar_cases import (
    SEED as OBJECT_GRAMMAR_SEED,
    SEMANTIC_OBJECT_GRAMMAR_CASES,
)
from tests.differential.semantic_overflow_cases import (
    NUMERIC_CONTROL_SEED,
    SEED as OVERFLOW_SEED,
    SEMANTIC_NUMERIC_CONTROL_CASES,
    SEMANTIC_OVERFLOW_CASES,
)
from tests.differential.semantic_overflow_write_cases import (
    SEMANTIC_OVERFLOW_WRITE_CASES,
    WRITE_SEED as OVERFLOW_WRITE_SEED,
)
from tests.differential.semantic_sum_cases import (
    SEED as SUM_SEED,
    SEMANTIC_SUM_CASES,
    SEMANTIC_SUM_STREAM_CASES,
    STREAM_SEED as SUM_STREAM_SEED,
)


@dataclass(frozen=True)
class SemanticCorpus:
    case_set: str
    module: str
    seed: list
    cases: tuple
    observation_encoding: str = "bson"


SEMANTIC_CORPORA = {
    "object_grammar": SemanticCorpus(
        "object-grammar",
        "semantic_object_grammar_cases",
        OBJECT_GRAMMAR_SEED,
        SEMANTIC_OBJECT_GRAMMAR_CASES,
    ),
    "command_let": SemanticCorpus(
        "command-let",
        "semantic_command_let_cases",
        COMMAND_LET_SEED,
        SEMANTIC_COMMAND_LET_CASES,
        observation_encoding="canonical-json",
    ),
    "expression_grammar": SemanticCorpus(
        "expression-grammar",
        "semantic_expression_grammar_cases",
        EXPRESSION_GRAMMAR_SEED,
        SEMANTIC_EXPRESSION_GRAMMAR_CASES,
    ),
    "overflow_numeric_controls": SemanticCorpus(
        "overflow-numeric-controls",
        "semantic_overflow_cases",
        NUMERIC_CONTROL_SEED,
        SEMANTIC_NUMERIC_CONTROL_CASES,
    ),
    "overflow": SemanticCorpus(
        "overflow-values",
        "semantic_overflow_cases",
        OVERFLOW_SEED,
        SEMANTIC_OVERFLOW_CASES,
    ),
    "overflow_writes": SemanticCorpus(
        "overflow-writes",
        "semantic_overflow_write_cases",
        OVERFLOW_WRITE_SEED,
        SEMANTIC_OVERFLOW_WRITE_CASES,
        observation_encoding="canonical-json",
    ),
    "average_routes": SemanticCorpus(
        "average-routes",
        "semantic_average_route_cases",
        AVERAGE_ROUTE_SEED,
        SEMANTIC_AVERAGE_ROUTE_CASES,
    ),
    "sum_stream": SemanticCorpus(
        "sum-stream",
        "semantic_sum_cases",
        SUM_STREAM_SEED,
        SEMANTIC_SUM_STREAM_CASES,
    ),
    "sum": SemanticCorpus(
        "sum-values", "semantic_sum_cases", SUM_SEED, SEMANTIC_SUM_CASES
    ),
    "decimal_controls": SemanticCorpus(
        "decimal-controls",
        "semantic_decimal_advanced_cases",
        DECIMAL_CONTROL_SEED,
        SEMANTIC_DECIMAL_CONTROL_CASES,
    ),
    "decimal_generated": SemanticCorpus(
        "decimal-generated",
        "semantic_decimal_advanced_cases",
        DECIMAL_GENERATED_SEED,
        SEMANTIC_DECIMAL_GENERATED_CASES,
    ),
    "decimal_advanced": SemanticCorpus(
        "decimal-advanced",
        "semantic_decimal_advanced_cases",
        DECIMAL_ADVANCED_SEED,
        SEMANTIC_DECIMAL_ADVANCED_CASES,
    ),
    "decimal_advanced_edges": SemanticCorpus(
        "decimal-advanced-edges",
        "semantic_decimal_advanced_cases",
        DECIMAL_ADVANCED_EDGE_SEED,
        SEMANTIC_DECIMAL_ADVANCED_EDGE_CASES,
    ),
    "decimal_edges": SemanticCorpus(
        "decimal-edges",
        "semantic_decimal_cases",
        DECIMAL_EDGE_SEED,
        SEMANTIC_DECIMAL_EDGE_CASES,
    ),
    "decimal": SemanticCorpus(
        "decimal-values",
        "semantic_decimal_cases",
        DECIMAL_SEED,
        SEMANTIC_DECIMAL_CASES,
    ),
    "document_order": SemanticCorpus(
        "document-order",
        "semantic_document_order_cases",
        DOCUMENT_ORDER_SEED,
        SEMANTIC_DOCUMENT_ORDER_CASES,
    ),
    "group_parity": SemanticCorpus(
        "semantic-fixes",
        "semantic_group_cases",
        [{"_id": 1}, {"_id": 2}],
        SEMANTIC_GROUP_CASES,
    ),
    "missing": SemanticCorpus(
        "missing-values",
        "semantic_missing_cases",
        MISSING_SEED,
        SEMANTIC_MISSING_CASES,
    ),
    "absence_consumers": SemanticCorpus(
        "absence-consumers",
        "semantic_absence_consumer_cases",
        ABSENCE_SEED,
        SEMANTIC_ABSENCE_CONSUMER_CASES,
    ),
    "numeric": SemanticCorpus(
        "numeric-values",
        "semantic_numeric_cases",
        NUMERIC_SEED,
        SEMANTIC_NUMERIC_CASES,
    ),
    "numeric_predicates": SemanticCorpus(
        "numeric-predicates",
        "semantic_numeric_predicate_cases",
        NUMERIC_PREDICATE_SEED,
        SEMANTIC_NUMERIC_PREDICATE_CASES,
    ),
}
