from importlib import import_module
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from mongoeco._types import DeleteResult, UpdateResult
    from mongoeco._types.aliases import Document
    from mongoeco.api.operations import UpdateOperation
    from mongoeco.core.operation_context import (
        ChangePublicationPolicy,
        OperationContext,
    )
    from mongoeco.core.search_execution import SearchRequest
    from mongoeco.core.search_models import (
        SearchCollectorPlan,
        SearchCountResult,
        SearchDegradation,
        SearchExecutionMetric,
        SearchExecutionMode,
        SearchExecutionOutcome,
        SearchExecutionPhase,
        SearchExecutionState,
        SearchExecutionTrace,
        SearchExplainVerbosity,
        SearchFacetBucket,
        SearchFacetDefinition,
        SearchFacetResult,
        SearchHighlightPassage,
        SearchHighlightSegment,
        SearchHighlightSpan,
        SearchHit,
        SearchMetadata,
        SearchMetricAvailability,
        SearchMetricDomain,
        SearchMetricExactness,
        SearchMetricName,
        SearchMetricOrigin,
    )
    from mongoeco.engines.base import AsyncStorageEngine
    from mongoeco.engines.capabilities import (
        EngineCapabilities,
        SearchEngineCapabilities,
    )
    from mongoeco.engines.memory import MemoryEngine
    from mongoeco.engines.results import (
        BulkOutcome,
        CommittedChange,
        DeleteOutcome,
        InsertOutcome,
        MergeOutcome,
        MutationOutcome,
    )
    from mongoeco.engines.semantic_core import EngineFindSemantics
    from mongoeco.engines.snapshots import (
        ReadSnapshot,
        SnapshotLifecycle,
        SnapshotMetadata,
        SnapshotPolicy,
    )
    from mongoeco.engines.sqlite import SQLiteEngine


__all__ = [
    "AsyncStorageEngine",
    "BulkOutcome",
    "ChangePublicationPolicy",
    "CommittedChange",
    "DeleteOutcome",
    "DeleteResult",
    "Document",
    "EngineCapabilities",
    "EngineFindSemantics",
    "InsertOutcome",
    "MemoryEngine",
    "MergeOutcome",
    "MutationOutcome",
    "OperationContext",
    "ReadSnapshot",
    "SQLiteEngine",
    "SearchCollectorPlan",
    "SearchCountResult",
    "SearchDegradation",
    "SearchEngineCapabilities",
    "SearchExecutionMetric",
    "SearchExecutionMode",
    "SearchExecutionOutcome",
    "SearchExecutionPhase",
    "SearchExecutionState",
    "SearchExecutionTrace",
    "SearchExplainVerbosity",
    "SearchFacetBucket",
    "SearchFacetDefinition",
    "SearchFacetResult",
    "SearchHighlightPassage",
    "SearchHighlightSegment",
    "SearchHighlightSpan",
    "SearchHit",
    "SearchMetadata",
    "SearchMetricAvailability",
    "SearchMetricDomain",
    "SearchMetricExactness",
    "SearchMetricName",
    "SearchMetricOrigin",
    "SearchRequest",
    "SnapshotLifecycle",
    "SnapshotMetadata",
    "SnapshotPolicy",
    "UpdateOperation",
    "UpdateResult",
]

_EXPORT_MODULES = {
    "AsyncStorageEngine": "mongoeco.engines.base",
    "BulkOutcome": "mongoeco.engines.results",
    "ChangePublicationPolicy": "mongoeco.core.operation_context",
    "CommittedChange": "mongoeco.engines.results",
    "DeleteOutcome": "mongoeco.engines.results",
    "DeleteResult": "mongoeco.types",
    "Document": "mongoeco.types",
    "EngineCapabilities": "mongoeco.engines.capabilities",
    "EngineFindSemantics": "mongoeco.engines.semantic_core",
    "SearchEngineCapabilities": "mongoeco.engines.capabilities",
    "SearchCountResult": "mongoeco.core.search_models",
    "SearchCollectorPlan": "mongoeco.core.search_models",
    "SearchExecutionMode": "mongoeco.core.search_models",
    "SearchExecutionMetric": "mongoeco.core.search_models",
    "SearchExecutionOutcome": "mongoeco.core.search_models",
    "SearchExecutionPhase": "mongoeco.core.search_models",
    "SearchExecutionState": "mongoeco.core.search_models",
    "SearchExecutionTrace": "mongoeco.core.search_models",
    "SearchExplainVerbosity": "mongoeco.core.search_models",
    "SearchFacetDefinition": "mongoeco.core.search_models",
    "SearchFacetBucket": "mongoeco.core.search_models",
    "SearchFacetResult": "mongoeco.core.search_models",
    "SearchHighlightPassage": "mongoeco.core.search_models",
    "SearchHighlightSegment": "mongoeco.core.search_models",
    "SearchHighlightSpan": "mongoeco.core.search_models",
    "SearchHit": "mongoeco.core.search_models",
    "SearchMetadata": "mongoeco.core.search_models",
    "SearchMetricAvailability": "mongoeco.core.search_models",
    "SearchMetricDomain": "mongoeco.core.search_models",
    "SearchMetricExactness": "mongoeco.core.search_models",
    "SearchMetricName": "mongoeco.core.search_models",
    "SearchMetricOrigin": "mongoeco.core.search_models",
    "SearchDegradation": "mongoeco.core.search_models",
    "SearchRequest": "mongoeco.core.search_execution",
    "InsertOutcome": "mongoeco.engines.results",
    "MemoryEngine": "mongoeco.engines.memory",
    "MergeOutcome": "mongoeco.engines.results",
    "MutationOutcome": "mongoeco.engines.results",
    "OperationContext": "mongoeco.core.operation_context",
    "ReadSnapshot": "mongoeco.engines.snapshots",
    "SnapshotMetadata": "mongoeco.engines.snapshots",
    "SnapshotLifecycle": "mongoeco.engines.snapshots",
    "SnapshotPolicy": "mongoeco.engines.snapshots",
    "SQLiteEngine": "mongoeco.engines.sqlite",
    "UpdateOperation": "mongoeco.api.operations",
    "UpdateResult": "mongoeco.types",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
