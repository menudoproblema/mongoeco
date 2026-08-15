from importlib import import_module


__all__ = [
    "AsyncStorageEngine",
    "BulkOutcome",
    "ChangePublicationPolicy",
    "CommittedChange",
    "DeleteOutcome",
    "EngineCapabilities",
    "InsertOutcome",
    "MemoryEngine",
    "MergeOutcome",
    "MutationOutcome",
    "OperationContext",
    "ReadSnapshot",
    "SQLiteEngine",
    "SearchCollectorPlan",
    "SearchCountResult",
    "SearchEngineCapabilities",
    "SearchExecutionMode",
    "SearchExecutionOutcome",
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
    "SearchRequest",
    "SnapshotLifecycle",
    "SnapshotMetadata",
    "SnapshotPolicy",
]

_EXPORT_MODULES = {
    "AsyncStorageEngine": "mongoeco.engines.base",
    "BulkOutcome": "mongoeco.engines.results",
    "ChangePublicationPolicy": "mongoeco.core.operation_context",
    "CommittedChange": "mongoeco.engines.results",
    "DeleteOutcome": "mongoeco.engines.results",
    "EngineCapabilities": "mongoeco.engines.capabilities",
    "SearchEngineCapabilities": "mongoeco.engines.capabilities",
    "SearchCountResult": "mongoeco.core.search_models",
    "SearchCollectorPlan": "mongoeco.core.search_models",
    "SearchExecutionMode": "mongoeco.core.search_models",
    "SearchExecutionOutcome": "mongoeco.core.search_models",
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
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
