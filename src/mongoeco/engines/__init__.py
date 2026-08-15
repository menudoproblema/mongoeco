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
    "SnapshotMetadata",
    "SnapshotLifecycle",
    "SnapshotPolicy",
    "SQLiteEngine",
]

_EXPORT_MODULES = {
    "AsyncStorageEngine": "mongoeco.engines.base",
    "BulkOutcome": "mongoeco.engines.results",
    "ChangePublicationPolicy": "mongoeco.core.operation_context",
    "CommittedChange": "mongoeco.engines.results",
    "DeleteOutcome": "mongoeco.engines.results",
    "EngineCapabilities": "mongoeco.engines.capabilities",
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
