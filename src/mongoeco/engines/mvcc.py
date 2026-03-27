from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from mongoeco.types import Document, EngineIndexRecord


@dataclass(slots=True)
class MemoryMvccState:
    snapshot_version: int
    storage: dict[str, dict[str, dict[Any, Any]]]
    indexes: dict[str, dict[str, list[EngineIndexRecord]]]
    collections: dict[str, set[str]]
    collection_options: dict[str, dict[str, Document]]

    @classmethod
    def capture(
        cls,
        *,
        snapshot_version: int,
        storage: dict[str, dict[str, dict[Any, Any]]],
        indexes: dict[str, dict[str, list[EngineIndexRecord]]],
        collections: dict[str, set[str]],
        collection_options: dict[str, dict[str, Document]],
    ) -> "MemoryMvccState":
        return cls(
            snapshot_version=snapshot_version,
            storage=deepcopy(storage),
            indexes=deepcopy(indexes),
            collections=deepcopy(collections),
            collection_options=deepcopy(collection_options),
        )


@dataclass(frozen=True, slots=True)
class EngineMvccSnapshot:
    snapshot_version: int
    transaction_active: bool
    engine_mode: str
    metadata: dict[str, object] = field(default_factory=dict)
