from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

from mongoeco.types import Document, EngineIndexRecord, SearchIndexDefinition


@dataclass(slots=True)
class MemoryMvccState:
    snapshot_version: int
    storage: dict[str, dict[str, dict[Any, Any]]]
    indexes: dict[str, dict[str, list[EngineIndexRecord]]]
    index_data: dict[str, dict[str, dict[str, dict[tuple[Any, ...], set[Any]]]]]
    search_indexes: dict[str, dict[str, list[SearchIndexDefinition]]]
    collections: dict[str, set[str]]
    collection_options: dict[str, dict[str, Document]]

    @classmethod
    def capture(
        cls,
        *,
        snapshot_version: int,
        storage: dict[str, dict[str, dict[Any, Any]]],
        indexes: dict[str, dict[str, list[EngineIndexRecord]]],
        index_data: dict[str, dict[str, dict[str, dict[tuple[Any, ...], set[Any]]]]],
        search_indexes: dict[str, dict[str, list[SearchIndexDefinition]]],
        collections: dict[str, set[str]],
        collection_options: dict[str, dict[str, Document]],
    ) -> "MemoryMvccState":
        # Optimizamos evitando deepcopy de todo el almacenamiento.
        # Solo copiamos las estructuras de contenedores (dict/list/set).
        # Los documentos individuales (valores en storage) no se copian
        # porque el motor los trata como inmutables al reemplazarlos siempre.
        return cls(
            snapshot_version=snapshot_version,
            storage={
                db: {coll: docs.copy() for coll, docs in db_colls.items()}
                for db, db_colls in storage.items()
            },
            indexes={
                db: {coll: idxs.copy() for coll, idxs in db_idxs.items()}
                for db, db_idxs in indexes.items()
            },
            index_data={
                db: {
                    coll: {
                        idx_name: {k: v.copy() for k, v in idx_map.items()}
                        for idx_name, idx_map in coll_idxs.items()
                    }
                    for coll, coll_idxs in db_colls.items()
                }
                for db, db_colls in index_data.items()
            },
            search_indexes={
                db: {coll: idxs.copy() for coll, idxs in db_idxs.items()}
                for db, db_idxs in search_indexes.items()
            },
            collections={db: colls.copy() for db, colls in collections.items()},
            collection_options={
                db: {coll: opts.copy() for coll, opts in db_opts.items()}
                for db, db_opts in collection_options.items()
            },
        )


@dataclass(frozen=True, slots=True)
class EngineMvccSnapshot:
    snapshot_version: int
    transaction_active: bool
    engine_mode: str
    metadata: dict[str, object] = field(default_factory=dict)
