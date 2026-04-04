from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import sqlite3
import threading

from mongoeco.core.search import MaterializedSearchDocument
from mongoeco.engines._sqlite_vector_backend import SQLiteVectorBackendState
from mongoeco.types import EngineIndexRecord


@dataclass(slots=True)
class SQLiteRuntimeState:
    connection: sqlite3.Connection | None = None
    connection_count: int = 0
    transaction_owner_session_id: str | None = None
    scan_condition: threading.Condition = field(default_factory=threading.Condition)
    active_scan_count: int = 0
    thread_local: threading.local = field(default_factory=threading.local)
    executor: ThreadPoolExecutor | None = None
    owns_executor: bool = False
    fts5_available: bool | None = None


@dataclass(slots=True)
class SQLiteCacheState:
    index_cache: dict[tuple[str, str], tuple[int, list[EngineIndexRecord]]] = field(default_factory=dict)
    index_metadata_versions: dict[tuple[str, str], int] = field(default_factory=dict)
    collection_id_cache: dict[tuple[str, str], int] = field(default_factory=dict)
    collection_features_cache: dict[tuple[str, str, str], bool | str] = field(default_factory=dict)
    ensured_multikey_physical_indexes: set[str] = field(default_factory=set)
    ensured_search_backends: set[str] = field(default_factory=set)
    vector_search_backends: dict[tuple[str, str], SQLiteVectorBackendState] = field(default_factory=dict)
    search_backend_versions: dict[tuple[str, str], int] = field(default_factory=dict)
    materialized_search_entry_cache: dict[
        tuple[str, str, str, int],
        dict[str, tuple[tuple[tuple[str, str], ...], MaterializedSearchDocument]],
    ] = field(default_factory=dict)
