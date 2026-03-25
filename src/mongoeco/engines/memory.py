import asyncio
import datetime
import threading
import uuid
from copy import deepcopy
from typing import Any, AsyncIterable, override

from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.projections import apply_projection
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.query_plan import MatchAll, QueryNode, ensure_query_plan
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import DuplicateKeyError
from mongoeco.session import ClientSession
from mongoeco.types import DeleteResult, Document, DocumentId, Filter, Projection, SortSpec, Update, UpdateResult, ObjectId


class _AsyncThreadLock:
    """Lock compatible con hilos que no bloquea directamente el event loop al adquirir."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

    async def __aenter__(self) -> "_AsyncThreadLock":
        await asyncio.to_thread(self._lock.acquire)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        self._lock.release()
        return False


class MemoryEngine(AsyncStorageEngine):
    """Motor de almacenamiento en memoria ultra-rápido."""

    def __init__(self, codec: type[DocumentCodec] = DocumentCodec):
        self._storage: dict[str, dict[str, dict[Any, Any]]] = {}
        self._locks: dict[str, _AsyncThreadLock] = {}
        self._indexes: dict[str, dict[str, list[dict[str, object]]]] = {}
        self._meta_lock = threading.Lock()
        self._connection_count = 0
        self._codec = codec

    @override
    def create_session_state(self, session: ClientSession) -> None:
        session.bind_engine_state(
            f"memory:{id(self)}",
            {
                "connected": self._connection_count > 0,
            },
        )

    def _lock_key(self, db: str, coll: str) -> str:
        return f"{db}.{coll}"

    def _get_lock(self, db: str, coll: str) -> _AsyncThreadLock:
        key = f"{db}.{coll}"
        with self._meta_lock:
            return self._locks.setdefault(key, _AsyncThreadLock())

    def _storage_key(self, value: Any) -> Any:
        return self._typed_engine_key(value)

    def _typed_engine_key(self, value: Any) -> Any:
        if value is None:
            return ("none", None)
        if isinstance(value, bool):
            return ("bool", value)
        if isinstance(value, int):
            return ("int", value)
        if isinstance(value, float):
            return ("float", value)
        if isinstance(value, str):
            return ("str", value)
        if isinstance(value, bytes):
            return ("bytes", value)
        if isinstance(value, uuid.UUID):
            return ("uuid", value)
        if isinstance(value, ObjectId):
            return ("objectid", value)
        if isinstance(value, datetime.datetime):
            return ("datetime", value)
        if isinstance(value, dict):
            return ("dict", tuple((key, self._typed_engine_key(item)) for key, item in value.items()))
        if isinstance(value, list):
            return ("list", tuple(self._typed_engine_key(item) for item in value))
        try:
            hash(value)
            return (value.__class__, value)
        except TypeError:
            return ("repr", repr(value))

    def _index_value(self, document: Document, field: str) -> Any:
        values = QueryEngine.extract_values(document, field)
        if not values:
            return None

        primary = values[0]
        return self._typed_engine_key(primary)

    def _index_key(self, document: Document, fields: list[str]) -> tuple[Any, ...]:
        return tuple(self._index_value(document, field) for field in fields)

    def _ensure_unique_indexes(
        self,
        db_name: str,
        coll_name: str,
        candidate: Document,
        *,
        exclude_storage_key: Any | None = None,
    ) -> None:
        indexes = self._indexes.get(db_name, {}).get(coll_name, [])
        coll = self._storage.get(db_name, {}).get(coll_name, {})
        normalized_exclude = exclude_storage_key
        if exclude_storage_key is not None and exclude_storage_key not in coll:
            normalized_exclude = self._storage_key(exclude_storage_key)

        for index in indexes:
            if not index.get("unique"):
                continue

            fields = index["fields"]
            candidate_key = self._index_key(candidate, fields)
            for storage_key, data in coll.items():
                if normalized_exclude is not None and storage_key == normalized_exclude:
                    continue
                existing = self._codec.decode(data)
                existing_key = self._index_key(existing, fields)
                if existing_key == candidate_key:
                    raise DuplicateKeyError(
                        f"Duplicate key for unique index '{index['name']}': {fields}={candidate_key!r}"
                    )

    @override
    async def connect(self) -> None:
        with self._meta_lock:
            self._connection_count += 1

    @override
    async def disconnect(self) -> None:
        with self._meta_lock:
            if self._connection_count == 0:
                return
            self._connection_count -= 1
            if self._connection_count != 0:
                return
            self._storage.clear()
            self._indexes.clear()
            self._locks.clear()

    @override
    async def put_document(self, db_name: str, coll_name: str, document: Document, overwrite: bool = True, *, context: ClientSession | None = None) -> bool:
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                db = self._storage.setdefault(db_name, {})
                coll = db.setdefault(coll_name, {})

            doc_id = document.get("_id")
            storage_key = self._storage_key(doc_id)
            if not overwrite and storage_key in coll:
                return False

            self._ensure_unique_indexes(
                db_name,
                coll_name,
                document,
                exclude_storage_key=storage_key if overwrite else None,
            )

            coll[storage_key] = self._codec.encode(document)
            return True

    @override
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, context: ClientSession | None = None) -> Document | None:
        async with self._get_lock(db_name, coll_name):
            storage_key = self._storage_key(doc_id)
            data = self._storage.get(db_name, {}).get(coll_name, {}).get(storage_key)
        if data is None:
            return None
        return apply_projection(self._codec.decode(data), projection)

    @override
    async def delete_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, context: ClientSession | None = None) -> bool:
        async with self._get_lock(db_name, coll_name):
            coll = self._storage.get(db_name, {}).get(coll_name, {})
            storage_key = self._storage_key(doc_id)
            if storage_key in coll:
                del coll[storage_key]
                return True
            return False

    @override
    def scan_collection(self, db_name: str, coll_name: str, filter_spec: Filter | None = None, *, plan: QueryNode | None = None, projection: Projection | None = None, sort: SortSpec | None = None, skip: int = 0, limit: int | None = None, context: ClientSession | None = None) -> AsyncIterable[Document]:
        async def _scan():
            if skip < 0:
                raise ValueError("skip must be >= 0")
            if limit is not None and limit < 0:
                raise ValueError("limit must be >= 0")
            query_plan = ensure_query_plan(filter_spec, plan)

            async with self._get_lock(db_name, coll_name):
                coll = self._storage.get(db_name, {}).get(coll_name, {})
                documents = [
                    self._codec.decode(data)
                    for data in list(coll.values())
                ]

            if not isinstance(query_plan, MatchAll):
                documents = [document for document in documents if QueryEngine.match_plan(document, query_plan)]
            documents = sort_documents(documents, sort)

            if skip:
                documents = documents[skip:]
            if limit is not None:
                documents = documents[:limit]

            for document in documents:
                yield apply_projection(document, projection)
        return _scan()

    @override
    async def update_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, update_spec: Update, upsert: bool = False, upsert_seed: Document | None = None, *, plan: QueryNode | None = None, context: ClientSession | None = None) -> UpdateResult[DocumentId]:
        query_plan = ensure_query_plan(filter_spec, plan)
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                db = self._storage.setdefault(db_name, {})
                coll = db.setdefault(coll_name, {})

            for storage_key, data in list(coll.items()):
                document = self._codec.decode(data)
                if not QueryEngine.match_plan(document, query_plan):
                    continue

                modified = UpdateEngine.apply_update(document, update_spec)
                self._ensure_unique_indexes(
                    db_name,
                    coll_name,
                    document,
                    exclude_storage_key=storage_key,
                )
                coll[storage_key] = self._codec.encode(document)
                return UpdateResult(
                    matched_count=1,
                    modified_count=1 if modified else 0,
                )

            if not upsert:
                return UpdateResult(matched_count=0, modified_count=0)

            new_doc = deepcopy(upsert_seed or {})
            UpdateEngine.apply_update(new_doc, update_spec)
            if "_id" not in new_doc:
                new_doc["_id"] = ObjectId()

            storage_key = self._storage_key(new_doc["_id"])
            if storage_key in coll:
                raise DuplicateKeyError(f"Duplicate key: _id={new_doc['_id']}")

            self._ensure_unique_indexes(db_name, coll_name, new_doc)
            coll[storage_key] = self._codec.encode(new_doc)
            return UpdateResult(
                matched_count=0,
                modified_count=0,
                upserted_id=new_doc["_id"],
            )

    @override
    async def delete_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, context: ClientSession | None = None) -> DeleteResult:
        query_plan = ensure_query_plan(filter_spec, plan)
        async with self._get_lock(db_name, coll_name):
            coll = self._storage.get(db_name, {}).get(coll_name, {})
            for storage_key, data in list(coll.items()):
                document = self._codec.decode(data)
                if not QueryEngine.match_plan(document, query_plan):
                    continue
                del coll[storage_key]
                return DeleteResult(deleted_count=1)
            return DeleteResult(deleted_count=0)

    @override
    async def count_matching_documents(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, context: ClientSession | None = None) -> int:
        query_plan = ensure_query_plan(filter_spec, plan)
        async with self._get_lock(db_name, coll_name):
            coll = self._storage.get(db_name, {}).get(coll_name, {})
            return sum(
                1
                for data in coll.values()
                if QueryEngine.match_plan(self._codec.decode(data), query_plan)
            )

    @override
    async def create_index(self, db_name: str, coll_name: str, fields: list[str], *, unique: bool = False, name: str | None = None, context: ClientSession | None = None) -> str:
        index_name = name or "_".join(f"{field}_1" for field in fields)
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                db_indexes = self._indexes.setdefault(db_name, {})
                coll_indexes = db_indexes.setdefault(coll_name, [])

            for index in coll_indexes:
                if index["name"] == index_name:
                    if index["fields"] != fields or index["unique"] != unique:
                        raise DuplicateKeyError(
                            f"Conflicting index definition for '{index_name}'"
                        )
                    return index_name

            if unique:
                seen: set[tuple[Any, ...]] = set()
                coll = self._storage.get(db_name, {}).get(coll_name, {})
                for data in coll.values():
                    document = self._codec.decode(data)
                    key = self._index_key(document, fields)
                    if key in seen:
                        raise DuplicateKeyError(
                            f"Duplicate key for unique index '{index_name}': {fields}={key!r}"
                        )
                    seen.add(key)

            coll_indexes.append(
                {
                    "name": index_name,
                    "fields": fields.copy(),
                    "unique": unique,
                }
            )
        return index_name

    @override
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[dict[str, object]]:
        async with self._get_lock(db_name, coll_name):
            indexes = self._indexes.get(db_name, {}).get(coll_name, [])
        return deepcopy(indexes)

    @override
    async def list_databases(self) -> list[str]:
        with self._meta_lock:
            return list(self._storage.keys())

    @override
    async def list_collections(self, db_name: str) -> list[str]:
        with self._meta_lock:
            return list(self._storage.get(db_name, {}).keys())

    @override
    async def drop_collection(self, db_name: str, coll_name: str) -> None:
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                if db_name in self._storage and coll_name in self._storage[db_name]:
                    del self._storage[db_name][coll_name]
                    if not self._storage[db_name]:
                        del self._storage[db_name]
                if db_name in self._indexes and coll_name in self._indexes[db_name]:
                    del self._indexes[db_name][coll_name]
                    if not self._indexes[db_name]:
                        del self._indexes[db_name]
        with self._meta_lock:
            self._locks.pop(self._lock_key(db_name, coll_name), None)
