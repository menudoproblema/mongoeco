import asyncio
import datetime
import threading
import uuid
from copy import deepcopy
from typing import Any, AsyncIterable, override

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.projections import apply_projection
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.query_plan import MatchAll, QueryNode, ensure_query_plan
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    DeleteResult, Document, DocumentId, Filter, IndexKeySpec, ObjectId,
    Projection, SortSpec, Update, UpdateResult, default_index_name,
    default_id_index_definition, default_id_index_document, default_id_index_information, index_fields,
    IndexDefinition, normalize_index_keys,
)


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
        engine_key = f"memory:{id(self)}"
        session.bind_engine_state(
            engine_key,
            {
                "connected": self._connection_count > 0,
                "supports_transactions": False,
            },
        )
        session.register_transaction_hooks(engine_key)

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

    @staticmethod
    def _is_builtin_id_index(keys: IndexKeySpec) -> bool:
        return keys == [("_id", 1)]

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
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        effective_dialect = dialect or MONGODB_DIALECT_70
        async with self._get_lock(db_name, coll_name):
            storage_key = self._storage_key(doc_id)
            data = self._storage.get(db_name, {}).get(coll_name, {}).get(storage_key)
        if data is None:
            return None
        if effective_dialect is MONGODB_DIALECT_70:
            return apply_projection(self._codec.decode(data), projection)
        return apply_projection(
            self._codec.decode(data),
            projection,
            dialect=effective_dialect,
        )

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
    def scan_collection(self, db_name: str, coll_name: str, filter_spec: Filter | None = None, *, plan: QueryNode | None = None, projection: Projection | None = None, sort: SortSpec | None = None, skip: int = 0, limit: int | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> AsyncIterable[Document]:
        async def _scan():
            effective_dialect = dialect or MONGODB_DIALECT_70
            if skip < 0:
                raise ValueError("skip must be >= 0")
            if limit is not None and limit < 0:
                raise ValueError("limit must be >= 0")
            query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)

            async with self._get_lock(db_name, coll_name):
                coll = self._storage.get(db_name, {}).get(coll_name, {})
                documents = [
                    self._codec.decode(data)
                    for data in list(coll.values())
                ]

            if not isinstance(query_plan, MatchAll):
                documents = [
                    document
                    for document in documents
                    if QueryEngine.match_plan(document, query_plan, dialect=effective_dialect)
                ]
            if effective_dialect is MONGODB_DIALECT_70:
                documents = sort_documents(documents, sort)
            else:
                documents = sort_documents(documents, sort, dialect=effective_dialect)

            if skip:
                documents = documents[skip:]
            if limit is not None:
                documents = documents[:limit]

            for document in documents:
                if effective_dialect is MONGODB_DIALECT_70:
                    yield apply_projection(document, projection)
                else:
                    yield apply_projection(
                        document,
                        projection,
                        dialect=effective_dialect,
                    )
        return _scan()

    @override
    async def update_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, update_spec: Update, upsert: bool = False, upsert_seed: Document | None = None, *, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> UpdateResult[DocumentId]:
        effective_dialect = dialect or MONGODB_DIALECT_70
        query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                db = self._storage.setdefault(db_name, {})
                coll = db.setdefault(coll_name, {})

            for storage_key, data in list(coll.items()):
                document = self._codec.decode(data)
                if not QueryEngine.match_plan(document, query_plan, dialect=effective_dialect):
                    continue

                modified = UpdateEngine.apply_update(
                    document,
                    update_spec,
                    dialect=effective_dialect,
                )
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
            UpdateEngine.apply_update(new_doc, update_spec, dialect=effective_dialect)
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
    async def delete_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> DeleteResult:
        effective_dialect = dialect or MONGODB_DIALECT_70
        query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        async with self._get_lock(db_name, coll_name):
            coll = self._storage.get(db_name, {}).get(coll_name, {})
            for storage_key, data in list(coll.items()):
                document = self._codec.decode(data)
                if not QueryEngine.match_plan(document, query_plan, dialect=effective_dialect):
                    continue
                del coll[storage_key]
                return DeleteResult(deleted_count=1)
            return DeleteResult(deleted_count=0)

    @override
    async def count_matching_documents(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> int:
        effective_dialect = dialect or MONGODB_DIALECT_70
        query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        async with self._get_lock(db_name, coll_name):
            coll = self._storage.get(db_name, {}).get(coll_name, {})
            return sum(
                1
                for data in coll.values()
                if QueryEngine.match_plan(
                    self._codec.decode(data),
                    query_plan,
                    dialect=effective_dialect,
                )
            )

    @override
    async def create_index(self, db_name: str, coll_name: str, keys: IndexKeySpec, *, unique: bool = False, name: str | None = None, context: ClientSession | None = None) -> str:
        normalized_keys = normalize_index_keys(keys)
        fields = index_fields(normalized_keys)
        index_name = name or default_index_name(normalized_keys)
        if self._is_builtin_id_index(normalized_keys):
            if name not in (None, "_id_"):
                raise OperationFailure("Conflicting index definition for '_id_'")
            return "_id_"
        if index_name == "_id_":
            raise OperationFailure("Conflicting index definition for '_id_'")
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                db_indexes = self._indexes.setdefault(db_name, {})
                coll_indexes = db_indexes.setdefault(coll_name, [])

            for index in coll_indexes:
                if index["name"] == index_name:
                    if index["key"] != normalized_keys or index["unique"] != unique:
                        raise OperationFailure(
                            f"Conflicting index definition for '{index_name}'"
                        )
                    return index_name
                if index["key"] == normalized_keys:
                    if index["unique"] != unique:
                        raise OperationFailure(
                            f"Conflicting index definition for key pattern '{normalized_keys!r}'"
                        )
                    return index["name"]

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
                    "key": deepcopy(normalized_keys),
                    "unique": unique,
                }
            )
        return index_name

    @override
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[dict[str, object]]:
        async with self._get_lock(db_name, coll_name):
            indexes = self._indexes.get(db_name, {}).get(coll_name, [])
        result = [default_id_index_definition().to_list_document()]
        result.extend(
            IndexDefinition(
                deepcopy(index["key"]),
                name=str(index["name"]),
                unique=bool(index["unique"]),
            ).to_list_document()
            for index in deepcopy(indexes)
        )
        return result

    @override
    async def index_information(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> dict[str, dict[str, object]]:
        async with self._get_lock(db_name, coll_name):
            indexes = deepcopy(self._indexes.get(db_name, {}).get(coll_name, []))
        result = default_id_index_information()
        result.update(
            {
                str(index["name"]): IndexDefinition(
                    deepcopy(index["key"]),
                    name=str(index["name"]),
                    unique=bool(index["unique"]),
                ).to_information_entry()
                for index in indexes
            }
        )
        return result

    @override
    async def drop_index(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        *,
        context: ClientSession | None = None,
    ) -> None:
        target_name: str | None = None
        if isinstance(index_or_name, str):
            if index_or_name == "_id_":
                raise OperationFailure("cannot drop _id index")
            target_name = index_or_name
        else:
            normalized_keys = normalize_index_keys(index_or_name)
            if self._is_builtin_id_index(normalized_keys):
                raise OperationFailure("cannot drop _id index")
            target_name = default_index_name(normalized_keys)
        async with self._get_lock(db_name, coll_name):
            indexes = self._indexes.get(db_name, {}).get(coll_name, [])
            for idx, index in enumerate(indexes):
                if index["name"] == target_name:
                    del indexes[idx]
                    break
            else:
                if isinstance(index_or_name, str):
                    raise OperationFailure(f"index not found with name [{index_or_name}]")
                raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")

            if db_name in self._indexes and coll_name in self._indexes[db_name] and not self._indexes[db_name][coll_name]:
                del self._indexes[db_name][coll_name]
                if not self._indexes[db_name]:
                    del self._indexes[db_name]

    @override
    async def drop_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        async with self._get_lock(db_name, coll_name):
            if db_name in self._indexes and coll_name in self._indexes[db_name]:
                del self._indexes[db_name][coll_name]
                if not self._indexes[db_name]:
                    del self._indexes[db_name]

    @override
    async def explain_query_plan(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter | None = None,
        *,
        plan: QueryNode | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        effective_dialect = dialect or MONGODB_DIALECT_70
        query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        async with self._get_lock(db_name, coll_name):
            indexes = deepcopy(self._indexes.get(db_name, {}).get(coll_name, []))
        return {
            "engine": "memory",
            "strategy": "python",
            "plan": repr(query_plan),
            "sort": sort,
            "skip": skip,
            "limit": limit,
            "indexes": indexes,
        }

    @override
    async def list_databases(self) -> list[str]:
        with self._meta_lock:
            return sorted(set(self._storage.keys()) | set(self._indexes.keys()))

    @override
    async def list_collections(self, db_name: str) -> list[str]:
        with self._meta_lock:
            return sorted(
                set(self._storage.get(db_name, {}).keys())
                | set(self._indexes.get(db_name, {}).keys())
            )

    @override
    async def drop_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
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
