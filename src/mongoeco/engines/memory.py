import asyncio
import threading
from copy import deepcopy
from functools import cmp_to_key
from typing import Any, AsyncIterable, override

from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.core.identity import canonical_document_id
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.projections import apply_projection
from mongoeco.core.codec import DocumentCodec
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
            "memory",
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
        return canonical_document_id(value)

    def _index_value(self, document: Document, field: str) -> Any:
        values = QueryEngine.extract_values(document, field)
        if not values:
            return None

        primary = values[0]
        if isinstance(primary, list):
            return canonical_document_id(primary)
        return canonical_document_id(primary)

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

        for index in indexes:
            if not index.get("unique"):
                continue

            fields = index["fields"]
            candidate_key = self._index_key(candidate, fields)
            for storage_key, data in coll.items():
                if exclude_storage_key is not None and storage_key == exclude_storage_key:
                    continue
                existing = self._codec.decode(data)
                existing_key = self._index_key(existing, fields)
                if existing_key == candidate_key:
                    raise DuplicateKeyError(
                        f"Duplicate key for unique index '{index['name']}': {fields}={candidate_key!r}"
                    )

    @staticmethod
    def _sort_value(document: Document, field: str, direction: int) -> Any:
        values = QueryEngine.extract_values(document, field)
        if not values:
            return None

        primary = values[0]
        if isinstance(primary, list):
            if not primary:
                return []
            members = values[1:] or primary
            ordered = sorted(members, key=cmp_to_key(BSONComparator.compare))
            return ordered[0] if direction == 1 else ordered[-1]
        return primary

    @classmethod
    def _compare_documents(cls, left: Document, right: Document, sort: SortSpec) -> int:
        for field, direction in sort:
            result = BSONComparator.compare(
                cls._sort_value(left, field, direction),
                cls._sort_value(right, field, direction),
            )
            if result != 0:
                return result if direction == 1 else -result
        return 0

    @classmethod
    def _sort_documents(cls, documents: list[Document], sort: SortSpec | None) -> list[Document]:
        if not sort:
            return documents
        return sorted(documents, key=cmp_to_key(lambda left, right: cls._compare_documents(left, right, sort)))

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
        if not data:
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
    def scan_collection(self, db_name: str, coll_name: str, filter_spec: Filter | None = None, *, projection: Projection | None = None, sort: SortSpec | None = None, skip: int = 0, limit: int | None = None, context: ClientSession | None = None) -> AsyncIterable[Document]:
        async def _scan():
            if skip < 0:
                raise ValueError("skip must be >= 0")
            if limit is not None and limit < 0:
                raise ValueError("limit must be >= 0")

            async with self._get_lock(db_name, coll_name):
                coll = self._storage.get(db_name, {}).get(coll_name, {})
                documents = [
                    self._codec.decode(data)
                    for data in list(coll.values())
                ]

            if filter_spec is not None:
                documents = [document for document in documents if QueryEngine.match(document, filter_spec)]
            documents = self._sort_documents(documents, sort)

            if skip:
                documents = documents[skip:]
            if limit is not None:
                documents = documents[:limit]

            for document in documents:
                yield apply_projection(document, projection)
        return _scan()

    @override
    async def update_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, update_spec: Update, upsert: bool = False, upsert_seed: Document | None = None, *, context: ClientSession | None = None) -> UpdateResult[DocumentId]:
        async with self._get_lock(db_name, coll_name):
            with self._meta_lock:
                db = self._storage.setdefault(db_name, {})
                coll = db.setdefault(coll_name, {})

            for storage_key, data in list(coll.items()):
                document = self._codec.decode(data)
                if not QueryEngine.match(document, filter_spec):
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
    async def delete_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, *, context: ClientSession | None = None) -> DeleteResult:
        async with self._get_lock(db_name, coll_name):
            coll = self._storage.get(db_name, {}).get(coll_name, {})
            for storage_key, data in list(coll.items()):
                document = self._codec.decode(data)
                if not QueryEngine.match(document, filter_spec):
                    continue
                del coll[storage_key]
                return DeleteResult(deleted_count=1)
            return DeleteResult(deleted_count=0)

    @override
    async def count_matching_documents(self, db_name: str, coll_name: str, filter_spec: Filter, *, context: ClientSession | None = None) -> int:
        async with self._get_lock(db_name, coll_name):
            coll = self._storage.get(db_name, {}).get(coll_name, {})
            return sum(
                1
                for data in coll.values()
                if QueryEngine.match(self._codec.decode(data), filter_spec)
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
