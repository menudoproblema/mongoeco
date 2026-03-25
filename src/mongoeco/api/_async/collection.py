from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api._async.cursor import AsyncCursor
from mongoeco.core.aggregation import Pipeline
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import compile_filter
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.core.upserts import seed_upsert_document
from mongoeco.core.validation import is_document, is_filter, is_projection, is_update
from mongoeco.session import ClientSession
from mongoeco.types import (
    ObjectId, Document, DocumentId, Filter, Update, Projection, InsertOneResult, UpdateResult, DeleteResult, SortSpec
)
from mongoeco.errors import DuplicateKeyError


class AsyncCollection:
    """Representa una colección de MongoDB."""

    def __init__(self, engine: AsyncStorageEngine, db_name: str, collection_name: str):
        self._engine = engine
        self._db_name = db_name
        self._collection_name = collection_name

    @staticmethod
    def _require_document(document: object) -> Document:
        if not is_document(document):
            raise TypeError("document must be a dict")
        return document

    @staticmethod
    def _normalize_filter(filter_spec: object | None) -> Filter:
        if filter_spec is None:
            return {}
        if not is_filter(filter_spec):
            raise TypeError("filter_spec must be a dict")
        return filter_spec

    @staticmethod
    def _normalize_projection(projection: object | None) -> Projection | None:
        if projection is None:
            return None
        if not is_projection(projection):
            raise TypeError("projection must be a dict")
        return projection

    @staticmethod
    def _require_update(update_spec: object) -> Update:
        if not is_update(update_spec):
            raise TypeError("update_spec must be a dict")
        if not update_spec:
            raise ValueError("update_spec must not be empty")
        if not all(isinstance(key, str) and key.startswith("$") for key in update_spec):
            raise ValueError("update_spec must contain only update operators")
        for operator, params in update_spec.items():
            if not is_document(params):
                raise TypeError(f"{operator} value must be a dict")
        return update_spec

    @staticmethod
    def _can_use_direct_id_lookup(filter_spec: Filter) -> bool:
        if len(filter_spec) != 1 or "_id" not in filter_spec:
            return False

        id_selector = filter_spec["_id"]
        return not (
            isinstance(id_selector, dict)
            and any(isinstance(key, str) and key.startswith("$") for key in id_selector)
        )

    async def insert_one(self, document: Document, *, session: ClientSession | None = None) -> InsertOneResult[DocumentId]:
        doc = self._require_document(document).copy()
        if "_id" not in doc:
            doc["_id"] = ObjectId()
        
        success = await self._engine.put_document(
            self._db_name, self._collection_name, doc, overwrite=False, context=session
        )
        if not success:
            raise DuplicateKeyError(f"Duplicate key: _id={doc['_id']}")
            
        return InsertOneResult(inserted_id=doc["_id"])

    async def find_one(self, filter_spec: Filter | None = None, projection: Projection | None = None, *, session: ClientSession | None = None) -> Document | None:
        filter_spec = self._normalize_filter(filter_spec)
        projection = self._normalize_projection(projection)
        doc = None
        
        if self._can_use_direct_id_lookup(filter_spec):
            doc = await self._engine.get_document(
                self._db_name,
                self._collection_name,
                filter_spec["_id"],
                projection=projection,
                context=session,
            )
        else:
            plan = compile_filter(filter_spec)
            async for d in self._engine.scan_collection(
                self._db_name,
                self._collection_name,
                filter_spec,
                plan=plan,
                projection=projection,
                context=session,
            ):
                doc = d
                break

        return apply_projection(doc, projection) if doc is not None else None

    def find(
        self,
        filter_spec: Filter | None = None,
        projection: Projection | None = None,
        *,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        session: ClientSession | None = None,
    ):
        filter_spec = self._normalize_filter(filter_spec)
        projection = self._normalize_projection(projection)
        plan = compile_filter(filter_spec)
        return AsyncCursor(
            self,
            filter_spec,
            plan,
            projection,
            sort=sort,
            skip=skip,
            limit=limit,
            session=session,
        )

    def aggregate(self, pipeline: Pipeline, *, session: ClientSession | None = None) -> AsyncAggregationCursor:
        if not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")
        return AsyncAggregationCursor(self, pipeline, session=session)

    async def update_one(self, filter_spec: Filter, update_spec: Update, upsert: bool = False, *, session: ClientSession | None = None) -> UpdateResult[DocumentId]:
        filter_spec = self._normalize_filter(filter_spec)
        update_spec = self._require_update(update_spec)
        plan = compile_filter(filter_spec)
        upsert_seed = None
        if upsert:
            upsert_seed = {}
            seed_upsert_document(upsert_seed, filter_spec)

        return await self._engine.update_matching_document(
            self._db_name,
            self._collection_name,
            filter_spec,
            update_spec,
            upsert=upsert,
            upsert_seed=upsert_seed,
            plan=plan,
            context=session,
        )

    async def delete_one(self, filter_spec: Filter, *, session: ClientSession | None = None) -> DeleteResult:
        filter_spec = self._normalize_filter(filter_spec)
        plan = compile_filter(filter_spec)
        return await self._engine.delete_matching_document(
            self._db_name,
            self._collection_name,
            filter_spec,
            plan=plan,
            context=session,
        )

    async def count_documents(self, filter_spec: Filter, *, session: ClientSession | None = None) -> int:
        filter_spec = self._normalize_filter(filter_spec)
        plan = compile_filter(filter_spec)
        return await self._engine.count_matching_documents(
            self._db_name,
            self._collection_name,
            filter_spec,
            plan=plan,
            context=session,
        )

    async def create_index(self, fields: list[str], *, unique: bool = False, name: str | None = None, session: ClientSession | None = None) -> str:
        return await self._engine.create_index(
            self._db_name,
            self._collection_name,
            fields,
            unique=unique,
            name=name,
            context=session,
        )

    async def list_indexes(self, *, session: ClientSession | None = None) -> list[dict[str, object]]:
        return await self._engine.list_indexes(self._db_name, self._collection_name, context=session)
