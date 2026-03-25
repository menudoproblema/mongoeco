from copy import deepcopy

from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api._async.cursor import AsyncCursor, _validate_sort_spec
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
    resolve_mongodb_dialect_resolution,
    resolve_pymongo_profile_resolution,
)
from mongoeco.core.aggregation import Pipeline
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import compile_filter
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.core.upserts import seed_upsert_document
from mongoeco.core.validation import is_document, is_filter, is_projection, is_update
from mongoeco.session import ClientSession
from mongoeco.types import (
    ObjectId, Document, DocumentId, Filter, Update, Projection, InsertManyResult, InsertOneResult, ReturnDocument, UpdateResult, DeleteResult, SortSpec
)
from mongoeco.errors import DuplicateKeyError, OperationFailure


class AsyncCollection:
    """Representa una colección de MongoDB."""

    def __init__(
        self,
        engine: AsyncStorageEngine,
        db_name: str,
        collection_name: str,
        *,
        mongodb_dialect: MongoDialect | str | None = None,
        mongodb_dialect_resolution: MongoDialectResolution | None = None,
        pymongo_profile: PyMongoProfile | str | None = None,
        pymongo_profile_resolution: PyMongoProfileResolution | None = None,
    ):
        self._engine = engine
        self._db_name = db_name
        self._collection_name = collection_name
        self._mongodb_dialect_resolution = (
            mongodb_dialect_resolution
            if mongodb_dialect_resolution is not None
            else resolve_mongodb_dialect_resolution(mongodb_dialect)
        )
        self._mongodb_dialect = self._mongodb_dialect_resolution.resolved_dialect
        self._pymongo_profile_resolution = (
            pymongo_profile_resolution
            if pymongo_profile_resolution is not None
            else resolve_pymongo_profile_resolution(pymongo_profile)
        )
        self._pymongo_profile = self._pymongo_profile_resolution.resolved_profile

    @staticmethod
    def _require_document(document: object) -> Document:
        if not is_document(document):
            raise TypeError("document must be a dict")
        return document

    @classmethod
    def _require_documents(cls, documents: object) -> list[Document]:
        if not isinstance(documents, list):
            raise TypeError("documents must be a list")
        if not documents:
            raise ValueError("documents must not be empty")
        return [cls._require_document(document) for document in documents]

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
    def _require_replacement(replacement: object) -> Document:
        if not is_document(replacement):
            raise TypeError("replacement must be a dict")
        if any(isinstance(key, str) and key.startswith("$") for key in replacement):
            raise ValueError("replacement must not contain update operators")
        return replacement

    @staticmethod
    def _normalize_sort(sort: object | None) -> SortSpec | None:
        if sort is None:
            return None
        _validate_sort_spec(sort)
        return sort

    @staticmethod
    def _normalize_return_document(value: object | None) -> ReturnDocument:
        if value is None:
            return ReturnDocument.BEFORE
        if isinstance(value, ReturnDocument):
            return value
        raise TypeError("return_document must be a ReturnDocument value")

    @staticmethod
    def _can_use_direct_id_lookup(filter_spec: Filter) -> bool:
        if len(filter_spec) != 1 or "_id" not in filter_spec:
            return False

        id_selector = filter_spec["_id"]
        return not (
            isinstance(id_selector, dict)
            and any(isinstance(key, str) and key.startswith("$") for key in id_selector)
        )

    async def _select_first_document(
        self,
        filter_spec: Filter,
        *,
        sort: SortSpec | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        return await self.find(
            filter_spec,
            sort=sort,
            limit=1,
            session=session,
        ).first()

    async def _put_replacement_document(
        self,
        document: Document,
        *,
        overwrite: bool,
        session: ClientSession | None = None,
    ) -> None:
        success = await self._engine.put_document(
            self._db_name,
            self._collection_name,
            document,
            overwrite=overwrite,
            context=session,
        )
        if not success:
            raise DuplicateKeyError(f"Duplicate key: _id={document['_id']}")

    def _build_upsert_replacement_document(
        self,
        filter_spec: Filter,
        replacement: Document,
    ) -> Document:
        seeded: Document = {}
        seed_upsert_document(seeded, filter_spec)
        document = deepcopy(seeded)
        document.update(deepcopy(replacement))
        if "_id" not in document:
            document["_id"] = ObjectId()
        return document

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

    async def insert_many(
        self,
        documents: list[Document],
        *,
        session: ClientSession | None = None,
    ) -> InsertManyResult[DocumentId]:
        inserted_ids: list[DocumentId] = []
        for original in self._require_documents(documents):
            doc = original.copy()
            if "_id" not in doc:
                doc["_id"] = ObjectId()

            success = await self._engine.put_document(
                self._db_name,
                self._collection_name,
                doc,
                overwrite=False,
                context=session,
            )
            if not success:
                raise DuplicateKeyError(f"Duplicate key: _id={doc['_id']}")
            inserted_ids.append(doc["_id"])

        return InsertManyResult(inserted_ids=inserted_ids)

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
                dialect=self._mongodb_dialect,
                context=session,
            )
        else:
            plan = compile_filter(filter_spec, dialect=self._mongodb_dialect)
            async for d in self._engine.scan_collection(
                self._db_name,
                self._collection_name,
                filter_spec,
                plan=plan,
                projection=projection,
                dialect=self._mongodb_dialect,
                context=session,
            ):
                doc = d
                break

        return (
            apply_projection(doc, projection, dialect=self._mongodb_dialect)
            if doc is not None
            else None
        )

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
        plan = compile_filter(filter_spec, dialect=self._mongodb_dialect)
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

    async def update_one(
        self,
        filter_spec: Filter,
        update_spec: Update,
        upsert: bool = False,
        *,
        sort: SortSpec | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        filter_spec = self._normalize_filter(filter_spec)
        update_spec = self._require_update(update_spec)
        sort = self._normalize_sort(sort)
        if sort is not None and not self._pymongo_profile.supports_update_one_sort():
            raise TypeError(
                f"sort is not supported by PyMongo profile {self._pymongo_profile.key} "
                "for update_one()"
            )
        if sort is not None:
            selected = await self.find(
                filter_spec,
                sort=sort,
                limit=1,
                session=session,
            ).first()
            if selected is None and not upsert:
                return UpdateResult(matched_count=0, modified_count=0)
            if selected is not None:
                identity_filter = {'_id': selected['_id']}
                identity_plan = compile_filter(identity_filter, dialect=self._mongodb_dialect)
                return await self._engine.update_matching_document(
                    self._db_name,
                    self._collection_name,
                    identity_filter,
                    update_spec,
                    upsert=False,
                    plan=identity_plan,
                    dialect=self._mongodb_dialect,
                    context=session,
                )
        plan = compile_filter(filter_spec, dialect=self._mongodb_dialect)
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
            dialect=self._mongodb_dialect,
            context=session,
        )

    async def update_many(
        self,
        filter_spec: Filter,
        update_spec: Update,
        upsert: bool = False,
        *,
        session: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        filter_spec = self._normalize_filter(filter_spec)
        update_spec = self._require_update(update_spec)
        matched_documents = await self.find(
            filter_spec,
            {"_id": 1},
            session=session,
        ).to_list()
        if not matched_documents:
            if upsert:
                return await self.update_one(
                    filter_spec,
                    update_spec,
                    upsert=True,
                    session=session,
                )
            return UpdateResult(matched_count=0, modified_count=0)

        modified_count = 0
        for matched in matched_documents:
            identity_filter = {"_id": matched["_id"]}
            identity_plan = compile_filter(identity_filter, dialect=self._mongodb_dialect)
            result = await self._engine.update_matching_document(
                self._db_name,
                self._collection_name,
                identity_filter,
                update_spec,
                upsert=False,
                plan=identity_plan,
                dialect=self._mongodb_dialect,
                context=session,
            )
            modified_count += result.modified_count

        return UpdateResult(
            matched_count=len(matched_documents),
            modified_count=modified_count,
        )

    async def replace_one(
        self,
        filter_spec: Filter,
        replacement: Document,
        upsert: bool = False,
        *,
        sort: SortSpec | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        filter_spec = self._normalize_filter(filter_spec)
        replacement = self._require_replacement(replacement)
        sort = self._normalize_sort(sort)
        if sort is not None and not self._pymongo_profile.supports_update_one_sort():
            raise TypeError(
                f"sort is not supported by PyMongo profile {self._pymongo_profile.key} "
                "for replace_one()"
            )

        selected = await self._select_first_document(
            filter_spec,
            sort=sort,
            session=session,
        )
        if selected is None:
            if not upsert:
                return UpdateResult(matched_count=0, modified_count=0)
            document = self._build_upsert_replacement_document(filter_spec, replacement)
            await self._put_replacement_document(document, overwrite=False, session=session)
            return UpdateResult(
                matched_count=0,
                modified_count=0,
                upserted_id=document["_id"],
            )

        document = deepcopy(replacement)
        if "_id" in document and not self._mongodb_dialect.values_equal(document["_id"], selected["_id"]):
            raise OperationFailure("The _id field cannot be changed in a replacement document")
        document.setdefault("_id", selected["_id"])
        modified_count = 0 if self._mongodb_dialect.values_equal(selected, document) else 1
        await self._put_replacement_document(document, overwrite=True, session=session)
        return UpdateResult(matched_count=1, modified_count=modified_count)

    async def find_one_and_update(
        self,
        filter_spec: Filter,
        update_spec: Update,
        *,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        filter_spec = self._normalize_filter(filter_spec)
        projection = self._normalize_projection(projection)
        update_spec = self._require_update(update_spec)
        sort = self._normalize_sort(sort)
        return_document = self._normalize_return_document(return_document)

        before = await self._select_first_document(filter_spec, sort=sort, session=session)
        if before is None:
            if not upsert:
                return None
            result = await self.update_one(
                filter_spec,
                update_spec,
                upsert=True,
                session=session,
            )
            if return_document is ReturnDocument.BEFORE:
                return None
            return await self.find_one({"_id": result.upserted_id}, projection, session=session)

        identity_filter = {"_id": before["_id"]}
        identity_plan = compile_filter(identity_filter, dialect=self._mongodb_dialect)
        await self._engine.update_matching_document(
            self._db_name,
            self._collection_name,
            identity_filter,
            update_spec,
            upsert=False,
            plan=identity_plan,
            dialect=self._mongodb_dialect,
            context=session,
        )
        if return_document is ReturnDocument.BEFORE:
            return apply_projection(before, projection, dialect=self._mongodb_dialect)
        return await self.find_one(identity_filter, projection, session=session)

    async def find_one_and_replace(
        self,
        filter_spec: Filter,
        replacement: Document,
        *,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        filter_spec = self._normalize_filter(filter_spec)
        projection = self._normalize_projection(projection)
        replacement = self._require_replacement(replacement)
        sort = self._normalize_sort(sort)
        return_document = self._normalize_return_document(return_document)

        before = await self._select_first_document(filter_spec, sort=sort, session=session)
        if before is None:
            if not upsert:
                return None
            result = await self.replace_one(
                filter_spec,
                replacement,
                upsert=True,
                sort=sort,
                session=session,
            )
            if return_document is ReturnDocument.BEFORE:
                return None
            return await self.find_one({"_id": result.upserted_id}, projection, session=session)

        identity_filter = {"_id": before["_id"]}
        await self.replace_one(
            identity_filter,
            replacement,
            upsert=False,
            session=session,
        )
        if return_document is ReturnDocument.BEFORE:
            return apply_projection(before, projection, dialect=self._mongodb_dialect)
        return await self.find_one(identity_filter, projection, session=session)

    async def find_one_and_delete(
        self,
        filter_spec: Filter,
        *,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        filter_spec = self._normalize_filter(filter_spec)
        projection = self._normalize_projection(projection)
        sort = self._normalize_sort(sort)

        before = await self._select_first_document(filter_spec, sort=sort, session=session)
        if before is None:
            return None

        await self._engine.delete_document(
            self._db_name,
            self._collection_name,
            before["_id"],
            context=session,
        )
        return apply_projection(before, projection, dialect=self._mongodb_dialect)

    async def delete_one(self, filter_spec: Filter, *, session: ClientSession | None = None) -> DeleteResult:
        filter_spec = self._normalize_filter(filter_spec)
        plan = compile_filter(filter_spec, dialect=self._mongodb_dialect)
        return await self._engine.delete_matching_document(
            self._db_name,
            self._collection_name,
            filter_spec,
            plan=plan,
            dialect=self._mongodb_dialect,
            context=session,
        )

    async def delete_many(self, filter_spec: Filter, *, session: ClientSession | None = None) -> DeleteResult:
        filter_spec = self._normalize_filter(filter_spec)
        matched_documents = await self.find(
            filter_spec,
            {"_id": 1},
            session=session,
        ).to_list()
        deleted_count = 0
        for matched in matched_documents:
            deleted = await self._engine.delete_document(
                self._db_name,
                self._collection_name,
                matched["_id"],
                context=session,
            )
            if deleted:
                deleted_count += 1
        return DeleteResult(deleted_count=deleted_count)

    async def count_documents(self, filter_spec: Filter, *, session: ClientSession | None = None) -> int:
        filter_spec = self._normalize_filter(filter_spec)
        plan = compile_filter(filter_spec, dialect=self._mongodb_dialect)
        return await self._engine.count_matching_documents(
            self._db_name,
            self._collection_name,
            filter_spec,
            plan=plan,
            dialect=self._mongodb_dialect,
            context=session,
        )

    async def distinct(
        self,
        key: str,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> list[object]:
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        filter_spec = self._normalize_filter(filter_spec)
        distinct_values: list[object] = []
        async for document in self.find(filter_spec, session=session):
            values = QueryEngine.extract_values(document, key)
            if not values:
                continue
            candidates = values[1:] if isinstance(values[0], list) else values
            for candidate in candidates:
                if not any(
                    self._mongodb_dialect.values_equal(existing, candidate)
                    for existing in distinct_values
                ):
                    distinct_values.append(candidate)
        return distinct_values

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

    @property
    def mongodb_dialect(self) -> MongoDialect:
        return self._mongodb_dialect

    @property
    def mongodb_dialect_resolution(self) -> MongoDialectResolution:
        return self._mongodb_dialect_resolution

    @property
    def pymongo_profile(self) -> PyMongoProfile:
        return self._pymongo_profile

    @property
    def pymongo_profile_resolution(self) -> PyMongoProfileResolution:
        return self._pymongo_profile_resolution
