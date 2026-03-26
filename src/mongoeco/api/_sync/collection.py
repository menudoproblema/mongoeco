from mongoeco.api._async.cursor import HintSpec
from mongoeco.api._sync.aggregation_cursor import AggregationCursor
from mongoeco.api._sync.cursor import Cursor
from mongoeco.api._sync.index_cursor import IndexCursor
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
)
from mongoeco.core.aggregation import Pipeline
from mongoeco.session import ClientSession
from mongoeco.types import (
    BulkWriteResult, DeleteResult, Document, DocumentId, Filter, InsertManyResult,
    IndexModel, IndexKeySpec, InsertOneResult, Projection, ReturnDocument, SortSpec, Update,
    UpdateResult, WriteModel,
)


class Collection:
    """Adaptador sincronico sobre AsyncCollection."""

    def __init__(self, client, db_name: str, collection_name: str):
        self._client = client
        self._db_name = db_name
        self._collection_name = collection_name

    def _async_collection(self):
        self._client._ensure_connected()
        return self._client._async_client.get_database(self._db_name).get_collection(self._collection_name)

    def insert_one(self, document: Document, *, session: ClientSession | None = None) -> InsertOneResult[DocumentId]:
        return self._client._run(self._async_collection().insert_one(document, session=session))

    def insert_many(
        self,
        documents: list[Document],
        *,
        session: ClientSession | None = None,
    ) -> InsertManyResult[DocumentId]:
        return self._client._run(self._async_collection().insert_many(documents, session=session))

    def find_one(self, filter_spec: Filter | None = None, projection: Projection | None = None, *, session: ClientSession | None = None) -> Document | None:
        return self._client._run(self._async_collection().find_one(filter_spec, projection, session=session))

    def bulk_write(
        self,
        requests: list[WriteModel],
        *,
        ordered: bool = True,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> BulkWriteResult[DocumentId]:
        return self._client._run(
            self._async_collection().bulk_write(
                requests,
                ordered=ordered,
                comment=comment,
                let=let,
                session=session,
            )
        )

    def find(
        self,
        filter_spec: Filter | None = None,
        projection: Projection | None = None,
        *,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        session: ClientSession | None = None,
    ) -> Cursor:
        async_collection = self._async_collection()
        return Cursor(
            self._client,
            async_collection,
            {} if filter_spec is None else filter_spec,
            projection,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            comment=comment,
            max_time_ms=max_time_ms,
            batch_size=batch_size,
            session=session,
        )

    def aggregate(
        self,
        pipeline: Pipeline,
        *,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> AggregationCursor:
        return AggregationCursor(
            self._client,
            self._async_collection().aggregate(
                pipeline,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                batch_size=batch_size,
                let=let,
                session=session,
            ),
        )

    def update_one(
        self,
        filter_spec: Filter,
        update_spec: Update,
        upsert: bool = False,
        *,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        return self._client._run(
            self._async_collection().update_one(
                filter_spec,
                update_spec,
                upsert,
                sort=sort,
                hint=hint,
                comment=comment,
                let=let,
                session=session,
            )
        )

    def replace_one(
        self,
        filter_spec: Filter,
        replacement: Document,
        upsert: bool = False,
        *,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        return self._client._run(
            self._async_collection().replace_one(
                filter_spec,
                replacement,
                upsert,
                sort=sort,
                hint=hint,
                comment=comment,
                let=let,
                session=session,
            )
        )

    def find_one_and_update(
        self,
        filter_spec: Filter,
        update_spec: Update,
        *,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        return self._client._run(
            self._async_collection().find_one_and_update(
                filter_spec,
                update_spec,
                projection=projection,
                sort=sort,
                upsert=upsert,
                return_document=return_document,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                let=let,
                session=session,
            )
        )

    def find_one_and_replace(
        self,
        filter_spec: Filter,
        replacement: Document,
        *,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        return self._client._run(
            self._async_collection().find_one_and_replace(
                filter_spec,
                replacement,
                projection=projection,
                sort=sort,
                upsert=upsert,
                return_document=return_document,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                let=let,
                session=session,
            )
        )

    def find_one_and_delete(
        self,
        filter_spec: Filter,
        *,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> Document | None:
        return self._client._run(
            self._async_collection().find_one_and_delete(
                filter_spec,
                projection=projection,
                sort=sort,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                let=let,
                session=session,
            )
        )

    def delete_one(
        self,
        filter_spec: Filter,
        *,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> DeleteResult:
        return self._client._run(
            self._async_collection().delete_one(
                filter_spec,
                hint=hint,
                comment=comment,
                let=let,
                session=session,
            )
        )

    def update_many(
        self,
        filter_spec: Filter,
        update_spec: Update,
        upsert: bool = False,
        *,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        return self._client._run(
            self._async_collection().update_many(
                filter_spec,
                update_spec,
                upsert,
                hint=hint,
                comment=comment,
                let=let,
                session=session,
            )
        )

    def delete_many(
        self,
        filter_spec: Filter,
        *,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> DeleteResult:
        return self._client._run(
            self._async_collection().delete_many(
                filter_spec,
                hint=hint,
                comment=comment,
                let=let,
                session=session,
            )
        )

    def count_documents(self, filter_spec: Filter, *, session: ClientSession | None = None) -> int:
        return self._client._run(self._async_collection().count_documents(filter_spec, session=session))

    def estimated_document_count(self, *, session: ClientSession | None = None) -> int:
        return self._client._run(self._async_collection().estimated_document_count(session=session))

    def distinct(
        self,
        key: str,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> list[object]:
        return self._client._run(self._async_collection().distinct(key, filter_spec, session=session))

    def create_index(
        self,
        keys: object,
        *,
        unique: bool = False,
        name: str | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> str:
        return self._client._run(
            self._async_collection().create_index(
                keys,
                unique=unique,
                name=name,
                comment=comment,
                max_time_ms=max_time_ms,
                session=session,
            )
        )

    def create_indexes(
        self,
        indexes: list[IndexModel],
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> list[str]:
        return self._client._run(
            self._async_collection().create_indexes(
                indexes,
                comment=comment,
                max_time_ms=max_time_ms,
                session=session,
            )
        )

    def list_indexes(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> IndexCursor:
        return IndexCursor(
            self._client,
            self._client._run(self._async_collection().list_indexes(comment=comment, session=session)),
        )

    def index_information(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> dict[str, dict[str, object]]:
        return self._client._run(
            self._async_collection().index_information(comment=comment, session=session)
        )

    def drop_index(
        self,
        index_or_name: str | IndexKeySpec,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> None:
        self._client._run(
            self._async_collection().drop_index(index_or_name, comment=comment, session=session)
        )

    def drop_indexes(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> None:
        self._client._run(self._async_collection().drop_indexes(comment=comment, session=session))

    def drop(self, *, session: ClientSession | None = None) -> None:
        self._client._run(self._async_collection().drop(session=session))

    @property
    def mongodb_dialect(self) -> MongoDialect:
        return self._client.mongodb_dialect

    @property
    def mongodb_dialect_resolution(self) -> MongoDialectResolution:
        return self._client.mongodb_dialect_resolution

    @property
    def pymongo_profile(self) -> PyMongoProfile:
        return self._client.pymongo_profile

    @property
    def pymongo_profile_resolution(self) -> PyMongoProfileResolution:
        return self._client.pymongo_profile_resolution
