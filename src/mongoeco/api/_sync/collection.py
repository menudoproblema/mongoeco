from mongoeco.api.argument_validation import HintSpec
from mongoeco.api._sync.aggregation_cursor import AggregationCursor
from mongoeco.api._sync.cursor import Cursor
from mongoeco.api._sync.index_cursor import IndexCursor
from mongoeco.api._sync.raw_batch_cursor import RawBatchCursor
from mongoeco.api._sync.search_index_cursor import SearchIndexCursor
from mongoeco.api.public_api import (
    ARG_UNSET,
    COLLECTION_COUNT_DOCUMENTS_SPEC,
    COLLECTION_DELETE_MANY_SPEC,
    COLLECTION_DELETE_ONE_SPEC,
    COLLECTION_DISTINCT_SPEC,
    COLLECTION_FIND_ONE_AND_DELETE_SPEC,
    COLLECTION_FIND_ONE_AND_REPLACE_SPEC,
    COLLECTION_FIND_ONE_AND_UPDATE_SPEC,
    COLLECTION_FIND_ONE_SPEC,
    COLLECTION_FIND_RAW_BATCHES_SPEC,
    COLLECTION_FIND_SPEC,
    COLLECTION_REPLACE_ONE_SPEC,
    COLLECTION_UPDATE_MANY_SPEC,
    COLLECTION_UPDATE_ONE_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.change_streams import ChangeStreamCursor
from mongoeco.compat import (
    MongoDialect,
    MongoDialectResolution,
    PyMongoProfile,
    PyMongoProfileResolution,
)
from mongoeco.core.aggregation import Pipeline
from mongoeco.session import ClientSession
from mongoeco.types import (
    ArrayFilters, BulkWriteResult, CodecOptions, CollationDocument, DeleteResult, Document, DocumentId, Filter, InsertManyResult,
    IndexInformation, IndexModel, IndexKeySpec, InsertOneResult, Projection, ReadConcern, ReadPreference,
    PlanningMode, ReturnDocument, SearchIndexModel, SortSpec, Update, UpdateResult, WriteConcern, WriteModel,
)

_FILTER_UNSET = ARG_UNSET
_UPDATE_UNSET = ARG_UNSET


class Collection:
    """Adaptador sincronico sobre AsyncCollection."""

    def __init__(
        self,
        client,
        db_name: str,
        collection_name: str,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        planning_mode: PlanningMode = PlanningMode.STRICT,
    ):
        self._client = client
        self._db_name = db_name
        self._collection_name = collection_name
        self._write_concern = (
            client.write_concern if write_concern is None else write_concern
        )
        self._read_concern = (
            client.read_concern if read_concern is None else read_concern
        )
        self._read_preference = (
            client.read_preference if read_preference is None else read_preference
        )
        self._codec_options = (
            client.codec_options if codec_options is None else codec_options
        )
        self._planning_mode = planning_mode

    def _async_collection(self):
        self._client._ensure_connected()
        return self._client._async_client.get_database(
            self._db_name,
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
        ).get_collection(
            self._collection_name,
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
        ).with_options(planning_mode=self._planning_mode)

    def _run_collection_method(self, method_name: str, /, *args, **kwargs):
        collection = self._async_collection()
        method = getattr(collection, method_name)
        return self._client._run(method(*args, **kwargs))

    def with_options(
        self,
        *,
        write_concern: WriteConcern | None = None,
        read_concern: ReadConcern | None = None,
        read_preference: ReadPreference | None = None,
        codec_options: CodecOptions | None = None,
        planning_mode: PlanningMode | None = None,
    ) -> "Collection":
        return type(self)(
            self._client,
            self._db_name,
            self._collection_name,
            write_concern=self._write_concern if write_concern is None else write_concern,
            read_concern=self._read_concern if read_concern is None else read_concern,
            read_preference=self._read_preference if read_preference is None else read_preference,
            codec_options=self._codec_options if codec_options is None else codec_options,
            planning_mode=self._planning_mode if planning_mode is None else planning_mode,
        )

    def __getattr__(self, name: str) -> "Collection":
        if name.startswith("_"):
            raise AttributeError(name)
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> "Collection":
        if not isinstance(name, str) or not name:
            raise TypeError("subcollection name must be a non-empty string")
        return self.database.get_collection(
            f"{self._collection_name}.{name}",
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
        ).with_options(planning_mode=self._planning_mode)

    def insert_one(
        self,
        document: Document,
        *,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
    ) -> InsertOneResult[DocumentId]:
        return self._run_collection_method(
            "insert_one",
            document,
            bypass_document_validation=bypass_document_validation,
            session=session,
        )

    def insert_many(
        self,
        documents: list[Document],
        *,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
    ) -> InsertManyResult[DocumentId]:
        return self._run_collection_method(
            "insert_many",
            documents,
            bypass_document_validation=bypass_document_validation,
            session=session,
        )

    def find_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "find_one",
            options.get("filter_spec", _FILTER_UNSET),
            options.get("projection"),
            collation=options.get("collation"),
            session=options.get("session"),
            **{
                key: options[key]
                for key in ("sort", "skip", "hint", "comment", "max_time_ms")
                if key in options
            },
        )

    def bulk_write(
        self,
        requests: list[WriteModel],
        *,
        ordered: bool = True,
        bypass_document_validation: bool = False,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> BulkWriteResult[DocumentId]:
        return self._run_collection_method(
            "bulk_write",
            requests,
            ordered=ordered,
            bypass_document_validation=bypass_document_validation,
            comment=comment,
            let=let,
            session=session,
        )

    def find(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Cursor:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "skip": skip,
                "limit": limit,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "batch_size": batch_size,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        async_collection = self._async_collection()
        return Cursor(
            self._client,
            async_collection,
            {} if options.get("filter_spec") is None else options.get("filter_spec"),
            options.get("projection"),
            collation=options.get("collation"),
            sort=options.get("sort"),
            skip=options.get("skip", 0),
            limit=options.get("limit"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            batch_size=options.get("batch_size"),
            session=options.get("session"),
        )

    def aggregate(
        self,
        pipeline: Pipeline,
        *,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        allow_disk_use: bool | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> AggregationCursor:
        return AggregationCursor(
            self._client,
            self._async_collection().aggregate(
                pipeline,
                collation=collation,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                batch_size=batch_size,
                allow_disk_use=allow_disk_use,
                let=let,
                session=session,
            ),
        )

    def find_raw_batches(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> RawBatchCursor:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_RAW_BATCHES_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "skip": skip,
                "limit": limit,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "batch_size": batch_size,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return RawBatchCursor(
            self._client,
            self._async_collection().find_raw_batches(
                options.get("filter_spec", _FILTER_UNSET),
                options.get("projection"),
                collation=options.get("collation"),
                sort=options.get("sort"),
                skip=options.get("skip", 0),
                limit=options.get("limit"),
                hint=options.get("hint"),
                comment=options.get("comment"),
                max_time_ms=options.get("max_time_ms"),
                batch_size=options.get("batch_size"),
                session=options.get("session"),
            ),
        )

    def aggregate_raw_batches(
        self,
        pipeline: Pipeline,
        *,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        batch_size: int | None = None,
        allow_disk_use: bool | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
    ) -> RawBatchCursor:
        return RawBatchCursor(
            self._client,
            self._async_collection().aggregate_raw_batches(
                pipeline,
                collation=collation,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                batch_size=batch_size,
                allow_disk_use=allow_disk_use,
                let=let,
                session=session,
            ),
        )

    def update_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        update_spec: Update | object = _UPDATE_UNSET,
        upsert: bool = False,
        *,
        filter: Filter | object = _FILTER_UNSET,
        update: Update | object = _UPDATE_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        array_filters: ArrayFilters | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> UpdateResult[DocumentId]:
        options = normalize_public_operation_arguments(
            COLLECTION_UPDATE_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "update_spec": update_spec,
                "upsert": upsert,
                "collation": collation,
                "sort": sort,
                "array_filters": array_filters,
                "hint": hint,
                "comment": comment,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, "update": update, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "update_one",
            options["filter_spec"],
            options["update_spec"],
            options.get("upsert", False),
            collation=options.get("collation"),
            sort=options.get("sort"),
            array_filters=options.get("array_filters"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            bypass_document_validation=options.get("bypass_document_validation", False),
            session=options.get("session"),
        )

    def replace_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        replacement: Document | object = ARG_UNSET,
        upsert: bool = False,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> UpdateResult[DocumentId]:
        options = normalize_public_operation_arguments(
            COLLECTION_REPLACE_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "replacement": replacement,
                "upsert": upsert,
                "collation": collation,
                "sort": sort,
                "hint": hint,
                "comment": comment,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "replace_one",
            options["filter_spec"],
            options["replacement"],
            options.get("upsert", False),
            collation=options.get("collation"),
            sort=options.get("sort"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            bypass_document_validation=options.get("bypass_document_validation", False),
            session=options.get("session"),
        )

    def find_one_and_update(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        update_spec: Update | object = _UPDATE_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        update: Update | object = _UPDATE_UNSET,
        projection: Projection | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        array_filters: ArrayFilters | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_AND_UPDATE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "update_spec": update_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "upsert": upsert,
                "return_document": return_document,
                "array_filters": array_filters,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, "update": update, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "find_one_and_update",
            options["filter_spec"],
            options["update_spec"],
            projection=options.get("projection"),
            collation=options.get("collation"),
            sort=options.get("sort"),
            upsert=options.get("upsert", False),
            return_document=options.get("return_document"),
            array_filters=options.get("array_filters"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            let=options.get("let"),
            bypass_document_validation=options.get("bypass_document_validation", False),
            session=options.get("session"),
        )

    def find_one_and_replace(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        replacement: Document | object = ARG_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        upsert: bool = False,
        return_document: ReturnDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_AND_REPLACE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "replacement": replacement,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "upsert": upsert,
                "return_document": return_document,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "find_one_and_replace",
            options["filter_spec"],
            options["replacement"],
            projection=options.get("projection"),
            collation=options.get("collation"),
            sort=options.get("sort"),
            upsert=options.get("upsert", False),
            return_document=options.get("return_document"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            let=options.get("let"),
            bypass_document_validation=options.get("bypass_document_validation", False),
            session=options.get("session"),
        )

    def find_one_and_delete(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        projection: Projection | None = None,
        collation: CollationDocument | None = None,
        sort: SortSpec | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> Document | None:
        options = normalize_public_operation_arguments(
            COLLECTION_FIND_ONE_AND_DELETE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "projection": projection,
                "collation": collation,
                "sort": sort,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "let": let,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "find_one_and_delete",
            options["filter_spec"],
            projection=options.get("projection"),
            collation=options.get("collation"),
            sort=options.get("sort"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            let=options.get("let"),
            session=options.get("session"),
        )

    def delete_one(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> DeleteResult:
        options = normalize_public_operation_arguments(
            COLLECTION_DELETE_ONE_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "let": let,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "delete_one",
            options["filter_spec"],
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            session=options.get("session"),
        )

    def update_many(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        update_spec: Update | object = _UPDATE_UNSET,
        upsert: bool = False,
        *,
        filter: Filter | object = _FILTER_UNSET,
        update: Update | object = _UPDATE_UNSET,
        collation: CollationDocument | None = None,
        array_filters: ArrayFilters | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        bypass_document_validation: bool = False,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> UpdateResult[DocumentId]:
        options = normalize_public_operation_arguments(
            COLLECTION_UPDATE_MANY_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "update_spec": update_spec,
                "upsert": upsert,
                "collation": collation,
                "array_filters": array_filters,
                "hint": hint,
                "comment": comment,
                "let": let,
                "bypass_document_validation": bypass_document_validation,
                "session": session,
            },
            extra_kwargs={"filter": filter, "update": update, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "update_many",
            options["filter_spec"],
            options["update_spec"],
            options.get("upsert", False),
            collation=options.get("collation"),
            array_filters=options.get("array_filters"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            bypass_document_validation=options.get("bypass_document_validation", False),
            session=options.get("session"),
        )

    def delete_many(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        let: dict[str, object] | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> DeleteResult:
        options = normalize_public_operation_arguments(
            COLLECTION_DELETE_MANY_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "let": let,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "delete_many",
            options["filter_spec"],
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            let=options.get("let"),
            session=options.get("session"),
        )

    def count_documents(
        self,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        skip: int = 0,
        limit: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> int:
        options = normalize_public_operation_arguments(
            COLLECTION_COUNT_DOCUMENTS_SPEC,
            explicit={
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "skip": skip,
                "limit": limit,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "count_documents",
            options["filter_spec"],
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            skip=options.get("skip", 0),
            limit=options.get("limit"),
            session=options.get("session"),
        )

    def estimated_document_count(
        self,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> int:
        return self._run_collection_method(
            "estimated_document_count",
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        )

    def distinct(
        self,
        key: str,
        filter_spec: Filter | object = _FILTER_UNSET,
        *,
        filter: Filter | object = _FILTER_UNSET,
        collation: CollationDocument | None = None,
        hint: HintSpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> list[object]:
        options = normalize_public_operation_arguments(
            COLLECTION_DISTINCT_SPEC,
            explicit={
                "key": key,
                "filter_spec": filter_spec,
                "collation": collation,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "session": session,
            },
            extra_kwargs={"filter": filter, **kwargs},
            profile=self._client.pymongo_profile,
        )
        return self._run_collection_method(
            "distinct",
            options["key"],
            options.get("filter_spec", _FILTER_UNSET),
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            session=options.get("session"),
        )

    def create_index(
        self,
        keys: object,
        *,
        unique: bool = False,
        name: str | None = None,
        sparse: bool = False,
        partial_filter_expression: dict[str, object] | None = None,
        expire_after_seconds: int | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> str:
        return self._client._run(
            self._async_collection().create_index(
                keys,
                unique=unique,
                name=name,
                sparse=sparse,
                partial_filter_expression=partial_filter_expression,
                expire_after_seconds=expire_after_seconds,
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
            self._async_collection().list_indexes(comment=comment, session=session),
        )

    def index_information(
        self,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> IndexInformation:
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

    def create_search_index(
        self,
        model: SearchIndexModel | dict[str, object],
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> str:
        return self._client._run(
            self._async_collection().create_search_index(
                model,
                comment=comment,
                max_time_ms=max_time_ms,
                session=session,
            )
        )

    def create_search_indexes(
        self,
        indexes: list[SearchIndexModel | dict[str, object]],
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> list[str]:
        return self._client._run(
            self._async_collection().create_search_indexes(
                indexes,
                comment=comment,
                max_time_ms=max_time_ms,
                session=session,
            )
        )

    def list_search_indexes(
        self,
        name: str | None = None,
        *,
        comment: object | None = None,
        session: ClientSession | None = None,
    ) -> SearchIndexCursor:
        return SearchIndexCursor(
            self._client,
            self._async_collection().list_search_indexes(
                name=name,
                comment=comment,
                session=session,
            ),
        )

    def update_search_index(
        self,
        name: str,
        definition: Document,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> None:
        self._client._run(
            self._async_collection().update_search_index(
                name,
                definition,
                comment=comment,
                max_time_ms=max_time_ms,
                session=session,
            )
        )

    def drop_search_index(
        self,
        name: str,
        *,
        comment: object | None = None,
        max_time_ms: int | None = None,
        session: ClientSession | None = None,
    ) -> None:
        self._client._run(
            self._async_collection().drop_search_index(
                name,
                comment=comment,
                max_time_ms=max_time_ms,
                session=session,
            )
        )

    def drop(self, *, session: ClientSession | None = None) -> None:
        self._client._run(self._async_collection().drop(session=session))

    def rename(
        self,
        new_name: str,
        *,
        session: ClientSession | None = None,
    ) -> "Collection":
        return self._client._run_resource(
            self._async_collection().rename(new_name, session=session),
            lambda: type(self)(self._client, self._db_name, new_name),
        )

    def options(self, *, session: ClientSession | None = None) -> dict[str, object]:
        return self._client._run(self._async_collection().options(session=session))

    def watch(
        self,
        pipeline: object | None = None,
        *,
        max_await_time_ms: int | None = None,
        resume_after: dict[str, object] | None = None,
        start_after: dict[str, object] | None = None,
        start_at_operation_time: int | None = None,
        full_document: str = "default",
        session: ClientSession | None = None,
    ) -> ChangeStreamCursor:
        return ChangeStreamCursor(
            self._client,
            self._async_collection().watch(
                pipeline,
                max_await_time_ms=max_await_time_ms,
                resume_after=resume_after,
                start_after=start_after,
                start_at_operation_time=start_at_operation_time,
                full_document=full_document,
                session=session,
            ),
        )

    @property
    def name(self) -> str:
        return self._collection_name

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

    @property
    def write_concern(self) -> WriteConcern:
        return self._write_concern

    @property
    def read_concern(self) -> ReadConcern:
        return self._read_concern

    @property
    def read_preference(self) -> ReadPreference:
        return self._read_preference

    @property
    def codec_options(self) -> CodecOptions:
        return self._codec_options

    @property
    def full_name(self) -> str:
        return f"{self._db_name}.{self._collection_name}"

    @property
    def database(self):
        return self._client.get_database(
            self._db_name,
            write_concern=self._write_concern,
            read_concern=self._read_concern,
            read_preference=self._read_preference,
            codec_options=self._codec_options,
        )
