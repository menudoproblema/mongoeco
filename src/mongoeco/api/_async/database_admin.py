import datetime
import os
import platform
import socket
import sys
from typing import TYPE_CHECKING

from mongoeco.api._async.cursor import (
    _validate_batch_size,
    _validate_hint_spec,
    _validate_max_time_ms,
    _validate_sort_spec,
)
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.core.aggregation import _bson_document_size
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.validation import is_filter, is_projection
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.errors import BulkWriteError, CollectionInvalid, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    BuildInfoDocument,
    CmdLineOptsDocument,
    CollectionListingDocument,
    CollectionStatsDocument,
    CollectionValidationDocument,
    ConnectionStatusDocument,
    DatabaseListingDocument,
    DatabaseStatsDocument,
    Document,
    Filter,
    HelloDocument,
    HostInfoDocument,
    IndexModel,
    ListCommandsDocument,
    ReturnDocument,
    WhatsMyUriDocument,
)

if TYPE_CHECKING:
    from mongoeco.api._async.client import AsyncDatabase
    from mongoeco.compat import MongoDialect


class AsyncDatabaseAdminService:
    def __init__(self, database: "AsyncDatabase"):
        self._database = database

    @property
    def _engine(self) -> AsyncStorageEngine:
        return self._database._engine

    @property
    def _db_name(self) -> str:
        return self._database._db_name

    @property
    def _mongodb_dialect(self) -> "MongoDialect":
        return self._database._mongodb_dialect

    @staticmethod
    def _normalize_filter(filter_spec: object | None) -> Filter:
        if filter_spec is None:
            return {}
        if not is_filter(filter_spec):
            raise TypeError("filter_spec must be a dict")
        return filter_spec

    async def list_collection_names(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> list[str]:
        if filter_spec is None:
            return await self._engine.list_collections(self._db_name, context=session)
        documents = await self.list_collections(filter_spec, session=session).to_list()
        return [str(document["name"]) for document in documents]

    def list_collections(
        self,
        filter_spec: Filter | None = None,
        *,
        session: ClientSession | None = None,
    ) -> AsyncListingCursor:
        normalized_filter = self._normalize_filter(filter_spec)

        async def _load() -> list[CollectionListingDocument]:
            names = await self._engine.list_collections(self._db_name, context=session)
            documents: list[CollectionListingDocument] = []
            for name in names:
                documents.append(
                    {
                        "name": name,
                        "type": "collection",
                        "options": await self._engine.collection_options(
                            self._db_name,
                            name,
                            context=session,
                        ),
                        "info": {"readOnly": False},
                    }
                )
            return [
                document
                for document in documents
                if QueryEngine.match(
                    document,
                    normalized_filter,
                    dialect=self._mongodb_dialect,
                )
            ]

        return AsyncListingCursor(_load)

    async def create_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
        **options: object,
    ):
        await self._engine.create_collection(
            self._db_name,
            name,
            options=dict(options),
            context=session,
        )
        return self._database.get_collection(name)

    async def drop_collection(
        self,
        name: str,
        *,
        session: ClientSession | None = None,
    ) -> None:
        await self._engine.drop_collection(self._db_name, name, context=session)

    @staticmethod
    def _normalize_command(command: object, kwargs: dict[str, object]) -> dict[str, object]:
        if isinstance(command, str):
            if not command:
                raise TypeError("command name must be a non-empty string")
            return {command: 1, **kwargs}
        if not isinstance(command, dict) or not command:
            raise TypeError("command must be a non-empty string or dict")
        if kwargs:
            raise TypeError("keyword arguments are only supported when command is a string")
        return command

    @staticmethod
    def _require_collection_name(value: object, field_name: str) -> str:
        if not isinstance(value, str) or not value:
            raise TypeError(f"{field_name} must be a non-empty string")
        return value

    @staticmethod
    def _resolve_collection_reference(value: object, field_name: str) -> str:
        if isinstance(value, str):
            return AsyncDatabaseAdminService._require_collection_name(value, field_name)
        name = getattr(value, "name", None)
        if isinstance(name, str) and name:
            return name
        raise TypeError(f"{field_name} must be a collection name or collection object")

    @staticmethod
    def _normalize_index_models_from_command(indexes: object) -> list[IndexModel]:
        if not isinstance(indexes, list) or not indexes:
            raise TypeError("indexes must be a non-empty list")

        normalized: list[IndexModel] = []
        for raw_index in indexes:
            if not isinstance(raw_index, dict):
                raise TypeError("each index specification must be a dict")
            if "key" not in raw_index:
                raise OperationFailure("index specification must contain 'key'")

            unsupported = set(raw_index) - {"key", "name", "unique"}
            if unsupported:
                unsupported_names = ", ".join(sorted(unsupported))
                raise TypeError(
                    f"unsupported createIndexes options in command: {unsupported_names}"
                )

            kwargs: dict[str, object] = {}
            if "name" in raw_index:
                kwargs["name"] = raw_index["name"]
            if "unique" in raw_index:
                kwargs["unique"] = raw_index["unique"]
            normalized.append(IndexModel(raw_index["key"], **kwargs))
        return normalized

    @staticmethod
    def _normalize_sort_document(sort: object | None) -> list[tuple[str, int]] | None:
        if sort is None:
            return None
        if not isinstance(sort, dict):
            raise TypeError("sort must be a document")
        normalized = list(sort.items())
        _validate_sort_spec(normalized)
        return normalized

    @staticmethod
    def _normalize_hint_from_command(hint: object | None) -> object | None:
        if hint is None:
            return None
        if isinstance(hint, dict):
            hint = list(hint.items())
        _validate_hint_spec(hint)
        return hint

    @staticmethod
    def _normalize_max_time_ms_from_command(max_time_ms: object | None) -> int | None:
        if max_time_ms is None:
            return None
        _validate_max_time_ms(max_time_ms)
        return max_time_ms

    @staticmethod
    def _normalize_projection_from_command(projection: object | None) -> dict[str, object] | None:
        if projection is None:
            return None
        if not is_projection(projection):
            raise TypeError("projection must be a dict")
        return projection

    @staticmethod
    def _normalize_batch_size_from_command(batch_size: object | None) -> int | None:
        if batch_size is None:
            return None
        _validate_batch_size(batch_size)
        return batch_size

    @staticmethod
    def _normalize_ordered_from_command(value: object | None) -> bool:
        if value is None:
            return True
        if not isinstance(value, bool):
            raise TypeError("ordered must be a bool")
        return value

    @staticmethod
    def _normalize_scale_from_command(scale: object | None) -> int:
        if scale is None:
            return 1
        if not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0:
            raise TypeError("scale must be a positive integer")
        return scale

    @staticmethod
    def _normalize_namespace(value: object, field_name: str) -> tuple[str, str]:
        if not isinstance(value, str) or "." not in value:
            raise TypeError(f"{field_name} must be a fully qualified namespace string")
        db_name, coll_name = value.split(".", 1)
        if not db_name or not coll_name:
            raise TypeError(f"{field_name} must be a fully qualified namespace string")
        return db_name, coll_name

    @staticmethod
    def _normalize_insert_documents(spec: object) -> list[Document]:
        if not isinstance(spec, list) or not spec:
            raise TypeError("documents must be a non-empty list of documents")
        if not all(isinstance(item, dict) for item in spec):
            raise TypeError("documents must be a non-empty list of documents")
        return spec

    @staticmethod
    def _normalize_update_specs(spec: object) -> list[dict[str, object]]:
        if not isinstance(spec, list) or not spec:
            raise TypeError("updates must be a non-empty list")
        if not all(isinstance(item, dict) for item in spec):
            raise TypeError("updates must be a non-empty list")
        return spec

    @staticmethod
    def _normalize_delete_specs(spec: object) -> list[dict[str, object]]:
        if not isinstance(spec, list) or not spec:
            raise TypeError("deletes must be a non-empty list")
        if not all(isinstance(item, dict) for item in spec):
            raise TypeError("deletes must be a non-empty list")
        return spec

    async def _collection_stats(
        self,
        collection_name: str,
        *,
        scale: int = 1,
        session: ClientSession | None = None,
    ) -> CollectionStatsDocument:
        collection = self._database.get_collection(collection_name)
        documents = await collection.find({}, session=session).to_list()
        indexes = await collection.list_indexes(session=session).to_list()
        data_size = sum(_bson_document_size(document) for document in documents)
        count = len(documents)
        return {
            "ns": f"{self._db_name}.{collection_name}",
            "count": count,
            "size": data_size // scale,
            "avgObjSize": ((data_size / count) / scale) if count else 0,
            "storageSize": data_size // scale,
            "nindexes": len(indexes),
            "totalIndexSize": 0,
            "ok": 1.0,
        }

    async def _database_stats(
        self,
        *,
        scale: int = 1,
        session: ClientSession | None = None,
    ) -> DatabaseStatsDocument:
        collection_names = await self._engine.list_collections(self._db_name, context=session)
        objects = 0
        data_size = 0
        indexes = 0
        for collection_name in collection_names:
            stats = await self._collection_stats(collection_name, scale=1, session=session)
            objects += int(stats["count"])
            data_size += int(stats["size"])
            indexes += int(stats["nindexes"])
        return {
            "db": self._db_name,
            "collections": len(collection_names),
            "objects": objects,
            "avgObjSize": ((data_size / objects) / scale) if objects else 0,
            "dataSize": data_size // scale,
            "storageSize": data_size // scale,
            "indexes": indexes,
            "indexSize": 0,
            "ok": 1.0,
        }

    async def _list_database_documents(
        self,
        *,
        session: ClientSession | None = None,
    ) -> list[DatabaseListingDocument]:
        database_names = await self._engine.list_databases(context=session)
        documents: list[DatabaseListingDocument] = []
        for database_name in database_names:
            database = type(self._database)(
                self._engine,
                database_name,
                mongodb_dialect=self._database._mongodb_dialect,
                mongodb_dialect_resolution=self._database._mongodb_dialect_resolution,
                pymongo_profile=self._database._pymongo_profile,
                pymongo_profile_resolution=self._database._pymongo_profile_resolution,
                write_concern=self._database._write_concern,
                read_concern=self._database._read_concern,
                read_preference=self._database._read_preference,
                codec_options=self._database._codec_options,
            )
            stats = await database._admin._database_stats(session=session)
            documents.append(
                {
                    "name": database_name,
                    "sizeOnDisk": stats["storageSize"],
                    "empty": (
                        int(stats["collections"]) == 0
                        and int(stats["objects"]) == 0
                    ),
                }
            )
        return documents

    async def validate_collection(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> CollectionValidationDocument:
        if not isinstance(scandata, bool):
            raise TypeError("scandata must be a bool")
        if not isinstance(full, bool):
            raise TypeError("full must be a bool")
        if background is not None and not isinstance(background, bool):
            raise TypeError("background must be a bool or None")
        if comment is not None and not isinstance(comment, str):
            raise TypeError("comment must be a string")

        collection_name = self._resolve_collection_reference(
            name_or_collection,
            "name_or_collection",
        )
        collection_names = await self._engine.list_collections(self._db_name, context=session)
        if collection_name not in collection_names:
            raise CollectionInvalid(f"collection '{collection_name}' does not exist")

        indexes = await self._database.get_collection(collection_name).list_indexes(
            session=session
        ).to_list()
        return {
            "ns": f"{self._db_name}.{collection_name}",
            "valid": True,
            "nrecords": await self._database.get_collection(collection_name).count_documents(
                {},
                session=session,
            ),
            "nIndexes": len(indexes),
            "keysPerIndex": {
                str(index["name"]): len(index.get("fields", []))
                for index in indexes
            },
            "warnings": [],
            "ok": 1.0,
        }

    async def command(
        self,
        command: object,
        *,
        session: ClientSession | None = None,
        **kwargs: object,
    ) -> dict[str, object]:
        spec = self._normalize_command(command, kwargs)
        command_name = next(iter(spec))
        if not isinstance(command_name, str):
            raise TypeError("command name must be a string")

        if command_name == "ping":
            return {"ok": 1.0}
        if command_name == "buildInfo":
            return build_info_document(self._mongodb_dialect)
        if command_name == "serverStatus":
            return server_status_document(
                self._mongodb_dialect,
                engine=self._engine,
            )
        if command_name == "hostInfo":
            return host_info_document()
        if command_name == "whatsmyuri":
            return whats_my_uri_document()
        if command_name == "getCmdLineOpts":
            return cmd_line_opts_document()
        if command_name in {"hello", "isMaster", "ismaster"}:
            return hello_document(self._mongodb_dialect, legacy_name=command_name != "hello")
        if command_name == "listCommands":
            return list_commands_document()
        if command_name == "connectionStatus":
            show_privileges = spec.get("showPrivileges", False)
            if not isinstance(show_privileges, bool):
                raise TypeError("showPrivileges must be a bool")
            return connection_status_document(show_privileges=show_privileges)
        if command_name == "listCollections":
            return await self._command_list_collections(spec, session=session)
        if command_name == "listDatabases":
            return await self._command_list_databases(spec, session=session)
        if command_name == "create":
            return await self._command_create(spec, session=session)
        if command_name == "drop":
            return await self._command_drop(spec, session=session)
        if command_name == "renameCollection":
            return await self._command_rename_collection(spec, session=session)
        if command_name == "count":
            return await self._command_count(spec, session=session)
        if command_name == "distinct":
            return await self._command_distinct(spec, session=session)
        if command_name == "insert":
            return await self._command_insert(spec, session=session)
        if command_name == "update":
            return await self._command_update(spec, session=session)
        if command_name == "delete":
            return await self._command_delete(spec, session=session)
        if command_name == "find":
            return await self._command_find(spec, session=session)
        if command_name == "aggregate":
            return await self._command_aggregate(spec, session=session)
        if command_name == "explain":
            return await self._command_explain(spec, session=session)
        if command_name == "findAndModify":
            return await self._command_find_and_modify(spec, session=session)
        if command_name == "listIndexes":
            return await self._command_list_indexes(spec, session=session)
        if command_name == "createIndexes":
            return await self._command_create_indexes(spec, session=session)
        if command_name == "dropIndexes":
            return await self._command_drop_indexes(spec, session=session)
        if command_name == "dropDatabase":
            return await self._command_drop_database(session=session)
        if command_name == "collStats":
            collection_name = self._require_collection_name(spec.get("collStats"), "collStats")
            scale = self._normalize_scale_from_command(spec.get("scale"))
            return await self._collection_stats(collection_name, scale=scale, session=session)
        if command_name == "dbStats":
            scale = self._normalize_scale_from_command(spec.get("scale"))
            return await self._database_stats(scale=scale, session=session)
        if command_name == "validate":
            collection_name = self._require_collection_name(spec.get("validate"), "validate")
            return await self.validate_collection(
                collection_name,
                scandata=bool(spec.get("scandata", False)),
                full=bool(spec.get("full", False)),
                background=spec.get("background"),
                session=session,
            )
        raise OperationFailure(f"Unsupported command: {command_name}")

    async def _command_list_collections(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        name_only = spec.get("nameOnly", False)
        if not isinstance(name_only, bool):
            raise TypeError("nameOnly must be a bool")
        authorized_collections = spec.get("authorizedCollections", False)
        if not isinstance(authorized_collections, bool):
            raise TypeError("authorizedCollections must be a bool")
        filter_spec = self._normalize_filter(spec.get("filter"))
        if name_only:
            collection_names = await self._engine.list_collections(
                self._db_name,
                context=session,
            )
            first_batch = [
                {"name": name, "type": "collection"}
                for name in collection_names
            ]
            first_batch = [
                document
                for document in first_batch
                if QueryEngine.match(
                    document,
                    filter_spec,
                    dialect=self._mongodb_dialect,
                )
            ]
        else:
            first_batch = await self.list_collections(
                filter_spec,
                session=session,
            ).to_list()
        return {
            "cursor": {
                "id": 0,
                "ns": f"{self._db_name}.$cmd.listCollections",
                "firstBatch": first_batch,
            },
            "ok": 1.0,
        }

    async def _command_list_databases(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        name_only = spec.get("nameOnly", False)
        if not isinstance(name_only, bool):
            raise TypeError("nameOnly must be a bool")
        filter_spec = self._normalize_filter(spec.get("filter"))
        databases = await self._list_database_documents(session=session)
        filtered = [
            document
            for document in databases
            if QueryEngine.match(
                document,
                filter_spec,
                dialect=self._mongodb_dialect,
            )
        ]
        total_size = sum(int(document.get("sizeOnDisk", 0)) for document in filtered)
        if name_only:
            filtered = [{"name": str(document["name"])} for document in filtered]
        return {
            "databases": filtered,
            "totalSize": total_size,
            "ok": 1.0,
        }

    async def _command_create(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("create"), "create")
        options = {
            key: value
            for key, value in spec.items()
            if key not in {"create", "$db"}
        }
        await self.create_collection(
            collection_name,
            session=session,
            **options,
        )
        return {"ok": 1.0}

    async def _command_drop(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("drop"), "drop")
        await self.drop_collection(collection_name, session=session)
        return {
            "ns": f"{self._db_name}.{collection_name}",
            "ok": 1.0,
        }

    async def _command_rename_collection(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        source_db, source_name = self._normalize_namespace(
            spec.get("renameCollection"),
            "renameCollection",
        )
        target_db, target_name = self._normalize_namespace(spec.get("to"), "to")
        if source_db != self._db_name or target_db != self._db_name:
            raise OperationFailure(
                "renameCollection only supports namespaces in the current database"
            )
        drop_target = spec.get("dropTarget", False)
        if not isinstance(drop_target, bool):
            raise TypeError("dropTarget must be a bool")
        if drop_target:
            target_names = await self._engine.list_collections(
                self._db_name,
                context=session,
            )
            if target_name in target_names:
                await self.drop_collection(target_name, session=session)
        await self._database.get_collection(source_name).rename(target_name, session=session)
        return {"ok": 1.0}

    async def _command_count(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("count"), "count")
        query = self._normalize_filter(spec.get("query"))
        skip = spec.get("skip", 0)
        if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
            raise TypeError("skip must be a non-negative integer")
        limit = spec.get("limit")
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 0
        ):
            raise TypeError("limit must be a non-negative integer")
        hint = self._normalize_hint_from_command(spec.get("hint"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        count = len(
            await self._database.get_collection(collection_name).find(
                query,
                {"_id": 1},
                skip=skip,
                limit=limit,
                hint=hint,
                comment=spec.get("comment"),
                max_time_ms=max_time_ms,
                session=session,
            ).to_list()
        )
        return {"n": count, "ok": 1.0}

    async def _command_distinct(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("distinct"),
            "distinct",
        )
        key = spec.get("key")
        if not isinstance(key, str) or not key:
            raise TypeError("key must be a non-empty string")
        query = self._normalize_filter(spec.get("query"))
        hint = self._normalize_hint_from_command(spec.get("hint"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        distinct_values: list[object] = []
        async for document in self._database.get_collection(collection_name).find(
            query,
            sort=None,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            session=session,
        ):
            values = QueryEngine.extract_values(document, key)
            if not values:
                found, value = QueryEngine._get_field_value(document, key)
                if not found:
                    values = [None]
                elif isinstance(value, list):
                    continue
                else:
                    values = [value]
            candidates = values[1:] if isinstance(values[0], list) else values
            for candidate in candidates:
                if not any(
                    self._mongodb_dialect.values_equal(existing, candidate)
                    for existing in distinct_values
                ):
                    distinct_values.append(candidate)
        return {"values": distinct_values, "ok": 1.0}

    async def _command_insert(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("insert"), "insert")
        documents = self._normalize_insert_documents(spec.get("documents"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        collection = self._database.get_collection(collection_name)
        inserted = 0
        write_errors: list[dict[str, object]] = []
        for index, document in enumerate(documents):
            try:
                await collection.insert_one(document, session=session)
                inserted += 1
            except Exception as exc:
                write_errors.append(
                    {
                        "index": index,
                        "errmsg": str(exc),
                    }
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "insert command failed",
                details={
                    "writeErrors": write_errors,
                    "n": inserted,
                },
            )
        return {"n": inserted, "ok": 1.0}

    async def _command_update(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("update"), "update")
        updates = self._normalize_update_specs(spec.get("updates"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        collection = self._database.get_collection(collection_name)
        matched = 0
        modified = 0
        upserted: list[dict[str, object]] = []
        write_errors: list[dict[str, object]] = []
        for index, update_spec in enumerate(updates):
            try:
                query = self._normalize_filter(update_spec.get("q"))
                update_document = update_spec.get("u")
                if not isinstance(update_document, dict):
                    raise TypeError("u must be a document")
                multi = update_spec.get("multi", False)
                if not isinstance(multi, bool):
                    raise TypeError("multi must be a bool")
                upsert = update_spec.get("upsert", False)
                if not isinstance(upsert, bool):
                    raise TypeError("upsert must be a bool")
                array_filters = update_spec.get("arrayFilters")
                if array_filters is not None and (
                    not isinstance(array_filters, list)
                    or not all(is_filter(item) for item in array_filters)
                ):
                    raise TypeError("arrayFilters must be a list of dicts")
                hint = self._normalize_hint_from_command(update_spec.get("hint"))
                let = update_spec.get("let")
                if let is not None and not isinstance(let, dict):
                    raise TypeError("let must be a dict")

                is_operator_update = all(
                    isinstance(key, str) and key.startswith("$")
                    for key in update_document
                )
                if multi:
                    if not is_operator_update:
                        raise OperationFailure("replacement updates cannot be multi")
                    result = await collection.update_many(
                        query,
                        update_document,
                        upsert=upsert,
                        array_filters=array_filters,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                elif is_operator_update:
                    result = await collection.update_one(
                        query,
                        update_document,
                        upsert=upsert,
                        array_filters=array_filters,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                else:
                    result = await collection.replace_one(
                        query,
                        update_document,
                        upsert=upsert,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                matched += result.matched_count
                modified += result.modified_count
                if result.upserted_id is not None:
                    upserted.append({"index": index, "_id": result.upserted_id})
            except Exception as exc:
                write_errors.append(
                    {
                        "index": index,
                        "errmsg": str(exc),
                    }
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "update command failed",
                details={
                    "writeErrors": write_errors,
                    "n": matched,
                    "nModified": modified,
                    "upserted": upserted,
                },
            )
        result: dict[str, object] = {"n": matched, "nModified": modified, "ok": 1.0}
        if upserted:
            result["upserted"] = upserted
        return result

    async def _command_delete(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("delete"), "delete")
        deletes = self._normalize_delete_specs(spec.get("deletes"))
        ordered = self._normalize_ordered_from_command(spec.get("ordered"))
        collection = self._database.get_collection(collection_name)
        deleted = 0
        write_errors: list[dict[str, object]] = []
        for index, delete_spec in enumerate(deletes):
            try:
                query = self._normalize_filter(delete_spec.get("q"))
                limit = delete_spec.get("limit", 0)
                if limit not in (0, 1):
                    raise TypeError("limit must be 0 or 1")
                hint = self._normalize_hint_from_command(delete_spec.get("hint"))
                let = delete_spec.get("let")
                if let is not None and not isinstance(let, dict):
                    raise TypeError("let must be a dict")
                if limit == 1:
                    result = await collection.delete_one(
                        query,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                else:
                    result = await collection.delete_many(
                        query,
                        hint=hint,
                        comment=spec.get("comment"),
                        let=let,
                        session=session,
                    )
                deleted += result.deleted_count
            except Exception as exc:
                write_errors.append(
                    {
                        "index": index,
                        "errmsg": str(exc),
                    }
                )
                if ordered:
                    break
        if write_errors:
            raise BulkWriteError(
                "delete command failed",
                details={
                    "writeErrors": write_errors,
                    "n": deleted,
                },
            )
        return {"n": deleted, "ok": 1.0}

    async def _command_find(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(spec.get("find"), "find")
        query = self._normalize_filter(spec.get("filter"))
        projection = self._normalize_projection_from_command(spec.get("projection"))
        sort = self._normalize_sort_document(spec.get("sort"))
        hint = self._normalize_hint_from_command(spec.get("hint"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        skip = spec.get("skip", 0)
        if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
            raise TypeError("skip must be a non-negative integer")
        limit = spec.get("limit")
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 0
        ):
            raise TypeError("limit must be a non-negative integer")
        batch_size = self._normalize_batch_size_from_command(spec.get("batchSize"))
        first_batch = await self._database.get_collection(collection_name).find(
            query,
            projection,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            batch_size=batch_size,
            session=session,
        ).to_list()
        return {
            "cursor": {
                "id": 0,
                "ns": f"{self._db_name}.{collection_name}",
                "firstBatch": first_batch,
            },
            "ok": 1.0,
        }

    async def _command_aggregate(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("aggregate"),
            "aggregate",
        )
        pipeline = spec.get("pipeline")
        if not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")
        cursor_spec = spec.get("cursor", {})
        if not isinstance(cursor_spec, dict):
            raise TypeError("cursor must be a document")
        batch_size = self._normalize_batch_size_from_command(
            cursor_spec.get("batchSize")
        )
        hint = self._normalize_hint_from_command(spec.get("hint"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        let = spec.get("let")
        if let is not None and not isinstance(let, dict):
            raise TypeError("let must be a dict")
        first_batch = await self._database.get_collection(collection_name).aggregate(
            pipeline,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            batch_size=batch_size,
            let=let,
            session=session,
        ).to_list()
        return {
            "cursor": {
                "id": 0,
                "ns": f"{self._db_name}.{collection_name}",
                "firstBatch": first_batch,
            },
            "ok": 1.0,
        }

    async def _command_explain(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        explain_spec = spec.get("explain")
        if not isinstance(explain_spec, dict) or not explain_spec:
            raise TypeError("explain must be a non-empty document")
        verbosity = spec.get("verbosity")
        if verbosity is not None and not isinstance(verbosity, str):
            raise TypeError("verbosity must be a string")

        explained_command_name = next(iter(explain_spec))
        if explained_command_name == "find":
            collection_name = self._require_collection_name(
                explain_spec.get("find"),
                "find",
            )
            query = self._normalize_filter(explain_spec.get("filter"))
            projection = self._normalize_projection_from_command(
                explain_spec.get("projection")
            )
            sort = self._normalize_sort_document(explain_spec.get("sort"))
            hint = self._normalize_hint_from_command(explain_spec.get("hint"))
            max_time_ms = self._normalize_max_time_ms_from_command(
                explain_spec.get("maxTimeMS")
            )
            skip = explain_spec.get("skip", 0)
            if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
                raise TypeError("skip must be a non-negative integer")
            limit = explain_spec.get("limit")
            if limit is not None and (
                not isinstance(limit, int) or isinstance(limit, bool) or limit < 0
            ):
                raise TypeError("limit must be a non-negative integer")
            batch_size = self._normalize_batch_size_from_command(
                explain_spec.get("batchSize")
            )
            explanation = await self._database.get_collection(collection_name).find(
                query,
                projection,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                comment=explain_spec.get("comment"),
                max_time_ms=max_time_ms,
                batch_size=batch_size,
                session=session,
            ).explain()
            result = dict(explanation)
            result["batch_size"] = batch_size
        elif explained_command_name == "aggregate":
            collection_name = self._require_collection_name(
                explain_spec.get("aggregate"),
                "aggregate",
            )
            pipeline = explain_spec.get("pipeline")
            if not isinstance(pipeline, list):
                raise TypeError("pipeline must be a list")
            cursor_spec = explain_spec.get("cursor", {})
            if not isinstance(cursor_spec, dict):
                raise TypeError("cursor must be a document")
            batch_size = self._normalize_batch_size_from_command(
                cursor_spec.get("batchSize")
            )
            hint = self._normalize_hint_from_command(explain_spec.get("hint"))
            max_time_ms = self._normalize_max_time_ms_from_command(
                explain_spec.get("maxTimeMS")
            )
            let = explain_spec.get("let")
            if let is not None and not isinstance(let, dict):
                raise TypeError("let must be a dict")
            explanation = await self._database.get_collection(collection_name).aggregate(
                pipeline,
                hint=hint,
                comment=explain_spec.get("comment"),
                max_time_ms=max_time_ms,
                batch_size=batch_size,
                let=let,
                session=session,
            ).explain()
            result = dict(explanation)
        elif explained_command_name == "update":
            collection_name = self._require_collection_name(
                explain_spec.get("update"),
                "update",
            )
            updates = self._normalize_update_specs(explain_spec.get("updates"))
            if len(updates) != 1:
                raise OperationFailure("explain update requires exactly one update specification")
            update_spec = updates[0]
            query = self._normalize_filter(update_spec.get("q"))
            multi = update_spec.get("multi", False)
            if not isinstance(multi, bool):
                raise TypeError("multi must be a bool")
            hint = self._normalize_hint_from_command(update_spec.get("hint"))
            max_time_ms = self._normalize_max_time_ms_from_command(
                explain_spec.get("maxTimeMS")
            )
            explanation = await self._database.get_collection(collection_name).find(
                query,
                {"_id": 1},
                limit=None if multi else 1,
                hint=hint,
                comment=explain_spec.get("comment"),
                max_time_ms=max_time_ms,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "update"
            result["multi"] = multi
        elif explained_command_name == "delete":
            collection_name = self._require_collection_name(
                explain_spec.get("delete"),
                "delete",
            )
            deletes = self._normalize_delete_specs(explain_spec.get("deletes"))
            if len(deletes) != 1:
                raise OperationFailure("explain delete requires exactly one delete specification")
            delete_spec = deletes[0]
            query = self._normalize_filter(delete_spec.get("q"))
            limit = delete_spec.get("limit", 0)
            if limit not in (0, 1):
                raise TypeError("limit must be 0 or 1")
            hint = self._normalize_hint_from_command(delete_spec.get("hint"))
            max_time_ms = self._normalize_max_time_ms_from_command(
                explain_spec.get("maxTimeMS")
            )
            explanation = await self._database.get_collection(collection_name).find(
                query,
                {"_id": 1},
                limit=1 if limit == 1 else None,
                hint=hint,
                comment=explain_spec.get("comment"),
                max_time_ms=max_time_ms,
                session=session,
            ).explain()
            result = dict(explanation)
            result["command"] = "delete"
            result["limit"] = limit
        else:
            raise OperationFailure(
                f"Unsupported explain command: {explained_command_name}"
            )

        result.setdefault("ok", 1.0)
        if verbosity is not None:
            result["verbosity"] = verbosity
        return result

    async def _command_find_and_modify(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("findAndModify"),
            "findAndModify",
        )
        query = self._normalize_filter(spec.get("query"))
        sort = self._normalize_sort_document(spec.get("sort"))
        fields = spec.get("fields")
        remove = spec.get("remove", False)
        if not isinstance(remove, bool):
            raise TypeError("remove must be a bool")
        return_new = spec.get("new", False)
        if not isinstance(return_new, bool):
            raise TypeError("new must be a bool")
        upsert = spec.get("upsert", False)
        if not isinstance(upsert, bool):
            raise TypeError("upsert must be a bool")
        array_filters = spec.get("arrayFilters")
        if array_filters is not None and (
            not isinstance(array_filters, list)
            or not all(is_filter(item) for item in array_filters)
        ):
            raise TypeError("arrayFilters must be a list of dicts")
        hint = self._normalize_hint_from_command(spec.get("hint"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        let = spec.get("let")
        if let is not None and not isinstance(let, dict):
            raise TypeError("let must be a dict")

        update_spec = spec.get("update")
        collection = self._database.get_collection(collection_name)
        if remove:
            if update_spec is not None:
                raise OperationFailure("findAndModify remove and update cannot be specified together")
            if upsert:
                raise OperationFailure("findAndModify remove does not support upsert")
            before = await collection.find_one_and_delete(
                query,
                projection=fields,
                sort=sort,
                hint=hint,
                comment=spec.get("comment"),
                max_time_ms=max_time_ms,
                let=let,
                session=session,
            )
            return {
                "lastErrorObject": {"n": 0 if before is None else 1},
                "value": before,
                "ok": 1.0,
            }

        if update_spec is None:
            raise OperationFailure("findAndModify requires either remove or update")

        before_full = await collection.find(
            query,
            sort=sort,
            limit=1,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            session=session,
        ).first()
        return_document = ReturnDocument.AFTER if return_new else ReturnDocument.BEFORE

        if isinstance(update_spec, dict) and all(
            isinstance(key, str) and key.startswith("$")
            for key in update_spec
        ):
            if before_full is None and upsert:
                result = await collection.update_one(
                    query,
                    update_spec,
                    upsert=True,
                    sort=sort,
                    array_filters=array_filters,
                    hint=hint,
                    comment=spec.get("comment"),
                    let=let,
                    session=session,
                )
                value = None
                if return_new:
                    value = await collection.find(
                        {"_id": result.upserted_id},
                        fields,
                        limit=1,
                        session=session,
                    ).first()
                return {
                    "lastErrorObject": {
                        "n": 1,
                        "updatedExisting": False,
                        "upserted": result.upserted_id,
                    },
                    "value": value,
                    "ok": 1.0,
                }
            value = await collection.find_one_and_update(
                query,
                update_spec,
                projection=fields,
                sort=sort,
                upsert=upsert,
                return_document=return_document,
                array_filters=array_filters,
                hint=hint,
                comment=spec.get("comment"),
                max_time_ms=max_time_ms,
                let=let,
                session=session,
            )
            return {
                "lastErrorObject": {
                    "n": 0 if before_full is None and not upsert else 1,
                    "updatedExisting": before_full is not None,
                },
                "value": value,
                "ok": 1.0,
            }

        if not isinstance(update_spec, dict):
            raise TypeError("update must be a document")

        if before_full is None and upsert:
            result = await collection.replace_one(
                query,
                update_spec,
                upsert=True,
                sort=sort,
                hint=hint,
                comment=spec.get("comment"),
                let=let,
                session=session,
            )
            value = None
            if return_new:
                value = await collection.find(
                    {"_id": result.upserted_id},
                    fields,
                    limit=1,
                    session=session,
                ).first()
            return {
                "lastErrorObject": {
                    "n": 1,
                    "updatedExisting": False,
                    "upserted": result.upserted_id,
                },
                "value": value,
                "ok": 1.0,
            }

        value = await collection.find_one_and_replace(
            query,
            update_spec,
            projection=fields,
            sort=sort,
            upsert=upsert,
            return_document=return_document,
            hint=hint,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            let=let,
            session=session,
        )
        return {
            "lastErrorObject": {
                "n": 0 if before_full is None and not upsert else 1,
                "updatedExisting": before_full is not None,
            },
            "value": value,
            "ok": 1.0,
        }

    async def _command_list_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("listIndexes"),
            "listIndexes",
        )
        first_batch = await self._database.get_collection(collection_name).list_indexes(
            comment=spec.get("comment"),
            session=session,
        ).to_list()
        return {
            "cursor": {
                "id": 0,
                "ns": f"{self._db_name}.{collection_name}",
                "firstBatch": first_batch,
            },
            "ok": 1.0,
        }

    async def _command_create_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("createIndexes"),
            "createIndexes",
        )
        indexes = self._normalize_index_models_from_command(spec.get("indexes"))
        max_time_ms = self._normalize_max_time_ms_from_command(spec.get("maxTimeMS"))
        collection_names_before = set(
            await self._engine.list_collections(self._db_name, context=session)
        )
        collection = self._database.get_collection(collection_name)
        info_before = await collection.index_information(session=session)
        await collection.create_indexes(
            indexes,
            comment=spec.get("comment"),
            max_time_ms=max_time_ms,
            session=session,
        )
        info_after = await collection.index_information(session=session)
        result: dict[str, object] = {
            "numIndexesBefore": len(info_before),
            "numIndexesAfter": len(info_after),
            "createdCollectionAutomatically": collection_name not in collection_names_before,
            "ok": 1.0,
        }
        if len(info_before) == len(info_after):
            result["note"] = "all indexes already exist"
        return result

    async def _command_drop_indexes(
        self,
        spec: dict[str, object],
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_name = self._require_collection_name(
            spec.get("dropIndexes"),
            "dropIndexes",
        )
        target = spec.get("index")
        collection = self._database.get_collection(collection_name)
        info_before = await collection.index_information(session=session)
        if target == "*":
            await collection.drop_indexes(comment=spec.get("comment"), session=session)
        elif isinstance(target, (str, list, tuple, dict)):
            await collection.drop_index(target, comment=spec.get("comment"), session=session)
        else:
            raise TypeError("index must be '*', a name, or a key specification")
        return (
            {
                "nIndexesWas": len(info_before),
                "ok": 1.0,
                "msg": f"non-_id indexes for collection {self._db_name}.{collection_name} dropped",
            }
            if target == "*"
            else {
                "nIndexesWas": len(info_before),
                "ok": 1.0,
            }
        )

    async def _command_drop_database(
        self,
        *,
        session: ClientSession | None = None,
    ) -> dict[str, object]:
        collection_names = await self._engine.list_collections(
            self._db_name,
            context=session,
        )
        for collection_name in collection_names:
            await self._engine.drop_collection(
                self._db_name,
                collection_name,
                context=session,
            )
        return {"dropped": self._db_name, "ok": 1.0}


def build_info_document(mongodb_dialect: "MongoDialect") -> BuildInfoDocument:
    version_parts = [int(part) for part in mongodb_dialect.server_version.split(".")]
    while len(version_parts) < 2:
        version_parts.append(0)
    major, minor = version_parts[:2]
    return {
        "version": f"{major}.{minor}.0",
        "versionArray": [major, minor, 0, 0],
        "gitVersion": "mongoeco",
        "ok": 1.0,
    }


def hello_document(
    mongodb_dialect: "MongoDialect",
    *,
    legacy_name: bool = False,
) -> HelloDocument:
    document: HelloDocument = {
        "helloOk": True,
        "isWritablePrimary": True,
        "maxBsonObjectSize": 16 * 1024 * 1024,
        "maxMessageSizeBytes": 48_000_000,
        "maxWriteBatchSize": 100_000,
        "localTime": datetime.datetime.now(datetime.UTC),
        "logicalSessionTimeoutMinutes": 30,
        "connectionId": 1,
        "minWireVersion": 0,
        "maxWireVersion": 20,
        "readOnly": False,
        "ok": 1.0,
    }
    if legacy_name:
        document["ismaster"] = True
    else:
        document["isWritablePrimary"] = True
    document.update(build_info_document(mongodb_dialect))
    return document


_PROCESS_STARTED_AT = datetime.datetime.now(datetime.UTC)


def _storage_engine_name(engine: AsyncStorageEngine) -> str:
    engine_name = type(engine).__name__.lower()
    if "sqlite" in engine_name:
        return "sqlite"
    if "memory" in engine_name:
        return "memory"
    return type(engine).__name__


def server_status_document(
    mongodb_dialect: "MongoDialect",
    *,
    engine: AsyncStorageEngine,
) -> ServerStatusDocument:
    local_time = datetime.datetime.now(datetime.UTC)
    uptime_delta = local_time - _PROCESS_STARTED_AT
    uptime_seconds = max(uptime_delta.total_seconds(), 0.0)
    return {
        "host": socket.gethostname(),
        "version": build_info_document(mongodb_dialect)["version"],
        "process": "mongod",
        "pid": os.getpid(),
        "uptime": uptime_seconds,
        "uptimeMillis": int(uptime_seconds * 1000),
        "uptimeEstimate": int(uptime_seconds),
        "localTime": local_time,
        "connections": {
            "current": 1,
            "available": 8388607,
            "totalCreated": 1,
        },
        "storageEngine": {
            "name": _storage_engine_name(engine),
        },
        "ok": 1.0,
    }


def host_info_document() -> HostInfoDocument:
    return {
        "system": {
            "hostname": socket.gethostname(),
            "cpuArch": platform.machine() or "unknown",
            "numCores": os.cpu_count() or 1,
            "memSizeMB": 0,
        },
        "os": {
            "type": platform.system() or "unknown",
            "name": platform.platform(),
            "version": platform.version(),
        },
        "extra": {
            "pythonVersion": platform.python_version(),
        },
        "ok": 1.0,
    }


def whats_my_uri_document() -> WhatsMyUriDocument:
    return {
        "you": "127.0.0.1:0",
        "ok": 1.0,
    }


def cmd_line_opts_document() -> CmdLineOptsDocument:
    return {
        "argv": list(sys.argv),
        "parsed": {
            "net": {"bindIp": "127.0.0.1", "port": 0},
            "storage": {},
        },
        "ok": 1.0,
    }


SUPPORTED_DATABASE_COMMANDS: tuple[str, ...] = (
    "aggregate",
    "buildInfo",
    "collStats",
    "connectionStatus",
    "count",
    "create",
    "createIndexes",
    "dbStats",
    "delete",
    "distinct",
    "drop",
    "dropDatabase",
    "dropIndexes",
    "explain",
    "find",
    "findAndModify",
    "getCmdLineOpts",
    "hello",
    "hostInfo",
    "insert",
    "isMaster",
    "ismaster",
    "listCollections",
    "listCommands",
    "listDatabases",
    "listIndexes",
    "ping",
    "renameCollection",
    "serverStatus",
    "update",
    "validate",
    "whatsmyuri",
)


def list_commands_document() -> ListCommandsDocument:
    return {
        "commands": {
            command_name: {
                "help": f"mongoeco local support for the {command_name} command",
            }
            for command_name in SUPPORTED_DATABASE_COMMANDS
        },
        "ok": 1.0,
    }


def connection_status_document(*, show_privileges: bool) -> ConnectionStatusDocument:
    auth_info: dict[str, object] = {
        "authenticatedUsers": [],
        "authenticatedUserRoles": [],
    }
    if show_privileges:
        auth_info["authenticatedUserPrivileges"] = []
    return {
        "authInfo": auth_info,
        "ok": 1.0,
    }
