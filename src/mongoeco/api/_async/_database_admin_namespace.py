from __future__ import annotations

from mongoeco.api.admin_parsing import normalize_validate_command_options, resolve_collection_reference
from mongoeco.core.aggregation import _bson_document_size
from mongoeco.core.filtering import QueryEngine
from mongoeco.errors import CollectionInvalid
from mongoeco.engines._shared_ttl import coerce_ttl_datetime
from mongoeco.session import ClientSession
from mongoeco.types import CollectionValidationSnapshot, DatabaseListingDocument, DatabaseListingSnapshot, DatabaseStatsSnapshot


class DatabaseNamespaceAdminService:
    def __init__(self, admin_service):
        self._admin = admin_service

    async def collection_stats(self, collection_name: str, *, scale: int = 1, session: ClientSession | None = None):
        collection = self._admin._database.get_collection(collection_name)
        documents = await collection.find({}, session=session).to_list()
        indexes = await collection.list_indexes(session=session).to_list()
        data_size = sum(_bson_document_size(document) for document in documents)
        total_index_size = sum(_bson_document_size(document) for document in indexes)
        from mongoeco.types import CollectionStatsSnapshot

        return CollectionStatsSnapshot(
            namespace=f"{self._admin._db_name}.{collection_name}",
            count=len(documents),
            data_size=data_size,
            index_count=len(indexes),
            total_index_size=total_index_size,
            scale=scale,
        )

    async def database_stats(self, *, scale: int = 1, session: ClientSession | None = None) -> DatabaseStatsSnapshot:
        collection_names = await self._admin._engine.list_collections(self._admin._db_name, context=session)
        objects = 0
        data_size = 0
        indexes = 0
        index_size = 0
        for collection_name in collection_names:
            stats = await self.collection_stats(collection_name, scale=1, session=session)
            objects += stats.count
            data_size += stats.data_size
            indexes += stats.index_count
            index_size += stats.total_index_size
        return DatabaseStatsSnapshot(
            db_name=self._admin._db_name,
            collection_count=len(collection_names),
            object_count=objects,
            data_size=data_size,
            index_count=indexes,
            index_size=index_size,
            scale=scale,
        )

    async def list_database_snapshots(self, *, session: ClientSession | None = None) -> list[DatabaseListingSnapshot]:
        database_names = await self._admin._engine.list_databases(context=session)
        snapshots: list[DatabaseListingSnapshot] = []
        for database_name in database_names:
            database = type(self._admin._database)(
                self._admin._engine,
                database_name,
                mongodb_dialect=self._admin._database._mongodb_dialect,
                mongodb_dialect_resolution=self._admin._database._mongodb_dialect_resolution,
                pymongo_profile=self._admin._database._pymongo_profile,
                pymongo_profile_resolution=self._admin._database._pymongo_profile_resolution,
                write_concern=self._admin._database._write_concern,
                read_concern=self._admin._database._read_concern,
                read_preference=self._admin._database._read_preference,
                codec_options=self._admin._database._codec_options,
            )
            stats = await database._admin._namespace_admin.database_stats(session=session)
            snapshots.append(
                DatabaseListingSnapshot(
                    name=database_name,
                    size_on_disk=stats.data_size,
                    empty=(stats.collection_count == 0 and stats.object_count == 0),
                )
            )
        return snapshots

    async def list_database_documents(self, *, session: ClientSession | None = None) -> list[DatabaseListingDocument]:
        return [
            snapshot.to_document()
            for snapshot in await self.list_database_snapshots(session=session)
        ]

    async def build_collection_validation_snapshot(
        self,
        name_or_collection: object,
        *,
        scandata: bool = False,
        full: bool = False,
        background: bool | None = None,
        session: ClientSession | None = None,
        comment: object | None = None,
    ) -> CollectionValidationSnapshot:
        normalize_validate_command_options(
            scandata=scandata,
            full=full,
            background=background,
            comment=comment,
        )
        collection_name = resolve_collection_reference(name_or_collection, "name_or_collection")
        collection_names = await self._admin._engine.list_collections(self._admin._db_name, context=session)
        if collection_name not in collection_names:
            raise CollectionInvalid(f"collection '{collection_name}' does not exist")

        collection = self._admin._database.get_collection(collection_name)
        documents = await collection.find({}, session=session).to_list()
        indexes = await collection.list_indexes(session=session).to_list()
        warnings: list[object] = []
        if scandata:
            warnings.append(
                "validate scandata is accepted for compatibility but does not change local validation behavior"
            )
        if full:
            warnings.append(
                "validate full is accepted for compatibility but does not change local validation behavior"
            )
        if background is not None:
            warnings.append(
                "validate background is accepted for compatibility but validation runs synchronously in mongoeco"
            )
        warnings.extend(self._ttl_validation_warnings(collection_name, documents=documents, indexes=indexes))
        return CollectionValidationSnapshot(
            namespace=f"{self._admin._db_name}.{collection_name}",
            record_count=len(documents),
            index_count=len(indexes),
            keys_per_index={
                str(index["name"]): len(index.get("key", index.get("fields", [])))
                for index in indexes
            },
            warnings=warnings,
        )

    def _ttl_validation_warnings(
        self,
        collection_name: str,
        *,
        documents: list[dict[str, object]],
        indexes: list[dict[str, object]],
    ) -> list[str]:
        warnings: list[str] = []
        for index in indexes:
            expire_after_seconds = index.get("expireAfterSeconds")
            if not isinstance(expire_after_seconds, int) or isinstance(expire_after_seconds, bool):
                continue
            key_document = index.get("key")
            ttl_field: str | None = None
            if isinstance(key_document, dict) and len(key_document) == 1:
                ttl_field = next(iter(key_document))
            raw_fields = index.get("fields")
            if ttl_field is None and isinstance(raw_fields, list) and len(raw_fields) == 1 and isinstance(raw_fields[0], str):
                ttl_field = raw_fields[0]
            if not isinstance(ttl_field, str) or not ttl_field:
                continue
            invalid_document_count = 0
            for document in documents:
                values = QueryEngine.extract_values(document, ttl_field)
                if not values:
                    continue
                if not any(coerce_ttl_datetime(value) is not None for value in values):
                    invalid_document_count += 1
            if invalid_document_count:
                index_name = str(index.get("name", ttl_field))
                warnings.append(
                    f"TTL index '{index_name}' on '{collection_name}.{ttl_field}' has {invalid_document_count} document(s) with no date values; those documents will not expire under local TTL semantics"
                )
        return warnings
