from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.api.admin_parsing import (
    normalize_command_batch_size,
    normalize_command_document,
    normalize_command_hint,
    normalize_command_max_time_ms,
    normalize_command_projection,
    normalize_command_sort_document,
    normalize_delete_specs,
    normalize_filter_document,
    normalize_index_models_from_command,
    normalize_insert_documents,
    normalize_namespace,
    normalize_update_specs,
    require_collection_name,
    resolve_collection_reference,
)
from mongoeco.api.operations import (
    AggregateOperation,
    FindOperation,
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.types import Document, Filter, IndexModel, Projection

if TYPE_CHECKING:
    from mongoeco.api._async.database_admin import AsyncDatabaseAdminService


class DatabaseAdminCommandCompiler:
    def __init__(self, admin: "AsyncDatabaseAdminService") -> None:
        self._admin = admin

    @staticmethod
    def normalize_filter(filter_spec: object | None) -> Filter:
        return normalize_filter_document(filter_spec)

    @staticmethod
    def normalize_command(command: object, kwargs: dict[str, object]) -> dict[str, object]:
        return normalize_command_document(command, kwargs)

    @staticmethod
    def require_collection_name(value: object, field_name: str) -> str:
        return require_collection_name(value, field_name)

    @staticmethod
    def resolve_collection_reference(value: object, field_name: str) -> str:
        return resolve_collection_reference(value, field_name)

    @staticmethod
    def normalize_index_models(indexes: object) -> list[IndexModel]:
        return normalize_index_models_from_command(indexes)

    @staticmethod
    def normalize_sort_document(sort: object | None) -> list[tuple[str, int]] | None:
        return normalize_command_sort_document(sort)

    @staticmethod
    def normalize_hint(hint: object | None) -> object | None:
        return normalize_command_hint(hint)

    @staticmethod
    def normalize_max_time_ms(max_time_ms: object | None) -> int | None:
        return normalize_command_max_time_ms(max_time_ms)

    @staticmethod
    def normalize_projection(projection: object | None) -> dict[str, object] | None:
        return normalize_command_projection(projection)

    @staticmethod
    def normalize_batch_size(batch_size: object | None) -> int | None:
        return normalize_command_batch_size(batch_size)

    def compile_find_operation(
        self,
        spec: dict[str, object],
        *,
        collection_field: str,
        filter_field: str = "filter",
        projection_field: str = "projection",
        sort_field: str = "sort",
        hint_field: str = "hint",
        comment_field: str = "comment",
        max_time_ms_field: str = "maxTimeMS",
        skip_field: str = "skip",
        limit_field: str = "limit",
        batch_size_field: str | None = "batchSize",
        default_projection: Projection | None = None,
        default_limit: int | None = None,
    ) -> tuple[str, FindOperation]:
        collection_name = self.require_collection_name(
            spec.get(collection_field),
            collection_field,
        )
        skip = spec.get(skip_field, 0)
        if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
            raise TypeError("skip must be a non-negative integer")
        limit = spec.get(limit_field, default_limit)
        if limit is not None and (
            not isinstance(limit, int) or isinstance(limit, bool) or limit < 0
        ):
            raise TypeError("limit must be a non-negative integer")
        batch_size = (
            None
            if batch_size_field is None
            else self.normalize_batch_size(spec.get(batch_size_field))
        )
        return (
            collection_name,
            compile_find_operation(
                self.normalize_filter(spec.get(filter_field)),
                projection=normalize_command_projection(
                    spec.get(projection_field, default_projection)
                ),
                collation=spec.get("collation"),
                sort=normalize_command_sort_document(spec.get(sort_field)),
                skip=skip,
                limit=limit,
                hint=normalize_command_hint(spec.get(hint_field)),
                comment=spec.get(comment_field),
                max_time_ms=normalize_command_max_time_ms(spec.get(max_time_ms_field)),
                batch_size=batch_size,
                dialect=self._admin._mongodb_dialect,
            ),
        )

    def compile_aggregate_operation(
        self,
        spec: dict[str, object],
        *,
        collection_field: str = "aggregate",
        pipeline_field: str = "pipeline",
        cursor_field: str = "cursor",
        batch_size_field: str = "batchSize",
        hint_field: str = "hint",
        comment_field: str = "comment",
        max_time_ms_field: str = "maxTimeMS",
        allow_disk_use_field: str = "allowDiskUse",
        let_field: str = "let",
    ) -> tuple[str, AggregateOperation]:
        collection_name = self.require_collection_name(
            spec.get(collection_field),
            collection_field,
        )
        pipeline = spec.get(pipeline_field)
        if not isinstance(pipeline, list):
            raise TypeError("pipeline must be a list")
        cursor_spec = spec.get(cursor_field, {})
        if not isinstance(cursor_spec, dict):
            raise TypeError("cursor must be a document")
        return (
            collection_name,
            compile_aggregate_operation(
                pipeline,
                collation=spec.get("collation"),
                hint=self.normalize_hint(spec.get(hint_field)),
                comment=spec.get(comment_field),
                max_time_ms=self.normalize_max_time_ms(spec.get(max_time_ms_field)),
                batch_size=self.normalize_batch_size(cursor_spec.get(batch_size_field)),
                allow_disk_use=spec.get(allow_disk_use_field),
                let=spec.get(let_field),
            ),
        )

    def compile_update_selection_operation(
        self,
        update_spec: dict[str, object],
        *,
        comment: object | None,
        max_time_ms: object | None,
        limit: int | None,
    ) -> FindOperation:
        return compile_find_selection_from_update_operation(
            compile_update_operation(
                self.normalize_filter(update_spec.get("q")),
                collation=update_spec.get("collation"),
                hint=self.normalize_hint(update_spec.get("hint")),
                comment=comment,
                max_time_ms=self.normalize_max_time_ms(max_time_ms),
                let=update_spec.get("let"),
                dialect=self._admin._mongodb_dialect,
            ),
            projection={"_id": 1},
            limit=limit,
        )

    def compile_delete_selection_operation(
        self,
        delete_spec: dict[str, object],
        *,
        comment: object | None,
        max_time_ms: object | None,
        limit: int | None,
    ) -> FindOperation:
        return compile_find_selection_from_update_operation(
            compile_update_operation(
                self.normalize_filter(delete_spec.get("q")),
                collation=delete_spec.get("collation"),
                hint=self.normalize_hint(delete_spec.get("hint")),
                comment=comment,
                max_time_ms=self.normalize_max_time_ms(max_time_ms),
                let=delete_spec.get("let"),
                dialect=self._admin._mongodb_dialect,
            ),
            projection={"_id": 1},
            limit=limit,
        )

    def compile_count_operation(self, spec: dict[str, object]) -> tuple[str, FindOperation]:
        return self.compile_find_operation(
            spec,
            collection_field="count",
            filter_field="query",
            default_projection={"_id": 1},
        )

    def compile_distinct_operation(self, spec: dict[str, object]) -> tuple[str, str, FindOperation]:
        collection_name = self.require_collection_name(
            spec.get("distinct"),
            "distinct",
        )
        key = spec.get("key")
        if not isinstance(key, str) or not key:
            raise TypeError("key must be a non-empty string")
        _, operation = self.compile_find_operation(
            spec,
            collection_field="distinct",
            filter_field="query",
            batch_size_field=None,
        )
        return collection_name, key, operation

    def compile_find_and_modify_selection_operation(
        self,
        options,
        *,
        projection: Projection | None = None,
        limit: int | None = 1,
    ) -> FindOperation:
        return compile_find_operation(
            options.query,
            projection=projection,
            sort=options.sort,
            limit=limit,
            hint=options.hint,
            comment=options.comment,
            max_time_ms=options.max_time_ms,
            dialect=self._admin._mongodb_dialect,
        )

    def compile_id_lookup_operation(
        self,
        document_id: object,
        *,
        projection: Projection | None = None,
    ) -> FindOperation:
        return compile_find_operation(
            {"_id": document_id},
            projection=projection,
            limit=1,
            dialect=self._admin._mongodb_dialect,
        )

    @staticmethod
    def normalize_namespace(value: object, field_name: str) -> tuple[str, str]:
        return normalize_namespace(value, field_name)

    @staticmethod
    def normalize_insert_documents(spec: object) -> list[Document]:
        return normalize_insert_documents(spec)

    @staticmethod
    def normalize_update_specs(spec: object) -> list[dict[str, object]]:
        return normalize_update_specs(spec)

    @staticmethod
    def normalize_delete_specs(spec: object) -> list[dict[str, object]]:
        return normalize_delete_specs(spec)
