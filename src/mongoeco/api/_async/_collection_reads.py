from __future__ import annotations

import time

from collections.abc import Mapping
from typing import TYPE_CHECKING

from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.api._async._response import ResponseBatchReader
from mongoeco.api.bson_input import encode_operation_bson
from mongoeco.api.operations import compile_find_operation
from mongoeco.api.public_api import (
    COLLECTION_COUNT_DOCUMENTS_SPEC,
    COLLECTION_DISTINCT_SPEC,
    COLLECTION_FIND_ONE_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.core._filtering_support import query_array_members
from mongoeco.core._public_materialization import materialize_public_value
from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.error_documents import command_errors, error_message
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.server_errors import response_operation_error


if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.api.argument_validation import ProjectionInput
    from mongoeco.types import (
        CollationDocument,
        Document,
        Filter,
        HintSpec,
    )


_FILTER_UNSET = object()


@command_errors
async def find_one(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    projection: ProjectionInput | None,
    *,
    filter: Filter | object = _FILTER_UNSET,
    collation: CollationDocument | None = None,
    session: object | None = None,
    extra_kwargs: dict[str, object] | None = None,
) -> Document | None:
    options = normalize_public_operation_arguments(
        COLLECTION_FIND_ONE_SPEC,
        explicit={
            "filter_spec": filter_spec,
            "projection": projection,
            "collation": collation,
            "session": session,
        },
        extra_kwargs={"filter": filter, **(extra_kwargs or {})},
        profile=collection._pymongo_profile,
    )
    query = options.get("filter_spec")
    if query is not None and not isinstance(query, Mapping):
        options["filter_spec"] = {"_id": query}
    options = encode_operation_bson(options, codec_options=collection._codec_options)
    operation = compile_find_operation(
        collection._normalize_filter(options.get("filter_spec")),
        projection=collection._normalize_projection(options.get("projection")),
        collation=options.get("collation"),
        sort=options.get("sort"),
        skip=options.get("skip", 0),
        limit=1,
        hint=options.get("hint"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
        variables=collection._normalize_let(options.get("let")),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    expressions = collection._new_execution_context().with_command_let(
        operation.let, dialect=collection._mongodb_dialect
    )
    operation_context = collection._new_operation_context(
        session=options.get("session"),
        collation=operation.collation,
        expressions=expressions,
    )
    operation = operation.bind(
        await collection._resolve_operation_context(operation_context)
    )
    document = None
    started_at = time.perf_counter_ns()
    record_runtime_opcounter = getattr(
        collection._engine, "_record_runtime_opcounter", None
    )
    if callable(record_runtime_opcounter):
        record_runtime_opcounter("query")
    try:
        with track_active_operation(
            collection._engine,
            command_name="find",
            operation_type="read",
            namespace=f"{collection._db_name}.{collection._collection_name}",
            session=options.get("session"),
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            metadata={"kind": "find_one"},
        ):
            if (
                operation.collation is None
                and operation.sort is None
                and operation.skip == 0
                and operation.hint is None
                and operation.comment is None
                and operation.max_time_ms is None
                and collection._can_use_direct_id_lookup(operation.filter_spec)
            ):
                document = await collection._runtime.engine_get_document(
                    operation.filter_spec["_id"],
                    projection=operation.projection,
                    operation_context=operation.context,
                )
            if document is None:
                async for candidate in collection._engine_scan_with_operation(
                    operation,
                    session=options.get("session"),
                ):
                    document = candidate
                    break
        documents = iter([] if document is None else [document])

        async def pull(_remaining):
            try:
                return next(documents)
            except StopIteration:
                raise StopAsyncIteration from None

        batch = await ResponseBatchReader(dialect=collection._mongodb_dialect).read(
            pull, 1, initial=True
        )
    except Exception as exc:
        resolved = response_operation_error(
            exc, "find_runtime", collection._mongodb_dialect.key,
            initial=True, namespace=collection.full_name,
        )
        await collection._profile_operation(
            op="query",
            command={
                "find": collection._collection_name,
                "filter": operation.filter_spec,
            },
            duration_ns=time.perf_counter_ns() - started_at,
            operation=operation,
            errmsg=error_message(resolved),
        )
        if resolved is not exc:
            raise resolved from exc
        raise
    await collection._profile_operation(
        op="query",
        command={"find": collection._collection_name, "filter": operation.filter_spec},
        duration_ns=time.perf_counter_ns() - started_at,
        operation=operation,
    )
    decoded, _count = collection._materialize_query_response(
        batch, cursor_id=0, initial=True
    )
    return decoded[0] if decoded else None


@command_errors
async def count_documents(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    *,
    filter: Filter | object = _FILTER_UNSET,
    collation: CollationDocument | None = None,
    hint: HintSpec | None = None,
    comment: object | None = None,
    max_time_ms: int | None = None,
    skip: int = 0,
    limit: int | None = None,
    let: dict[str, object] | None = None,
    session: object | None = None,
    extra_kwargs: dict[str, object] | None = None,
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
            "let": let,
            "session": session,
        },
        extra_kwargs={"filter": filter, **(extra_kwargs or {})},
        profile=collection._pymongo_profile,
    )
    options = encode_operation_bson(options, codec_options=collection._codec_options)
    operation = compile_find_operation(
        collection._normalize_filter(options["filter_spec"]),
        projection={"_id": 1},
        collation=options.get("collation"),
        skip=options.get("skip", 0),
        limit=options.get("limit"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
        variables=collection._normalize_let(options.get("let")),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation_context = collection._new_operation_context(
        session=options.get("session"),
        collation=operation.collation,
        bindings=operation.let,
    )
    operation = operation.bind(
        await collection._resolve_operation_context(operation_context)
    )
    with track_active_operation(
        collection._engine,
        command_name="count",
        operation_type="read",
        namespace=f"{collection._db_name}.{collection._collection_name}",
        session=options.get("session"),
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
    ):
        count = await collection._engine_count_with_operation(
            operation, session=options.get("session")
        )
    collection._record_operation_metadata(
        operation="count_documents",
        comment=operation.comment,
        hint=operation.hint,
        max_time_ms=operation.max_time_ms,
        session=options.get("session"),
    )
    return count


async def estimated_document_count(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: object | None = None,
) -> int:
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    count = len(
        await collection.find(
            {},
            {"_id": 1},
            comment=comment,
            max_time_ms=max_time_ms,
            session=session,
        ).to_list()
    )
    collection._record_operation_metadata(
        operation="estimated_document_count",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    return count


@command_errors
async def distinct(
    collection: AsyncCollection,
    key: str,
    filter_spec: Filter | object,
    *,
    filter: Filter | object = _FILTER_UNSET,
    collation: CollationDocument | None = None,
    hint: HintSpec | None = None,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: object | None = None,
    extra_kwargs: dict[str, object] | None = None,
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
        extra_kwargs={"filter": filter, **(extra_kwargs or {})},
        profile=collection._pymongo_profile,
    )
    if not isinstance(options["key"], str):
        raise TypeError("key must be a string")
    options = encode_operation_bson(options, codec_options=collection._codec_options)
    operation_context = await collection._resolve_operation_context(
        collection._new_operation_context(
            session=options.get("session"), collation=options.get("collation")
        )
    )
    normalized_collation = operation_context.collation
    distinct_values: list[object] = []
    with track_active_operation(
        collection._engine,
        command_name="distinct",
        operation_type="read",
        namespace=f"{collection._db_name}.{collection._collection_name}",
        session=options.get("session"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
    ):
        operation = compile_find_operation(
            options.get("filter_spec"), collation=options.get("collation"),
            hint=options.get("hint"), comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            dialect=collection._mongodb_dialect,
            planning_mode=collection._planning_mode,
        )
        cursor = collection._build_cursor(
            operation.bind(operation_context), session=options.get("session")
        )
        cursor._expression_error_operation = "distinct"
        # Compare stored values before response decoding can erase field order.
        cursor._apply_codec_options = False
        distinct_values = await collect_distinct_values(
            cursor, options["key"], dialect=collection._mongodb_dialect,
            collation=normalized_collation,
        )
    collection._record_operation_metadata(
        operation="distinct",
        comment=options.get("comment"),
        hint=options.get("hint"),
        max_time_ms=options.get("max_time_ms"),
        session=options.get("session"),
    )
    return DocumentCodec.apply_codec_options(
        materialize_public_value(distinct_values),
        codec_options=collection._codec_options,
    )


async def collect_distinct_values(cursor, key, *, dialect, collation):
    """Select semantic values before any public response decoding."""
    distinct_values = []
    async for document in cursor:
        candidates = resolve_distinct_candidates(document, key)
        for candidate in candidates:
            if not any(
                QueryEngine._values_equal(
                    existing, candidate, dialect=dialect, collation=collation
                )
                for existing in distinct_values
            ):
                distinct_values.append(candidate)
    return distinct_values


def resolve_distinct_candidates(
    document: object,
    key: str,
) -> list[object]:
    return [
        value for value in query_array_members(document, key) if value is not MISSING
    ]
