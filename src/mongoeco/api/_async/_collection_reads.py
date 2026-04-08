from __future__ import annotations

import time
from typing import TYPE_CHECKING

from mongoeco.api.public_api import (
    COLLECTION_COUNT_DOCUMENTS_SPEC,
    COLLECTION_DISTINCT_SPEC,
    COLLECTION_FIND_ONE_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.api.operations import compile_find_operation
from mongoeco.core.collation import normalize_collation
from mongoeco.core.filtering import QueryEngine

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.types import (
        CollationDocument,
        Document,
        Filter,
        HintSpec,
        Projection,
    )


_FILTER_UNSET = object()


async def find_one(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    projection: Projection | None,
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
    operation = compile_find_operation(
        options.get("filter_spec"),
        projection=options.get("projection"),
        collation=options.get("collation"),
        sort=options.get("sort"),
        skip=options.get("skip", 0),
        limit=1,
        hint=options.get("hint"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    document = None
    started_at = time.perf_counter_ns()
    record_runtime_opcounter = getattr(collection._engine, "_record_runtime_opcounter", None)
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
                document = await collection._engine.get_document(
                    collection._db_name,
                    collection._collection_name,
                    operation.filter_spec["_id"],
                    projection=operation.projection,
                    dialect=collection._mongodb_dialect,
                    context=options.get("session"),
                )
            else:
                async for candidate in collection._engine_scan_with_operation(
                    operation,
                    session=options.get("session"),
                ):
                    document = candidate
                    break
    except Exception as exc:
        await collection._profile_operation(
            op="query",
            command={"find": collection._collection_name, "filter": operation.filter_spec},
            duration_ns=time.perf_counter_ns() - started_at,
            operation=operation,
            errmsg=str(exc),
        )
        raise
    await collection._profile_operation(
        op="query",
        command={"find": collection._collection_name, "filter": operation.filter_spec},
        duration_ns=time.perf_counter_ns() - started_at,
        operation=operation,
    )
    return collection._apply_codec_options_to_optional_document(document)


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
            "session": session,
        },
        extra_kwargs={"filter": filter, **(extra_kwargs or {})},
        profile=collection._pymongo_profile,
    )
    operation = compile_find_operation(
        options["filter_spec"],
        projection={"_id": 1},
        collation=options.get("collation"),
        skip=options.get("skip", 0),
        limit=options.get("limit"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
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
        count = await collection._engine_count_with_operation(operation, session=options.get("session"))
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
    normalized_collation = normalize_collation(options.get("collation"))
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
        async for document in collection.find(
            options.get("filter_spec"),
            collation=options.get("collation"),
            hint=options.get("hint"),
            comment=options.get("comment"),
            max_time_ms=options.get("max_time_ms"),
            session=options.get("session"),
        ):
            values = QueryEngine.extract_values(document, options["key"])
            found, value = QueryEngine._get_field_value(document, options["key"])
            candidates = resolve_distinct_candidates(
                values,
                exact_found=found,
                exact_value=value,
            )
            for candidate in candidates:
                if not any(
                    QueryEngine._values_equal(
                        existing,
                        candidate,
                        dialect=collection._mongodb_dialect,
                        collation=normalized_collation,
                    )
                    for existing in distinct_values
                ):
                    distinct_values.append(candidate)
    collection._record_operation_metadata(
        operation="distinct",
        comment=options.get("comment"),
        hint=options.get("hint"),
        max_time_ms=options.get("max_time_ms"),
        session=options.get("session"),
    )
    return distinct_values


def resolve_distinct_candidates(
    values: list[object],
    *,
    exact_found: bool,
    exact_value: object,
) -> list[object]:
    if not values:
        if not exact_found:
            return [None]
        if isinstance(exact_value, list):
            return []
        return [exact_value]
    if not exact_found or not isinstance(exact_value, list):
        return values
    if exact_value == [] and values == [[]]:
        return []
    if values and values[0] == exact_value:
        expanded_members = list(exact_value)
        if values[1:] == expanded_members:
            return expanded_members
    return values
