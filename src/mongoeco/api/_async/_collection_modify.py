from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from mongoeco.api.operations import (
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.api.public_api import (
    COLLECTION_DELETE_MANY_SPEC,
    COLLECTION_DELETE_ONE_SPEC,
    COLLECTION_FIND_ONE_AND_DELETE_SPEC,
    COLLECTION_FIND_ONE_AND_REPLACE_SPEC,
    COLLECTION_FIND_ONE_AND_UPDATE_SPEC,
    COLLECTION_REPLACE_ONE_SPEC,
    COLLECTION_UPDATE_MANY_SPEC,
    COLLECTION_UPDATE_ONE_SPEC,
    normalize_public_operation_arguments,
)
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.upserts import seed_upsert_document
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    ArrayFilters,
    CollationDocument,
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    ObjectId,
    Projection,
    ReturnDocument,
    SortSpec,
    Update,
    UpdateResult,
)

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection


async def perform_upsert_update(
    collection: AsyncCollection,
    filter_spec: Filter,
    update_spec: Update,
    *,
    session: ClientSession | None = None,
    array_filters: ArrayFilters | None = None,
    let: dict[str, object] | None = None,
    bypass_document_validation: bool = False,
) -> UpdateResult[DocumentId]:
    new_doc: Document = {}
    seed_upsert_document(new_doc, filter_spec)
    UpdateEngine.apply_update(
        new_doc,
        update_spec,
        dialect=collection._mongodb_dialect,
        array_filters=array_filters,
        is_upsert_insert=True,
        variables=let,
    )
    if "_id" not in new_doc:
        new_doc["_id"] = ObjectId()
    await collection._put_replacement_document(
        new_doc,
        overwrite=False,
        session=session,
        bypass_document_validation=bypass_document_validation,
    )
    collection._publish_change_event(
        operation_type="insert",
        document_key={"_id": deepcopy(new_doc["_id"])},
        full_document=deepcopy(new_doc),
    )
    return UpdateResult(
        matched_count=0,
        modified_count=0,
        upserted_id=new_doc["_id"],
    )


async def update_one(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    update_spec: Update | object,
    upsert: bool,
    *,
    filter: Filter | object,
    update: Update | object,
    collation: CollationDocument | None,
    sort: SortSpec | None,
    array_filters: ArrayFilters | None,
    hint: object | None,
    comment: object | None,
    let: dict[str, object] | None,
    bypass_document_validation: bool,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, "update": update, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    update_spec = collection._require_update(options["update_spec"])
    upsert = bool(options.get("upsert", False))
    bypass_document_validation = bool(options.get("bypass_document_validation", False))
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        sort=options.get("sort"),
        array_filters=options.get("array_filters"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    event_selected_id: DocumentId | None = None
    if collection._change_hub is not None and operation.sort is None and operation.hint is None:
        selected = await collection._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={"_id": 1},
                limit=1,
            ),
            session=session,
        ).first()
        if selected is not None:
            event_selected_id = selected["_id"]
    if operation.sort is not None:
        selected = await collection._build_cursor(
            compile_find_selection_from_update_operation(operation, limit=1),
            session=session,
        ).first()
        if selected is None and not upsert:
            return UpdateResult(matched_count=0, modified_count=0)
        if selected is not None:
            identity_filter = {"_id": selected["_id"]}
            identity_plan = compile_filter(identity_filter, dialect=collection._mongodb_dialect)
            result = await collection._engine_update_with_operation(
                operation.with_overrides(
                    filter_spec=identity_filter,
                    plan=identity_plan,
                    sort=None,
                    hint=None,
                ),
                upsert=False,
                selector_filter=operation.filter_spec,
                session=session,
                bypass_document_validation=bypass_document_validation,
            )
            collection._record_operation_metadata(
                operation="update_one",
                comment=operation.comment,
                hint=operation.hint,
                session=session,
            )
            updated = await collection._document_by_id(selected["_id"], session=session)
            if updated is not None:
                collection._publish_change_event(
                    operation_type="update",
                    document_key={"_id": deepcopy(selected["_id"])},
                    full_document=deepcopy(updated),
                )
            return result
        return await perform_upsert_update(
            collection,
            operation.filter_spec,
            update_spec,
            session=session,
            array_filters=operation.array_filters,
            let=operation.let,
            bypass_document_validation=bypass_document_validation,
        )
    if operation.hint is not None:
        selected = await collection._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={"_id": 1},
                limit=1,
            ),
            session=session,
        ).first()
        if selected is None:
            if upsert:
                return await perform_upsert_update(
                    collection,
                    operation.filter_spec,
                    update_spec,
                    session=session,
                    array_filters=operation.array_filters,
                    let=operation.let,
                    bypass_document_validation=bypass_document_validation,
                )
            return UpdateResult(matched_count=0, modified_count=0)
        identity_filter = {"_id": selected["_id"]}
        identity_plan = compile_filter(identity_filter, dialect=collection._mongodb_dialect)
        result = await collection._engine_update_with_operation(
            operation.with_overrides(
                filter_spec=identity_filter,
                plan=identity_plan,
                hint=None,
            ),
            upsert=False,
            selector_filter=operation.filter_spec,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        collection._record_operation_metadata(
            operation="update_one",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        updated = await collection._document_by_id(selected["_id"], session=session)
        if updated is not None:
            collection._publish_change_event(
                operation_type="update",
                document_key={"_id": deepcopy(selected["_id"])},
                full_document=deepcopy(updated),
            )
        return result
    upsert_seed = None
    if upsert:
        upsert_seed = {}
        seed_upsert_document(upsert_seed, operation.filter_spec)

    result = await collection._engine_update_with_operation(
        operation,
        upsert=upsert,
        upsert_seed=upsert_seed,
        selector_filter=operation.filter_spec,
        session=session,
        bypass_document_validation=bypass_document_validation,
    )
    collection._record_operation_metadata(
        operation="update_one",
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    if result.upserted_id is not None:
        inserted = await collection._document_by_id(result.upserted_id, session=session)
        if inserted is not None:
            collection._publish_change_event(
                operation_type="insert",
                document_key={"_id": deepcopy(result.upserted_id)},
                full_document=deepcopy(inserted),
            )
    elif event_selected_id is not None:
        updated = await collection._document_by_id(event_selected_id, session=session)
        if updated is not None:
            collection._publish_change_event(
                operation_type="update",
                document_key={"_id": deepcopy(event_selected_id)},
                full_document=deepcopy(updated),
            )
    return result


async def update_many(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    update_spec: Update | object,
    upsert: bool,
    *,
    filter: Filter | object,
    update: Update | object,
    collation: CollationDocument | None,
    array_filters: ArrayFilters | None,
    hint: object | None,
    comment: object | None,
    let: dict[str, object] | None,
    bypass_document_validation: bool,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, "update": update, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    update_spec = collection._require_update(options["update_spec"])
    upsert = bool(options.get("upsert", False))
    bypass_document_validation = bool(options.get("bypass_document_validation", False))
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        array_filters=options.get("array_filters"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    matched_documents = await collection._build_cursor(
        compile_find_selection_from_update_operation(
            operation,
            projection={"_id": 1},
        ),
        session=session,
    ).to_list()
    if not matched_documents:
        if upsert:
            return await update_one(
                collection,
                operation.filter_spec,
                update_spec,
                True,
                filter=filter,
                update=update,
                collation=operation.collation,
                sort=None,
                array_filters=operation.array_filters,
                hint=operation.hint,
                comment=operation.comment,
                let=operation.let,
                bypass_document_validation=bypass_document_validation,
                session=session,
                extra_kwargs={},
            )
        return UpdateResult(matched_count=0, modified_count=0)

    modified_count = 0
    for matched in matched_documents:
        identity_filter = {"_id": matched["_id"]}
        identity_plan = compile_filter(identity_filter, dialect=collection._mongodb_dialect)
        result = await collection._engine_update_with_operation(
            operation.with_overrides(
                filter_spec=identity_filter,
                plan=identity_plan,
                hint=None,
            ),
            upsert=False,
            selector_filter=operation.filter_spec,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        modified_count += result.modified_count
        updated = await collection._document_by_id(matched["_id"], session=session)
        if updated is not None:
            collection._publish_change_event(
                operation_type="update",
                document_key={"_id": deepcopy(matched["_id"])},
                full_document=deepcopy(updated),
            )

    collection._record_operation_metadata(
        operation="update_many",
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    return UpdateResult(
        matched_count=len(matched_documents),
        modified_count=modified_count,
    )


async def replace_one(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    replacement: Document | object,
    upsert: bool,
    *,
    filter: Filter | object,
    collation: CollationDocument | None,
    sort: SortSpec | None,
    hint: object | None,
    comment: object | None,
    let: dict[str, object] | None,
    bypass_document_validation: bool,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    replacement = collection._require_replacement(options["replacement"])
    upsert = bool(options.get("upsert", False))
    bypass_document_validation = bool(options.get("bypass_document_validation", False))
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        sort=options.get("sort"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    selected = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        session=session,
    )
    if selected is None:
        if not upsert:
            return UpdateResult(matched_count=0, modified_count=0)
        document = collection._build_upsert_replacement_document(operation.filter_spec, replacement)
        await collection._put_replacement_document(
            document,
            overwrite=False,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        collection._record_operation_metadata(
            operation="replace_one",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        collection._publish_change_event(
            operation_type="insert",
            document_key={"_id": deepcopy(document["_id"])},
            full_document=deepcopy(document),
        )
        return UpdateResult(
            matched_count=0,
            modified_count=0,
            upserted_id=document["_id"],
        )

    if "_id" in replacement and not collection._mongodb_dialect.values_equal(replacement["_id"], selected["_id"]):
        raise OperationFailure("The _id field cannot be changed in a replacement document")
    document = collection._materialize_replacement_document(selected, replacement)
    modified_count = 0 if collection._mongodb_dialect.values_equal(selected, document) else 1
    await collection._put_replacement_document(
        document,
        overwrite=True,
        session=session,
        bypass_document_validation=bypass_document_validation,
    )
    collection._record_operation_metadata(
        operation="replace_one",
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    collection._publish_change_event(
        operation_type="replace",
        document_key={"_id": deepcopy(document["_id"])},
        full_document=deepcopy(document),
    )
    return UpdateResult(matched_count=1, modified_count=modified_count)


async def find_one_and_update(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    update_spec: Update | object,
    *,
    filter: Filter | object,
    update: Update | object,
    projection: Projection | None,
    collation: CollationDocument | None,
    sort: SortSpec | None,
    upsert: bool,
    return_document: ReturnDocument | None,
    array_filters: ArrayFilters | None,
    hint: object | None,
    comment: object | None,
    max_time_ms: int | None,
    let: dict[str, object] | None,
    bypass_document_validation: bool,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, "update": update, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    update_spec = collection._require_update(options["update_spec"])
    projection = collection._normalize_projection(options.get("projection"))
    return_document = collection._normalize_return_document(options.get("return_document"))
    upsert = bool(options.get("upsert", False))
    bypass_document_validation = bool(options.get("bypass_document_validation", False))
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        sort=options.get("sort"),
        array_filters=options.get("array_filters"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    before = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        session=session,
    )
    if before is None:
        if not upsert:
            return None
        result = await collection.update_one(
            operation.filter_spec,
            update_spec,
            upsert=True,
            collation=operation.collation,
            sort=operation.sort,
            array_filters=operation.array_filters,
            hint=operation.hint,
            comment=operation.comment,
            let=operation.let,
            bypass_document_validation=bypass_document_validation,
            session=session,
        )
        if return_document is ReturnDocument.BEFORE:
            return None
        return await collection.find(
            {"_id": result.upserted_id},
            projection,
            collation=operation.collation,
            limit=1,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            session=session,
        ).first()

    identity_filter = {"_id": before["_id"]}
    identity_plan = compile_filter(identity_filter, dialect=collection._mongodb_dialect)
    await collection._engine_update_with_operation(
        operation.with_overrides(
            filter_spec=identity_filter,
            plan=identity_plan,
            sort=None,
            hint=None,
        ),
        upsert=False,
        selector_filter=operation.filter_spec,
        session=session,
        bypass_document_validation=bypass_document_validation,
    )
    after = await collection._document_by_id(before["_id"], session=session)
    if after is not None:
        collection._publish_change_event(
            operation_type="update",
            document_key={"_id": deepcopy(before["_id"])},
            full_document=deepcopy(after),
        )
    if return_document is ReturnDocument.BEFORE:
        return apply_projection(
            before,
            projection,
            selector_filter=operation.filter_spec,
            dialect=collection._mongodb_dialect,
        )
    if after is None:
        return None
    return apply_projection(
        after,
        projection,
        selector_filter=operation.filter_spec,
        dialect=collection._mongodb_dialect,
    )


async def find_one_and_replace(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    replacement: Document | object,
    *,
    filter: Filter | object,
    projection: Projection | None,
    collation: CollationDocument | None,
    sort: SortSpec | None,
    upsert: bool,
    return_document: ReturnDocument | None,
    hint: object | None,
    comment: object | None,
    max_time_ms: int | None,
    let: dict[str, object] | None,
    bypass_document_validation: bool,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    replacement = collection._require_replacement(options["replacement"])
    projection = collection._normalize_projection(options.get("projection"))
    return_document = collection._normalize_return_document(options.get("return_document"))
    upsert = bool(options.get("upsert", False))
    bypass_document_validation = bool(options.get("bypass_document_validation", False))
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        sort=options.get("sort"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )

    before = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        session=session,
    )
    if before is None:
        if not upsert:
            return None
        result = await collection.replace_one(
            operation.filter_spec,
            replacement,
            upsert=True,
            collation=operation.collation,
            sort=operation.sort,
            hint=operation.hint,
            comment=operation.comment,
            let=operation.let,
            bypass_document_validation=bypass_document_validation,
            session=session,
        )
        if return_document is ReturnDocument.BEFORE:
            return None
        return await collection.find(
            {"_id": result.upserted_id},
            projection,
            collation=operation.collation,
            limit=1,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            session=session,
        ).first()

    identity_filter = {"_id": before["_id"]}
    await replace_one(
        collection,
        identity_filter,
        replacement,
        False,
        filter=filter,
        collation=operation.collation,
        sort=None,
        hint=None,
        comment=operation.comment,
        let=operation.let,
        bypass_document_validation=bypass_document_validation,
        session=session,
        extra_kwargs={},
    )
    if return_document is ReturnDocument.BEFORE:
        return apply_projection(
            before,
            projection,
            selector_filter=operation.filter_spec,
            dialect=collection._mongodb_dialect,
        )
    after = await collection._document_by_id(before["_id"], session=session)
    if after is None:
        return None
    return apply_projection(
        after,
        projection,
        selector_filter=operation.filter_spec,
        dialect=collection._mongodb_dialect,
    )


async def find_one_and_delete(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    *,
    filter: Filter | object,
    projection: Projection | None,
    collation: CollationDocument | None,
    sort: SortSpec | None,
    hint: object | None,
    comment: object | None,
    max_time_ms: int | None,
    let: dict[str, object] | None,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    projection = collection._normalize_projection(options.get("projection"))
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        sort=options.get("sort"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        max_time_ms=options.get("max_time_ms"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )

    before = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        session=session,
    )
    if before is None:
        return None

    await collection._engine.delete_document(
        collection._db_name,
        collection._collection_name,
        before["_id"],
        context=session,
    )
    collection._publish_change_event(
        operation_type="delete",
        document_key={"_id": deepcopy(before["_id"])},
    )
    return apply_projection(
        before,
        projection,
        selector_filter=operation.filter_spec,
        dialect=collection._mongodb_dialect,
    )


async def delete_one(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    *,
    filter: Filter | object,
    collation: CollationDocument | None,
    hint: object | None,
    comment: object | None,
    let: dict[str, object] | None,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    event_selected_id: DocumentId | None = None
    if collection._change_hub is not None and operation.hint is None:
        selected_for_event = await collection._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={"_id": 1},
                limit=1,
            ),
            session=session,
        ).first()
        if selected_for_event is not None:
            event_selected_id = selected_for_event["_id"]
    if operation.hint is not None:
        selected = await collection._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={"_id": 1},
                limit=1,
            ),
            session=session,
        ).first()
        if selected is None:
            return DeleteResult(deleted_count=0)
        deleted = await collection._engine.delete_document(
            collection._db_name,
            collection._collection_name,
            selected["_id"],
            context=session,
        )
        collection._record_operation_metadata(
            operation="delete_one",
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        if deleted:
            collection._publish_change_event(
                operation_type="delete",
                document_key={"_id": deepcopy(selected["_id"])},
            )
        return DeleteResult(deleted_count=1 if deleted else 0)
    result = await collection._engine_delete_with_operation(operation, session=session)
    collection._record_operation_metadata(
        operation="delete_one",
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    if result.deleted_count and event_selected_id is not None:
        collection._publish_change_event(
            operation_type="delete",
            document_key={"_id": deepcopy(event_selected_id)},
        )
    return result


async def delete_many(
    collection: AsyncCollection,
    filter_spec: Filter | object,
    *,
    filter: Filter | object,
    collation: CollationDocument | None,
    hint: HintSpec | None,
    comment: object | None,
    let: dict[str, object] | None,
    session: ClientSession | None,
    extra_kwargs: dict[str, object],
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
        extra_kwargs={"filter": filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options["filter_spec"])
    session = options.get("session")
    operation = compile_update_operation(
        filter_spec,
        collation=options.get("collation"),
        hint=options.get("hint"),
        comment=options.get("comment"),
        let=options.get("let"),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    matched_documents = await collection._build_cursor(
        compile_find_selection_from_update_operation(
            operation,
            projection={"_id": 1},
        ),
        session=session,
    ).to_list()
    deleted_count = 0
    for matched in matched_documents:
        deleted = await collection._engine.delete_document(
            collection._db_name,
            collection._collection_name,
            matched["_id"],
            context=session,
        )
        if deleted:
            deleted_count += 1
            collection._publish_change_event(
                operation_type="delete",
                document_key={"_id": deepcopy(matched["_id"])},
            )
    collection._record_operation_metadata(
        operation="delete_many",
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    return DeleteResult(deleted_count=deleted_count)
