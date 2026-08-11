from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from mongoeco.api.argument_validation import HintSpec
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
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.identity import assert_document_matches_stored_lookup
from mongoeco.core.upserts import seed_upsert_document
from mongoeco.errors import OperationFailure, WriteError
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


def _bind_execution_context(operation, collection: AsyncCollection):
    """Enlaza los bindings de una operación al comando que la ejecuta."""
    if isinstance(operation.let, ExpressionExecutionContext):
        return operation
    return operation.with_overrides(
        let=collection._new_execution_context().with_bindings(operation.let)
    )


def _require_selected_document_id(document: Document) -> DocumentId:
    if '_id' not in document:
        raise OperationFailure('Cannot target a selected document without _id')
    return document['_id']


async def _require_stable_selected_storage_identity(
    collection: AsyncCollection,
    document: Document,
    *,
    session: ClientSession | None,
) -> DocumentId:
    document_id = document.get('_id')
    stored = await collection._document_by_id(document_id, session=session)
    assert_document_matches_stored_lookup(
        document,
        stored,
        dialect=collection._mongodb_dialect,
    )
    return document_id


async def _require_stable_selected_document_id(
    collection: AsyncCollection,
    document: Document,
    *,
    session: ClientSession | None,
) -> DocumentId:
    document_id = _require_selected_document_id(document)
    await _require_stable_selected_storage_identity(
        collection,
        document,
        session=session,
    )
    return document_id


async def _delete_selected_document(
    collection: AsyncCollection,
    document: Document,
    *,
    session: ClientSession | None,
) -> tuple[bool, Document | None]:
    document_key = (
        {'_id': deepcopy(document['_id'])} if '_id' in document else None
    )
    document_id = await _require_stable_selected_storage_identity(
        collection,
        document,
        session=session,
    )
    deleted = await collection._engine.delete_document(
        collection._db_name,
        collection._collection_name,
        document_id,
        context=session,
    )
    return deleted, document_key


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
        variables=(
            let
            if isinstance(let, ExpressionExecutionContext)
            else collection._new_execution_context().with_bindings(let)
        ),
    )
    if '_id' not in new_doc:
        new_doc['_id'] = ObjectId()
    await collection._put_replacement_document(
        new_doc,
        overwrite=False,
        session=session,
        bypass_document_validation=bypass_document_validation,
    )
    if collection._should_publish_change_events(session=session):
        collection._publish_change_event(
            operation_type='insert',
            document_key={'_id': deepcopy(new_doc['_id'])},
            full_document=deepcopy(new_doc),
            session=session,
        )
    else:
        collection._mark_change_event_gap()
    return UpdateResult(
        matched_count=0,
        modified_count=0,
        upserted_id=new_doc['_id'],
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
            'filter_spec': filter_spec,
            'update_spec': update_spec,
            'upsert': upsert,
            'collation': collation,
            'sort': sort,
            'array_filters': array_filters,
            'hint': hint,
            'comment': comment,
            'let': let,
            'bypass_document_validation': bypass_document_validation,
            'session': session,
        },
        extra_kwargs={'filter': filter, 'update': update, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    update_spec = collection._require_update(options['update_spec'])
    upsert = bool(options.get('upsert', False))
    bypass_document_validation = bool(
        options.get('bypass_document_validation', False)
    )
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        sort=options.get('sort'),
        array_filters=options.get('array_filters'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)
    should_publish_change_events = collection._should_publish_change_events(
        session=session,
    )
    event_selected_id: DocumentId | None = None
    if (
        should_publish_change_events
        and operation.sort is None
        and operation.hint is None
    ):
        selected = await collection._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={'_id': 1},
                limit=1,
            ),
            session=session,
            apply_codec_options=False,
        ).first()
        if selected is not None and '_id' in selected:
            event_selected_id = selected['_id']
    if operation.sort is not None:
        selected = await collection._build_cursor(
            compile_find_selection_from_update_operation(operation, limit=1),
            session=session,
            apply_codec_options=False,
        ).first()
        if selected is None and not upsert:
            return UpdateResult(matched_count=0, modified_count=0)
        if selected is not None:
            selected_id = await _require_stable_selected_storage_identity(
                collection, selected, session=session
            )
            identity_filter = {'_id': selected_id}
            identity_plan = compile_filter(
                identity_filter, dialect=collection._mongodb_dialect
            )
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
                operation='update_one',
                comment=operation.comment,
                hint=operation.hint,
                session=session,
            )
            if should_publish_change_events:
                updated = await collection._document_by_id(
                    selected_id, session=session
                )
                if updated is not None and '_id' in selected:
                    collection._publish_change_event(
                        operation_type='update',
                        document_key={'_id': deepcopy(selected_id)},
                        full_document=deepcopy(updated),
                        session=session,
                    )
            elif result.matched_count > 0:
                collection._mark_change_event_gap()
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
                limit=1,
            ),
            session=session,
            apply_codec_options=False,
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
        selected_id = await _require_stable_selected_storage_identity(
            collection, selected, session=session
        )
        identity_filter = {'_id': selected_id}
        identity_plan = compile_filter(
            identity_filter, dialect=collection._mongodb_dialect
        )
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
            operation='update_one',
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        if should_publish_change_events:
            updated = await collection._document_by_id(
                selected_id, session=session
            )
            if updated is not None and '_id' in selected:
                collection._publish_change_event(
                    operation_type='update',
                    document_key={'_id': deepcopy(selected_id)},
                    full_document=deepcopy(updated),
                    session=session,
                )
        elif result.matched_count > 0:
            collection._mark_change_event_gap()
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
        operation='update_one',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    if not should_publish_change_events:
        if result.upserted_id is not None or result.matched_count > 0:
            collection._mark_change_event_gap()
    elif result.upserted_id is not None:
        inserted = await collection._document_by_id(
            result.upserted_id, session=session
        )
        if inserted is not None:
            collection._publish_change_event(
                operation_type='insert',
                document_key={'_id': deepcopy(result.upserted_id)},
                full_document=deepcopy(inserted),
                session=session,
            )
    elif event_selected_id is not None:
        updated = await collection._document_by_id(
            event_selected_id, session=session
        )
        if updated is not None:
            collection._publish_change_event(
                operation_type='update',
                document_key={'_id': deepcopy(event_selected_id)},
                full_document=deepcopy(updated),
                session=session,
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
            'filter_spec': filter_spec,
            'update_spec': update_spec,
            'upsert': upsert,
            'collation': collation,
            'array_filters': array_filters,
            'hint': hint,
            'comment': comment,
            'let': let,
            'bypass_document_validation': bypass_document_validation,
            'session': session,
        },
        extra_kwargs={'filter': filter, 'update': update, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    update_spec = collection._require_update(options['update_spec'])
    upsert = bool(options.get('upsert', False))
    bypass_document_validation = bool(
        options.get('bypass_document_validation', False)
    )
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        array_filters=options.get('array_filters'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)
    matched_documents = await collection._build_cursor(
        compile_find_selection_from_update_operation(
            operation,
        ),
        session=session,
        apply_codec_options=False,
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
        matched_id = await _require_stable_selected_storage_identity(
            collection, matched, session=session
        )
        identity_filter = {'_id': matched_id}
        identity_plan = compile_filter(
            identity_filter, dialect=collection._mongodb_dialect
        )
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
        updated = await collection._document_by_id(matched_id, session=session)
        if updated is not None and '_id' in matched:
            collection._publish_change_event(
                operation_type='update',
                document_key={'_id': deepcopy(matched_id)},
                full_document=deepcopy(updated),
                session=session,
            )

    collection._record_operation_metadata(
        operation='update_many',
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
            'filter_spec': filter_spec,
            'replacement': replacement,
            'upsert': upsert,
            'collation': collation,
            'sort': sort,
            'hint': hint,
            'comment': comment,
            'let': let,
            'bypass_document_validation': bypass_document_validation,
            'session': session,
        },
        extra_kwargs={'filter': filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    replacement = collection._require_replacement(options['replacement'])
    upsert = bool(options.get('upsert', False))
    bypass_document_validation = bool(
        options.get('bypass_document_validation', False)
    )
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        sort=options.get('sort'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)
    selected = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        variables=operation.let,
        session=session,
    )
    if selected is None:
        if not upsert:
            return UpdateResult(matched_count=0, modified_count=0)
        document = collection._build_upsert_replacement_document(
            operation.filter_spec, replacement
        )
        await collection._put_replacement_document(
            document,
            overwrite=False,
            session=session,
            bypass_document_validation=bypass_document_validation,
        )
        collection._record_operation_metadata(
            operation='replace_one',
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        collection._publish_change_event(
            operation_type='insert',
            document_key={'_id': deepcopy(document['_id'])},
            full_document=deepcopy(document),
            session=session,
        )
        return UpdateResult(
            matched_count=0,
            modified_count=0,
            upserted_id=document['_id'],
        )

    if '_id' in replacement and (
        '_id' not in selected
        or not collection._mongodb_dialect.values_equal(
            replacement['_id'], selected['_id']
        )
    ):
        raise WriteError(
            'The _id field cannot be changed in a replacement document',
            code=66,
        )
    if '_id' in selected:
        await _require_stable_selected_document_id(
            collection, selected, session=session
        )
    document = collection._materialize_replacement_document(
        selected, replacement
    )
    modified_count = (
        0
        if collection._mongodb_dialect.values_equal(selected, document)
        else 1
    )
    await collection._put_replacement_document(
        document,
        overwrite=True,
        session=session,
        bypass_document_validation=bypass_document_validation,
    )
    collection._record_operation_metadata(
        operation='replace_one',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    if '_id' in document:
        collection._publish_change_event(
            operation_type='replace',
            document_key={'_id': deepcopy(document['_id'])},
            full_document=deepcopy(document),
            session=session,
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
            'filter_spec': filter_spec,
            'update_spec': update_spec,
            'projection': projection,
            'collation': collation,
            'sort': sort,
            'upsert': upsert,
            'return_document': return_document,
            'array_filters': array_filters,
            'hint': hint,
            'comment': comment,
            'max_time_ms': max_time_ms,
            'let': let,
            'bypass_document_validation': bypass_document_validation,
            'session': session,
        },
        extra_kwargs={'filter': filter, 'update': update, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    update_spec = collection._require_update(options['update_spec'])
    projection = collection._normalize_projection(options.get('projection'))
    return_document = collection._normalize_return_document(
        options.get('return_document')
    )
    upsert = bool(options.get('upsert', False))
    bypass_document_validation = bool(
        options.get('bypass_document_validation', False)
    )
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        sort=options.get('sort'),
        array_filters=options.get('array_filters'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        max_time_ms=options.get('max_time_ms'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)
    before = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        variables=operation.let,
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
            {'_id': result.upserted_id},
            projection,
            collation=operation.collation,
            limit=1,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            session=session,
        ).first()

    before_id = await _require_stable_selected_storage_identity(
        collection, before, session=session
    )
    identity_filter = {'_id': before_id}
    identity_plan = compile_filter(
        identity_filter, dialect=collection._mongodb_dialect
    )
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
    after = await collection._document_by_id(before_id, session=session)
    if after is not None and '_id' in before:
        collection._publish_change_event(
            operation_type='update',
            document_key={'_id': deepcopy(before_id)},
            full_document=deepcopy(after),
            session=session,
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
            'filter_spec': filter_spec,
            'replacement': replacement,
            'projection': projection,
            'collation': collation,
            'sort': sort,
            'upsert': upsert,
            'return_document': return_document,
            'hint': hint,
            'comment': comment,
            'max_time_ms': max_time_ms,
            'let': let,
            'bypass_document_validation': bypass_document_validation,
            'session': session,
        },
        extra_kwargs={'filter': filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    replacement = collection._require_replacement(options['replacement'])
    projection = collection._normalize_projection(options.get('projection'))
    return_document = collection._normalize_return_document(
        options.get('return_document')
    )
    upsert = bool(options.get('upsert', False))
    bypass_document_validation = bool(
        options.get('bypass_document_validation', False)
    )
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        sort=options.get('sort'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        max_time_ms=options.get('max_time_ms'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)

    before = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        variables=operation.let,
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
            {'_id': result.upserted_id},
            projection,
            collation=operation.collation,
            limit=1,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            session=session,
        ).first()

    before_id = await _require_stable_selected_storage_identity(
        collection, before, session=session
    )
    identity_filter = {'_id': before_id}
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
    after = await collection._document_by_id(before_id, session=session)
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
            'filter_spec': filter_spec,
            'projection': projection,
            'collation': collation,
            'sort': sort,
            'hint': hint,
            'comment': comment,
            'max_time_ms': max_time_ms,
            'let': let,
            'session': session,
        },
        extra_kwargs={'filter': filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    projection = collection._normalize_projection(options.get('projection'))
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        sort=options.get('sort'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        max_time_ms=options.get('max_time_ms'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)

    before = await collection._select_first_document(
        operation.filter_spec,
        plan=operation.plan,
        collation=operation.collation,
        sort=operation.sort,
        hint=operation.hint,
        comment=operation.comment,
        max_time_ms=operation.max_time_ms,
        variables=operation.let,
        session=session,
    )
    if before is None:
        return None

    deleted, document_key = await _delete_selected_document(
        collection,
        before,
        session=session,
    )
    if not deleted:
        return None
    if document_key is not None:
        collection._publish_change_event(
            operation_type='delete',
            document_key=document_key,
            session=session,
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
            'filter_spec': filter_spec,
            'collation': collation,
            'hint': hint,
            'comment': comment,
            'let': let,
            'session': session,
        },
        extra_kwargs={'filter': filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)
    should_publish_change_events = collection._should_publish_change_events(
        session=session,
    )
    event_selected_id: DocumentId | None = None
    if should_publish_change_events and operation.hint is None:
        selected_for_event = await collection._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                projection={'_id': 1},
                limit=1,
            ),
            session=session,
            apply_codec_options=False,
        ).first()
        if selected_for_event is not None:
            if '_id' in selected_for_event:
                event_selected_id = selected_for_event['_id']
    if operation.hint is not None:
        selected = await collection._build_cursor(
            compile_find_selection_from_update_operation(
                operation,
                limit=1,
            ),
            session=session,
            apply_codec_options=False,
        ).first()
        if selected is None:
            return DeleteResult(deleted_count=0)
        deleted, document_key = await _delete_selected_document(
            collection,
            selected,
            session=session,
        )
        collection._record_operation_metadata(
            operation='delete_one',
            comment=operation.comment,
            hint=operation.hint,
            session=session,
        )
        if deleted:
            if should_publish_change_events and document_key is not None:
                collection._publish_change_event(
                    operation_type='delete',
                    document_key=document_key,
                    session=session,
                )
            elif not should_publish_change_events:
                collection._mark_change_event_gap()
        return DeleteResult(deleted_count=1 if deleted else 0)
    result = await collection._engine_delete_with_operation(
        operation, session=session
    )
    collection._record_operation_metadata(
        operation='delete_one',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    if (
        should_publish_change_events
        and result.deleted_count
        and event_selected_id is not None
    ):
        collection._publish_change_event(
            operation_type='delete',
            document_key={'_id': deepcopy(event_selected_id)},
            session=session,
        )
    elif result.deleted_count and not should_publish_change_events:
        collection._mark_change_event_gap()
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
            'filter_spec': filter_spec,
            'collation': collation,
            'hint': hint,
            'comment': comment,
            'let': let,
            'session': session,
        },
        extra_kwargs={'filter': filter, **extra_kwargs},
        profile=collection._pymongo_profile,
    )
    filter_spec = collection._normalize_filter(options['filter_spec'])
    session = options.get('session')
    collection._ensure_session_active(session)
    operation = compile_update_operation(
        filter_spec,
        collation=options.get('collation'),
        hint=options.get('hint'),
        comment=options.get('comment'),
        let=options.get('let'),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(operation, collection)
    matched_documents = await collection._build_cursor(
        compile_find_selection_from_update_operation(
            operation,
        ),
        session=session,
        apply_codec_options=False,
    ).to_list()
    deleted_count = 0
    for matched in matched_documents:
        deleted, document_key = await _delete_selected_document(
            collection,
            matched,
            session=session,
        )
        if deleted:
            deleted_count += 1
            if document_key is not None:
                collection._publish_change_event(
                    operation_type='delete',
                    document_key=document_key,
                    session=session,
                )
    collection._record_operation_metadata(
        operation='delete_many',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    return DeleteResult(deleted_count=deleted_count)
