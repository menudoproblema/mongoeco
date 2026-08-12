from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.api.argument_validation import HintSpec
from mongoeco.api.operations import (
    UpdateOperation,
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
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import ChangeOperationType, ChangePublicationPolicy
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.upserts import seed_upsert_document
from mongoeco.engines.results import (
    EngineDeleteResult,
    EngineUpdateResult,
    FindAndModifyOutcome,
)
from mongoeco.session import ClientSession
from mongoeco.types import (
    ArrayFilters,
    CollationDocument,
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    Projection,
    ReturnDocument,
    SortSpec,
    Update,
    UpdateResult,
)

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection


def _bind_execution_context(
    operation,
    collection: AsyncCollection,
    *,
    session: ClientSession | None = None,
    change_operation_type: ChangeOperationType,
):
    """Bind one immutable context to the operation at the public boundary."""
    if operation.context is not None:
        return operation
    context = collection._new_operation_context(
        session=session,
        collation=operation.collation,
        bindings=(
            None
            if isinstance(operation.let, ExpressionExecutionContext)
            else operation.let
        ),
        expressions=(
            operation.let
            if isinstance(operation.let, ExpressionExecutionContext)
            else None
        ),
        publication=(
            ChangePublicationPolicy.EMIT
            if collection._should_publish_change_events(session=session)
            else ChangePublicationPolicy.RECORD_GAP
        ),
        change_operation_type=change_operation_type,
    )
    return operation.with_overrides(
        let=context.expressions,
        context=context,
    )


def _selected_document_id(document: Document) -> DocumentId:
    # Legacy engine fixtures may contain documents persisted before automatic
    # _id materialization. MongoDB lookup semantics target those with _id=None.
    return document.get('_id')


async def _delete_selected_document(
    collection: AsyncCollection,
    document: Document,
    *,
    selector_operation: UpdateOperation,
    session: ClientSession | None,
) -> bool:
    document_id = _selected_document_id(document)
    identity_filter = {'_id': document_id}
    identity_operation = compile_update_operation(
        identity_filter,
        collation=selector_operation.collation,
        let=selector_operation.let,
        dialect=collection._mongodb_dialect,
        plan=compile_filter(
            identity_filter,
            dialect=collection._mongodb_dialect,
        ),
        planning_mode=collection._planning_mode,
    ).with_overrides(context=selector_operation.context)
    result = await collection._engine_delete_with_operation(
        identity_operation,
        selector_filter=selector_operation.filter_spec,
        session=session,
        publish_change_event=True,
    )
    if not isinstance(result, EngineDeleteResult):
        raise TypeError('engine did not return the deleted document')
    return result.result.deleted_count == 1


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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='update'
    )
    upsert_seed = None
    if upsert:
        upsert_seed = {}
        seed_upsert_document(upsert_seed, operation.filter_spec)

    captured = await collection._engine_update_with_operation(
        operation,
        upsert=upsert,
        upsert_seed=upsert_seed,
        session=session,
        bypass_document_validation=bypass_document_validation,
        publish_operation_type='update',
    )
    if not isinstance(captured, EngineUpdateResult):
        raise TypeError('engine did not return captured update documents')
    result = captured.result
    collection._record_operation_metadata(
        operation='update_one',
        comment=operation.comment,
        hint=operation.hint,
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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='update'
    )
    matched_documents = await collection._build_cursor(
        compile_find_selection_from_update_operation(
            operation,
        ),
        session=session,
        apply_codec_options=False,
    ).to_list()
    if not matched_documents:
        if upsert:
            upsert_seed: Document = {}
            seed_upsert_document(upsert_seed, operation.filter_spec)
            captured = await collection._engine_update_with_operation(
                operation,
                upsert=True,
                upsert_seed=upsert_seed,
                session=session,
                bypass_document_validation=bypass_document_validation,
                publish_operation_type='update',
            )
            if not isinstance(captured, EngineUpdateResult):
                raise TypeError(
                    'engine did not return captured update documents'
                )
            collection._record_operation_metadata(
                operation='update_many',
                comment=operation.comment,
                hint=operation.hint,
                session=session,
            )
            return captured.result
        return UpdateResult(matched_count=0, modified_count=0)

    matched_count = 0
    modified_count = 0
    for matched in matched_documents:
        matched_id = _selected_document_id(matched)
        identity_filter = {'_id': matched_id}
        identity_plan = compile_filter(
            identity_filter, dialect=collection._mongodb_dialect
        )
        captured = await collection._engine_update_with_operation(
            operation.with_overrides(
                filter_spec=identity_filter,
                plan=identity_plan,
                hint=None,
            ),
            upsert=False,
            selector_filter=operation.filter_spec,
            session=session,
            bypass_document_validation=bypass_document_validation,
            publish_operation_type='update',
        )
        if not isinstance(captured, EngineUpdateResult):
            raise TypeError('engine did not return captured update documents')
        result = captured.result
        matched_count += result.matched_count
        modified_count += result.modified_count

    collection._record_operation_metadata(
        operation='update_many',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    return UpdateResult(
        matched_count=matched_count,
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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        update_spec={'$set': {}},
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='replace'
    )
    upsert_seed = (
        collection._build_upsert_replacement_document(
            operation.filter_spec, replacement
        )
        if upsert
        else None
    )
    captured = await collection._engine_update_with_operation(
        operation,
        upsert=upsert,
        upsert_seed=upsert_seed,
        session=session,
        bypass_document_validation=bypass_document_validation,
        replacement_document=replacement,
        publish_operation_type='replace',
    )
    if not isinstance(captured, EngineUpdateResult):
        raise TypeError('engine did not return captured replacement documents')
    result = captured.result
    collection._record_operation_metadata(
        operation='replace_one',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    return result


async def find_one_and_update_outcome(
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
) -> FindAndModifyOutcome:
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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        update_spec=update_spec,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='update'
    )
    upsert_seed = None
    if upsert:
        upsert_seed = {}
        seed_upsert_document(upsert_seed, operation.filter_spec)
    captured = await collection._engine_update_with_operation(
        operation,
        upsert=upsert,
        upsert_seed=upsert_seed,
        session=session,
        bypass_document_validation=bypass_document_validation,
        publish_operation_type='update',
    )
    if not isinstance(captured, EngineUpdateResult):
        raise TypeError('engine did not return captured update documents')
    selected = (
        captured.before_document
        if return_document is ReturnDocument.BEFORE
        else captured.after_document
    )
    value = (
        None
        if selected is None
        else apply_projection(
            selected,
            projection,
            selector_filter=operation.filter_spec,
            dialect=collection._mongodb_dialect,
        )
    )
    outcome = FindAndModifyOutcome(captured=captured, value=value)
    return outcome


async def find_one_and_replace_outcome(
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
) -> FindAndModifyOutcome:
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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        update_spec={'$set': {}},
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='replace'
    )

    upsert_seed = (
        collection._build_upsert_replacement_document(
            operation.filter_spec, replacement
        )
        if upsert
        else None
    )
    captured = await collection._engine_update_with_operation(
        operation,
        upsert=upsert,
        upsert_seed=upsert_seed,
        session=session,
        bypass_document_validation=bypass_document_validation,
        replacement_document=replacement,
        publish_operation_type='replace',
    )
    if not isinstance(captured, EngineUpdateResult):
        raise TypeError('engine did not return captured replacement documents')
    selected = (
        captured.before_document
        if return_document is ReturnDocument.BEFORE
        else captured.after_document
    )
    value = (
        None
        if selected is None
        else apply_projection(
            selected,
            projection,
            selector_filter=operation.filter_spec,
            dialect=collection._mongodb_dialect,
        )
    )
    outcome = FindAndModifyOutcome(captured=captured, value=value)
    return outcome


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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='delete'
    )

    captured = await collection._engine_delete_with_operation(
        operation,
        session=session,
        publish_change_event=True,
    )
    if not isinstance(captured, EngineDeleteResult):
        raise TypeError('engine did not return the deleted document')
    if captured.result.deleted_count == 0 or captured.deleted_document is None:
        return None
    before = captured.deleted_document
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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='delete'
    )
    captured = await collection._engine_delete_with_operation(
        operation,
        session=session,
        publish_change_event=True,
    )
    if not isinstance(captured, EngineDeleteResult):
        raise TypeError('engine did not return the deleted document')
    result = captured.result
    collection._record_operation_metadata(
        operation='delete_one',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
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
        let=collection._normalize_let(options.get('let')),
        dialect=collection._mongodb_dialect,
        planning_mode=collection._planning_mode,
    )
    operation = _bind_execution_context(
        operation, collection, session=session, change_operation_type='delete'
    )
    matched_documents = await collection._build_cursor(
        compile_find_selection_from_update_operation(
            operation,
        ),
        session=session,
        apply_codec_options=False,
    ).to_list()
    deleted_count = 0
    for matched in matched_documents:
        deleted = await _delete_selected_document(
            collection,
            matched,
            selector_operation=operation,
            session=session,
        )
        if deleted:
            deleted_count += 1
    collection._record_operation_metadata(
        operation='delete_many',
        comment=operation.comment,
        hint=operation.hint,
        session=session,
    )
    return DeleteResult(deleted_count=deleted_count)
