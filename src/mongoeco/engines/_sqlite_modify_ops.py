from __future__ import annotations

import sqlite3

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy

from mongoeco.core.identity import (
    assert_document_kept_storage_key,
    assert_document_matches_storage_key,
    assert_valid_root_document_id,
    materialize_replacement_document,
)
from mongoeco.core.sorting import sort_documents
from mongoeco.engines._sqlite_write_scope import sqlite_write_scope
from mongoeco.engines.results import DeleteOutcome, MutationOutcome
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import (
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    UpdateResult,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mongoeco.compat import MongoDialect


def delete_matching_document(
    *,
    db_name: str,
    coll_name: str,
    filter_spec: Filter,
    selector_filter: Filter | None = None,
    plan: object | None,
    dialect: MongoDialect | None,
    collation: dict[str, object] | None,
    variables: Mapping[str, object] | None = None,
    compile_find_semantics: Callable[..., object],
    ensure_query_plan: Callable[[Filter, object | None], object],
    require_connection: Callable[[], sqlite3.Connection],
    purge_expired_documents: Callable[[sqlite3.Connection, str, str], None],
    select_first_document_for_plan: Callable[[str, str, object], tuple[str, Document] | None],
    storage_key_for_id: Callable[[DocumentId], str],
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    delete_multikey_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    delete_scalar_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    delete_search_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    load_documents: Callable[[str, str], Iterable[tuple[str, Document]]],
    match_plan: Callable[[Document, object, MongoDialect, object | None, object], bool],
    invalidate_collection_features_cache: Callable[[str, str], None],
    sort: object | None = None,
) -> DeleteOutcome:
    effective_dialect = dialect
    plan = ensure_query_plan(filter_spec, plan)
    semantics = compile_find_semantics(
        filter_spec,
        plan=plan,
        collation=collation,
        dialect=effective_dialect,
        variables=variables,
    )
    selector_semantics = (
        compile_find_semantics(
            selector_filter,
            collation=collation,
            dialect=effective_dialect,
            variables=variables,
        )
        if selector_filter is not None
        else None
    )
    conn = require_connection()
    purge_expired_documents(conn, db_name, coll_name)
    try:
        if semantics.collation is not None or sort:
            raise NotImplementedError("Collation requires Python delete fallback")
        selected = select_first_document_for_plan(db_name, coll_name, semantics.query_plan)
        if selected is None:
            result = DeleteResult(deleted_count=0)
            return DeleteOutcome(result=result)
        storage_key, document = selected
        if selector_semantics is not None and not match_plan(
            document,
            selector_semantics.query_plan,
            selector_semantics.dialect,
            selector_semantics.collation,
            selector_semantics.variables,
        ):
            result = DeleteResult(deleted_count=0)
            return DeleteOutcome(result=result)
        assert_document_matches_storage_key(
            document,
            storage_key,
            storage_key_for_id=storage_key_for_id,
        )
        with sqlite_write_scope(
            conn,
            begin_write=begin_write,
            commit_write=commit_write,
            rollback_write=rollback_write,
        ):
            conn.execute(
                """
                DELETE FROM documents
                WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                """,
                (db_name, coll_name, storage_key),
            )
            delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
            delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
            delete_search_entries_for_storage_key(conn, db_name, coll_name, storage_key)
            invalidate_collection_features_cache(db_name, coll_name)
        result = DeleteResult(deleted_count=1)
        return DeleteOutcome(
            result=result,
            deleted_document=deepcopy(document),
        )
    except (NotImplementedError, TypeError):
        pass
    except Exception:
        raise

    matching_documents: list[tuple[str, Document]] = []
    for storage_key, document in load_documents(db_name, coll_name):
        if not match_plan(
            document,
            semantics.query_plan,
            semantics.dialect,
            semantics.collation,
            semantics.variables,
        ):
            continue
        if selector_semantics is not None and not match_plan(
            document,
            selector_semantics.query_plan,
            selector_semantics.dialect,
            selector_semantics.collation,
            selector_semantics.variables,
        ):
            continue
        matching_documents.append((storage_key, document))

    if sort:
        item_by_document_id = {
            id(document): (storage_key, document)
            for storage_key, document in matching_documents
        }
        matching_documents = [
            item_by_document_id[id(document)]
            for document in sort_documents(
                [document for _, document in matching_documents],
                sort,
                dialect=semantics.dialect,
                collation=semantics.collation,
            )
        ]

    for storage_key, document in matching_documents:
        assert_document_matches_storage_key(
            document,
            storage_key,
            storage_key_for_id=storage_key_for_id,
        )
        with sqlite_write_scope(
            conn,
            begin_write=begin_write,
            commit_write=commit_write,
            rollback_write=rollback_write,
        ):
            conn.execute(
                """
                DELETE FROM documents
                WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                """,
                (db_name, coll_name, storage_key),
            )
            delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
            delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
            delete_search_entries_for_storage_key(conn, db_name, coll_name, storage_key)
            invalidate_collection_features_cache(db_name, coll_name)
        result = DeleteResult(deleted_count=1)
        return DeleteOutcome(
            result=result,
            deleted_document=deepcopy(document),
        )
    result = DeleteResult(deleted_count=0)
    return DeleteOutcome(result=result)


def update_with_operation(
    *,
    db_name: str,
    coll_name: str,
    operation: object,
    upsert: bool,
    upsert_seed: Document | None,
    selector_filter: Filter | None,
    dialect: MongoDialect | None,
    bypass_document_validation: bool,
    compile_update_semantics: Callable[..., object],
    require_connection: Callable[[], sqlite3.Connection],
    purge_expired_documents: Callable[[sqlite3.Connection, str, str], None],
    collection_options_or_empty: Callable[[sqlite3.Connection, str, str], dict[str, object]],
    dialect_requires_python_fallback: Callable[[MongoDialect], bool],
    select_first_document_for_plan: Callable[[str, str, object], tuple[str, Document] | None],
    load_documents: Callable[[str, str], Iterable[tuple[str, Document]]],
    match_plan: Callable[[Document, object, MongoDialect, object | None, object], bool],
    enforce_collection_document_validation: Callable[..., None],
    validate_document_against_unique_indexes: Callable[[str, str, Document, str | None], None],
    load_indexes: Callable[[str, str], list[object]],
    load_search_index_rows: Callable[[str, str], list[tuple[object, str | None, float | None]]],
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    translate_compiled_update_plan: Callable[[object, Document], tuple[str, tuple[object, ...]]],
    compiled_update_plan_type: type,
    rebuild_multikey_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, list[object]], None],
    rebuild_scalar_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, list[object]], None],
    replace_search_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, list[tuple[object, str | None, float | None]]], None],
    serialize_document: Callable[[Document], str],
    storage_key_for_id: Callable[[DocumentId], str],
    new_object_id: Callable[[], DocumentId],
    invalidate_collection_features_cache: Callable[[str, str], None],
    replacement_document: Document | None = None,
) -> MutationOutcome:
    semantics = compile_update_semantics(
        operation,
        dialect=dialect,
        selector_filter=selector_filter,
    )
    conn = require_connection()
    purge_expired_documents(conn, db_name, coll_name)
    selected: tuple[str, Document] | None = None
    sql_selection_supported = False
    collection_options = collection_options_or_empty(conn, db_name, coll_name)
    try:
        if dialect_requires_python_fallback(semantics.dialect):
            raise NotImplementedError("Custom dialect requires Python fallback")
        if semantics.collation is not None or getattr(semantics, 'sort', None):
            raise NotImplementedError("Collation requires Python update fallback")
        selected = select_first_document_for_plan(db_name, coll_name, semantics.query_plan)
        sql_selection_supported = True
    except (NotImplementedError, TypeError):
        pass

    if selected is None and not sql_selection_supported:
        matching_documents: list[tuple[str, Document]] = []
        for storage_key, document in load_documents(db_name, coll_name):
            if not match_plan(
                document,
                semantics.query_plan,
                semantics.dialect,
                semantics.collation,
                semantics.variables,
            ):
                continue
            if semantics.selector_plan is not None and not match_plan(
                document,
                semantics.selector_plan,
                semantics.dialect,
                semantics.collation,
                semantics.variables,
            ):
                continue
            matching_documents.append((storage_key, document))
        if semantics.sort:
            item_by_document_id = {
                id(document): (storage_key, document)
                for storage_key, document in matching_documents
            }
            matching_documents = [
                item_by_document_id[id(document)]
                for document in sort_documents(
                    [document for _, document in matching_documents],
                    semantics.sort,
                    dialect=semantics.dialect,
                    collation=semantics.collation,
                )
            ]
        selected = matching_documents[0] if matching_documents else None

    if selected is not None:
        storage_key, original_document = selected
        selector_plan = getattr(semantics, "selector_plan", None)
        if selector_plan is not None and not match_plan(
            original_document,
            selector_plan,
            semantics.dialect,
            semantics.collation,
            semantics.variables,
        ):
            selected = None

    if selected is not None:
        storage_key, original_document = selected
        assert_document_matches_storage_key(
            original_document,
            storage_key,
            storage_key_for_id=storage_key_for_id,
        )
        document = (
            materialize_replacement_document(
                original_document,
                replacement_document,
            )
            if replacement_document is not None
            else deepcopy(original_document)
        )
        modified = (
            not semantics.dialect.values_equal(document, original_document)
            if replacement_document is not None
            else semantics.compiled_update_plan.apply(
                document,
                variables=semantics.variables,
            )
        )
        if not modified:
            result = UpdateResult(matched_count=1, modified_count=0)
            return MutationOutcome(
                result=result,
                before_document=deepcopy(original_document),
                after_document=deepcopy(document),
            )
        assert_document_kept_storage_key(
            document,
            storage_key,
            storage_key_for_id=storage_key_for_id,
        )
        if not bypass_document_validation:
            enforce_collection_document_validation(
                document,
                options=collection_options,
                original_document=original_document,
                dialect=semantics.dialect,
            )
        validate_document_against_unique_indexes(
            db_name,
            coll_name,
            document,
            storage_key,
        )
        indexes = load_indexes(db_name, coll_name)
        search_indexes = load_search_index_rows(db_name, coll_name)

        try:
            if dialect_requires_python_fallback(semantics.dialect):
                raise NotImplementedError("Custom dialect requires Python fallback")
            if operation.array_filters is not None:
                raise NotImplementedError("array_filters require Python update fallback")
            if replacement_document is not None:
                raise NotImplementedError("replacement updates require Python update fallback")
            if not isinstance(semantics.compiled_update_plan, compiled_update_plan_type):
                raise NotImplementedError("Aggregation pipeline updates require Python update fallback")
            update_sql, update_params = translate_compiled_update_plan(
                semantics.compiled_update_plan,
                original_document,
            )
            with sqlite_write_scope(
                conn,
                begin_write=begin_write,
                commit_write=commit_write,
                rollback_write=rollback_write,
            ):
                conn.execute(
                    f"""
                    UPDATE documents
                    SET document = {update_sql}
                    WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                    """,
                    (*update_params, db_name, coll_name, storage_key),
                )
                rebuild_multikey_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    indexes,
                )
                rebuild_scalar_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    indexes,
                )
                replace_search_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    search_indexes,
                )
                invalidate_collection_features_cache(db_name, coll_name)
            result = UpdateResult(matched_count=1, modified_count=1)
            return MutationOutcome(
                result=result,
                before_document=deepcopy(original_document),
                after_document=deepcopy(document),
            )
        except (NotImplementedError, TypeError):
            pass
        except sqlite3.IntegrityError as exc:
            raise DuplicateKeyError(str(exc)) from exc
        except Exception:
            raise

        try:
            with sqlite_write_scope(
                conn,
                begin_write=begin_write,
                commit_write=commit_write,
                rollback_write=rollback_write,
            ):
                conn.execute(
                    """
                    UPDATE documents
                    SET document = ?
                    WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                    """,
                    (serialize_document(document), db_name, coll_name, storage_key),
                )
                rebuild_multikey_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    indexes,
                )
                rebuild_scalar_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    indexes,
                )
                replace_search_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    search_indexes,
                )
                invalidate_collection_features_cache(db_name, coll_name)
            result = UpdateResult(matched_count=1, modified_count=1)
            return MutationOutcome(
                result=result,
                before_document=deepcopy(original_document),
                after_document=deepcopy(document),
            )
        except sqlite3.IntegrityError as exc:
            raise DuplicateKeyError(str(exc)) from exc
        except Exception:
            raise

    if not upsert:
        result = UpdateResult(matched_count=0, modified_count=0)
        return MutationOutcome(result=result)

    new_doc = deepcopy(
        upsert_seed
        if replacement_document is not None and upsert_seed is not None
        else (
            replacement_document
            if replacement_document is not None
            else (upsert_seed or {})
        )
    )
    if replacement_document is None:
        semantics.compiled_upsert_plan.apply(new_doc, variables=semantics.variables)
    if "_id" not in new_doc:
        new_doc["_id"] = new_object_id()
    assert_valid_root_document_id(new_doc["_id"])
    if not bypass_document_validation:
        enforce_collection_document_validation(
            new_doc,
            options=collection_options,
            original_document=None,
            is_upsert_insert=True,
            dialect=semantics.dialect,
        )
    validate_document_against_unique_indexes(db_name, coll_name, new_doc, None)

    storage_key = storage_key_for_id(new_doc["_id"])
    indexes = load_indexes(db_name, coll_name)
    search_indexes = load_search_index_rows(db_name, coll_name)
    try:
        with sqlite_write_scope(
            conn,
            begin_write=begin_write,
            commit_write=commit_write,
            rollback_write=rollback_write,
        ):
            conn.execute(
                """
                INSERT INTO documents (db_name, coll_name, storage_key, document)
                VALUES (?, ?, ?, ?)
                """,
                (db_name, coll_name, storage_key, serialize_document(new_doc)),
            )
            rebuild_multikey_entries_for_document(
                conn,
                db_name,
                coll_name,
                storage_key,
                new_doc,
                indexes,
            )
            rebuild_scalar_entries_for_document(
                conn,
                db_name,
                coll_name,
                storage_key,
                new_doc,
                indexes,
            )
            replace_search_entries_for_document(
                conn,
                db_name,
                coll_name,
                storage_key,
                new_doc,
                search_indexes,
            )
            invalidate_collection_features_cache(db_name, coll_name)
    except sqlite3.IntegrityError as exc:
        raise DuplicateKeyError(str(exc)) from exc
    except Exception:
        raise

    result = UpdateResult(
        matched_count=0,
        modified_count=0,
        upserted_id=new_doc["_id"],
    )
    return MutationOutcome(
        result=result,
        after_document=deepcopy(new_doc),
    )
