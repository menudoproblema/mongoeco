from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
import sqlite3

from mongoeco.compat import MongoDialect
from mongoeco.core.identity import (
    assert_document_kept_storage_key,
    assert_document_matches_storage_key,
    assert_valid_root_document_id,
)
from mongoeco.engines._sqlite_write_scope import sqlite_write_scope
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import DeleteResult, Document, DocumentId, Filter, UpdateResult


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
) -> DeleteResult:
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
        if semantics.collation is not None:
            raise NotImplementedError("Collation requires Python delete fallback")
        selected = select_first_document_for_plan(db_name, coll_name, semantics.query_plan)
        if selected is None:
            return DeleteResult(deleted_count=0)
        storage_key, document = selected
        if selector_semantics is not None and not match_plan(
            document,
            selector_semantics.query_plan,
            selector_semantics.dialect,
            selector_semantics.collation,
            selector_semantics.variables,
        ):
            return DeleteResult(deleted_count=0)
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
        return DeleteResult(deleted_count=1)
    except (NotImplementedError, TypeError):
        pass
    except Exception:
        raise

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
        return DeleteResult(deleted_count=1)
    return DeleteResult(deleted_count=0)


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
) -> UpdateResult[DocumentId]:
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
        if semantics.collation is not None:
            raise NotImplementedError("Collation requires Python update fallback")
        selected = select_first_document_for_plan(db_name, coll_name, semantics.query_plan)
        sql_selection_supported = True
    except (NotImplementedError, TypeError):
        pass

    if selected is None and not sql_selection_supported:
        for storage_key, document in load_documents(db_name, coll_name):
            if not match_plan(
                document,
                semantics.query_plan,
                semantics.dialect,
                semantics.collation,
                semantics.variables,
            ):
                continue
            selected = (storage_key, document)
            break

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
        document = deepcopy(original_document)
        modified = semantics.compiled_update_plan.apply(
            document,
            variables=semantics.variables,
        )
        if not modified:
            return UpdateResult(matched_count=1, modified_count=0)
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
            return UpdateResult(matched_count=1, modified_count=1)
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
            return UpdateResult(matched_count=1, modified_count=1)
        except sqlite3.IntegrityError as exc:
            raise DuplicateKeyError(str(exc)) from exc
        except Exception:
            raise

    if not upsert:
        return UpdateResult(matched_count=0, modified_count=0)

    new_doc = deepcopy(upsert_seed or {})
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

    return UpdateResult(
        matched_count=0,
        modified_count=0,
        upserted_id=new_doc["_id"],
    )
