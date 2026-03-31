from copy import deepcopy
import random
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.paths import delete_document_value, set_document_value
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import compile_filter
from mongoeco.errors import OperationFailure
from mongoeco.types import Document

from mongoeco.core.aggregation.runtime import (
    _expression_truthy,
    evaluate_expression,
)
from mongoeco.core.aggregation.planning import (
    _match_spec_contains_expr,
    _projection_flag,
    _require_projection_for_dialect,
    _require_sample_spec,
    _require_unset_spec,
)


def _apply_match(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$match requires a document specification")

    def _match_spec(document: Document, match_spec: dict[str, Any]) -> bool:
        expr = match_spec.get("$expr")
        filter_spec = {key: value for key, value in match_spec.items() if key != "$expr"}
        if "$and" in filter_spec:
            clauses = filter_spec.pop("$and")
            if not isinstance(clauses, list):
                raise OperationFailure("$and in $match requires a list")
            if not all(_match_spec(document, clause) for clause in clauses):
                return False
        if "$or" in filter_spec:
            clauses = filter_spec.pop("$or")
            if not isinstance(clauses, list):
                raise OperationFailure("$or in $match requires a list")
            if not any(_match_spec(document, clause) for clause in clauses):
                return False
        if "$nor" in filter_spec:
            clauses = filter_spec.pop("$nor")
            if not isinstance(clauses, list):
                raise OperationFailure("$nor in $match requires a list")
            if any(_match_spec(document, clause) for clause in clauses):
                return False
        if filter_spec:
            plan = compile_filter(filter_spec, dialect=dialect)
            if not QueryEngine.match_plan(document, plan, dialect=dialect, collation=collation):
                return False
        if expr is not None and not _expression_truthy(
            evaluate_expression(document, expr, variables, dialect=dialect),
            dialect=dialect,
        ):
            return False
        return True

    if _match_spec_contains_expr(spec):
        return [document for document in documents if _match_spec(document, spec)]

    plan = compile_filter(spec, dialect=dialect) if spec else None
    result: list[Document] = []
    for document in documents:
        if plan is not None and not QueryEngine.match_plan(
            document,
            plan,
            dialect=dialect,
            collation=collation,
        ):
            continue
        result.append(document)
    return result


def _apply_add_fields(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$addFields requires a document specification")
    for path in spec:
        if not isinstance(path, str):
            raise OperationFailure("$addFields field names must be strings")
    result: list[Document] = []
    for document in documents:
        enriched = deepcopy(document)
        evaluated = {
            path: evaluate_expression(document, expression, variables, dialect=dialect)
            for path, expression in spec.items()
        }
        for path in spec:
            if evaluated[path] is _REMOVE:
                delete_document_value(enriched, path)
                continue
            if evaluated[path] is _MISSING:
                continue
            set_document_value(enriched, path, evaluated[path])
        result.append(enriched)
    return result


def _apply_unset(
    documents: list[Document],
    spec: object,
) -> list[Document]:
    fields = _require_unset_spec(spec)
    result: list[Document] = []
    for document in documents:
        trimmed = deepcopy(document)
        for field in fields:
            delete_document_value(trimmed, field)
        result.append(trimmed)
    return result


def _apply_sample(documents: list[Document], spec: object) -> list[Document]:
    size = _require_sample_spec(spec)
    if size == 0:
        return []
    sample_size = min(size, len(documents))
    return random.sample(documents, sample_size)


def _apply_project(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    projection = _require_projection_for_dialect(spec, dialect=dialect)
    computed_fields = {
        key: value
        for key, value in projection.items()
        if _projection_flag(value, dialect=dialect) is None
    }
    if not computed_fields:
        return [
            apply_projection(document, projection, dialect=dialect)
            for document in documents
        ]

    include_fields = {
        key: _projection_flag(value, dialect=dialect)
        for key, value in projection.items()
        if _projection_flag(value, dialect=dialect) is not None
    }
    include_id = include_fields.get("_id", 1) != 0
    result: list[Document] = []
    for document in documents:
        projected: Document = {}
        include_mode = any(value == 1 for key, value in include_fields.items() if key != "_id")
        if include_mode:
            projected = apply_projection(document, include_fields, dialect=dialect)
        elif include_id and "_id" in document:
            projected["_id"] = deepcopy(document["_id"])
        for path, expression in computed_fields.items():
            value = evaluate_expression(document, expression, variables, dialect=dialect)
            if value is _REMOVE:
                delete_document_value(projected, path)
                continue
            if value is _MISSING:
                continue
            set_document_value(projected, path, value)
        result.append(projected)
    return result


def _apply_replace_root(
    documents: list[Document],
    spec: object,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> list[Document]:
    if not isinstance(spec, dict) or "newRoot" not in spec:
        raise OperationFailure("$replaceRoot requires a document with newRoot")
    result: list[Document] = []
    for document in documents:
        new_root = evaluate_expression(document, spec["newRoot"], variables, dialect=dialect)
        if not isinstance(new_root, dict):
            raise OperationFailure("$replaceRoot newRoot must evaluate to a document")
        result.append(deepcopy(new_root))
    return result


from mongoeco.core.aggregation.runtime import _MISSING, _REMOVE  # noqa: E402
