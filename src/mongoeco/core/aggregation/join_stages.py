from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
from mongoeco.core.filtering import QueryEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import Document

from mongoeco.core.aggregation.runtime import (
    _CURRENT_COLLECTION_RESOLVER_KEY,
    _lookup_matches,
    _require_lookup_spec,
    _require_pipeline_spec,
    _require_union_with_spec,
    evaluate_expression,
)


def _apply_lookup(
    documents: list[Document],
    spec: object,
    collection_resolver,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    spill_policy=None,
) -> list[Document]:
    lookup = _require_lookup_spec(spec)
    if collection_resolver is None:
        raise OperationFailure("$lookup requires collection resolver support")

    foreign_documents = list(collection_resolver(lookup["from"]) or [])
    result: list[Document] = []
    for document in documents:
        candidate_documents = foreign_documents
        if "localField" in lookup and "foreignField" in lookup:
            local_values = QueryEngine.extract_values(document, lookup["localField"])
            candidate_documents = [
                foreign_document
                for foreign_document in foreign_documents
                if _lookup_matches(
                    local_values,
                    QueryEngine.extract_values(foreign_document, lookup["foreignField"]),
                    dialect=dialect,
                    collation=collation,
                )
            ]
        if "pipeline" in lookup:
            scoped = dict(variables or {})
            for name, expression in lookup["let"].items():
                scoped[name] = evaluate_expression(document, expression, variables, dialect=dialect)
            from mongoeco.core.aggregation.stages import apply_pipeline

            matches = apply_pipeline(
                [deepcopy(candidate) for candidate in candidate_documents],
                lookup["pipeline"],
                collection_resolver=collection_resolver,
                variables=scoped,
                dialect=dialect,
                collation=collation,
                spill_policy=spill_policy,
            )
        else:
            matches = [deepcopy(candidate) for candidate in candidate_documents]
        joined = deepcopy(document)
        joined[lookup["as"]] = matches
        result.append(joined)
    return result


def _apply_union_with(
    documents: list[Document],
    spec: object,
    collection_resolver,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    spill_policy=None,
) -> list[Document]:
    union_with = _require_union_with_spec(spec)
    if collection_resolver is None:
        raise OperationFailure("$unionWith requires collection resolver support")

    resolver_key = union_with["coll"] if union_with["coll"] is not None else _CURRENT_COLLECTION_RESOLVER_KEY
    resolved_foreign_documents = collection_resolver(resolver_key)
    if resolved_foreign_documents is None:
        foreign_documents = [deepcopy(document) for document in documents]
    else:
        foreign_documents = [deepcopy(document) for document in resolved_foreign_documents]
    if union_with["pipeline"]:
        from mongoeco.core.aggregation.stages import apply_pipeline

        foreign_documents = apply_pipeline(
            foreign_documents,
            union_with["pipeline"],
            collection_resolver=collection_resolver,
            variables=variables,
            dialect=dialect,
            collation=collation,
            spill_policy=spill_policy,
        )
    return [deepcopy(document) for document in documents] + foreign_documents


def _apply_facet(
    documents: list[Document],
    spec: object,
    collection_resolver,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    spill_policy=None,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$facet requires a document specification")
    from mongoeco.core.aggregation.stages import apply_pipeline

    faceted: Document = {}
    for field, pipeline in spec.items():
        if not isinstance(field, str):
            raise OperationFailure("$facet field names must be strings")
        faceted[field] = apply_pipeline(
            documents,
            _require_pipeline_spec("$facet", pipeline),
            collection_resolver=collection_resolver,
            variables=variables,
            dialect=dialect,
            collation=collation,
            spill_policy=spill_policy,
        )
    return [faceted]
