import re

from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.evaluation_environment import (
    scoped_environment,
)
from mongoeco.core.aggregation.lookup_physical import (
    build_bounded_lookup_hash_plan,
)
from mongoeco.core.aggregation.runtime import (
    _CURRENT_COLLECTION_RESOLVER_KEY,
    _lookup_matches,
    _require_lookup_spec,
    _require_pipeline_spec,
    _require_union_with_spec,
    evaluate_expression,
)
from mongoeco.core.work_control import iter_with_deadline
from mongoeco.core.collation import CollationSpec
from mongoeco.core.filtering import QueryEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import Document


_LOOKUP_LET_VARIABLE_RE = re.compile(
    r"^(?:[a-z]|[^\x00-\x7f])(?:[A-Za-z0-9_]|[^\x00-\x7f])*$"
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
    lookup_hash_max_associations: int | None = None,
    deadline: float | None = None,
) -> list[Document]:
    lookup = _require_lookup_spec(spec)
    if collection_resolver is None:
        raise OperationFailure("$lookup requires collection resolver support")

    foreign_documents = list(collection_resolver(lookup["from"]) or [])
    hash_plan = None
    if "pipeline" not in lookup:
        hash_plan = build_bounded_lookup_hash_plan(
            foreign_documents,
            lookup["foreignField"],
            document_getter=lambda document: document,
            dialect=dialect,
            collation=collation,
            max_associations=lookup_hash_max_associations,
            deadline=deadline,
        )
    result: list[Document] = []
    for document in iter_with_deadline(documents, deadline):
        candidate_documents = foreign_documents
        if "localField" in lookup and "foreignField" in lookup:
            local_values = QueryEngine.extract_values(document, lookup["localField"])
            if hash_plan is not None:
                candidate_indices = hash_plan.candidate_indices(local_values)
                if candidate_indices is not None:
                    candidate_documents = [
                        foreign_documents[index] for index in candidate_indices
                    ]
            candidate_documents = [
                foreign_document
                for foreign_document in iter_with_deadline(
                    candidate_documents,
                    deadline,
                )
                if _lookup_matches(
                    local_values,
                    QueryEngine.extract_values(
                        foreign_document, lookup["foreignField"]
                    ),
                    dialect=dialect,
                    collation=collation,
                )
            ]
        if "pipeline" in lookup:
            scoped = scoped_environment(variables)
            for name, expression in lookup["let"].items():
                if not _LOOKUP_LET_VARIABLE_RE.match(name):
                    raise OperationFailure(
                        "$lookup let variable names must begin with a lowercase letter or non-ascii character"
                    )
                scoped[name] = evaluate_expression(
                    document, expression, variables, dialect=dialect
                )
            from mongoeco.core.aggregation.stages import apply_pipeline

            matches = apply_pipeline(
                [deepcopy(candidate) for candidate in candidate_documents],
                lookup["pipeline"],
                collection_resolver=collection_resolver,
                variables=scoped,
                dialect=dialect,
                collation=collation,
                spill_policy=spill_policy,
                lookup_hash_max_associations=lookup_hash_max_associations,
                deadline=deadline,
            )
        else:
            matches = [
                deepcopy(candidate)
                for candidate in iter_with_deadline(candidate_documents, deadline)
            ]
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
    lookup_hash_max_associations: int | None = None,
    deadline: float | None = None,
) -> list[Document]:
    union_with = _require_union_with_spec(spec)
    if collection_resolver is None:
        raise OperationFailure("$unionWith requires collection resolver support")

    resolver_key = (
        union_with["coll"]
        if union_with["coll"] is not None
        else _CURRENT_COLLECTION_RESOLVER_KEY
    )
    resolved_foreign_documents = collection_resolver(resolver_key)
    if resolved_foreign_documents is None:
        foreign_documents = [
            deepcopy(document) for document in iter_with_deadline(documents, deadline)
        ]
    else:
        foreign_documents = [
            deepcopy(document)
            for document in iter_with_deadline(
                resolved_foreign_documents,
                deadline,
            )
        ]
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
            lookup_hash_max_associations=lookup_hash_max_associations,
            deadline=deadline,
        )
    return [
        deepcopy(document) for document in iter_with_deadline(documents, deadline)
    ] + foreign_documents


def _apply_facet(
    documents: list[Document],
    spec: object,
    collection_resolver,
    variables: dict[str, Any] | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    spill_policy=None,
    lookup_hash_max_associations: int | None = None,
    deadline: float | None = None,
) -> list[Document]:
    if not isinstance(spec, dict):
        raise OperationFailure("$facet requires a document specification")
    from mongoeco.core.aggregation.stages import apply_pipeline

    faceted: Document = {}
    for field, pipeline in spec.items():
        if not isinstance(field, str):
            raise OperationFailure("$facet field names must be strings")
        faceted[field] = apply_pipeline(
            list(documents),
            _require_pipeline_spec("$facet", pipeline),
            collection_resolver=collection_resolver,
            variables=variables,
            dialect=dialect,
            collation=collation,
            spill_policy=spill_policy,
            lookup_hash_max_associations=lookup_hash_max_associations,
            deadline=deadline,
        )
    return [faceted]
