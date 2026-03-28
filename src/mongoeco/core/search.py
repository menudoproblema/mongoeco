from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.types import Document, SearchIndexDefinition, SearchIndexDocument


SUPPORTED_SEARCH_INDEX_TYPES = {"search", "vectorSearch"}
TEXTUAL_SEARCH_INDEX_TYPES = {"search"}


@dataclass(frozen=True, slots=True)
class SearchTextQuery:
    index_name: str
    raw_query: str
    terms: tuple[str, ...]
    paths: tuple[str, ...] | None = None


def validate_search_index_definition(
    definition: Document,
    *,
    index_type: str,
) -> Document:
    if not isinstance(definition, dict):
        raise TypeError("search index definition must be a document")
    if index_type not in SUPPORTED_SEARCH_INDEX_TYPES:
        raise OperationFailure(f"unsupported search index type: {index_type}")
    normalized = deepcopy(definition)
    if index_type == "search":
        _validate_text_search_definition(normalized)
    return normalized


def build_search_index_document(definition: SearchIndexDefinition) -> SearchIndexDocument:
    queryable = definition.index_type in TEXTUAL_SEARCH_INDEX_TYPES
    return {
        "name": definition.name,
        "type": definition.index_type,
        "definition": deepcopy(definition.definition),
        "latestDefinition": deepcopy(definition.definition),
        "queryable": queryable,
        "status": "READY" if queryable else "UNSUPPORTED",
    }


def validate_search_stage_pipeline(pipeline: object) -> None:
    if not isinstance(pipeline, list):
        return
    found_search_operator: str | None = None
    for index, stage in enumerate(pipeline):
        if not isinstance(stage, dict) or len(stage) != 1:
            continue
        operator = next(iter(stage))
        if operator not in {"$search", "$vectorSearch"}:
            continue
        if index != 0:
            raise OperationFailure(f"{operator} must be the first stage in the pipeline")
        if found_search_operator is not None:
            raise OperationFailure("only one of $search or $vectorSearch may appear in a pipeline")
        found_search_operator = operator


def compile_search_stage(
    operator: str,
    spec: object,
) -> SearchTextQuery:
    if operator == "$vectorSearch":
        raise OperationFailure("$vectorSearch is not supported by the local search runtime")
    if operator != "$search":
        raise OperationFailure("unsupported search stage operator")
    return compile_search_text_query(spec)


def compile_search_text_query(spec: object) -> SearchTextQuery:
    if not isinstance(spec, dict):
        raise OperationFailure("$search requires a document specification")
    index_name = spec.get("index", "default")
    if not isinstance(index_name, str) or not index_name:
        raise OperationFailure("$search index must be a non-empty string")
    text_spec = spec.get("text")
    if not isinstance(text_spec, dict):
        raise OperationFailure("$search currently supports only the text operator")
    raw_query = text_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.text.query must be a non-empty string")
    paths = _normalize_search_paths(text_spec.get("path"))
    terms = tuple(term for term in raw_query.strip().split() if term)
    if not terms:
        raise OperationFailure("$search.text.query must contain at least one term")
    return SearchTextQuery(
        index_name=index_name,
        raw_query=raw_query,
        terms=terms,
        paths=paths,
    )


def matches_search_text_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchTextQuery,
) -> bool:
    entries = iter_searchable_text_entries(document, definition)
    if query.paths is not None:
        allowed = set(query.paths)
        entries = [entry for entry in entries if entry[0] in allowed]
    if not entries:
        return False
    lowered_entries = [value.lower() for _, value in entries if value]
    return all(any(term.lower() in value for value in lowered_entries) for term in query.terms)


def iter_searchable_text_entries(
    document: Document,
    definition: SearchIndexDefinition,
) -> list[tuple[str, str]]:
    if definition.index_type not in TEXTUAL_SEARCH_INDEX_TYPES:
        return []
    mappings = definition.definition.get("mappings")
    if not isinstance(mappings, dict):
        return _collect_dynamic_text_entries(document)
    return _collect_entries_from_mapping(document, mappings)


def sqlite_fts5_query(query: SearchTextQuery) -> str:
    return " AND ".join(_quote_fts_term(term) for term in query.terms)


def _quote_fts_term(term: str) -> str:
    escaped = term.replace('"', '""')
    return f'"{escaped}"'


def _normalize_search_paths(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        if not value:
            raise OperationFailure("$search.text.path must be a non-empty string")
        return (value,)
    if isinstance(value, list) and value and all(isinstance(item, str) and item for item in value):
        return tuple(value)
    if isinstance(value, dict) and value == {"wildcard": "*"}:
        return None
    raise OperationFailure("$search.text.path must be a string, a list of strings or {'wildcard': '*'}")


def _validate_text_search_definition(definition: Document) -> None:
    allowed_top_level = {"mappings", "analyzer", "searchAnalyzer"}
    unsupported = set(definition) - allowed_top_level
    if unsupported:
        raise OperationFailure(
            "unsupported local search index options: " + ", ".join(sorted(unsupported))
        )
    mappings = definition.get("mappings", {"dynamic": True})
    if not isinstance(mappings, dict):
        raise OperationFailure("search index mappings must be a document")
    _validate_mappings_document(mappings)


def _validate_mappings_document(mappings: Document) -> None:
    allowed = {"dynamic", "fields"}
    unsupported = set(mappings) - allowed
    if unsupported:
        raise OperationFailure(
            "unsupported local search mappings options: " + ", ".join(sorted(unsupported))
        )
    dynamic = mappings.get("dynamic", False)
    if not isinstance(dynamic, bool):
        raise OperationFailure("search index mappings.dynamic must be a boolean")
    fields = mappings.get("fields", {})
    if not isinstance(fields, dict):
        raise OperationFailure("search index mappings.fields must be a document")
    for field_name, field_spec in fields.items():
        if not isinstance(field_name, str) or not field_name:
            raise OperationFailure("search index field names must be non-empty strings")
        if not isinstance(field_spec, dict):
            raise OperationFailure("search index field mappings must be documents")
        _validate_field_mapping(field_spec)


def _validate_field_mapping(field_spec: Document) -> None:
    mapping_type = field_spec.get("type", "document")
    if mapping_type == "document":
        _validate_mappings_document(field_spec)
        return
    if mapping_type not in {"string", "autocomplete", "token"}:
        raise OperationFailure(f"unsupported local search field mapping type: {mapping_type}")
    unsupported = set(field_spec) - {"type", "analyzer", "searchAnalyzer"}
    if unsupported:
        raise OperationFailure(
            "unsupported local search field options: " + ", ".join(sorted(unsupported))
        )


def _collect_entries_from_mapping(document: object, mappings: Document, prefix: str = "") -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    if mappings.get("dynamic", False):
        entries.extend(_collect_dynamic_text_entries(document, prefix=prefix))
    fields = mappings.get("fields", {})
    if not isinstance(fields, dict) or not isinstance(document, dict):
        return entries
    for field_name, field_spec in fields.items():
        if field_name not in document or not isinstance(field_spec, dict):
            continue
        path = f"{prefix}.{field_name}" if prefix else field_name
        value = document[field_name]
        mapping_type = field_spec.get("type", "document")
        if mapping_type == "document":
            entries.extend(_collect_entries_from_mapping(value, field_spec, prefix=path))
            continue
        entries.extend(_collect_text_leaf_entries(value, path))
    return entries


def _collect_dynamic_text_entries(value: object, *, prefix: str = "") -> list[tuple[str, str]]:
    if isinstance(value, dict):
        entries: list[tuple[str, str]] = []
        for field_name, field_value in value.items():
            if not isinstance(field_name, str):
                continue
            path = f"{prefix}.{field_name}" if prefix else field_name
            entries.extend(_collect_dynamic_text_entries(field_value, prefix=path))
        return entries
    if isinstance(value, list):
        entries: list[tuple[str, str]] = []
        for item in value:
            entries.extend(_collect_dynamic_text_entries(item, prefix=prefix))
        return entries
    return _collect_text_leaf_entries(value, prefix)


def _collect_text_leaf_entries(value: object, path: str) -> list[tuple[str, str]]:
    if not path:
        return []
    if isinstance(value, str):
        return [(path, value)]
    if isinstance(value, list):
        entries: list[tuple[str, str]] = []
        for item in value:
            if isinstance(item, str):
                entries.append((path, item))
        return entries
    return []
