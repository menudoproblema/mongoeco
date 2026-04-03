from __future__ import annotations

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass
import fnmatch
import math
import re
from typing import Any, Callable
import unicodedata

from mongoeco.core._search_contract import (
    SEARCH_STAGE_OPERATORS,
    TEXT_SEARCH_INDEX_CAPABILITIES,
    TEXT_SEARCH_OPERATOR_NAMES,
)
from mongoeco.core.paths import get_document_value
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, EngineIndexRecord, SearchIndexDefinition, SearchIndexDocument


SUPPORTED_SEARCH_INDEX_TYPES = {"search", "vectorSearch"}
TEXTUAL_SEARCH_INDEX_TYPES = {"search"}
TEXT_SCORE_FIELD = "__mongoeco_textScore__"
_TEXT_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


@dataclass(frozen=True, slots=True)
class ClassicTextQuery:
    raw_query: str
    terms: tuple[str, ...]
    case_sensitive: bool = False
    diacritic_sensitive: bool = False


@dataclass(frozen=True, slots=True)
class SearchTextQuery:
    index_name: str
    raw_query: str
    terms: tuple[str, ...]
    paths: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class SearchPhraseQuery:
    index_name: str
    raw_query: str
    paths: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class SearchAutocompleteQuery:
    index_name: str
    raw_query: str
    terms: tuple[str, ...]
    paths: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class SearchWildcardQuery:
    index_name: str
    raw_query: str
    normalized_pattern: str
    paths: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class SearchCompoundQuery:
    index_name: str
    must: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | "SearchCompoundQuery", ...] = ()
    should: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | "SearchCompoundQuery", ...] = ()
    filter: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | "SearchCompoundQuery", ...] = ()
    must_not: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | "SearchCompoundQuery", ...] = ()
    minimum_should_match: int = 0


@dataclass(frozen=True, slots=True)
class SearchVectorQuery:
    index_name: str
    path: str
    query_vector: tuple[float, ...]
    limit: int
    num_candidates: int
    filter_spec: Document | None = None
    similarity: str = "cosine"


type SearchTextLikeQuery = (
    SearchTextQuery
    | SearchPhraseQuery
    | SearchAutocompleteQuery
    | SearchWildcardQuery
    | SearchCompoundQuery
)
type SearchQuery = SearchTextLikeQuery | SearchVectorQuery
type _SearchClauseCompiler = Callable[[str, object], SearchTextLikeQuery]
type _SearchMatcher = Callable[[Document, SearchIndexDefinition, SearchTextLikeQuery], bool]
type _SearchExplainBuilder = Callable[[SearchQuery], dict[str, object | None]]


def compile_classic_text_query(spec: object) -> ClassicTextQuery:
    if not isinstance(spec, dict):
        raise OperationFailure("$text requires a document specification")
    unsupported = sorted(set(spec) - {"$search", "$caseSensitive", "$diacriticSensitive"})
    if unsupported:
        raise OperationFailure(
            "$text local runtime supports only $search, $caseSensitive and $diacriticSensitive; unsupported keys: "
            + ", ".join(unsupported)
        )
    raw_query = spec.get("$search")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$text.$search must be a non-empty string")
    case_sensitive = bool(spec.get("$caseSensitive", False))
    diacritic_sensitive = bool(spec.get("$diacriticSensitive", False))
    if case_sensitive:
        raise OperationFailure("$text.$caseSensitive=true is not supported in the local runtime")
    if diacritic_sensitive:
        raise OperationFailure("$text.$diacriticSensitive=true is not supported in the local runtime")
    terms = tuple(tokenize_classic_text(raw_query))
    if not terms:
        raise OperationFailure("$text.$search must contain at least one searchable token")
    return ClassicTextQuery(
        raw_query=raw_query,
        terms=terms,
        case_sensitive=False,
        diacritic_sensitive=False,
    )


def split_classic_text_filter(filter_spec: Document) -> tuple[Document, ClassicTextQuery | None]:
    if "$text" not in filter_spec:
        return filter_spec, None
    raw_text_spec = filter_spec.get("$text")
    if raw_text_spec is None:
        raise OperationFailure("$text requires a document specification")
    remaining = {key: value for key, value in filter_spec.items() if key != "$text"}
    return remaining, compile_classic_text_query(raw_text_spec)


def tokenize_classic_text(
    value: str,
    *,
    case_sensitive: bool = False,
    diacritic_sensitive: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, str):
        return ()
    normalized = value
    if not diacritic_sensitive:
        normalized = "".join(
            character
            for character in unicodedata.normalize("NFKD", normalized)
            if not unicodedata.combining(character)
        )
    if not case_sensitive:
        normalized = normalized.lower()
    return tuple(match.group(0) for match in _TEXT_TOKEN_RE.finditer(normalized))


def resolve_classic_text_index(
    indexes: list[EngineIndexRecord],
    *,
    hinted_name: str | None = None,
) -> tuple[str, str]:
    candidates = [
        index
        for index in indexes
        if len(index.key) == 1 and index.key[0][1] == "text"
    ]
    if hinted_name is not None:
        candidates = [index for index in candidates if index.name == hinted_name]
    if not candidates:
        raise OperationFailure("classic $text requires a single-field text index on the collection")
    if len(candidates) > 1:
        if hinted_name is not None:
            raise OperationFailure(f"text index not found with name [{hinted_name}]")
        raise OperationFailure(
            "classic $text is ambiguous with multiple text indexes; use a single text index per collection"
        )
    index = candidates[0]
    return index.name, index.key[0][0]


def classic_text_score(
    document: Document,
    *,
    field: str,
    query: ClassicTextQuery,
) -> float | None:
    token_counter = Counter()
    for value in iter_classic_text_values(document, field):
        token_counter.update(
            tokenize_classic_text(
                value,
                case_sensitive=query.case_sensitive,
                diacritic_sensitive=query.diacritic_sensitive,
            )
        )
    if not token_counter:
        return None
    score = sum(token_counter.get(term, 0) for term in query.terms)
    if score <= 0:
        return None
    return float(score)


def attach_text_score(document: Document, score: float) -> Document:
    enriched = dict(document)
    enriched[TEXT_SCORE_FIELD] = float(score)
    return enriched


def iter_classic_text_values(document: Document, field: str) -> tuple[str, ...]:
    found, value = get_document_value(document, field)
    if not found:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list):
        return tuple(item for item in value if isinstance(item, str))
    return ()


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
    elif index_type == "vectorSearch":
        _validate_vector_search_definition(normalized)
    return normalized


def is_queryable_search_definition(definition: SearchIndexDefinition) -> bool:
    if definition.index_type in TEXTUAL_SEARCH_INDEX_TYPES:
        return True
    if definition.index_type == "vectorSearch":
        return bool(_vector_field_specs(definition))
    return False


def build_search_index_document(
    definition: SearchIndexDefinition,
    *,
    ready: bool = True,
    ready_at_epoch: float | None = None,
) -> SearchIndexDocument:
    queryable = is_queryable_search_definition(definition)
    if definition.index_type == "vectorSearch":
        capabilities: tuple[str, ...] = ("vectorSearch",)
        query_mode = "vector"
        experimental = True
    else:
        capabilities = TEXT_SEARCH_INDEX_CAPABILITIES
        query_mode = "text"
        experimental = False
    status = "READY" if queryable and ready else "PENDING" if queryable else "UNSUPPORTED"
    status_detail = "ready" if status == "READY" else "pending-build" if status == "PENDING" else "unsupported-definition"
    return {
        "name": definition.name,
        "type": definition.index_type,
        "definition": deepcopy(definition.definition),
        "latestDefinition": deepcopy(definition.definition),
        "queryable": queryable,
        "status": status,
        "statusDetail": status_detail,
        "queryMode": query_mode,
        "experimental": experimental,
        "capabilities": list(capabilities),
        "readyAtEpoch": ready_at_epoch,
    }


def validate_search_stage_pipeline(pipeline: object) -> None:
    if not isinstance(pipeline, list):
        return
    for index, stage in enumerate(pipeline):
        if not isinstance(stage, dict) or len(stage) != 1:
            continue
        operator = next(iter(stage))
        if operator not in SEARCH_STAGE_OPERATORS:
            continue
        if index != 0:
            raise OperationFailure(f"{operator} must be the first stage in the pipeline")


def compile_search_stage(operator: str, spec: object) -> SearchQuery:
    compiler = _SEARCH_STAGE_COMPILERS.get(operator)
    if compiler is None:
        raise OperationFailure("unsupported search stage operator")
    return compiler(spec)


def compile_search_text_like_query(spec: object) -> SearchTextLikeQuery:
    if not isinstance(spec, dict):
        raise OperationFailure("$search requires a document specification")
    unsupported_operators = sorted(set(spec) - {"index", *TEXT_SEARCH_OPERATOR_NAMES})
    if unsupported_operators:
        raise OperationFailure(
            "$search local runtime supports only "
            + ", ".join(TEXT_SEARCH_OPERATOR_NAMES[:-1])
            + " and "
            + TEXT_SEARCH_OPERATOR_NAMES[-1]
            + "; unsupported keys: "
            + ", ".join(unsupported_operators)
        )
    index_name = spec.get("index", "default")
    if not isinstance(index_name, str) or not index_name:
        raise OperationFailure("$search index must be a non-empty string")
    clause_names = [name for name in TEXT_SEARCH_OPERATOR_NAMES if name in spec]
    if len(clause_names) != 1:
        raise OperationFailure(
            "$search requires exactly one of "
            + ", ".join(TEXT_SEARCH_OPERATOR_NAMES[:-1])
            + " or "
            + TEXT_SEARCH_OPERATOR_NAMES[-1]
        )
    clause_name = clause_names[0]
    clause_spec = spec.get(clause_name)
    compiler = _SEARCH_CLAUSE_COMPILERS.get(clause_name)
    if compiler is None:
        raise OperationFailure(f"unsupported local $search operator: {clause_name}")
    return compiler(index_name, clause_spec)


def compile_search_text_query(spec: object) -> SearchTextQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchTextQuery):
        raise OperationFailure("$search.text specification is required")
    return query


def compile_search_phrase_query(spec: object) -> SearchPhraseQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchPhraseQuery):
        raise OperationFailure("$search.phrase specification is required")
    return query


def compile_search_autocomplete_query(spec: object) -> SearchAutocompleteQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchAutocompleteQuery):
        raise OperationFailure("$search.autocomplete specification is required")
    return query


def compile_search_wildcard_query(spec: object) -> SearchWildcardQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchWildcardQuery):
        raise OperationFailure("$search.wildcard specification is required")
    return query


def compile_search_compound_query(spec: object) -> SearchCompoundQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchCompoundQuery):
        raise OperationFailure("$search.compound specification is required")
    return query


def compile_vector_search_query(spec: object) -> SearchVectorQuery:
    if not isinstance(spec, dict):
        raise OperationFailure("$vectorSearch requires a document specification")
    unsupported = sorted(set(spec) - {"index", "path", "queryVector", "limit", "numCandidates", "filter"})
    if unsupported:
        raise OperationFailure(
            "$vectorSearch local runtime supports only index, path, queryVector, limit, numCandidates and filter; "
            "unsupported keys: " + ", ".join(unsupported)
        )
    index_name = spec.get("index", "default")
    if not isinstance(index_name, str) or not index_name:
        raise OperationFailure("$vectorSearch index must be a non-empty string")
    path = spec.get("path")
    if not isinstance(path, str) or not path:
        raise OperationFailure("$vectorSearch.path must be a non-empty string")
    raw_query_vector = spec.get("queryVector")
    if not isinstance(raw_query_vector, list) or not raw_query_vector:
        raise OperationFailure("$vectorSearch.queryVector must be a non-empty array")
    if not all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in raw_query_vector):
        raise OperationFailure("$vectorSearch.queryVector must contain only numeric values")
    limit = spec.get("limit")
    if not isinstance(limit, int) or isinstance(limit, bool) or limit <= 0:
        raise OperationFailure("$vectorSearch.limit must be a positive integer")
    num_candidates = spec.get("numCandidates", limit)
    if not isinstance(num_candidates, int) or isinstance(num_candidates, bool) or num_candidates < limit:
        raise OperationFailure("$vectorSearch.numCandidates must be an integer >= limit")
    filter_spec = spec.get("filter")
    if filter_spec is not None and not isinstance(filter_spec, dict):
        raise OperationFailure("$vectorSearch.filter must be a document")
    return SearchVectorQuery(
        index_name=index_name,
        path=path,
        query_vector=tuple(float(value) for value in raw_query_vector),
        limit=limit,
        num_candidates=num_candidates,
        filter_spec=deepcopy(filter_spec),
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


def matches_search_phrase_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchPhraseQuery,
) -> bool:
    entries = iter_searchable_text_entries(document, definition)
    if query.paths is not None:
        allowed = set(query.paths)
        entries = [entry for entry in entries if entry[0] in allowed]
    if not entries:
        return False
    phrase = query.raw_query.strip().lower()
    return any(phrase in value.lower() for _, value in entries if value)


def matches_search_autocomplete_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchAutocompleteQuery,
) -> bool:
    entries = iter_searchable_text_entries(document, definition)
    if query.paths is not None:
        allowed = set(query.paths)
        entries = [entry for entry in entries if entry[0] in allowed]
    if not entries:
        return False
    query_terms = tuple(term.lower() for term in query.terms)
    for _, value in entries:
        if not value:
            continue
        candidate_tokens = tokenize_classic_text(value)
        if not candidate_tokens:
            continue
        lowered_tokens = tuple(token.lower() for token in candidate_tokens)
        if all(any(token.startswith(term) for token in lowered_tokens) for term in query_terms):
            return True
    return False


def matches_search_wildcard_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchWildcardQuery,
) -> bool:
    entries = iter_searchable_text_entries(document, definition)
    if query.paths is not None:
        allowed = set(query.paths)
        entries = [entry for entry in entries if entry[0] in allowed]
    if not entries:
        return False
    pattern = query.normalized_pattern
    return any(fnmatch.fnmatchcase(value.lower(), pattern) for _, value in entries if value)


def matches_search_compound_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchCompoundQuery,
) -> bool:
    for clause in query.must:
        if not matches_search_query(document, definition=definition, query=clause):
            return False
    for clause in query.filter:
        if not matches_search_query(document, definition=definition, query=clause):
            return False
    for clause in query.must_not:
        if matches_search_query(document, definition=definition, query=clause):
            return False
    if not query.should:
        return True
    matched_should = sum(
        1
        for clause in query.should
        if matches_search_query(document, definition=definition, query=clause)
    )
    return matched_should >= query.minimum_should_match


def matches_search_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchTextLikeQuery,
) -> bool:
    matcher = _SEARCH_QUERY_MATCHERS.get(type(query))
    if matcher is None:
        raise OperationFailure(f"unsupported local search query type: {type(query).__name__}")
    return matcher(document, definition, query)


def is_text_search_query(query: SearchQuery) -> bool:
    return type(query) in _SEARCH_QUERY_MATCHERS


def search_query_operator_name(query: SearchQuery) -> str | None:
    if isinstance(query, SearchVectorQuery):
        return None
    return _SEARCH_QUERY_OPERATOR_NAMES.get(type(query))


def search_query_explain_details(query: SearchQuery) -> dict[str, object | None]:
    details = _SEARCH_QUERY_EXPLAINERS[type(query)](query)
    details["queryOperator"] = search_query_operator_name(query)
    return details


def score_vector_document(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchVectorQuery,
) -> float | None:
    field_specs = _vector_field_specs(definition)
    field_spec = field_specs.get(query.path)
    if field_spec is None:
        return None
    found, value = get_document_value(document, query.path)
    if not found:
        return None
    if not isinstance(value, list) or not value:
        return None
    if not all(isinstance(item, (int, float)) and not isinstance(item, bool) for item in value):
        return None
    candidate = tuple(float(item) for item in value)
    if len(candidate) != field_spec["numDimensions"] or len(candidate) != len(query.query_vector):
        return None
    similarity = str(field_spec.get("similarity", query.similarity))
    if similarity == "dotProduct":
        return _dot_product(query.query_vector, candidate)
    if similarity == "euclidean":
        return _negative_euclidean_distance(query.query_vector, candidate)
    return _cosine_similarity(query.query_vector, candidate)


def vector_field_paths(definition: SearchIndexDefinition) -> tuple[str, ...]:
    return tuple(_vector_field_specs(definition))


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


def sqlite_fts5_query(query: SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery) -> str:
    if isinstance(query, SearchPhraseQuery):
        return _quote_fts_term(query.raw_query.strip())
    if isinstance(query, SearchAutocompleteQuery):
        return " AND ".join(f"{_quote_fts_term(term)}*" for term in query.terms)
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


def _compile_search_clause(
    *,
    index_name: str,
    clause_name: str,
    clause_spec: object,
) -> SearchTextLikeQuery:
    compiler = _SEARCH_CLAUSE_COMPILERS.get(clause_name)
    if compiler is None:
        raise OperationFailure(f"unsupported local $search operator: {clause_name}")
    return compiler(index_name, clause_spec)


def _compile_search_text_clause(index_name: str, clause_spec: object) -> SearchTextQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.text requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"query", "path"})
    if unsupported_options:
        raise OperationFailure(
            "$search.text only supports query and path; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.text.query must be a non-empty string")
    paths = _normalize_search_paths(clause_spec.get("path"))
    return SearchTextQuery(
        index_name=index_name,
        raw_query=raw_query,
        terms=tuple(term for term in raw_query.strip().split() if term),
        paths=paths,
    )


def _compile_search_phrase_clause(index_name: str, clause_spec: object) -> SearchPhraseQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.phrase requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"query", "path"})
    if unsupported_options:
        raise OperationFailure(
            "$search.phrase only supports query and path; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.phrase.query must be a non-empty string")
    return SearchPhraseQuery(
        index_name=index_name,
        raw_query=raw_query,
        paths=_normalize_search_paths(clause_spec.get("path")),
    )


def _compile_search_autocomplete_clause(index_name: str, clause_spec: object) -> SearchAutocompleteQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.autocomplete requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"query", "path"})
    if unsupported_options:
        raise OperationFailure(
            "$search.autocomplete only supports query and path; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.autocomplete.query must be a non-empty string")
    terms = tokenize_classic_text(raw_query)
    if not terms:
        raise OperationFailure("$search.autocomplete.query must contain at least one searchable token")
    return SearchAutocompleteQuery(
        index_name=index_name,
        raw_query=raw_query,
        terms=terms,
        paths=_normalize_search_paths(clause_spec.get("path")),
    )


def _compile_search_wildcard_clause(index_name: str, clause_spec: object) -> SearchWildcardQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.wildcard requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"query", "path"})
    if unsupported_options:
        raise OperationFailure(
            "$search.wildcard only supports query and path; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.wildcard.query must be a non-empty string")
    return SearchWildcardQuery(
        index_name=index_name,
        raw_query=raw_query,
        normalized_pattern=raw_query.strip().lower(),
        paths=_normalize_search_paths(clause_spec.get("path")),
    )


def _compile_search_compound_clause(
    index_name: str,
    clause_spec: object,
) -> SearchCompoundQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.compound requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"must", "should", "filter", "mustNot", "minimumShouldMatch"})
    if unsupported_options:
        raise OperationFailure(
            "$search.compound only supports must, should, filter, mustNot and minimumShouldMatch; unsupported keys: "
            + ", ".join(unsupported_options)
        )

    def _compile_clause_list(field_name: str) -> tuple[SearchTextLikeQuery, ...]:
        raw_value = clause_spec.get(field_name, [])
        if raw_value in (None, []):
            return ()
        if not isinstance(raw_value, list) or not raw_value:
            raise OperationFailure(f"$search.compound.{field_name} must be a non-empty array")
        compiled: list[SearchTextLikeQuery] = []
        for entry in raw_value:
            if not isinstance(entry, dict):
                raise OperationFailure(f"$search.compound.{field_name} entries must be documents")
            entry_clause_names = [name for name in TEXT_SEARCH_OPERATOR_NAMES if name in entry]
            if len(entry_clause_names) != 1:
                raise OperationFailure(
                    f"$search.compound.{field_name} entries require exactly one operator"
                )
            entry_clause_name = entry_clause_names[0]
            compiler = _SEARCH_CLAUSE_COMPILERS.get(entry_clause_name)
            if compiler is None:
                raise OperationFailure(
                    f"$search.compound.{field_name} uses unsupported operator {entry_clause_name}"
                )
            compiled.append(compiler(index_name, entry.get(entry_clause_name)))
        return tuple(compiled)

    must = _compile_clause_list("must")
    should = _compile_clause_list("should")
    filter_clauses = _compile_clause_list("filter")
    must_not = _compile_clause_list("mustNot")
    if not any((must, should, filter_clauses, must_not)):
        raise OperationFailure("$search.compound requires at least one clause")
    minimum_should_match = clause_spec.get("minimumShouldMatch")
    if minimum_should_match is None:
        minimum_should_match = 0 if (must or filter_clauses or must_not) else 1
    if (
        not isinstance(minimum_should_match, int)
        or isinstance(minimum_should_match, bool)
        or minimum_should_match < 0
    ):
        raise OperationFailure("$search.compound.minimumShouldMatch must be a non-negative integer")
    if minimum_should_match and not should:
        raise OperationFailure("$search.compound.minimumShouldMatch requires should clauses")
    return SearchCompoundQuery(
        index_name=index_name,
        must=must,
        should=should,
        filter=filter_clauses,
        must_not=must_not,
        minimum_should_match=minimum_should_match,
    )


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


def _validate_vector_search_definition(definition: Document) -> None:
    if set(definition) != {"fields"}:
        raise OperationFailure("local vectorSearch definitions require a top-level fields array")
    fields = definition.get("fields")
    if not isinstance(fields, list) or not fields:
        raise OperationFailure("local vectorSearch definitions require a non-empty fields array")
    for field in fields:
        if not isinstance(field, dict):
            raise OperationFailure("vectorSearch field definitions must be documents")
        unsupported = set(field) - {"type", "path", "numDimensions", "similarity", "connectivity", "expansionAdd", "expansionSearch"}
        if unsupported:
            raise OperationFailure(
                "unsupported local vectorSearch field options: " + ", ".join(sorted(unsupported))
            )
        if field.get("type") != "vector":
            raise OperationFailure("local vectorSearch fields must use type 'vector'")
        path = field.get("path")
        if not isinstance(path, str) or not path:
            raise OperationFailure("local vectorSearch field path must be a non-empty string")
        num_dimensions = field.get("numDimensions")
        if not isinstance(num_dimensions, int) or isinstance(num_dimensions, bool) or num_dimensions <= 0:
            raise OperationFailure("local vectorSearch numDimensions must be a positive integer")
        similarity = field.get("similarity", "cosine")
        if similarity not in {"cosine", "dotProduct", "euclidean"}:
            raise OperationFailure("local vectorSearch currently supports cosine, dotProduct and euclidean similarity")
        for option_name in ("connectivity", "expansionAdd", "expansionSearch"):
            option_value = field.get(option_name)
            if option_value is None:
                continue
            if not isinstance(option_value, int) or isinstance(option_value, bool) or option_value <= 0:
                raise OperationFailure(f"local vectorSearch {option_name} must be a positive integer")


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


def _explain_text_like_query(query: SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery) -> dict[str, object | None]:
    return {
        "query": query.raw_query,
        "paths": list(query.paths) if query.paths is not None else None,
        "compound": None,
        "path": None,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
    }


def _explain_compound_query(query: SearchCompoundQuery) -> dict[str, object | None]:
    return {
        "query": None,
        "paths": None,
        "compound": {
            "must": len(query.must),
            "should": len(query.should),
            "filter": len(query.filter),
            "mustNot": len(query.must_not),
            "minimumShouldMatch": query.minimum_should_match,
        },
        "path": None,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
    }


def _explain_vector_query(query: SearchVectorQuery) -> dict[str, object | None]:
    return {
        "query": None,
        "paths": None,
        "compound": None,
        "path": query.path,
        "queryVector": list(query.query_vector),
        "limit": query.limit,
        "numCandidates": query.num_candidates,
        "filter": deepcopy(query.filter_spec) if query.filter_spec is not None else None,
        "similarity": query.similarity,
    }


_SEARCH_STAGE_COMPILERS: dict[str, Callable[[object], SearchQuery]] = {
    "$search": compile_search_text_like_query,
    "$vectorSearch": compile_vector_search_query,
}

_SEARCH_CLAUSE_COMPILERS: dict[str, _SearchClauseCompiler] = {
    "text": _compile_search_text_clause,
    "phrase": _compile_search_phrase_clause,
    "autocomplete": _compile_search_autocomplete_clause,
    "wildcard": _compile_search_wildcard_clause,
    "compound": _compile_search_compound_clause,
}

_SEARCH_QUERY_OPERATOR_NAMES: dict[type[Any], str] = {
    SearchTextQuery: "text",
    SearchPhraseQuery: "phrase",
    SearchAutocompleteQuery: "autocomplete",
    SearchWildcardQuery: "wildcard",
    SearchCompoundQuery: "compound",
}

_SEARCH_QUERY_MATCHERS: dict[type[Any], _SearchMatcher] = {
    SearchTextQuery: lambda document, definition, query: matches_search_text_query(document, definition=definition, query=query),  # type: ignore[arg-type]
    SearchPhraseQuery: lambda document, definition, query: matches_search_phrase_query(document, definition=definition, query=query),  # type: ignore[arg-type]
    SearchAutocompleteQuery: lambda document, definition, query: matches_search_autocomplete_query(document, definition=definition, query=query),  # type: ignore[arg-type]
    SearchWildcardQuery: lambda document, definition, query: matches_search_wildcard_query(document, definition=definition, query=query),  # type: ignore[arg-type]
    SearchCompoundQuery: lambda document, definition, query: matches_search_compound_query(document, definition=definition, query=query),  # type: ignore[arg-type]
}

_SEARCH_QUERY_EXPLAINERS: dict[type[Any], _SearchExplainBuilder] = {
    SearchTextQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchPhraseQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchAutocompleteQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchWildcardQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchCompoundQuery: lambda query: _explain_compound_query(query),  # type: ignore[arg-type]
    SearchVectorQuery: lambda query: _explain_vector_query(query),  # type: ignore[arg-type]
}


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


def _vector_field_specs(definition: SearchIndexDefinition) -> dict[str, dict[str, object]]:
    if definition.index_type != "vectorSearch":
        return {}
    raw_fields = definition.definition.get("fields")
    if not isinstance(raw_fields, list):
        return {}
    field_specs: dict[str, dict[str, object]] = {}
    for field in raw_fields:
        if not isinstance(field, dict):
            continue
        path = field.get("path")
        num_dimensions = field.get("numDimensions")
        if (
            field.get("type") == "vector"
            and isinstance(path, str)
            and path
            and isinstance(num_dimensions, int)
            and not isinstance(num_dimensions, bool)
            and num_dimensions > 0
        ):
            field_specs[path] = {
                "numDimensions": num_dimensions,
                "similarity": field.get("similarity", "cosine"),
            }
    return field_specs


def _cosine_similarity(left: tuple[float, ...], right: tuple[float, ...]) -> float | None:
    dot = sum(a * b for a, b in zip(left, right, strict=True))
    left_norm = math.sqrt(sum(a * a for a in left))
    right_norm = math.sqrt(sum(b * b for b in right))
    if left_norm == 0 or right_norm == 0:
        return None
    return dot / (left_norm * right_norm)


def _dot_product(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    return sum(a * b for a, b in zip(left, right, strict=True))


def _negative_euclidean_distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    return -math.sqrt(sum((a - b) ** 2 for a, b in zip(left, right, strict=True)))
