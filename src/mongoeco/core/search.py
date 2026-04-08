from __future__ import annotations

from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, field, replace
import datetime
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
from mongoeco.types import (
    Document,
    EngineIndexRecord,
    IndexKeySpec,
    SearchIndexDefinition,
    SearchIndexDocument,
    normalize_index_keys,
)


SUPPORTED_SEARCH_INDEX_TYPES = {"search", "vectorSearch"}
TEXTUAL_SEARCH_INDEX_TYPES = {"search"}
TEXT_SCORE_FIELD = "__mongoeco_textScore__"
VECTOR_SEARCH_SCORE_FIELD = "__mongoeco_vectorSearchScore__"
SEARCH_RESULT_METADATA_FIELDS = frozenset({TEXT_SCORE_FIELD, VECTOR_SEARCH_SCORE_FIELD})
_TEXT_TOKEN_RE = re.compile(r"\w+", re.UNICODE)
_TEXT_QUERY_CHUNK_RE = re.compile(r'-?"[^"]+"|-?\S+')
_SUPPORTED_REGEX_FLAGS = {
    "a": re.ASCII,
    "i": re.IGNORECASE,
    "m": re.MULTILINE,
    "s": re.DOTALL,
    "u": re.UNICODE,
    "x": re.VERBOSE,
}
_SUPPORTED_REGEX_FLAGS_LABEL = ", ".join(sorted(_SUPPORTED_REGEX_FLAGS))
_SUPPORTED_SEARCH_FACET_TYPES = frozenset({"string", "number", "date"})
TEXTUAL_SEARCH_FIELD_MAPPING_TYPES = frozenset({"string", "autocomplete", "token"})
EXACT_FILTER_SEARCH_FIELD_MAPPING_TYPES = frozenset(
    {
        "number",
        "date",
        "boolean",
        "objectId",
        "uuid",
    }
)
STRUCTURED_SEARCH_FIELD_MAPPING_TYPES = frozenset(
    {"document", "embeddedDocuments"}
)
SUPPORTED_SEARCH_FIELD_MAPPING_TYPES = frozenset(
    TEXTUAL_SEARCH_FIELD_MAPPING_TYPES
    | EXACT_FILTER_SEARCH_FIELD_MAPPING_TYPES
    | STRUCTURED_SEARCH_FIELD_MAPPING_TYPES
)
SEARCH_HIGHLIGHTS_FIELD = "searchHighlights"


@dataclass(frozen=True, slots=True)
class SearchCountSpec:
    mode: str = "total"


@dataclass(frozen=True, slots=True)
class SearchHighlightSpec:
    paths: tuple[str, ...] | None = None
    max_chars: int = 120


@dataclass(frozen=True, slots=True)
class SearchFacetSpec:
    path: str = ""
    num_buckets: int = 10
    facet_type: str = "string"
    facets: tuple[tuple[str, str, str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class SearchStageOptions:
    count: SearchCountSpec | None = None
    highlight: SearchHighlightSpec | None = None
    facet: SearchFacetSpec | None = None


@dataclass(frozen=True, slots=True)
class ClassicTextQuery:
    raw_query: str
    terms: tuple[str, ...]
    excluded_terms: tuple[str, ...] = ()
    required_phrases: tuple[tuple[str, ...], ...] = ()
    excluded_phrases: tuple[tuple[str, ...], ...] = ()
    case_sensitive: bool = False
    diacritic_sensitive: bool = False
    language: str | None = None


@dataclass(frozen=True, slots=True)
class SearchTextQuery:
    index_name: str
    raw_query: str
    terms: tuple[str, ...]
    paths: tuple[str, ...] | None = None
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchPhraseQuery:
    index_name: str
    raw_query: str
    paths: tuple[str, ...] | None = None
    slop: int = 0
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchAutocompleteQuery:
    index_name: str
    raw_query: str
    terms: tuple[str, ...]
    paths: tuple[str, ...] | None = None
    token_order: str = "any"
    fuzzy_max_edits: int = 0
    fuzzy_prefix_length: int = 0
    fuzzy_max_expansions: int = 0
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchWildcardQuery:
    index_name: str
    raw_query: str
    normalized_pattern: str
    paths: tuple[str, ...] | None = None
    allow_analyzed_field: bool = False
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchRegexQuery:
    index_name: str
    raw_query: str
    paths: tuple[str, ...] | None = None
    flags: str = ""
    allow_analyzed_field: bool = False
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchExistsQuery:
    index_name: str
    paths: tuple[str, ...] | None = None
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchInQuery:
    index_name: str
    path: str
    values: tuple[object, ...]
    normalized_values: frozenset[tuple[str, object]]
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchEqualsQuery:
    index_name: str
    path: str
    value: object
    value_kind: str
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchRangeQuery:
    index_name: str
    path: str
    gt: object | None = None
    gte: object | None = None
    lt: object | None = None
    lte: object | None = None
    bound_kind: str = "number"
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchNearQuery:
    index_name: str
    path: str
    origin: float | datetime.date | datetime.datetime
    pivot: float
    origin_kind: str
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchCompoundQuery:
    index_name: str
    must: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery | SearchExistsQuery | SearchInQuery | SearchEqualsQuery | SearchRangeQuery | SearchNearQuery | "SearchCompoundQuery", ...] = ()
    should: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery | SearchExistsQuery | SearchInQuery | SearchEqualsQuery | SearchRangeQuery | SearchNearQuery | "SearchCompoundQuery", ...] = ()
    filter: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery | SearchExistsQuery | SearchInQuery | SearchEqualsQuery | SearchRangeQuery | SearchNearQuery | "SearchCompoundQuery", ...] = ()
    must_not: tuple[SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery | SearchExistsQuery | SearchInQuery | SearchEqualsQuery | SearchRangeQuery | SearchNearQuery | "SearchCompoundQuery", ...] = ()
    minimum_should_match: int = 0
    stage_options: SearchStageOptions = field(default_factory=SearchStageOptions)


@dataclass(frozen=True, slots=True)
class SearchVectorQuery:
    index_name: str
    path: str
    query_vector: tuple[float, ...]
    limit: int
    num_candidates: int
    filter_spec: Document | None = None
    similarity: str = "cosine"
    min_score: float | None = None


@dataclass(frozen=True, slots=True)
class MaterializedSearchDocument:
    entries: tuple[tuple[str, str], ...]
    searchable_paths: frozenset[str]
    lowered_values: tuple[str, ...]
    lowered_values_by_path: dict[str, tuple[str, ...]]
    token_counter_by_path: dict[str, Counter[str]]
    token_counter: Counter[str]
    token_sets_by_path: dict[str, frozenset[str]]
    token_set: frozenset[str]


type SearchTextLikeQuery = (
    SearchTextQuery
    | SearchPhraseQuery
    | SearchAutocompleteQuery
    | SearchWildcardQuery
    | SearchRegexQuery
    | SearchExistsQuery
    | SearchInQuery
    | SearchEqualsQuery
    | SearchRangeQuery
    | SearchNearQuery
    | SearchCompoundQuery
)
type SearchQuery = SearchTextLikeQuery | SearchVectorQuery
type _SearchClauseCompiler = Callable[[str, object], SearchTextLikeQuery]
type _SearchMatcher = Callable[
    [Document, SearchIndexDefinition, SearchTextLikeQuery, MaterializedSearchDocument | None],
    bool,
]
type _SearchExplainBuilder = Callable[[SearchQuery], dict[str, object | None]]


def compile_classic_text_query(spec: object) -> ClassicTextQuery:
    if not isinstance(spec, dict):
        raise OperationFailure("$text requires a document specification")
    unsupported = sorted(
        set(spec) - {"$search", "$caseSensitive", "$diacriticSensitive", "$language"}
    )
    if unsupported:
        raise OperationFailure(
            "$text local runtime supports only $search, $caseSensitive, $diacriticSensitive and $language; unsupported keys: "
            + ", ".join(unsupported)
        )
    raw_query = spec.get("$search")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$text.$search must be a non-empty string")
    case_sensitive = spec.get("$caseSensitive", False)
    if not isinstance(case_sensitive, bool):
        raise OperationFailure("$text.$caseSensitive must be a boolean")
    diacritic_sensitive = spec.get("$diacriticSensitive", False)
    if not isinstance(diacritic_sensitive, bool):
        raise OperationFailure("$text.$diacriticSensitive must be a boolean")
    language = spec.get("$language")
    if language is not None and not isinstance(language, str):
        raise OperationFailure("$text.$language must be a string")
    terms, excluded_terms, required_phrases, excluded_phrases = _parse_classic_text_terms(
        raw_query,
        case_sensitive=case_sensitive,
        diacritic_sensitive=diacritic_sensitive,
    )
    if not terms and not required_phrases:
        raise OperationFailure("$text.$search must contain at least one searchable token")
    return ClassicTextQuery(
        raw_query=raw_query,
        terms=terms,
        excluded_terms=excluded_terms,
        required_phrases=required_phrases,
        excluded_phrases=excluded_phrases,
        case_sensitive=case_sensitive,
        diacritic_sensitive=diacritic_sensitive,
        language=language,
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


def _parse_classic_text_terms(
    value: str,
    *,
    case_sensitive: bool,
    diacritic_sensitive: bool,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[tuple[str, ...], ...], tuple[tuple[str, ...], ...]]:
    include_terms: list[str] = []
    exclude_terms: list[str] = []
    include_phrases: list[tuple[str, ...]] = []
    exclude_phrases: list[tuple[str, ...]] = []
    for raw_chunk in _TEXT_QUERY_CHUNK_RE.findall(value):
        chunk = raw_chunk.strip()
        is_exclusion = chunk.startswith("-")
        payload = chunk[1:] if is_exclusion else chunk
        if not payload:
            continue
        if payload.startswith('"') and payload.endswith('"') and len(payload) >= 2:
            phrase_tokens = tokenize_classic_text(
                payload[1:-1],
                case_sensitive=case_sensitive,
                diacritic_sensitive=diacritic_sensitive,
            )
            if not phrase_tokens:
                continue
            if is_exclusion:
                exclude_phrases.append(phrase_tokens)
            else:
                include_phrases.append(phrase_tokens)
                include_terms.extend(phrase_tokens)
            continue
        chunk_terms = tokenize_classic_text(
            payload,
            case_sensitive=case_sensitive,
            diacritic_sensitive=diacritic_sensitive,
        )
        if not chunk_terms:
            continue
        if is_exclusion:
            exclude_terms.extend(chunk_terms)
        else:
            include_terms.extend(chunk_terms)
    return (
        tuple(include_terms),
        tuple(exclude_terms),
        tuple(include_phrases),
        tuple(exclude_phrases),
    )


def resolve_classic_text_index(
    indexes: list[EngineIndexRecord],
    *,
    hinted_name: str | None = None,
) -> tuple[str, tuple[str, ...]]:
    candidates = []
    for index in indexes:
        if not index.key:
            continue
        directions = tuple(direction for _field, direction in index.key)
        has_text = "text" in directions
        unsupported_directions = [direction for direction in directions if direction not in (1, -1, "text")]
        if has_text and not unsupported_directions:
            candidates.append(index)
    if hinted_name is not None:
        candidates = [index for index in candidates if index.name == hinted_name]
    if not candidates:
        if hinted_name is not None:
            raise OperationFailure(f"text index not found with name [{hinted_name}]")
        raise OperationFailure("classic $text requires a local text index on the collection")
    if len(candidates) > 1:
        if hinted_name is not None:
            raise OperationFailure(f"text index not found with name [{hinted_name}]")
        raise OperationFailure(
            "classic $text is ambiguous with multiple text indexes; use a single local text index per collection"
        )
    index = candidates[0]
    return index.name, tuple(field for field, direction in index.key if direction == "text")


def resolve_classic_text_index_for_hint(
    indexes: list[EngineIndexRecord],
    hint: str | IndexKeySpec | None,
) -> tuple[str, tuple[str, ...]]:
    if hint is None:
        return resolve_classic_text_index(indexes)
    if isinstance(hint, str):
        index_name, fields = resolve_classic_text_index(indexes, hinted_name=hint)
        matched_index = next((index for index in indexes if index.name == index_name), None)
        if matched_index is not None and matched_index.hidden:
            raise OperationFailure("hint does not correspond to a usable index for this query")
        return index_name, fields

    normalized_hint = normalize_index_keys(hint)
    matching_indexes = [index for index in indexes if index.key == normalized_hint]
    if not matching_indexes:
        raise OperationFailure("hint does not correspond to an existing index")
    usable_indexes = [index for index in matching_indexes if not index.hidden]
    if not usable_indexes:
        raise OperationFailure("hint does not correspond to a usable index for this query")

    text_indexes = []
    for index in usable_indexes:
        directions = tuple(direction for _field, direction in index.key)
        has_text = "text" in directions
        unsupported_directions = [direction for direction in directions if direction not in (1, -1, "text")]
        if has_text and not unsupported_directions:
            text_indexes.append(index)
    if not text_indexes:
        raise OperationFailure("hint does not correspond to a text index for classic $text query")
    if len(text_indexes) > 1:
        raise OperationFailure(
            f"multiple text indexes found with key pattern {normalized_hint!r}; hint by name instead"
        )
    index = text_indexes[0]
    return index.name, tuple(field for field, direction in index.key if direction == "text")


def classic_text_score(
    document: Document,
    *,
    field: str | tuple[str, ...] | list[str],
    query: ClassicTextQuery,
    weights: dict[str, int] | None = None,
) -> float | None:
    fields = (field,) if isinstance(field, str) else tuple(field)
    if not fields:
        return None
    field_tokens: dict[str, tuple[tuple[str, ...], ...]] = {}
    field_counters: dict[str, Counter[str]] = {}
    all_tokens: list[str] = []
    score = 0.0
    for current_field in fields:
        values_tokens: list[tuple[str, ...]] = []
        for value in iter_classic_text_values(document, current_field):
            value_tokens = tokenize_classic_text(
                value,
                case_sensitive=query.case_sensitive,
                diacritic_sensitive=query.diacritic_sensitive,
            )
            if value_tokens:
                values_tokens.append(value_tokens)
                all_tokens.extend(value_tokens)
        token_counter = Counter(token for tokens in values_tokens for token in tokens)
        if not token_counter:
            continue
        field_tokens[current_field] = tuple(values_tokens)
        field_counters[current_field] = token_counter
    if not field_counters:
        return None
    if query.excluded_terms:
        present_tokens = frozenset(all_tokens)
        if any(term in present_tokens for term in query.excluded_terms):
            return None
    if query.required_phrases:
        all_sequences = tuple(tokens for sequences in field_tokens.values() for tokens in sequences)
        for phrase in query.required_phrases:
            if not any(_token_sequence_contains(sequence, phrase) for sequence in all_sequences):
                return None
    if query.excluded_phrases:
        all_sequences = tuple(tokens for sequences in field_tokens.values() for tokens in sequences)
        for phrase in query.excluded_phrases:
            if any(_token_sequence_contains(sequence, phrase) for sequence in all_sequences):
                return None
    for current_field in fields:
        token_counter = field_counters.get(current_field)
        if not token_counter:
            continue
        field_score = sum(token_counter.get(term, 0) for term in query.terms)
        if field_score <= 0:
            continue
        weight = 1.0
        if weights is not None:
            candidate = weights.get(current_field)
            if (
                isinstance(candidate, int)
                and not isinstance(candidate, bool)
                and candidate > 0
            ):
                weight = float(candidate)
        score += field_score * weight
    if score <= 0:
        return None
    return float(score)


def _token_sequence_contains(tokens: tuple[str, ...], phrase: tuple[str, ...]) -> bool:
    phrase_length = len(phrase)
    if phrase_length <= 0 or len(tokens) < phrase_length:
        return False
    for start in range(0, len(tokens) - phrase_length + 1):
        if tokens[start : start + phrase_length] == phrase:
            return True
    return False


def attach_text_score(document: Document, score: float) -> Document:
    enriched = dict(document)
    enriched[TEXT_SCORE_FIELD] = float(score)
    return enriched


def attach_vector_search_score(document: Document, score: float) -> Document:
    enriched = dict(document)
    enriched[VECTOR_SEARCH_SCORE_FIELD] = float(score)
    return enriched


def strip_search_result_metadata(document: Document) -> Document:
    if not any(field in document for field in SEARCH_RESULT_METADATA_FIELDS):
        return document
    cleaned = dict(document)
    for field in SEARCH_RESULT_METADATA_FIELDS:
        cleaned.pop(field, None)
    return cleaned


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
    unsupported_operators = sorted(
        set(spec) - {"index", "count", "highlight", "facet", *TEXT_SEARCH_OPERATOR_NAMES}
    )
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
    query = compiler(index_name, clause_spec)
    stage_options = SearchStageOptions(
        count=_compile_search_count_spec(spec.get("count")),
        highlight=_compile_search_highlight_spec(spec.get("highlight")),
        facet=_compile_search_facet_spec(spec.get("facet")),
    )
    return replace(query, stage_options=stage_options)


def compile_search_meta_text_like_query(spec: object) -> SearchTextLikeQuery:
    if not isinstance(spec, dict):
        raise OperationFailure("$searchMeta requires a document specification")
    facet_spec = spec.get("facet")
    if (
        isinstance(facet_spec, dict)
        and "operator" in facet_spec
    ):
        operator_spec = facet_spec.get("operator")
        if not isinstance(operator_spec, dict):
            raise OperationFailure("$searchMeta.facet.operator must be a document specification")
        top_level_clause_names = [name for name in TEXT_SEARCH_OPERATOR_NAMES if name in spec]
        if top_level_clause_names:
            raise OperationFailure(
                "$searchMeta.facet.operator cannot be combined with top-level search operators"
            )
        clause_names = [name for name in TEXT_SEARCH_OPERATOR_NAMES if name in operator_spec]
        if len(clause_names) != 1:
            raise OperationFailure(
                "$searchMeta.facet.operator requires exactly one of "
                + ", ".join(TEXT_SEARCH_OPERATOR_NAMES[:-1])
                + " or "
                + TEXT_SEARCH_OPERATOR_NAMES[-1]
            )
        unsupported_operator_keys = sorted(set(operator_spec) - set(TEXT_SEARCH_OPERATOR_NAMES))
        if unsupported_operator_keys:
            raise OperationFailure(
                "$searchMeta.facet.operator supports only search operators; unsupported keys: "
                + ", ".join(unsupported_operator_keys)
            )
        clause_name = clause_names[0]
        normalized_spec = dict(spec)
        normalized_facet_spec = dict(facet_spec)
        normalized_facet_spec.pop("operator", None)
        normalized_spec["facet"] = normalized_facet_spec
        normalized_spec[clause_name] = operator_spec[clause_name]
        return compile_search_text_like_query(normalized_spec)
    return compile_search_text_like_query(spec)


def _compile_search_count_spec(spec: object) -> SearchCountSpec | None:
    if spec is None:
        return None
    if not isinstance(spec, dict):
        raise OperationFailure("$search.count requires a document specification")
    unsupported_options = sorted(set(spec) - {"type"})
    if unsupported_options:
        raise OperationFailure(
            "$search.count only supports type; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    mode = spec.get("type", "total")
    if mode not in {"total", "lowerBound"}:
        raise OperationFailure("$search.count.type must be 'total' or 'lowerBound'")
    return SearchCountSpec(mode=mode)


def _compile_search_highlight_spec(spec: object) -> SearchHighlightSpec | None:
    if spec is None:
        return None
    if not isinstance(spec, dict):
        raise OperationFailure("$search.highlight requires a document specification")
    unsupported_options = sorted(set(spec) - {"path", "maxChars"})
    if unsupported_options:
        raise OperationFailure(
            "$search.highlight only supports path and maxChars; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    paths = _normalize_search_paths(spec.get("path"))
    if paths is None:
        raise OperationFailure("$search.highlight.path must be a string or list of strings")
    max_chars = spec.get("maxChars", 120)
    if (
        not isinstance(max_chars, int)
        or isinstance(max_chars, bool)
        or max_chars <= 0
    ):
        raise OperationFailure("$search.highlight.maxChars must be a positive integer")
    return SearchHighlightSpec(paths=paths, max_chars=max_chars)


def _compile_search_facet_spec(spec: object) -> SearchFacetSpec | None:
    if spec is None:
        return None
    if not isinstance(spec, dict):
        raise OperationFailure("$search.facet requires a document specification")
    if "facets" in spec:
        unsupported_options = sorted(set(spec) - {"facets"})
        if unsupported_options:
            raise OperationFailure(
                "$search.facet only supports facets in collector mode; unsupported keys: "
                + ", ".join(unsupported_options)
            )
        raw_facets = spec.get("facets")
        if not isinstance(raw_facets, dict) or not raw_facets:
            raise OperationFailure("$search.facet.facets must be a non-empty document")
        resolved_facets: list[tuple[str, str, str, int]] = []
        for facet_name, facet_spec in raw_facets.items():
            if not isinstance(facet_name, str) or not facet_name:
                raise OperationFailure("$search.facet facet names must be non-empty strings")
            if not isinstance(facet_spec, dict):
                raise OperationFailure(
                    f"$search.facet facet '{facet_name}' must be a document specification"
                )
            facet_unsupported_options = sorted(
                set(facet_spec) - {"type", "path", "numBuckets"}
            )
            if facet_unsupported_options:
                raise OperationFailure(
                    "$search.facet facets only support type, path and numBuckets; unsupported keys: "
                    + ", ".join(facet_unsupported_options)
                )
            facet_type = facet_spec.get("type", "string")
            if facet_type not in _SUPPORTED_SEARCH_FACET_TYPES:
                raise OperationFailure(
                    "$search.facet facet type must be one of: "
                    + ", ".join(sorted(_SUPPORTED_SEARCH_FACET_TYPES))
                )
            facet_path = facet_spec.get("path")
            if not isinstance(facet_path, str) or not facet_path:
                raise OperationFailure("$search.facet facet path must be a non-empty string")
            facet_num_buckets = facet_spec.get("numBuckets", 10)
            if (
                not isinstance(facet_num_buckets, int)
                or isinstance(facet_num_buckets, bool)
                or facet_num_buckets <= 0
            ):
                raise OperationFailure("$search.facet facet numBuckets must be a positive integer")
            resolved_facets.append((facet_name, facet_path, facet_type, facet_num_buckets))
        return SearchFacetSpec(facets=tuple(resolved_facets))
    unsupported_options = sorted(set(spec) - {"type", "path", "numBuckets"})
    if unsupported_options:
        raise OperationFailure(
            "$search.facet only supports type, path and numBuckets; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    path = spec.get("path")
    if not isinstance(path, str) or not path:
        raise OperationFailure("$search.facet.path must be a non-empty string")
    facet_type = spec.get("type", "string")
    if facet_type not in _SUPPORTED_SEARCH_FACET_TYPES:
        raise OperationFailure(
            "$search.facet.type must be one of: "
            + ", ".join(sorted(_SUPPORTED_SEARCH_FACET_TYPES))
        )
    num_buckets = spec.get("numBuckets", 10)
    if (
        not isinstance(num_buckets, int)
        or isinstance(num_buckets, bool)
        or num_buckets <= 0
    ):
        raise OperationFailure("$search.facet.numBuckets must be a positive integer")
    return SearchFacetSpec(path=path, num_buckets=num_buckets, facet_type=facet_type)


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


def compile_search_regex_query(spec: object) -> SearchRegexQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchRegexQuery):
        raise OperationFailure("$search.regex specification is required")
    return query


def compile_search_exists_query(spec: object) -> SearchExistsQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchExistsQuery):
        raise OperationFailure("$search.exists specification is required")
    return query


def compile_search_in_query(spec: object) -> SearchInQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchInQuery):
        raise OperationFailure("$search.in specification is required")
    return query


def compile_search_equals_query(spec: object) -> SearchEqualsQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchEqualsQuery):
        raise OperationFailure("$search.equals specification is required")
    return query


def compile_search_range_query(spec: object) -> SearchRangeQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchRangeQuery):
        raise OperationFailure("$search.range specification is required")
    return query


def compile_search_near_query(spec: object) -> SearchNearQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchNearQuery):
        raise OperationFailure("$search.near specification is required")
    return query


def compile_search_compound_query(spec: object) -> SearchCompoundQuery:
    query = compile_search_text_like_query(spec)
    if not isinstance(query, SearchCompoundQuery):
        raise OperationFailure("$search.compound specification is required")
    return query


def compile_vector_search_query(spec: object) -> SearchVectorQuery:
    if not isinstance(spec, dict):
        raise OperationFailure("$vectorSearch requires a document specification")
    unsupported = sorted(set(spec) - {"index", "path", "queryVector", "limit", "numCandidates", "filter", "minScore"})
    if unsupported:
        raise OperationFailure(
            "$vectorSearch local runtime supports only index, path, queryVector, limit, numCandidates, filter and minScore; "
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
    min_score = spec.get("minScore")
    if min_score is not None:
        if not isinstance(min_score, (int, float)) or isinstance(min_score, bool) or not math.isfinite(float(min_score)):
            raise OperationFailure("$vectorSearch.minScore must be a finite numeric value")
        min_score = float(min_score)
    return SearchVectorQuery(
        index_name=index_name,
        path=path,
        query_vector=tuple(float(value) for value in raw_query_vector),
        limit=limit,
        num_candidates=num_candidates,
        filter_spec=deepcopy(filter_spec),
        min_score=min_score,
    )


def matches_search_text_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchTextQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    prepared = materialized or materialize_search_document(document, definition)
    return all(term.lower() in _materialized_token_set(prepared, query.paths) for term in query.terms)


def matches_search_phrase_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchPhraseQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    prepared = materialized or materialize_search_document(document, definition)
    return _phrase_query_score(prepared, query) > 0.0


def matches_search_autocomplete_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchAutocompleteQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    prepared = materialized or materialize_search_document(document, definition)
    query_terms = tuple(term.lower() for term in query.terms)
    token_counters = _materialized_token_counters(prepared, query.paths)
    if not token_counters:
        return False
    for token_counter in token_counters:
        ordered_tokens = tuple(token_counter)
        if not ordered_tokens:
            continue
        if query.token_order == "sequential":
            allowed_tokens_by_term = {
                term: frozenset(
                    _autocomplete_matching_tokens(
                        ordered_tokens,
                        term,
                        query=query,
                    )
                )
                for term in query_terms
            }
            if any(not matches for matches in allowed_tokens_by_term.values()):
                continue
            if _token_sequence_matches(
                ordered_tokens,
                query_terms,
                matcher=lambda token, term: token in allowed_tokens_by_term[term],
            ):
                return True
            continue
        if all(
            bool(
                _autocomplete_matching_tokens(
                    ordered_tokens,
                    term,
                    query=query,
                )
            )
            for term in query_terms
        ):
            return True
    return False


def matches_search_wildcard_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchWildcardQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    pattern = query.normalized_pattern
    prepared = materialized or materialize_search_document(document, definition)
    return any(fnmatch.fnmatchcase(value, pattern) for value in _materialized_lowered_values(prepared, query.paths))


def matches_search_regex_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchRegexQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    prepared = materialized or materialize_search_document(document, definition)
    try:
        compiled = re.compile(query.raw_query, _regex_compile_flags(query.flags))
    except re.error:
        return False
    allowed_paths = None if query.paths is None else frozenset(
        _resolved_materialized_paths(prepared, query.paths)
    )
    return any(
        compiled.search(value) is not None
        for field_path, value in prepared.entries
        if allowed_paths is None or field_path in allowed_paths
    )


def _token_sequence_matches(
    tokens: tuple[str, ...],
    query_terms: tuple[str, ...],
    *,
    matcher: Callable[[str, str], bool] | None = None,
) -> bool:
    if not query_terms:
        return False
    term_matcher = matcher or (lambda token, term: token.startswith(term))
    position = 0
    for term in query_terms:
        while position < len(tokens) and not term_matcher(tokens[position], term):
            position += 1
        if position >= len(tokens):
            return False
        position += 1
    return True


def _autocomplete_token_matches(
    token: str,
    term: str,
    *,
    query: SearchAutocompleteQuery,
) -> bool:
    if token.startswith(term):
        return True
    if query.fuzzy_max_edits <= 0:
        return False
    prefix_length = query.fuzzy_prefix_length
    if prefix_length > 0:
        if len(token) < prefix_length or len(term) < prefix_length:
            return False
        if token[:prefix_length] != term[:prefix_length]:
            return False
    max_edits = query.fuzzy_max_edits
    prefix_candidate = token[: max(len(term), prefix_length)]
    distance = min(
        _bounded_levenshtein_distance(term, token, max_distance=max_edits),
        _bounded_levenshtein_distance(term, prefix_candidate, max_distance=max_edits),
    )
    return distance <= max_edits


def _autocomplete_matching_tokens(
    tokens: tuple[str, ...],
    term: str,
    *,
    query: SearchAutocompleteQuery,
) -> tuple[str, ...]:
    matches = tuple(
        token
        for token in tokens
        if _autocomplete_token_matches(token, term, query=query)
    )
    if (
        query.fuzzy_max_edits > 0
        and query.fuzzy_max_expansions > 0
        and len(matches) > query.fuzzy_max_expansions
    ):
        return matches[: query.fuzzy_max_expansions]
    return matches


def _bounded_levenshtein_distance(a: str, b: str, *, max_distance: int) -> int:
    if abs(len(a) - len(b)) > max_distance:
        return max_distance + 1
    previous = list(range(len(b) + 1))
    for index_a, char_a in enumerate(a, start=1):
        current = [index_a]
        row_min = current[0]
        for index_b, char_b in enumerate(b, start=1):
            cost = 0 if char_a == char_b else 1
            value = min(
                previous[index_b] + 1,
                current[index_b - 1] + 1,
                previous[index_b - 1] + cost,
            )
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    distance = previous[-1]
    if distance > max_distance:
        return max_distance + 1
    return distance


def _regex_compile_flags(flags: str) -> int:
    compiled_flags = 0
    for flag in flags:
        compiled_flags |= _SUPPORTED_REGEX_FLAGS[flag]
    return compiled_flags


def matches_search_exists_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchExistsQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    prepared = materialized or materialize_search_document(document, definition)
    if query.paths is None:
        mapped_paths = _mapped_search_paths(definition)
        if mapped_paths:
            return any(_search_path_exists(document, path) for path in mapped_paths)
        return bool(prepared.entries)
    return any(_search_path_exists(document, path) for path in query.paths)


def matches_search_in_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchInQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    del definition
    del materialized
    return any(
        (candidate_kind, candidate_value) in query.normalized_values
        for candidate_kind, candidate_value in _search_path_scalar_values(document, query.path)
    )


def matches_search_equals_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchEqualsQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    del definition
    del materialized
    return any(
        candidate_kind == query.value_kind and candidate_value == query.value
        for candidate_kind, candidate_value in _search_path_scalar_values(document, query.path)
    )


def matches_search_range_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchRangeQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    del definition
    del materialized
    return any(
        _search_range_matches_value(
            candidate_kind=candidate_kind,
            candidate_value=candidate_value,
            query=query,
        )
        for candidate_kind, candidate_value in _search_path_scalar_values(document, query.path)
    )


def matches_search_near_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchNearQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    del definition
    del materialized
    return search_near_distance(document, query=query) is not None


def matches_search_compound_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchCompoundQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    prepared = materialized or materialize_search_document(document, definition)
    for clause in query.must:
        if not matches_search_query(document, definition=definition, query=clause, materialized=prepared):
            return False
    for clause in query.filter:
        if not matches_search_query(document, definition=definition, query=clause, materialized=prepared):
            return False
    for clause in query.must_not:
        if matches_search_query(document, definition=definition, query=clause, materialized=prepared):
            return False
    if not query.should:
        return True
    matched_should = sum(
        1
        for clause in query.should
        if matches_search_query(document, definition=definition, query=clause, materialized=prepared)
    )
    return matched_should >= query.minimum_should_match


def matches_search_query(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchTextLikeQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> bool:
    matcher = _SEARCH_QUERY_MATCHERS.get(type(query))
    if matcher is None:
        raise OperationFailure(f"unsupported local search query type: {type(query).__name__}")
    return matcher(document, definition, query, materialized)


def is_text_search_query(query: SearchQuery) -> bool:
    return type(query) in _SEARCH_QUERY_MATCHERS


def search_query_operator_name(query: SearchQuery) -> str | None:
    if isinstance(query, SearchVectorQuery):
        return "vectorSearch"
    return _SEARCH_QUERY_OPERATOR_NAMES.get(type(query))


def search_query_explain_details(
    query: SearchQuery,
    *,
    definition: SearchIndexDefinition | None = None,
) -> dict[str, object | None]:
    details = _SEARCH_QUERY_EXPLAINERS[type(query)](query)
    if definition is not None:
        _enrich_search_explain_details(
            details,
            query=query,
            definition=definition,
        )
    details["queryOperator"] = search_query_operator_name(query)
    stage_options = _serialized_search_stage_options(query)
    if stage_options:
        details["stageOptions"] = stage_options
    return details


def _search_stage_options(query: SearchQuery) -> SearchStageOptions:
    if isinstance(query, SearchVectorQuery):
        return SearchStageOptions()
    return getattr(query, "stage_options", SearchStageOptions())


def _serialized_search_stage_options(query: SearchQuery) -> dict[str, object] | None:
    options = _search_stage_options(query)
    serialized: dict[str, object] = {}
    if options.count is not None:
        serialized["count"] = {"type": options.count.mode}
    if options.highlight is not None:
        serialized["highlight"] = {
            "paths": list(options.highlight.paths) if options.highlight.paths is not None else None,
            "maxChars": options.highlight.max_chars,
            "resultField": SEARCH_HIGHLIGHTS_FIELD,
        }
    if options.facet is not None:
        if options.facet.facets:
            serialized["facet"] = {
                "facets": {
                    name: {"type": facet_type, "path": path, "numBuckets": num_buckets}
                    for name, path, facet_type, num_buckets in options.facet.facets
                },
                "previewOnly": True,
            }
        else:
            serialized["facet"] = {
                "type": options.facet.facet_type,
                "path": options.facet.path,
                "numBuckets": options.facet.num_buckets,
                "previewOnly": True,
            }
    return serialized or None


def search_clause_ranking(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchTextLikeQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> tuple[bool, float, float | None]:
    prepared = materialized or materialize_search_document(document, definition)
    if isinstance(query, SearchNearQuery):
        distance = search_near_distance(document, query=query)
        if distance is None:
            return False, 0.0, None
        return True, 1.0 + (1.0 / (1.0 + (distance / query.pivot))), distance
    if isinstance(query, SearchTextQuery):
        token_counter = _materialized_token_counter(prepared, query.paths)
        score = float(sum(token_counter.get(term.lower(), 0) for term in query.terms))
        return score > 0.0, score, None
    if isinstance(query, SearchPhraseQuery):
        score = _phrase_query_score(prepared, query)
        return score > 0.0, score, None
    if isinstance(query, SearchAutocompleteQuery):
        score = 0.0
        for token_counter in _materialized_token_counters(prepared, query.paths):
            ordered_tokens = tuple(token_counter)
            if not ordered_tokens:
                continue
            if query.token_order == "sequential":
                allowed_tokens_by_term = {
                    term.lower(): frozenset(
                        _autocomplete_matching_tokens(
                            ordered_tokens,
                            term.lower(),
                            query=query,
                        )
                    )
                    for term in query.terms
                }
                if any(not matches for matches in allowed_tokens_by_term.values()):
                    continue
                if _token_sequence_matches(
                    ordered_tokens,
                    tuple(term.lower() for term in query.terms),
                    matcher=lambda token, term: token in allowed_tokens_by_term[term],
                ):
                    score = max(score, float(len(query.terms)))
                continue
            local = 0.0
            for term in query.terms:
                matches = _autocomplete_matching_tokens(
                    ordered_tokens,
                    term.lower(),
                    query=query,
                )
                if not matches:
                    local = 0.0
                    break
                local += float(sum(token_counter[token] for token in matches))
            score = max(score, local)
        return score > 0.0, score, None
    if isinstance(query, SearchWildcardQuery):
        score = float(
            sum(
                1
                for value in _materialized_lowered_values(prepared, query.paths)
                if fnmatch.fnmatchcase(value, query.normalized_pattern)
            )
        )
        return score > 0.0, score, None
    if isinstance(query, SearchRegexQuery):
        compiled = re.compile(query.raw_query, _regex_compile_flags(query.flags))
        allowed_paths = None if query.paths is None else frozenset(
            _resolved_materialized_paths(prepared, query.paths)
        )
        score = float(
            sum(
                1
                for field_path, value in prepared.entries
                if (allowed_paths is None or field_path in allowed_paths)
                and compiled.search(value) is not None
            )
        )
        return score > 0.0, score, None
    if isinstance(query, SearchExistsQuery):
        if query.paths is None:
            mapped_paths = _mapped_search_paths(definition)
            if mapped_paths:
                score = float(sum(1 for path in mapped_paths if _search_path_exists(document, path)))
            else:
                score = float(len(prepared.searchable_paths))
        else:
            score = float(sum(1 for path in query.paths if _search_path_exists(document, path)))
        return score > 0.0, score, None
    if isinstance(query, SearchInQuery):
        score = float(
            sum(
                1
                for candidate_kind, candidate_value in _search_path_scalar_values(document, query.path)
                if (candidate_kind, candidate_value) in query.normalized_values
            )
        )
        return score > 0.0, score, None
    if isinstance(query, SearchEqualsQuery):
        score = float(
            sum(
                1
                for candidate_kind, candidate_value in _search_path_scalar_values(document, query.path)
                if candidate_kind == query.value_kind and candidate_value == query.value
            )
        )
        return score > 0.0, score, None
    if isinstance(query, SearchRangeQuery):
        score = float(
            sum(
                1
                for candidate_kind, candidate_value in _search_path_scalar_values(document, query.path)
                if _search_range_matches_value(
                    candidate_kind=candidate_kind,
                    candidate_value=candidate_value,
                    query=query,
                )
            )
        )
        return score > 0.0, score, None
    if isinstance(query, SearchCompoundQuery):
        if not matches_search_compound_query(
            document,
            definition=definition,
            query=query,
            materialized=prepared,
        ):
            return False, 0.0, None
        matched_should, should_score, best_near_distance = search_compound_ranking(
            document,
            definition=definition,
            query=query,
            materialized=prepared,
        )
        return True, 1.0 + should_score + float(matched_should), best_near_distance
    if matches_search_query(
        document,
        definition=definition,
        query=query,
        materialized=prepared,
    ):
        return True, 1.0, None
    return False, 0.0, None


def search_compound_ranking(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchCompoundQuery,
    materialized: MaterializedSearchDocument | None = None,
) -> tuple[int, float, float]:
    prepared = materialized or materialize_search_document(document, definition)
    matched_should = 0
    total_score = 0.0
    best_near_distance = float("inf")
    for clause in query.should:
        matched, clause_score, clause_near_distance = search_clause_ranking(
            document,
            definition=definition,
            query=clause,
            materialized=prepared,
        )
        if not matched:
            continue
        matched_should += 1
        total_score += clause_score
        if clause_near_distance is not None:
            best_near_distance = min(best_near_distance, clause_near_distance)
    return matched_should, total_score, best_near_distance


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
    return score_vector_values(
        query.query_vector,
        candidate,
        similarity=similarity,
    )


def materialize_search_document(
    document: Document,
    definition: SearchIndexDefinition,
) -> MaterializedSearchDocument:
    entries = tuple(iter_searchable_text_entries(document, definition))
    searchable_paths: set[str] = set()
    lowered_values: list[str] = []
    lowered_values_by_path: dict[str, list[str]] = {}
    token_counter_by_path: dict[str, Counter[str]] = {}
    token_counter: Counter[str] = Counter()
    token_sets_by_path: dict[str, set[str]] = {}
    token_set: set[str] = set()

    for field_path, value in entries:
        searchable_paths.add(field_path)
        lowered = value.lower()
        lowered_values.append(lowered)
        lowered_values_by_path.setdefault(field_path, []).append(lowered)
        tokens_counter = Counter(tokenize_classic_text(value))
        tokens = set(tokens_counter)
        token_counter.update(tokens_counter)
        token_counter_by_path.setdefault(field_path, Counter()).update(tokens_counter)
        token_set.update(tokens)
        token_sets_by_path.setdefault(field_path, set()).update(tokens)

    return MaterializedSearchDocument(
        entries=entries,
        searchable_paths=frozenset(searchable_paths),
        lowered_values=tuple(lowered_values),
        lowered_values_by_path={
            field_path: tuple(values)
            for field_path, values in lowered_values_by_path.items()
        },
        token_counter_by_path=token_counter_by_path,
        token_counter=token_counter,
        token_sets_by_path={
            field_path: frozenset(values)
            for field_path, values in token_sets_by_path.items()
        },
        token_set=frozenset(token_set),
    )


def _materialized_lowered_values(
    prepared: MaterializedSearchDocument,
    paths: tuple[str, ...] | None,
) -> tuple[str, ...]:
    if paths is None:
        return prepared.lowered_values
    values: list[str] = []
    for path in _resolved_materialized_paths(prepared, paths):
        values.extend(prepared.lowered_values_by_path.get(path, ()))
    return tuple(values)


def _materialized_token_sets(
    prepared: MaterializedSearchDocument,
    paths: tuple[str, ...] | None,
) -> tuple[frozenset[str], ...]:
    if paths is None:
        return tuple(prepared.token_sets_by_path.values())
    return tuple(
        prepared.token_sets_by_path[path]
        for path in _resolved_materialized_paths(prepared, paths)
        if path in prepared.token_sets_by_path
    )


def _materialized_token_set(
    prepared: MaterializedSearchDocument,
    paths: tuple[str, ...] | None,
) -> frozenset[str]:
    if paths is None:
        return prepared.token_set
    tokens: set[str] = set()
    for path in _resolved_materialized_paths(prepared, paths):
        tokens.update(prepared.token_sets_by_path.get(path, ()))
    return frozenset(tokens)


def _materialized_token_counter(
    prepared: MaterializedSearchDocument,
    paths: tuple[str, ...] | None,
) -> Counter[str]:
    if paths is None:
        return prepared.token_counter
    tokens: Counter[str] = Counter()
    for path in _resolved_materialized_paths(prepared, paths):
        tokens.update(prepared.token_counter_by_path.get(path, Counter()))
    return tokens


def _materialized_token_counters(
    prepared: MaterializedSearchDocument,
    paths: tuple[str, ...] | None,
) -> tuple[Counter[str], ...]:
    if paths is None:
        return tuple(prepared.token_counter_by_path.values())
    return tuple(
        prepared.token_counter_by_path[path]
        for path in _resolved_materialized_paths(prepared, paths)
        if path in prepared.token_counter_by_path
    )


def _resolved_materialized_paths(
    prepared: MaterializedSearchDocument,
    paths: tuple[str, ...],
) -> tuple[str, ...]:
    available_paths = tuple(prepared.lowered_values_by_path)
    resolved: list[str] = []
    seen: set[str] = set()
    for path in paths:
        for candidate in available_paths:
            if candidate != path and not candidate.startswith(f"{path}."):
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            resolved.append(candidate)
    return tuple(resolved)


def vector_field_paths(definition: SearchIndexDefinition) -> tuple[str, ...]:
    return tuple(_vector_field_specs(definition))


def vector_field_specs(definition: SearchIndexDefinition) -> dict[str, dict[str, object]]:
    return deepcopy(_vector_field_specs(definition))


def score_vector_values(
    query_vector: tuple[float, ...],
    candidate_vector: tuple[float, ...],
    *,
    similarity: str,
) -> float:
    if similarity == "dotProduct":
        return _dot_product(query_vector, candidate_vector)
    if similarity == "euclidean":
        return _negative_euclidean_distance(query_vector, candidate_vector)
    return _cosine_similarity(query_vector, candidate_vector)


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
        if query.slop > 0:
            return " AND ".join(_quote_fts_term(term) for term in tokenize_classic_text(query.raw_query))
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
    terms = tokenize_classic_text(raw_query)
    if not terms:
        raise OperationFailure("$search.text.query must contain at least one searchable token")
    paths = _normalize_search_paths(clause_spec.get("path"))
    return SearchTextQuery(
        index_name=index_name,
        raw_query=raw_query,
        terms=terms,
        paths=paths,
    )


def _compile_search_phrase_clause(index_name: str, clause_spec: object) -> SearchPhraseQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.phrase requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"query", "path", "slop"})
    if unsupported_options:
        raise OperationFailure(
            "$search.phrase only supports query, path and slop; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.phrase.query must be a non-empty string")
    if not tokenize_classic_text(raw_query):
        raise OperationFailure("$search.phrase.query must contain at least one searchable token")
    slop = clause_spec.get("slop", 0)
    if not isinstance(slop, int) or isinstance(slop, bool) or slop < 0:
        raise OperationFailure("$search.phrase.slop must be a non-negative integer")
    return SearchPhraseQuery(
        index_name=index_name,
        raw_query=raw_query,
        paths=_normalize_search_paths(clause_spec.get("path")),
        slop=slop,
    )


def _compile_search_autocomplete_clause(index_name: str, clause_spec: object) -> SearchAutocompleteQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.autocomplete requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"query", "path", "tokenOrder", "fuzzy"})
    if unsupported_options:
        raise OperationFailure(
            "$search.autocomplete only supports query, path, tokenOrder and fuzzy; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.autocomplete.query must be a non-empty string")
    terms = tokenize_classic_text(raw_query)
    if not terms:
        raise OperationFailure("$search.autocomplete.query must contain at least one searchable token")
    token_order = clause_spec.get("tokenOrder", "any")
    if token_order not in {"any", "sequential"}:
        raise OperationFailure("$search.autocomplete.tokenOrder must be 'any' or 'sequential'")
    fuzzy_max_edits, fuzzy_prefix_length, fuzzy_max_expansions = _compile_search_autocomplete_fuzzy_spec(
        clause_spec.get("fuzzy")
    )
    return SearchAutocompleteQuery(
        index_name=index_name,
        raw_query=raw_query,
        terms=terms,
        paths=_normalize_search_paths(clause_spec.get("path")),
        token_order=token_order,
        fuzzy_max_edits=fuzzy_max_edits,
        fuzzy_prefix_length=fuzzy_prefix_length,
        fuzzy_max_expansions=fuzzy_max_expansions,
    )


def _compile_search_autocomplete_fuzzy_spec(spec: object) -> tuple[int, int, int]:
    if spec is None:
        return 0, 0, 0
    if not isinstance(spec, dict):
        raise OperationFailure("$search.autocomplete.fuzzy must be a document specification")
    unsupported_options = sorted(set(spec) - {"maxEdits", "prefixLength", "maxExpansions"})
    if unsupported_options:
        raise OperationFailure(
            "$search.autocomplete.fuzzy only supports maxEdits, prefixLength and maxExpansions; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    max_edits = spec.get("maxEdits", 1)
    if (
        not isinstance(max_edits, int)
        or isinstance(max_edits, bool)
        or max_edits not in (1, 2)
    ):
        raise OperationFailure("$search.autocomplete.fuzzy.maxEdits must be 1 or 2")
    prefix_length = spec.get("prefixLength", 0)
    if (
        not isinstance(prefix_length, int)
        or isinstance(prefix_length, bool)
        or prefix_length < 0
    ):
        raise OperationFailure("$search.autocomplete.fuzzy.prefixLength must be a non-negative integer")
    max_expansions = spec.get("maxExpansions", 50)
    if (
        not isinstance(max_expansions, int)
        or isinstance(max_expansions, bool)
        or max_expansions <= 0
    ):
        raise OperationFailure(
            "$search.autocomplete.fuzzy.maxExpansions must be a positive integer"
        )
    return max_edits, prefix_length, max_expansions


def _compile_search_wildcard_clause(index_name: str, clause_spec: object) -> SearchWildcardQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.wildcard requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"query", "path", "allowAnalyzedField"})
    if unsupported_options:
        raise OperationFailure(
            "$search.wildcard only supports query, path and allowAnalyzedField; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.wildcard.query must be a non-empty string")
    allow_analyzed_field = clause_spec.get("allowAnalyzedField", False)
    if not isinstance(allow_analyzed_field, bool):
        raise OperationFailure("$search.wildcard.allowAnalyzedField must be a boolean")
    return SearchWildcardQuery(
        index_name=index_name,
        raw_query=raw_query,
        normalized_pattern=raw_query.strip().lower(),
        paths=_normalize_search_paths(clause_spec.get("path")),
        allow_analyzed_field=allow_analyzed_field,
    )


def _compile_search_regex_clause(index_name: str, clause_spec: object) -> SearchRegexQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.regex requires a document specification")
    unsupported_options = sorted(
        set(clause_spec) - {"query", "path", "flags", "options", "allowAnalyzedField"}
    )
    if unsupported_options:
        raise OperationFailure(
            "$search.regex only supports query, path, flags, options and allowAnalyzedField; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    raw_query = clause_spec.get("query")
    if not isinstance(raw_query, str) or not raw_query.strip():
        raise OperationFailure("$search.regex.query must be a non-empty string")
    raw_flags = clause_spec.get("flags")
    raw_options = clause_spec.get("options")
    if raw_flags is not None and not isinstance(raw_flags, str):
        raise OperationFailure("$search.regex.flags must be a string")
    if raw_options is not None and not isinstance(raw_options, str):
        raise OperationFailure("$search.regex.options must be a string")
    flags = raw_flags if raw_flags is not None else raw_options
    if flags is None:
        flags = ""
    if raw_flags is not None and raw_options is not None and raw_flags != raw_options:
        raise OperationFailure("$search.regex.flags and $search.regex.options must match when both are provided")
    allow_analyzed_field = clause_spec.get("allowAnalyzedField", False)
    if not isinstance(allow_analyzed_field, bool):
        raise OperationFailure("$search.regex.allowAnalyzedField must be a boolean")
    unsupported_flags = sorted(
        {flag for flag in flags if flag not in _SUPPORTED_REGEX_FLAGS}
    )
    if unsupported_flags:
        raise OperationFailure(
            "$search.regex.flags only supports "
            + _SUPPORTED_REGEX_FLAGS_LABEL
            + "; unsupported: "
            + ", ".join(unsupported_flags)
        )
    try:
        re.compile(raw_query, _regex_compile_flags(flags))
    except (re.error, ValueError) as exc:
        raise OperationFailure(f"$search.regex.query must be a valid regular expression: {exc}") from exc
    return SearchRegexQuery(
        index_name=index_name,
        raw_query=raw_query,
        paths=_normalize_search_paths(clause_spec.get("path")),
        flags=flags,
        allow_analyzed_field=allow_analyzed_field,
    )


def _compile_search_exists_clause(index_name: str, clause_spec: object) -> SearchExistsQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.exists requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"path"})
    if unsupported_options:
        raise OperationFailure(
            "$search.exists only supports path; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    return SearchExistsQuery(
        index_name=index_name,
        paths=_normalize_search_paths(clause_spec.get("path")),
    )


def _compile_search_in_clause(index_name: str, clause_spec: object) -> SearchInQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.in requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"path", "value"})
    if unsupported_options:
        raise OperationFailure(
            "$search.in only supports path and value; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    path = clause_spec.get("path")
    if not isinstance(path, str) or not path:
        raise OperationFailure("$search.in.path must be a non-empty string")
    raw_values = clause_spec.get("value")
    if not isinstance(raw_values, list) or not raw_values:
        raise OperationFailure("$search.in.value must be a non-empty array")
    ordered_values: list[object] = []
    normalized_values: list[tuple[str, object]] = []
    seen: set[tuple[str, object]] = set()
    for item in raw_values:
        normalized_value = _normalize_search_scalar_value(item)
        if normalized_value is None:
            raise OperationFailure(
                "$search.in.value entries must be null, bool, finite number, string, date or datetime"
            )
        if normalized_value in seen:
            continue
        seen.add(normalized_value)
        normalized_values.append(normalized_value)
        ordered_values.append(normalized_value[1])
    return SearchInQuery(
        index_name=index_name,
        path=path,
        values=tuple(ordered_values),
        normalized_values=frozenset(normalized_values),
    )


def _compile_search_equals_clause(index_name: str, clause_spec: object) -> SearchEqualsQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.equals requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"path", "value"})
    if unsupported_options:
        raise OperationFailure(
            "$search.equals only supports path and value; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    path = clause_spec.get("path")
    if not isinstance(path, str) or not path:
        raise OperationFailure("$search.equals.path must be a non-empty string")
    normalized_value = _normalize_search_scalar_value(clause_spec.get("value"))
    if normalized_value is None:
        raise OperationFailure(
            "$search.equals.value must be null, bool, finite number, string, date or datetime"
        )
    value_kind, value = normalized_value
    return SearchEqualsQuery(
        index_name=index_name,
        path=path,
        value=value,
        value_kind=value_kind,
    )


def _compile_search_range_clause(index_name: str, clause_spec: object) -> SearchRangeQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.range requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"path", "gt", "gte", "lt", "lte"})
    if unsupported_options:
        raise OperationFailure(
            "$search.range only supports path, gt, gte, lt and lte; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    path = clause_spec.get("path")
    if not isinstance(path, str) or not path:
        raise OperationFailure("$search.range.path must be a non-empty string")
    normalized_bounds: dict[str, tuple[str, object]] = {}
    for option_name in ("gt", "gte", "lt", "lte"):
        if option_name not in clause_spec:
            continue
        normalized_value = _normalize_search_scalar_value(clause_spec.get(option_name))
        if normalized_value is None:
            raise OperationFailure(
                f"$search.range.{option_name} must be a finite number, date or datetime"
            )
        bound_kind, bound_value = normalized_value
        if bound_kind not in {"number", "date", "datetime"}:
            raise OperationFailure(
                f"$search.range.{option_name} must be a finite number, date or datetime"
            )
        normalized_bounds[option_name] = (bound_kind, bound_value)
    if not normalized_bounds:
        raise OperationFailure("$search.range requires at least one of gt, gte, lt or lte")
    bound_kinds = {kind for kind, _value in normalized_bounds.values()}
    if len(bound_kinds) != 1:
        raise OperationFailure("$search.range bounds must use the same value family")
    bound_kind = next(iter(bound_kinds))
    return SearchRangeQuery(
        index_name=index_name,
        path=path,
        gt=normalized_bounds.get("gt", (bound_kind, None))[1],
        gte=normalized_bounds.get("gte", (bound_kind, None))[1],
        lt=normalized_bounds.get("lt", (bound_kind, None))[1],
        lte=normalized_bounds.get("lte", (bound_kind, None))[1],
        bound_kind=bound_kind,
    )


def _compile_search_near_clause(index_name: str, clause_spec: object) -> SearchNearQuery:
    if not isinstance(clause_spec, dict):
        raise OperationFailure("$search.near requires a document specification")
    unsupported_options = sorted(set(clause_spec) - {"path", "origin", "pivot"})
    if unsupported_options:
        raise OperationFailure(
            "$search.near only supports path, origin and pivot; unsupported keys: "
            + ", ".join(unsupported_options)
        )
    path = clause_spec.get("path")
    if not isinstance(path, str) or not path:
        raise OperationFailure("$search.near.path must be a non-empty string")
    if "." in path:
        # Nested paths are allowed semantically, but keeping this stage explicit
        # avoids silently broadening runtime support without tests/explain work.
        pass
    origin = clause_spec.get("origin")
    origin_kind = _search_near_origin_kind(origin)
    if origin_kind is None:
        raise OperationFailure("$search.near.origin must be a finite number or a date/datetime value")
    pivot = clause_spec.get("pivot")
    if not isinstance(pivot, (int, float)) or isinstance(pivot, bool) or not math.isfinite(float(pivot)) or float(pivot) <= 0:
        raise OperationFailure("$search.near.pivot must be a positive finite number")
    return SearchNearQuery(
        index_name=index_name,
        path=path,
        origin=origin,
        pivot=float(pivot),
        origin_kind=origin_kind,
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
    if mapping_type in STRUCTURED_SEARCH_FIELD_MAPPING_TYPES:
        nested_mapping = {
            key: value for key, value in field_spec.items() if key != "type"
        }
        _validate_mappings_document(nested_mapping)
        return
    if mapping_type not in SUPPORTED_SEARCH_FIELD_MAPPING_TYPES:
        raise OperationFailure(f"unsupported local search field mapping type: {mapping_type}")
    unsupported = set(field_spec) - {"type", "analyzer", "searchAnalyzer"}
    if unsupported:
        raise OperationFailure(
            "unsupported local search field options: " + ", ".join(sorted(unsupported))
        )


def _explain_text_like_query(query: SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery) -> dict[str, object | None]:
    paths = list(query.paths) if query.paths is not None else None
    query_semantics: dict[str, object]
    if isinstance(query, SearchPhraseQuery):
        query_semantics = {
            "matchingMode": "ordered-token-window",
            "supportsSlop": True,
            "scope": "local-text-tier",
        }
    elif isinstance(query, SearchAutocompleteQuery):
        query_semantics = {
            "matchingMode": "token-prefix",
            "tokenization": "classic-text-local",
            "atlasParity": "subset",
            "tokenOrder": query.token_order,
            "scope": "local-text-tier",
        }
        if query.fuzzy_max_edits > 0:
            query_semantics["fuzzy"] = {
                "maxEdits": query.fuzzy_max_edits,
                "prefixLength": query.fuzzy_prefix_length,
                "maxExpansions": query.fuzzy_max_expansions,
            }
    elif isinstance(query, SearchWildcardQuery):
        query_semantics = {
            "matchingMode": "glob-local",
            "patternSyntax": "fnmatch-like",
            "allowAnalyzedField": query.allow_analyzed_field,
            "atlasParity": "subset",
            "scope": "local-text-tier",
        }
    elif isinstance(query, SearchRegexQuery):
        query_semantics = {
            "matchingMode": "python-regex-local",
            "flags": query.flags,
            "supportsFlags": True,
            "allowAnalyzedField": query.allow_analyzed_field,
            "atlasParity": "subset",
            "scope": "local-text-tier",
        }
    else:
        query_semantics = {
            "matchingMode": "tokenized-any-term",
            "scoringMode": "term-frequency-local",
            "scope": "local-text-tier",
        }
    return {
        "query": query.raw_query,
        "paths": paths,
        "slop": query.slop if isinstance(query, SearchPhraseQuery) else None,
        "querySemantics": query_semantics,
        "compound": None,
        "value": None,
        "range": None,
        "origin": None,
        "originKind": None,
        "pivot": None,
        "path": None,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
        "pathSummary": _search_path_summary(paths),
    }


def _phrase_query_score(
    prepared: MaterializedSearchDocument,
    query: SearchPhraseQuery,
) -> float:
    terms = tokenize_classic_text(query.raw_query)
    if not terms:
        return 0.0
    score = 0.0
    for value in _materialized_lowered_values(prepared, query.paths):
        score += _phrase_value_score(value, terms=terms, slop=query.slop)
    return score


def _phrase_value_score(
    value: str,
    *,
    terms: tuple[str, ...],
    slop: int,
) -> float:
    tokens = tokenize_classic_text(value)
    if len(tokens) < len(terms):
        return 0.0
    score = 0.0
    for start_index, token in enumerate(tokens):
        if token != terms[0]:
            continue
        gap_sum = _phrase_match_gap_sum(tokens, terms=terms, slop=slop, start_index=start_index)
        if gap_sum is None:
            continue
        score += 1.0 / (1.0 + float(gap_sum))
    return score


def _phrase_match_gap_sum(
    tokens: tuple[str, ...],
    *,
    terms: tuple[str, ...],
    slop: int,
    start_index: int,
) -> int | None:
    current_index = start_index
    total_gap = 0
    for term in terms[1:]:
        next_index = _find_phrase_term_index(
            tokens,
            term=term,
            start=current_index + 1,
            stop=min(len(tokens), current_index + slop + 2),
        )
        if next_index is None:
            return None
        total_gap += next_index - current_index - 1
        current_index = next_index
    return total_gap


def _find_phrase_term_index(
    tokens: tuple[str, ...],
    *,
    term: str,
    start: int,
    stop: int,
) -> int | None:
    for index in range(start, stop):
        if tokens[index] == term:
            return index
    return None


def _explain_exists_query(query: SearchExistsQuery) -> dict[str, object | None]:
    paths = list(query.paths) if query.paths is not None else None
    return {
        "query": None,
        "paths": paths,
        "querySemantics": {
            "matchingMode": "field-presence",
            "scope": "local-text-tier",
        },
        "compound": None,
        "value": None,
        "range": None,
        "origin": None,
        "originKind": None,
        "pivot": None,
        "path": None,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
        "pathSummary": _search_path_summary(paths),
    }


def _explain_in_query(query: SearchInQuery) -> dict[str, object | None]:
    return {
        "query": None,
        "paths": None,
        "querySemantics": {
            "matchingMode": "exact-membership",
            "scope": "local-filter-tier",
        },
        "compound": None,
        "value": list(query.values),
        "range": None,
        "origin": None,
        "originKind": None,
        "pivot": None,
        "path": query.path,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
        "pathSummary": _scalar_search_path_summary(query.path, section_name="in"),
    }


def _explain_equals_query(query: SearchEqualsQuery) -> dict[str, object | None]:
    return {
        "query": None,
        "paths": None,
        "querySemantics": {
            "matchingMode": "exact-equality",
            "scope": "local-filter-tier",
        },
        "compound": None,
        "value": query.value,
        "range": None,
        "origin": None,
        "originKind": None,
        "pivot": None,
        "path": query.path,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
        "pathSummary": _scalar_search_path_summary(query.path, section_name="equals"),
    }


def _explain_range_query(query: SearchRangeQuery) -> dict[str, object | None]:
    return {
        "query": None,
        "paths": None,
        "querySemantics": {
            "matchingMode": "range-comparison",
            "scope": "local-filter-tier",
        },
        "compound": None,
        "value": None,
        "range": {
            "gt": query.gt,
            "gte": query.gte,
            "lt": query.lt,
            "lte": query.lte,
            "boundKind": query.bound_kind,
        },
        "origin": None,
        "originKind": None,
        "pivot": None,
        "path": query.path,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
        "pathSummary": _scalar_search_path_summary(query.path, section_name="range"),
    }


def _explain_near_query(query: SearchNearQuery) -> dict[str, object | None]:
    return {
        "query": None,
        "paths": None,
        "querySemantics": {
            "matchingMode": "distance-ranking",
            "scope": "local-filter-tier",
        },
        "compound": None,
        "ranking": {
            "distanceMode": "pivot-decay",
            "distanceUnit": query.origin_kind,
            "scoreFormula": "1 + 1 / (1 + distance / pivot)",
        },
        "value": None,
        "range": None,
        "origin": query.origin,
        "originKind": query.origin_kind,
        "pivot": query.pivot,
        "path": query.path,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
        "pathSummary": _scalar_search_path_summary(query.path, section_name="near"),
        "supportedOriginKinds": ["number", "date", "datetime"],
        "pivotDecay": {
            "baselineScore": 1.0,
            "maxScore": 2.0,
            "pivot": query.pivot,
        },
    }


def _explain_compound_query(query: SearchCompoundQuery) -> dict[str, object | None]:
    def _clause_operator_names(
        clauses: tuple[SearchTextLikeQuery, ...],
    ) -> list[str]:
        return [
            _SEARCH_QUERY_OPERATOR_NAMES.get(type(clause), type(clause).__name__)
            for clause in clauses
        ]

    def _clause_paths(
        clauses: tuple[SearchTextLikeQuery, ...],
    ) -> list[str]:
        collected: set[str] = set()
        for clause in clauses:
            collected.update(_query_paths(clause))
        return sorted(collected)

    def _typed_clause_paths(
        clauses: tuple[SearchTextLikeQuery, ...],
        *,
        query_type: type[SearchTextLikeQuery] | tuple[type[SearchTextLikeQuery], ...],
    ) -> list[str]:
        collected: set[str] = set()
        for clause in clauses:
            if isinstance(clause, query_type):
                collected.update(_query_paths(clause))
            elif isinstance(clause, SearchCompoundQuery):
                for nested_clauses in (
                    clause.must,
                    clause.should,
                    clause.filter,
                    clause.must_not,
                ):
                    collected.update(
                        _typed_clause_paths(
                            nested_clauses,
                            query_type=query_type,
                        )
                    )
        return sorted(collected)

    def _embedded_paths(paths: list[str]) -> list[str]:
        return [path for path in paths if "." in path]

    def _parent_paths(paths: list[str]) -> list[str]:
        return [path for path in paths if "." not in path]

    def _leaf_paths(paths: list[str]) -> list[str]:
        return [path for path in paths if "." in path]

    must_paths = _clause_paths(query.must)
    should_paths = _clause_paths(query.should)
    filter_paths = _clause_paths(query.filter)
    must_not_paths = _clause_paths(query.must_not)
    all_paths = sorted(_query_paths(query))
    textual_types = (
        SearchTextQuery,
        SearchPhraseQuery,
        SearchAutocompleteQuery,
        SearchWildcardQuery,
        SearchRegexQuery,
    )
    scalar_types = (
        SearchInQuery,
        SearchEqualsQuery,
        SearchRangeQuery,
        SearchNearQuery,
    )
    textual_paths = sorted(
        set(_typed_clause_paths(query.must, query_type=textual_types))
        | set(_typed_clause_paths(query.should, query_type=textual_types))
        | set(_typed_clause_paths(query.filter, query_type=textual_types))
        | set(_typed_clause_paths(query.must_not, query_type=textual_types))
    )
    scalar_paths = sorted(
        set(_typed_clause_paths(query.must, query_type=scalar_types))
        | set(_typed_clause_paths(query.should, query_type=scalar_types))
        | set(_typed_clause_paths(query.filter, query_type=scalar_types))
        | set(_typed_clause_paths(query.must_not, query_type=scalar_types))
    )
    embedded_path_sections = [
        section
        for section, paths in (
            ("must", must_paths),
            ("should", should_paths),
            ("filter", filter_paths),
            ("mustNot", must_not_paths),
        )
        if any("." in path for path in paths)
    ]

    return {
        "query": None,
        "paths": None,
        "compound": {
            "must": len(query.must),
            "should": len(query.should),
            "filter": len(query.filter),
            "mustNot": len(query.must_not),
            "minimumShouldMatch": query.minimum_should_match,
            "mustOperators": _clause_operator_names(query.must),
            "shouldOperators": _clause_operator_names(query.should),
            "filterOperators": _clause_operator_names(query.filter),
            "mustNotOperators": _clause_operator_names(query.must_not),
        },
        "pathSummary": {
            "must": must_paths,
            "should": should_paths,
            "filter": filter_paths,
            "mustNot": must_not_paths,
            "all": all_paths,
            "parentPaths": _parent_paths(all_paths),
            "leafPaths": _leaf_paths(all_paths),
            "textualPaths": textual_paths,
            "textualParentPaths": _parent_paths(textual_paths),
            "textualLeafPaths": _leaf_paths(textual_paths),
            "scalarPaths": scalar_paths,
            "scalarParentPaths": _parent_paths(scalar_paths),
            "scalarLeafPaths": _leaf_paths(scalar_paths),
            "embeddedPaths": _embedded_paths(all_paths),
            "embeddedTextualPaths": _embedded_paths(textual_paths),
            "embeddedScalarPaths": _embedded_paths(scalar_paths),
            "embeddedPathSections": embedded_path_sections,
            "usesEmbeddedPaths": bool(embedded_path_sections),
            "nestedCompoundCount": _count_nested_compounds(query),
            "maxClauseDepth": _compound_max_depth(query),
        },
        "ranking": {
            "usesShouldRanking": bool(query.should),
            "nearAware": any(isinstance(clause, SearchNearQuery) for clause in query.should),
            "nearAwareShouldCount": sum(
                1 for clause in query.should if isinstance(clause, SearchNearQuery)
            ),
            "shouldRankingMode": (
                "matched-should-plus-clause-score"
                if query.should
                else "no-should-ranking"
            ),
            "minimumShouldMatchApplied": query.minimum_should_match > 0,
        },
        "value": None,
        "range": None,
        "origin": None,
        "originKind": None,
        "pivot": None,
        "path": None,
        "queryVector": None,
        "limit": None,
        "numCandidates": None,
        "filter": None,
        "similarity": None,
    }


def _query_paths(query: SearchTextLikeQuery) -> tuple[str, ...]:
    if isinstance(
        query,
        (
            SearchTextQuery,
            SearchPhraseQuery,
            SearchAutocompleteQuery,
            SearchWildcardQuery,
            SearchRegexQuery,
            SearchExistsQuery,
        ),
    ):
        return query.paths or ()
    if isinstance(
        query,
        (SearchInQuery, SearchEqualsQuery, SearchRangeQuery, SearchNearQuery),
    ):
        return (query.path,)
    if isinstance(query, SearchCompoundQuery):
        collected: list[str] = []
        for clauses in (query.must, query.should, query.filter, query.must_not):
            for clause in clauses:
                collected.extend(_query_paths(clause))
        return tuple(dict.fromkeys(collected))
    return ()


def attach_search_highlights(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchTextLikeQuery,
) -> Document:
    options = _search_stage_options(query)
    if options.highlight is None:
        return document
    highlights = build_search_highlights(
        document,
        definition=definition,
        query=query,
        spec=options.highlight,
    )
    if not highlights:
        return document
    highlighted = deepcopy(document)
    highlighted[SEARCH_HIGHLIGHTS_FIELD] = highlights
    return highlighted


def build_search_stage_option_previews(
    documents: list[Document],
    *,
    definition: SearchIndexDefinition,
    query: SearchTextLikeQuery,
) -> dict[str, object]:
    options = _search_stage_options(query)
    previews: dict[str, object] = {}
    if options.count is not None:
        previews["countPreview"] = {
            "type": options.count.mode,
            "value": len(documents),
            "exact": options.count.mode == "total",
        }
    if options.highlight is not None:
        preview_fragments: list[dict[str, object]] = []
        for document in documents:
            value = document.get(SEARCH_HIGHLIGHTS_FIELD)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        preview_fragments.append(deepcopy(item))
            if len(preview_fragments) >= 3:
                break
        previews["highlightPreview"] = {
            "resultField": SEARCH_HIGHLIGHTS_FIELD,
            "requestedPaths": list(options.highlight.paths or ()),
            "fragmentCount": sum(
                len(document.get(SEARCH_HIGHLIGHTS_FIELD, ()))
                for document in documents
                if isinstance(document.get(SEARCH_HIGHLIGHTS_FIELD), list)
            ),
            "sample": preview_fragments[:3],
        }
    if options.facet is not None:
        previews["facetPreview"] = _facet_preview_payload(documents, options.facet)
    return previews


def build_search_meta_document(
    documents: list[Document],
    *,
    query: SearchTextLikeQuery,
) -> Document:
    options = _search_stage_options(query)
    if options.highlight is not None:
        raise OperationFailure("$searchMeta does not support highlight")
    if options.count is None and options.facet is None:
        raise OperationFailure("$searchMeta requires at least one of count or facet")
    result: Document = {}
    if options.count is not None:
        count_value = len(documents)
        if options.count.mode == "lowerBound":
            result["count"] = {"lowerBound": count_value}
        else:
            result["count"] = {"total": count_value}
    if options.facet is not None:
        result["facet"] = _facet_preview_payload(documents, options.facet)
    return result


def _facet_preview_payload(
    documents: list[Document],
    spec: SearchFacetSpec,
) -> dict[str, object]:
    if spec.facets:
        return {
            "facets": {
                name: _facet_preview(
                    documents,
                    SearchFacetSpec(path=path, num_buckets=num_buckets, facet_type=facet_type),
                )
                for name, path, facet_type, num_buckets in spec.facets
            }
        }
    return _facet_preview(documents, spec)


def build_search_highlights(
    document: Document,
    *,
    definition: SearchIndexDefinition,
    query: SearchTextLikeQuery,
    spec: SearchHighlightSpec,
) -> list[dict[str, object]]:
    available_leaf_paths = _mapped_textual_search_paths(definition)
    requested_paths = list(spec.paths or ())
    resolved_paths = _resolve_requested_leaf_paths(
        requested_paths,
        available_leaf_paths=available_leaf_paths,
    )
    clauses = _highlightable_textual_clauses(query)
    highlights: list[dict[str, object]] = []
    seen: set[tuple[str, str, str]] = set()
    for path in resolved_paths:
        values = [
            value
            for value in _search_path_values(document, path)
            if isinstance(value, str)
        ]
        if not values:
            continue
        for clause in clauses:
            clause_paths = _resolved_highlight_clause_paths(
                clause,
                available_leaf_paths=available_leaf_paths,
            )
            if clause_paths is not None and path not in clause_paths:
                continue
            for value in values:
                if not _text_value_matches_clause(value, clause):
                    continue
                payload = _highlight_payload_for_clause(
                    path,
                    value,
                    clause=clause,
                    max_chars=spec.max_chars,
                )
                marker = (
                    str(payload["path"]),
                    str(payload["operator"]),
                    str(payload["text"]),
                )
                if marker in seen:
                    continue
                seen.add(marker)
                highlights.append(payload)
    return highlights


def _highlightable_textual_clauses(
    query: SearchTextLikeQuery,
) -> tuple[
    SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery,
    ...,
]:
    if isinstance(
        query,
        (
            SearchTextQuery,
            SearchPhraseQuery,
            SearchAutocompleteQuery,
            SearchWildcardQuery,
            SearchRegexQuery,
        ),
    ):
        return (query,)
    if not isinstance(query, SearchCompoundQuery):
        return ()
    clauses: list[
        SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery
    ] = []
    for group in (query.must, query.should, query.filter, query.must_not):
        for clause in group:
            clauses.extend(_highlightable_textual_clauses(clause))
    return tuple(clauses)


def _resolved_highlight_clause_paths(
    clause: SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery,
    *,
    available_leaf_paths: tuple[str, ...],
) -> set[str] | None:
    if clause.paths is None:
        return None
    return set(
        _resolve_requested_leaf_paths(
            list(clause.paths),
            available_leaf_paths=available_leaf_paths,
        )
    )


def _text_value_matches_clause(
    value: str,
    clause: SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery,
) -> bool:
    lowered = value.lower()
    if isinstance(clause, SearchTextQuery):
        token_set = set(tokenize_classic_text(value))
        return all(term.lower() in token_set for term in clause.terms)
    if isinstance(clause, SearchPhraseQuery):
        return _phrase_value_score(
            value,
            terms=tokenize_classic_text(clause.raw_query),
            slop=clause.slop,
        ) > 0.0
    if isinstance(clause, SearchAutocompleteQuery):
        tokens = tokenize_classic_text(value)
        if clause.token_order == "sequential":
            return _token_sequence_matches(
                tokens,
                tuple(term.lower() for term in clause.terms),
                matcher=lambda token, term: _autocomplete_token_matches(
                    token,
                    term,
                    query=clause,
                ),
            )
        return all(
            any(_autocomplete_token_matches(token, term.lower(), query=clause) for token in tokens)
            for term in clause.terms
        )
    if isinstance(clause, SearchWildcardQuery):
        return fnmatch.fnmatchcase(lowered, clause.normalized_pattern)
    compiled = re.compile(clause.raw_query, _regex_compile_flags(clause.flags))
    return compiled.search(value) is not None


def _highlight_payload_for_clause(
    path: str,
    value: str,
    *,
    clause: SearchTextQuery | SearchPhraseQuery | SearchAutocompleteQuery | SearchWildcardQuery | SearchRegexQuery,
    max_chars: int,
) -> dict[str, object]:
    operator = _SEARCH_QUERY_OPERATOR_NAMES[type(clause)]
    payload: dict[str, object] = {
        "path": path,
        "operator": operator,
        "text": _truncate_highlight_value(value, max_chars=max_chars),
    }
    if isinstance(clause, (SearchTextQuery, SearchAutocompleteQuery)):
        payload["matchedTerms"] = list(clause.terms)
    elif isinstance(clause, SearchPhraseQuery):
        payload["matchedPhrase"] = clause.raw_query
    elif isinstance(clause, SearchWildcardQuery):
        payload["pattern"] = clause.raw_query
    elif isinstance(clause, SearchRegexQuery):
        payload["pattern"] = clause.raw_query
        payload["flags"] = clause.flags
    return payload


def _truncate_highlight_value(value: str, *, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 1] + "…"


def _facet_preview(
    documents: list[Document],
    spec: SearchFacetSpec,
) -> dict[str, object]:
    counts: dict[tuple[str, object], int] = {}
    labels: dict[tuple[str, object], object] = {}
    for document in documents:
        seen_in_document: set[tuple[str, object]] = set()
        for value in _search_path_values(document, spec.path):
            normalized = _normalize_search_scalar_value(value)
            if normalized is None:
                continue
            normalized_kind = normalized[0]
            if spec.facet_type == "date":
                if normalized_kind not in {"date", "datetime"}:
                    continue
            elif normalized_kind != spec.facet_type:
                continue
            if normalized in seen_in_document:
                continue
            seen_in_document.add(normalized)
            counts[normalized] = counts.get(normalized, 0) + 1
            labels[normalized] = normalized[1]
    buckets = [
        {"value": labels[key], "count": count}
        for key, count in sorted(
            counts.items(),
            key=lambda item: (-item[1], str(labels[item[0]])),
        )[: spec.num_buckets]
    ]
    return {
        "type": spec.facet_type,
        "path": spec.path,
        "numBuckets": spec.num_buckets,
        "buckets": buckets,
    }


def _search_path_summary(paths: list[str] | None) -> dict[str, object] | None:
    if paths is None:
        return None
    embedded_paths = [path for path in paths if "." in path]
    parent_paths = [path for path in paths if "." not in path]
    leaf_paths = [path for path in paths if "." in path]
    return {
        "all": list(paths),
        "pathCount": len(paths),
        "multiPath": len(paths) > 1,
        "usesEmbeddedPaths": bool(embedded_paths),
        "embeddedPaths": embedded_paths,
        "parentPaths": parent_paths,
        "leafPaths": leaf_paths,
    }


def _scalar_search_path_summary(path: str, *, section_name: str) -> dict[str, object]:
    path_summary = _search_path_summary([path]) or {}
    path_summary["sections"] = [section_name]
    return path_summary


def _vector_score_formula(similarity: str) -> str:
    if similarity == "dotProduct":
        return "sum(query[i] * candidate[i])"
    if similarity == "euclidean":
        return "-sqrt(sum((query[i] - candidate[i])^2))"
    return "dot(query, candidate) / (||query|| * ||candidate||)"


def _enrich_search_explain_details(
    details: dict[str, object | None],
    *,
    query: SearchQuery,
    definition: SearchIndexDefinition,
) -> None:
    path_summary = details.get("pathSummary")
    if not isinstance(path_summary, dict):
        return
    if isinstance(
        query,
        (
            SearchTextQuery,
            SearchPhraseQuery,
            SearchAutocompleteQuery,
            SearchWildcardQuery,
            SearchRegexQuery,
        ),
    ):
        _attach_resolved_leaf_paths(
            path_summary,
            paths=list(query.paths) if query.paths is not None else None,
            available_leaf_paths=_mapped_textual_search_paths(definition),
        )
        return
    if isinstance(query, SearchExistsQuery):
        _attach_resolved_leaf_paths(
            path_summary,
            paths=list(query.paths) if query.paths is not None else None,
            available_leaf_paths=_mapped_leaf_search_paths(definition),
        )
        return
    if isinstance(query, SearchVectorQuery):
        path_value = details.get("path")
        if isinstance(path_value, str) and path_value:
            _attach_resolved_leaf_paths(
                path_summary,
                paths=[path_value],
                available_leaf_paths=vector_field_paths(definition),
            )
        return
    if isinstance(query, (SearchInQuery, SearchEqualsQuery, SearchRangeQuery, SearchNearQuery)):
        path_value = details.get("path")
        if isinstance(path_value, str) and path_value:
            _attach_resolved_leaf_paths(
                path_summary,
                paths=[path_value],
                available_leaf_paths=_mapped_scalar_search_paths(definition),
            )
        return
    if isinstance(query, SearchCompoundQuery):
        all_paths = path_summary.get("all")
        textual_paths = path_summary.get("textualPaths")
        scalar_paths = path_summary.get("scalarPaths")
        all_available_leaf_paths = _mapped_leaf_search_paths(definition)
        if isinstance(all_paths, list):
            _attach_resolved_leaf_paths(path_summary, paths=all_paths, available_leaf_paths=all_available_leaf_paths)
        if isinstance(textual_paths, list):
            path_summary["resolvedTextualLeafPaths"] = _resolve_requested_leaf_paths(
                textual_paths,
                available_leaf_paths=_mapped_textual_search_paths(definition),
            )
        if isinstance(scalar_paths, list):
            path_summary["resolvedScalarLeafPaths"] = _resolve_requested_leaf_paths(
                scalar_paths,
                available_leaf_paths=_mapped_scalar_search_paths(definition),
            )


def _attach_resolved_leaf_paths(
    path_summary: dict[str, object | None],
    *,
    paths: list[str] | None,
    available_leaf_paths: tuple[str, ...],
) -> None:
    if paths is None:
        return
    resolved_leaf_paths = _resolve_requested_leaf_paths(
        paths,
        available_leaf_paths=available_leaf_paths,
    )
    path_summary["resolvedLeafPaths"] = resolved_leaf_paths
    path_summary["unresolvedPaths"] = _unresolved_requested_paths(
        paths,
        available_leaf_paths=available_leaf_paths,
    )


def _count_nested_compounds(query: SearchCompoundQuery) -> int:
    total = 0
    for clauses in (query.must, query.should, query.filter, query.must_not):
        for clause in clauses:
            if isinstance(clause, SearchCompoundQuery):
                total += 1 + _count_nested_compounds(clause)
    return total


def _compound_max_depth(query: SearchCompoundQuery) -> int:
    max_depth = 1
    for clauses in (query.must, query.should, query.filter, query.must_not):
        for clause in clauses:
            if isinstance(clause, SearchCompoundQuery):
                max_depth = max(max_depth, 1 + _compound_max_depth(clause))
    return max_depth


def _explain_vector_query(query: SearchVectorQuery) -> dict[str, object | None]:
    similarity = query.similarity
    return {
        "query": None,
        "paths": None,
        "querySemantics": {
            "matchingMode": "nearest-neighbor",
            "scope": "local-vector-tier",
            "requiresLeadingStage": True,
            "supportsStructuredFilter": True,
        },
        "scoreBreakdown": {
            "similarity": similarity,
            "scoreField": "vectorSearchScore",
            "scoreDirection": "higher-is-better",
            "scoreFormula": _vector_score_formula(similarity),
            "minScoreApplied": query.min_score is not None,
        },
        "candidatePlan": {
            "requestedCandidates": query.num_candidates,
            "resultLimit": query.limit,
            "structuredFilterPresent": query.filter_spec is not None,
            "minScorePresent": query.min_score is not None,
        },
        "compound": None,
        "value": None,
        "range": None,
        "origin": None,
        "originKind": None,
        "pivot": None,
        "path": query.path,
        "queryVector": list(query.query_vector),
        "limit": query.limit,
        "numCandidates": query.num_candidates,
        "filter": deepcopy(query.filter_spec) if query.filter_spec is not None else None,
        "similarity": similarity,
        "minScore": query.min_score,
        "pathSummary": _scalar_search_path_summary(query.path, section_name="vectorSearch"),
    }


_SEARCH_STAGE_COMPILERS: dict[str, Callable[[object], SearchQuery]] = {
    "$search": compile_search_text_like_query,
    "$searchMeta": compile_search_meta_text_like_query,
    "$vectorSearch": compile_vector_search_query,
}

_SEARCH_CLAUSE_COMPILERS: dict[str, _SearchClauseCompiler] = {
    "text": _compile_search_text_clause,
    "phrase": _compile_search_phrase_clause,
    "autocomplete": _compile_search_autocomplete_clause,
    "wildcard": _compile_search_wildcard_clause,
    "regex": _compile_search_regex_clause,
    "exists": _compile_search_exists_clause,
    "in": _compile_search_in_clause,
    "equals": _compile_search_equals_clause,
    "range": _compile_search_range_clause,
    "near": _compile_search_near_clause,
    "compound": _compile_search_compound_clause,
}

_SEARCH_QUERY_OPERATOR_NAMES: dict[type[Any], str] = {
    SearchTextQuery: "text",
    SearchPhraseQuery: "phrase",
    SearchAutocompleteQuery: "autocomplete",
    SearchWildcardQuery: "wildcard",
    SearchRegexQuery: "regex",
    SearchExistsQuery: "exists",
    SearchInQuery: "in",
    SearchEqualsQuery: "equals",
    SearchRangeQuery: "range",
    SearchNearQuery: "near",
    SearchCompoundQuery: "compound",
}

_SEARCH_QUERY_MATCHERS: dict[type[Any], _SearchMatcher] = {
    SearchTextQuery: lambda document, definition, query, materialized=None: matches_search_text_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchPhraseQuery: lambda document, definition, query, materialized=None: matches_search_phrase_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchAutocompleteQuery: lambda document, definition, query, materialized=None: matches_search_autocomplete_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchWildcardQuery: lambda document, definition, query, materialized=None: matches_search_wildcard_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchRegexQuery: lambda document, definition, query, materialized=None: matches_search_regex_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchExistsQuery: lambda document, definition, query, materialized=None: matches_search_exists_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchInQuery: lambda document, definition, query, materialized=None: matches_search_in_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchEqualsQuery: lambda document, definition, query, materialized=None: matches_search_equals_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchRangeQuery: lambda document, definition, query, materialized=None: matches_search_range_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchNearQuery: lambda document, definition, query, materialized=None: matches_search_near_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
    SearchCompoundQuery: lambda document, definition, query, materialized=None: matches_search_compound_query(document, definition=definition, query=query, materialized=materialized),  # type: ignore[arg-type]
}

_SEARCH_QUERY_EXPLAINERS: dict[type[Any], _SearchExplainBuilder] = {
    SearchTextQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchPhraseQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchAutocompleteQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchWildcardQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchRegexQuery: lambda query: _explain_text_like_query(query),  # type: ignore[arg-type]
    SearchExistsQuery: lambda query: _explain_exists_query(query),  # type: ignore[arg-type]
    SearchInQuery: lambda query: _explain_in_query(query),  # type: ignore[arg-type]
    SearchEqualsQuery: lambda query: _explain_equals_query(query),  # type: ignore[arg-type]
    SearchRangeQuery: lambda query: _explain_range_query(query),  # type: ignore[arg-type]
    SearchNearQuery: lambda query: _explain_near_query(query),  # type: ignore[arg-type]
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
        if mapping_type == "embeddedDocuments":
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        entries.extend(
                            _collect_entries_from_mapping(
                                item,
                                field_spec,
                                prefix=path,
                            )
                        )
            continue
        if mapping_type not in TEXTUAL_SEARCH_FIELD_MAPPING_TYPES:
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


def _mapped_search_paths(definition: SearchIndexDefinition) -> tuple[str, ...]:
    if definition.index_type not in TEXTUAL_SEARCH_INDEX_TYPES:
        return ()
    mappings = definition.definition.get("mappings")
    if not isinstance(mappings, dict):
        return ()
    return tuple(_iter_mapped_search_paths(mappings))


def _mapped_leaf_search_paths(definition: SearchIndexDefinition) -> tuple[str, ...]:
    if definition.index_type not in TEXTUAL_SEARCH_INDEX_TYPES:
        return ()
    mappings = definition.definition.get("mappings")
    if not isinstance(mappings, dict):
        return ()
    return tuple(_iter_mapped_leaf_search_paths(mappings))


def _mapped_textual_search_paths(definition: SearchIndexDefinition) -> tuple[str, ...]:
    if definition.index_type not in TEXTUAL_SEARCH_INDEX_TYPES:
        return ()
    mappings = definition.definition.get("mappings")
    if not isinstance(mappings, dict):
        return ()
    return tuple(
        _iter_mapped_leaf_search_paths(
            mappings,
            allowed_mapping_types=TEXTUAL_SEARCH_FIELD_MAPPING_TYPES,
        )
    )


def _mapped_scalar_search_paths(definition: SearchIndexDefinition) -> tuple[str, ...]:
    if definition.index_type not in TEXTUAL_SEARCH_INDEX_TYPES:
        return ()
    mappings = definition.definition.get("mappings")
    if not isinstance(mappings, dict):
        return ()
    return tuple(
        _iter_mapped_leaf_search_paths(
            mappings,
            allowed_mapping_types=EXACT_FILTER_SEARCH_FIELD_MAPPING_TYPES,
        )
    )


def _iter_mapped_search_paths(mappings: Document, prefix: str = "") -> tuple[str, ...]:
    fields = mappings.get("fields", {})
    if not isinstance(fields, dict):
        return ()
    collected: list[str] = []
    for field_name, field_spec in fields.items():
        if not isinstance(field_name, str) or not field_name or not isinstance(field_spec, dict):
            continue
        path = f"{prefix}.{field_name}" if prefix else field_name
        mapping_type = field_spec.get("type", "document")
        collected.append(path)
        if mapping_type in STRUCTURED_SEARCH_FIELD_MAPPING_TYPES:
            nested_mapping = {key: value for key, value in field_spec.items() if key != "type"}
            collected.extend(_iter_mapped_search_paths(nested_mapping, prefix=path))
    return tuple(dict.fromkeys(collected))


def _iter_mapped_leaf_search_paths(
    mappings: Document,
    prefix: str = "",
    *,
    allowed_mapping_types: frozenset[str] | None = None,
) -> tuple[str, ...]:
    fields = mappings.get("fields", {})
    if not isinstance(fields, dict):
        return ()
    collected: list[str] = []
    for field_name, field_spec in fields.items():
        if not isinstance(field_name, str) or not field_name or not isinstance(field_spec, dict):
            continue
        path = f"{prefix}.{field_name}" if prefix else field_name
        mapping_type = field_spec.get("type", "document")
        if mapping_type in STRUCTURED_SEARCH_FIELD_MAPPING_TYPES:
            nested_mapping = {key: value for key, value in field_spec.items() if key != "type"}
            collected.extend(
                _iter_mapped_leaf_search_paths(
                    nested_mapping,
                    prefix=path,
                    allowed_mapping_types=allowed_mapping_types,
                )
            )
            continue
        if allowed_mapping_types is not None and mapping_type not in allowed_mapping_types:
            continue
        collected.append(path)
    return tuple(dict.fromkeys(collected))


def _resolve_requested_leaf_paths(
    requested_paths: list[str],
    *,
    available_leaf_paths: tuple[str, ...],
) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for requested_path in requested_paths:
        for candidate_path in available_leaf_paths:
            if candidate_path != requested_path and not candidate_path.startswith(f"{requested_path}."):
                continue
            if candidate_path in seen:
                continue
            seen.add(candidate_path)
            resolved.append(candidate_path)
    return sorted(resolved)


def _unresolved_requested_paths(
    requested_paths: list[str],
    *,
    available_leaf_paths: tuple[str, ...],
) -> list[str]:
    unresolved: list[str] = []
    for requested_path in requested_paths:
        if any(
            candidate_path == requested_path
            or candidate_path.startswith(f"{requested_path}.")
            for candidate_path in available_leaf_paths
        ):
            continue
        unresolved.append(requested_path)
    return sorted(unresolved)


def _search_path_values(value: object, path: str) -> tuple[object, ...]:
    if not path:
        if isinstance(value, list):
            return tuple(value)
        return (value,)

    if isinstance(value, list):
        segment = path.split(".", 1)[0]
        if segment.isdigit():
            index = int(segment)
            if index >= len(value):
                return ()
            rest = path.split(".", 1)[1] if "." in path else ""
            return _search_path_values(value[index], rest)
        values: list[object] = []
        for item in value:
            values.extend(_search_path_values(item, path))
        return tuple(values)

    if not isinstance(value, dict):
        return ()

    if "." not in path:
        if path not in value:
            return ()
        return _search_path_values(value[path], "")

    first, rest = path.split(".", 1)
    if first not in value:
        return ()
    return _search_path_values(value[first], rest)


def _search_path_exists(value: object, path: str) -> bool:
    if not path:
        return True

    if isinstance(value, list):
        segment = path.split(".", 1)[0]
        if segment.isdigit():
            index = int(segment)
            if index >= len(value):
                return False
            rest = path.split(".", 1)[1] if "." in path else ""
            return _search_path_exists(value[index], rest)
        return any(_search_path_exists(item, path) for item in value)

    if not isinstance(value, dict):
        return False

    if "." not in path:
        return path in value

    first, rest = path.split(".", 1)
    if first not in value:
        return False
    return _search_path_exists(value[first], rest)


def _search_near_origin_kind(value: object) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return "number" if math.isfinite(float(value)) else None
    if isinstance(value, datetime.datetime):
        return "date"
    if isinstance(value, datetime.date):
        return "date"
    return None


def _normalize_search_scalar_value(value: object) -> tuple[str, object] | None:
    if value is None:
        return "null", None
    if isinstance(value, bool):
        return "bool", value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        return "number", numeric
    if isinstance(value, datetime.datetime):
        return "datetime", value
    if isinstance(value, datetime.date):
        return "date", value
    if isinstance(value, str):
        return "string", value
    return None


def _search_path_scalar_values(
    document: Document,
    path: str,
) -> tuple[tuple[str, object], ...]:
    values: list[tuple[str, object]] = []
    for item in _search_path_values(document, path):
        normalized_item = _normalize_search_scalar_value(item)
        if normalized_item is not None:
            values.append(normalized_item)
    return tuple(values)


def _search_range_matches_value(
    *,
    candidate_kind: str,
    candidate_value: object,
    query: SearchRangeQuery,
) -> bool:
    if candidate_kind != query.bound_kind:
        return False
    if query.gt is not None and not candidate_value > query.gt:
        return False
    if query.gte is not None and not candidate_value >= query.gte:
        return False
    if query.lt is not None and not candidate_value < query.lt:
        return False
    if query.lte is not None and not candidate_value <= query.lte:
        return False
    return True


def _datetime_to_sortable_number(value: datetime.date | datetime.datetime) -> float:
    if isinstance(value, datetime.datetime):
        return (
            value.toordinal() * 86400.0
            + value.hour * 3600.0
            + value.minute * 60.0
            + value.second
            + value.microsecond / 1_000_000.0
        )
    return float(value.toordinal() * 86400)


def _search_near_scalar_distance(
    candidate: object,
    *,
    origin: float | datetime.date | datetime.datetime,
    origin_kind: str,
) -> float | None:
    if origin_kind == "number":
        if not isinstance(candidate, (int, float)) or isinstance(candidate, bool):
            return None
        if not math.isfinite(float(candidate)):
            return None
        return abs(float(candidate) - float(origin))
    if origin_kind == "date":
        if not isinstance(candidate, (datetime.date, datetime.datetime)):
            return None
        return abs(_datetime_to_sortable_number(candidate) - _datetime_to_sortable_number(origin))
    return None


def search_near_distance(
    document: Document,
    *,
    query: SearchNearQuery,
) -> float | None:
    distances = [
        distance
        for item in _search_path_values(document, query.path)
        if (
            distance := _search_near_scalar_distance(
                item,
                origin=query.origin,
                origin_kind=query.origin_kind,
            )
        )
        is not None
    ]
    if not distances:
        return None
    best = min(distances)
    return best if best <= query.pivot else None


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
