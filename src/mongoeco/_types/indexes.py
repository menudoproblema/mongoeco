from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

from mongoeco.core._search_contract import TEXT_SEARCH_INDEX_CAPABILITIES

type Filter = dict[str, Any]
type Document = dict[str, Any]
type SortDirection = Literal[1, -1]
type SpecialIndexDirection = Literal["text", "hashed", "2dsphere", "2d"]
type IndexDirection = SortDirection | SpecialIndexDirection
type IndexKeySpec = list[tuple[str, IndexDirection]]
type IndexDocument = dict[str, object]
type IndexInformationEntry = dict[str, object]
type IndexInformation = dict[str, IndexInformationEntry]
type SearchIndexDocument = dict[str, object]
type TextIndexWeights = dict[str, int]

_SPECIAL_INDEX_DIRECTIONS = frozenset({"text", "hashed", "2dsphere", "2d"})


def normalize_index_direction(direction: object) -> IndexDirection:
    if isinstance(direction, bool):
        raise ValueError("index directions must be 1, -1, or a supported index type string")
    if direction in (1, -1):
        return direction
    if isinstance(direction, str) and direction in _SPECIAL_INDEX_DIRECTIONS:
        return direction
    raise ValueError("index directions must be 1, -1, or a supported index type string")


def is_ordered_index_direction(direction: object) -> bool:
    return direction in (1, -1) and not isinstance(direction, bool)


def is_special_index_direction(direction: object) -> bool:
    return isinstance(direction, str) and direction in _SPECIAL_INDEX_DIRECTIONS


def is_ordered_index_spec(keys: IndexKeySpec) -> bool:
    return all(is_ordered_index_direction(direction) for _field, direction in keys)


def special_index_directions(keys: IndexKeySpec) -> tuple[SpecialIndexDirection, ...]:
    return tuple(
        direction
        for _field, direction in keys
        if is_special_index_direction(direction)
    )


def normalize_index_keys(keys: object) -> IndexKeySpec:
    if isinstance(keys, str):
        if not keys:
            raise ValueError("index field names must be non-empty strings")
        return [(keys, 1)]

    if isinstance(keys, dict):
        if not keys:
            raise ValueError("keys must not be empty")
        normalized: IndexKeySpec = []
        for field, direction in keys.items():
            if not isinstance(field, str) or not field:
                raise TypeError("index field names must be non-empty strings")
            normalized.append((field, normalize_index_direction(direction)))
        return normalized

    if not isinstance(keys, Sequence) or isinstance(keys, (bytes, bytearray, dict)):
        raise TypeError("keys must be a string or a sequence of strings or (field, direction) tuples")

    normalized_items = list(keys)
    if not normalized_items:
        raise ValueError("keys must not be empty")

    normalized: IndexKeySpec = []
    for item in normalized_items:
        if isinstance(item, str):
            if not item:
                raise ValueError("index field names must be non-empty strings")
            normalized.append((item, 1))
            continue
        if (
            not isinstance(item, Sequence)
            or isinstance(item, (str, bytes, bytearray, dict))
            or len(item) != 2
        ):
            raise TypeError("keys must be a list of strings or (field, direction) tuples")
        field, direction = item
        if not isinstance(field, str) or not field:
            raise TypeError("index field names must be non-empty strings")
        normalized.append((field, normalize_index_direction(direction)))
    return normalized


def default_index_name(keys: IndexKeySpec) -> str:
    return "_".join(f"{field}_{direction}" for field, direction in keys)


def index_fields(keys: IndexKeySpec) -> list[str]:
    return [field for field, _direction in keys]


def index_key_document(keys: IndexKeySpec) -> dict[str, IndexDirection]:
    return {field: direction for field, direction in keys}


def _text_index_fields(keys: IndexKeySpec) -> tuple[str, ...]:
    return tuple(field for field, direction in keys if direction == "text")


def _is_wildcard_index_field(field: str) -> bool:
    return field == "$**" or field.endswith(".$**")


def _has_wildcard_index_path(keys: IndexKeySpec) -> bool:
    return any(_is_wildcard_index_field(field) for field, _direction in keys)


def _validate_text_index_weights(
    weights: object,
    *,
    text_fields: tuple[str, ...],
) -> TextIndexWeights:
    if not isinstance(weights, dict):
        raise TypeError("weights must be a dict or None")
    normalized: TextIndexWeights = {}
    for field, value in weights.items():
        if not isinstance(field, str) or not field:
            raise TypeError("weights field names must be non-empty strings")
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or value <= 0
        ):
            raise TypeError("weights values must be positive integers")
        normalized[field] = value
    unknown_fields = sorted(set(normalized) - set(text_fields))
    if unknown_fields:
        raise ValueError(
            "weights fields must belong to text index keys; unknown fields: "
            + ", ".join(unknown_fields)
        )
    return normalized


def _normalize_text_index_language(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise TypeError(f"{field_name} must be a non-empty string or None")
    return value


def _validate_wildcard_projection(value: object) -> Document:
    if not isinstance(value, dict):
        raise TypeError("wildcard_projection must be a dict or None")
    normalized: Document = {}
    for field, projection in value.items():
        if not isinstance(field, str) or not field:
            raise TypeError(
                "wildcard_projection field names must be non-empty strings"
            )
        if isinstance(projection, bool):
            normalized[field] = int(projection)
            continue
        if (
            isinstance(projection, int)
            and not isinstance(projection, bool)
            and projection in (0, 1)
        ):
            normalized[field] = projection
            continue
        raise TypeError(
            "wildcard_projection values must be booleans or 0/1 integers"
        )
    return normalized


@dataclass(frozen=True, slots=True)
class IndexDefinition:
    keys: IndexKeySpec
    name: str
    unique: bool = False
    sparse: bool = False
    hidden: bool = False
    collation: Document | None = None
    partial_filter_expression: Filter | None = None
    expire_after_seconds: int | None = None
    weights: TextIndexWeights | None = None
    wildcard_projection: Document | None = None
    default_language: str | None = None
    language_override: str | None = None

    def __init__(
        self,
        keys: object,
        *,
        name: str,
        unique: bool = False,
        sparse: bool = False,
        hidden: bool = False,
        collation: Document | None = None,
        partial_filter_expression: Filter | None = None,
        expire_after_seconds: int | None = None,
        weights: TextIndexWeights | None = None,
        wildcard_projection: Document | None = None,
        default_language: str | None = None,
        language_override: str | None = None,
    ):
        normalized = normalize_index_keys(keys)
        text_fields = _text_index_fields(normalized)
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        if not isinstance(sparse, bool):
            raise TypeError("sparse must be a bool")
        if not isinstance(hidden, bool):
            raise TypeError("hidden must be a bool")
        if collation is not None and not isinstance(collation, dict):
            raise TypeError("collation must be a dict or None")
        if partial_filter_expression is not None and not isinstance(partial_filter_expression, dict):
            raise TypeError("partial_filter_expression must be a dict or None")
        if expire_after_seconds is not None and (
            not isinstance(expire_after_seconds, int)
            or isinstance(expire_after_seconds, bool)
            or expire_after_seconds < 0
        ):
            raise TypeError("expire_after_seconds must be a non-negative int or None")
        normalized_weights: TextIndexWeights | None = None
        if weights is not None:
            if not text_fields:
                raise ValueError("weights are only supported for text indexes")
            normalized_weights = _validate_text_index_weights(
                weights,
                text_fields=text_fields,
            )
        normalized_wildcard_projection: Document | None = None
        if wildcard_projection is not None:
            if not _has_wildcard_index_path(normalized):
                raise ValueError(
                    "wildcard_projection is only supported for wildcard indexes"
                )
            normalized_wildcard_projection = _validate_wildcard_projection(
                wildcard_projection
            )
        normalized_default_language = _normalize_text_index_language(
            default_language,
            field_name="default_language",
        )
        normalized_language_override = _normalize_text_index_language(
            language_override,
            field_name="language_override",
        )
        if (
            normalized_default_language is not None
            or normalized_language_override is not None
        ) and not text_fields:
            raise ValueError(
                "default_language and language_override are only supported for text indexes"
            )
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)
        object.__setattr__(self, "sparse", sparse)
        object.__setattr__(self, "hidden", hidden)
        object.__setattr__(self, "collation", deepcopy(collation))
        object.__setattr__(self, "partial_filter_expression", deepcopy(partial_filter_expression))
        object.__setattr__(self, "expire_after_seconds", expire_after_seconds)
        object.__setattr__(self, "weights", deepcopy(normalized_weights))
        object.__setattr__(
            self,
            "wildcard_projection",
            deepcopy(normalized_wildcard_projection),
        )
        object.__setattr__(self, "default_language", normalized_default_language)
        object.__setattr__(self, "language_override", normalized_language_override)

    @property
    def fields(self) -> list[str]:
        return index_fields(self.keys)

    def to_list_document(self) -> IndexDocument:
        document: IndexDocument = {
            "name": self.name,
            "key": index_key_document(self.keys),
            "unique": self.unique,
        }
        if self.sparse:
            document["sparse"] = True
        if self.hidden:
            document["hidden"] = True
        if self.collation is not None:
            document["collation"] = deepcopy(self.collation)
        if self.partial_filter_expression is not None:
            document["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        if self.expire_after_seconds is not None:
            document["expireAfterSeconds"] = self.expire_after_seconds
        if self.weights is not None:
            document["weights"] = deepcopy(self.weights)
        if self.wildcard_projection is not None:
            document["wildcardProjection"] = deepcopy(self.wildcard_projection)
        if self.default_language is not None:
            document["default_language"] = self.default_language
        if self.language_override is not None:
            document["language_override"] = self.language_override
        return document

    def to_model_document(self) -> IndexDocument:
        document: IndexDocument = {
            "name": self.name,
            "key": index_key_document(self.keys),
        }
        if self.unique:
            document["unique"] = True
        if self.sparse:
            document["sparse"] = True
        if self.hidden:
            document["hidden"] = True
        if self.collation is not None:
            document["collation"] = deepcopy(self.collation)
        if self.partial_filter_expression is not None:
            document["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        if self.expire_after_seconds is not None:
            document["expireAfterSeconds"] = self.expire_after_seconds
        if self.weights is not None:
            document["weights"] = deepcopy(self.weights)
        if self.wildcard_projection is not None:
            document["wildcardProjection"] = deepcopy(self.wildcard_projection)
        if self.default_language is not None:
            document["default_language"] = self.default_language
        if self.language_override is not None:
            document["language_override"] = self.language_override
        return document

    def to_information_entry(self) -> IndexInformationEntry:
        entry: IndexInformationEntry = {"key": list(self.keys)}
        if self.unique:
            entry["unique"] = True
        if self.sparse:
            entry["sparse"] = True
        if self.hidden:
            entry["hidden"] = True
        if self.collation is not None:
            entry["collation"] = deepcopy(self.collation)
        if self.partial_filter_expression is not None:
            entry["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        if self.expire_after_seconds is not None:
            entry["expireAfterSeconds"] = self.expire_after_seconds
        if self.weights is not None:
            entry["weights"] = deepcopy(self.weights)
        if self.wildcard_projection is not None:
            entry["wildcardProjection"] = deepcopy(self.wildcard_projection)
        if self.default_language is not None:
            entry["default_language"] = self.default_language
        if self.language_override is not None:
            entry["language_override"] = self.language_override
        return entry

    def to_information_entry_map(self) -> IndexInformation:
        return {self.name: self.to_information_entry()}


def default_id_index_definition() -> IndexDefinition:
    return IndexDefinition([("_id", 1)], name="_id_", unique=True)


def default_id_index_information() -> IndexInformation:
    return default_id_index_definition().to_information_entry_map()


def default_id_index_document() -> IndexDocument:
    return default_id_index_definition().to_list_document()


@dataclass(frozen=True, slots=True)
class IndexModel:
    keys: IndexKeySpec
    name: str | None = None
    unique: bool = False
    sparse: bool = False
    background: bool = False
    hidden: bool = False
    collation: Document | None = None
    partial_filter_expression: Filter | None = None
    expire_after_seconds: int | None = None
    weights: TextIndexWeights | None = None
    wildcard_projection: Document | None = None
    default_language: str | None = None
    language_override: str | None = None

    def __init__(self, keys: object, **kwargs: Any):
        normalized = normalize_index_keys(keys)
        name = kwargs.pop("name", None)
        unique = kwargs.pop("unique", False)
        sparse = kwargs.pop("sparse", False)
        background = kwargs.pop("background", False)
        hidden = kwargs.pop("hidden", False)
        collation = kwargs.pop("collation", None)
        partial_filter_expression = kwargs.pop("partialFilterExpression", None)
        if partial_filter_expression is None:
            partial_filter_expression = kwargs.pop("partial_filter_expression", None)
        expire_after_seconds = kwargs.pop("expireAfterSeconds", None)
        if expire_after_seconds is None:
            expire_after_seconds = kwargs.pop("expire_after_seconds", None)
        weights = kwargs.pop("weights", None)
        wildcard_projection = kwargs.pop("wildcardProjection", None)
        if wildcard_projection is None:
            wildcard_projection = kwargs.pop("wildcard_projection", None)
        default_language = kwargs.pop("defaultLanguage", None)
        if default_language is None:
            default_language = kwargs.pop("default_language", None)
        language_override = kwargs.pop("languageOverride", None)
        if language_override is None:
            language_override = kwargs.pop("language_override", None)
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        if not isinstance(sparse, bool):
            raise TypeError("sparse must be a bool")
        if not isinstance(background, bool):
            raise TypeError("background must be a bool")
        if not isinstance(hidden, bool):
            raise TypeError("hidden must be a bool")
        if collation is not None and not isinstance(collation, dict):
            raise TypeError("collation must be a dict or None")
        if partial_filter_expression is not None and not isinstance(partial_filter_expression, dict):
            raise TypeError("partial_filter_expression must be a dict or None")
        if expire_after_seconds is not None and (
            not isinstance(expire_after_seconds, int)
            or isinstance(expire_after_seconds, bool)
            or expire_after_seconds < 0
        ):
            raise TypeError("expire_after_seconds must be a non-negative int or None")
        text_fields = _text_index_fields(normalized)
        normalized_weights: TextIndexWeights | None = None
        if weights is not None:
            if not text_fields:
                raise ValueError("weights are only supported for text indexes")
            normalized_weights = _validate_text_index_weights(
                weights,
                text_fields=text_fields,
            )
        normalized_wildcard_projection: Document | None = None
        if wildcard_projection is not None:
            if not _has_wildcard_index_path(normalized):
                raise ValueError(
                    "wildcard_projection is only supported for wildcard indexes"
                )
            normalized_wildcard_projection = _validate_wildcard_projection(
                wildcard_projection
            )
        normalized_default_language = _normalize_text_index_language(
            default_language,
            field_name="default_language",
        )
        normalized_language_override = _normalize_text_index_language(
            language_override,
            field_name="language_override",
        )
        if (
            normalized_default_language is not None
            or normalized_language_override is not None
        ) and not text_fields:
            raise ValueError(
                "default_language and language_override are only supported for text indexes"
            )
        if kwargs:
            unsupported = ", ".join(sorted(kwargs))
            raise TypeError(f"unsupported IndexModel options: {unsupported}")
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)
        object.__setattr__(self, "sparse", sparse)
        object.__setattr__(self, "background", background)
        object.__setattr__(self, "hidden", hidden)
        object.__setattr__(self, "collation", deepcopy(collation))
        object.__setattr__(self, "partial_filter_expression", deepcopy(partial_filter_expression))
        object.__setattr__(self, "expire_after_seconds", expire_after_seconds)
        object.__setattr__(self, "weights", deepcopy(normalized_weights))
        object.__setattr__(
            self,
            "wildcard_projection",
            deepcopy(normalized_wildcard_projection),
        )
        object.__setattr__(self, "default_language", normalized_default_language)
        object.__setattr__(self, "language_override", normalized_language_override)

    @property
    def resolved_name(self) -> str:
        return self.name or default_index_name(self.keys)

    @property
    def definition(self) -> IndexDefinition:
        return IndexDefinition(
            self.keys,
            name=self.resolved_name,
            unique=self.unique,
            sparse=self.sparse,
            hidden=self.hidden,
            collation=self.collation,
            partial_filter_expression=self.partial_filter_expression,
            expire_after_seconds=self.expire_after_seconds,
            weights=self.weights,
            wildcard_projection=self.wildcard_projection,
            default_language=self.default_language,
            language_override=self.language_override,
        )

    @property
    def document(self) -> IndexDocument:
        document = self.definition.to_model_document()
        if self.background:
            document["background"] = True
        return document


@dataclass(frozen=True, slots=True)
class SearchIndexDefinition:
    definition: Document
    name: str
    index_type: str = "search"

    def __init__(
        self,
        definition: Document,
        *,
        name: str,
        index_type: str = "search",
    ):
        if not isinstance(definition, dict):
            raise TypeError("definition must be a dict")
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(index_type, str) or not index_type:
            raise ValueError("index_type must be a non-empty string")
        object.__setattr__(self, "definition", deepcopy(definition))
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "index_type", index_type)

    def to_document(self) -> SearchIndexDocument:
        queryable = self.index_type == "search"
        query_mode = "text"
        experimental = False
        capabilities: list[str] = list(TEXT_SEARCH_INDEX_CAPABILITIES)
        if self.index_type == "vectorSearch":
            fields = self.definition.get("fields")
            queryable = isinstance(fields, list) and any(
                isinstance(field, dict)
                and field.get("type") == "vector"
                and isinstance(field.get("path"), str)
                and bool(field.get("path"))
                for field in fields
            )
            query_mode = "vector"
            experimental = True
            capabilities = ["vectorSearch"]
        status = "READY" if queryable else "UNSUPPORTED"
        return {
            "name": self.name,
            "type": self.index_type,
            "definition": deepcopy(self.definition),
            "latestDefinition": deepcopy(self.definition),
            "queryable": queryable,
            "status": status,
            "statusDetail": "ready" if queryable else "unsupported-definition",
            "queryMode": query_mode,
            "experimental": experimental,
            "capabilities": capabilities,
            "readyAtEpoch": None,
        }


@dataclass(frozen=True, slots=True)
class SearchIndexModel:
    definition: Document
    name: str | None = None
    index_type: str = "search"

    def __init__(self, definition: Document, **kwargs: Any):
        if not isinstance(definition, dict):
            raise TypeError("definition must be a dict")
        name = kwargs.pop("name", None)
        index_type = kwargs.pop("type", kwargs.pop("index_type", "search"))
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("name must be a non-empty string")
        if not isinstance(index_type, str) or not index_type:
            raise ValueError("index_type must be a non-empty string")
        if kwargs:
            unsupported = ", ".join(sorted(kwargs))
            raise TypeError(f"unsupported SearchIndexModel options: {unsupported}")
        object.__setattr__(self, "definition", deepcopy(definition))
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "index_type", index_type)

    @property
    def resolved_name(self) -> str:
        return self.name or "default"

    @property
    def definition_snapshot(self) -> SearchIndexDefinition:
        return SearchIndexDefinition(
            self.definition,
            name=self.resolved_name,
            index_type=self.index_type,
        )
