from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

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


@dataclass(frozen=True, slots=True)
class IndexDefinition:
    keys: IndexKeySpec
    name: str
    unique: bool = False
    sparse: bool = False
    hidden: bool = False
    partial_filter_expression: Filter | None = None
    expire_after_seconds: int | None = None

    def __init__(
        self,
        keys: object,
        *,
        name: str,
        unique: bool = False,
        sparse: bool = False,
        hidden: bool = False,
        partial_filter_expression: Filter | None = None,
        expire_after_seconds: int | None = None,
    ):
        normalized = normalize_index_keys(keys)
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        if not isinstance(sparse, bool):
            raise TypeError("sparse must be a bool")
        if not isinstance(hidden, bool):
            raise TypeError("hidden must be a bool")
        if partial_filter_expression is not None and not isinstance(partial_filter_expression, dict):
            raise TypeError("partial_filter_expression must be a dict or None")
        if expire_after_seconds is not None and (
            not isinstance(expire_after_seconds, int)
            or isinstance(expire_after_seconds, bool)
            or expire_after_seconds < 0
        ):
            raise TypeError("expire_after_seconds must be a non-negative int or None")
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)
        object.__setattr__(self, "sparse", sparse)
        object.__setattr__(self, "hidden", hidden)
        object.__setattr__(self, "partial_filter_expression", deepcopy(partial_filter_expression))
        object.__setattr__(self, "expire_after_seconds", expire_after_seconds)

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
        if self.partial_filter_expression is not None:
            document["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        if self.expire_after_seconds is not None:
            document["expireAfterSeconds"] = self.expire_after_seconds
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
        if self.partial_filter_expression is not None:
            document["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        if self.expire_after_seconds is not None:
            document["expireAfterSeconds"] = self.expire_after_seconds
        return document

    def to_information_entry(self) -> IndexInformationEntry:
        entry: IndexInformationEntry = {"key": list(self.keys)}
        if self.unique:
            entry["unique"] = True
        if self.sparse:
            entry["sparse"] = True
        if self.hidden:
            entry["hidden"] = True
        if self.partial_filter_expression is not None:
            entry["partialFilterExpression"] = deepcopy(self.partial_filter_expression)
        if self.expire_after_seconds is not None:
            entry["expireAfterSeconds"] = self.expire_after_seconds
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
    hidden: bool = False
    partial_filter_expression: Filter | None = None
    expire_after_seconds: int | None = None

    def __init__(self, keys: object, **kwargs: Any):
        normalized = normalize_index_keys(keys)
        name = kwargs.pop("name", None)
        unique = kwargs.pop("unique", False)
        sparse = kwargs.pop("sparse", False)
        hidden = kwargs.pop("hidden", False)
        partial_filter_expression = kwargs.pop("partialFilterExpression", None)
        if partial_filter_expression is None:
            partial_filter_expression = kwargs.pop("partial_filter_expression", None)
        expire_after_seconds = kwargs.pop("expireAfterSeconds", None)
        if expire_after_seconds is None:
            expire_after_seconds = kwargs.pop("expire_after_seconds", None)
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        if not isinstance(sparse, bool):
            raise TypeError("sparse must be a bool")
        if not isinstance(hidden, bool):
            raise TypeError("hidden must be a bool")
        if partial_filter_expression is not None and not isinstance(partial_filter_expression, dict):
            raise TypeError("partial_filter_expression must be a dict or None")
        if expire_after_seconds is not None and (
            not isinstance(expire_after_seconds, int)
            or isinstance(expire_after_seconds, bool)
            or expire_after_seconds < 0
        ):
            raise TypeError("expire_after_seconds must be a non-negative int or None")
        if kwargs:
            unsupported = ", ".join(sorted(kwargs))
            raise TypeError(f"unsupported IndexModel options: {unsupported}")
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)
        object.__setattr__(self, "sparse", sparse)
        object.__setattr__(self, "hidden", hidden)
        object.__setattr__(self, "partial_filter_expression", deepcopy(partial_filter_expression))
        object.__setattr__(self, "expire_after_seconds", expire_after_seconds)

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
            partial_filter_expression=self.partial_filter_expression,
            expire_after_seconds=self.expire_after_seconds,
        )

    @property
    def document(self) -> IndexDocument:
        return self.definition.to_model_document()


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
        capabilities: list[str] = ["text", "phrase"]
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
