import binascii
import os
import threading
import time
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, Self


class UndefinedType:
    """Representa el tipo BSON `undefined` legado de MongoDB."""

    __slots__ = ()

    def __repr__(self) -> str:
        return 'UNDEFINED'

    def __hash__(self) -> int:
        return hash(UndefinedType)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, UndefinedType)


UNDEFINED = UndefinedType()


class ObjectId:
    """
    Implementacion nativa minima de ObjectId (BSON).
    12 bytes: 4 de timestamp + 5 aleatorios + 3 de contador.
    """

    _inc_lock = threading.Lock()
    _counter = int.from_bytes(os.urandom(3), "big")
    _random = os.urandom(5)

    __slots__ = ("_oid",)

    def __init__(self, oid: str | bytes | Self | None = None):
        if oid is None:
            self._oid = self._generate()
        elif isinstance(oid, ObjectId):
            self._oid = oid._oid
        elif isinstance(oid, bytes):
            if len(oid) != 12:
                raise ValueError(f"bytes de ObjectId deben ser de 12, no {len(oid)}")
            self._oid = oid
        elif isinstance(oid, str):
            if len(oid) != 24:
                raise ValueError(f"string de ObjectId debe ser de 24 hex, no {len(oid)}")
            try:
                self._oid = binascii.unhexlify(oid)
            except binascii.Error as exc:
                raise ValueError(f"'{oid}' no es un hexadecimal valido") from exc
        else:
            raise TypeError(f"ID invalido tipo {type(oid)}: {oid}")

    @classmethod
    def _generate(cls) -> bytes:
        timestamp = int(time.time())
        with cls._inc_lock:
            cls._counter = (cls._counter + 1) & 0xFFFFFF
            counter = cls._counter

        return (
            timestamp.to_bytes(4, "big")
            + cls._random
            + counter.to_bytes(3, "big")
        )

    @classmethod
    def is_valid(cls, oid: Any) -> bool:
        if isinstance(oid, ObjectId):
            return True
        if isinstance(oid, bytes):
            return len(oid) == 12
        if isinstance(oid, str) and len(oid) == 24:
            return all(ch in "0123456789abcdefABCDEF" for ch in oid)
        return False

    @property
    def binary(self) -> bytes:
        return self._oid

    @property
    def generation_time(self) -> int:
        return int.from_bytes(self._oid[:4], "big")

    def __str__(self) -> str:
        return binascii.hexlify(self._oid).decode("ascii")

    def __repr__(self) -> str:
        return f"ObjectId('{self}')"

    def __hash__(self) -> int:
        return hash(self._oid)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ObjectId):
            return False
        return self._oid == other._oid

    def __lt__(self, other: Self) -> bool:
        if not isinstance(other, ObjectId):
            return NotImplemented
        return self._oid < other._oid


type Document = dict[str, Any]
type Filter = dict[str, Any]
type Update = dict[str, Any]
type Projection = dict[str, Any]
type SortDirection = Literal[1, -1]
type SortSpec = list[tuple[str, SortDirection]]
type IndexKeySpec = SortSpec
type DocumentScalarId = ObjectId | str | bytes | int | float | bool | None | UndefinedType
type DocumentId = DocumentScalarId | list[DocumentId] | dict[str, DocumentId]


class ReturnDocument(Enum):
    BEFORE = 'before'
    AFTER = 'after'


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
            if direction not in (1, -1) or isinstance(direction, bool):
                raise ValueError("index directions must be 1 or -1")
            normalized.append((field, direction))
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
        if direction not in (1, -1) or isinstance(direction, bool):
            raise ValueError("index directions must be 1 or -1")
        normalized.append((field, direction))
    return normalized


def default_index_name(keys: IndexKeySpec) -> str:
    return "_".join(f"{field}_{direction}" for field, direction in keys)


def index_fields(keys: IndexKeySpec) -> list[str]:
    return [field for field, _direction in keys]


def index_key_document(keys: IndexKeySpec) -> dict[str, SortDirection]:
    return {field: direction for field, direction in keys}


def default_id_index_information() -> dict[str, dict[str, object]]:
    return default_id_index_definition().to_information_entry_map()


def default_id_index_document() -> dict[str, object]:
    return default_id_index_definition().to_list_document()


@dataclass(frozen=True, slots=True)
class IndexDefinition:
    keys: IndexKeySpec
    name: str
    unique: bool = False

    def __init__(self, keys: object, *, name: str, unique: bool = False):
        normalized = normalize_index_keys(keys)
        if not isinstance(name, str) or not name:
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)

    @property
    def fields(self) -> list[str]:
        return index_fields(self.keys)

    def to_list_document(self) -> dict[str, object]:
        return {
            "name": self.name,
            "key": index_key_document(self.keys),
            "fields": self.fields,
            "unique": self.unique,
        }

    def to_information_entry(self) -> dict[str, object]:
        entry: dict[str, object] = {"key": list(self.keys)}
        if self.unique:
            entry["unique"] = True
        return entry

    def to_information_entry_map(self) -> dict[str, dict[str, object]]:
        return {self.name: self.to_information_entry()}


def default_id_index_definition() -> IndexDefinition:
    return IndexDefinition([("_id", 1)], name="_id_", unique=True)


@dataclass(frozen=True, slots=True)
class IndexModel:
    keys: IndexKeySpec
    name: str | None = None
    unique: bool = False

    def __init__(self, keys: object, **kwargs: Any):
        normalized = normalize_index_keys(keys)
        name = kwargs.pop("name", None)
        unique = kwargs.pop("unique", False)
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("name must be a non-empty string")
        if not isinstance(unique, bool):
            raise TypeError("unique must be a bool")
        if kwargs:
            unsupported = ", ".join(sorted(kwargs))
            raise TypeError(f"unsupported IndexModel options: {unsupported}")
        object.__setattr__(self, "keys", normalized)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "unique", unique)

    @property
    def resolved_name(self) -> str:
        return self.name or default_index_name(self.keys)

    @property
    def document(self) -> dict[str, Any]:
        document: dict[str, Any] = {
            "name": self.resolved_name,
            "key": index_key_document(self.keys),
        }
        if self.unique:
            document["unique"] = True
        return document


@dataclass(frozen=True, slots=True)
class InsertOne:
    document: Document


@dataclass(frozen=True, slots=True)
class UpdateOne:
    filter: Filter
    update: Update
    upsert: bool = False
    sort: SortSpec | None = None
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class UpdateMany:
    filter: Filter
    update: Update
    upsert: bool = False
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ReplaceOne:
    filter: Filter
    replacement: Document
    upsert: bool = False
    sort: SortSpec | None = None
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DeleteOne:
    filter: Filter
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DeleteMany:
    filter: Filter
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


type WriteModel = InsertOne | UpdateOne | UpdateMany | ReplaceOne | DeleteOne | DeleteMany


@dataclass(frozen=True, slots=True)
class InsertOneResult[T]:
    inserted_id: T
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class InsertManyResult[T]:
    inserted_ids: list[T]
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class UpdateResult[T]:
    matched_count: int
    modified_count: int
    upserted_id: T | None = None
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class DeleteResult:
    deleted_count: int
    acknowledged: bool = True


@dataclass(frozen=True, slots=True)
class BulkWriteResult[T]:
    inserted_count: int
    matched_count: int
    modified_count: int
    deleted_count: int
    upserted_count: int
    upserted_ids: dict[int, T]
    acknowledged: bool = True
