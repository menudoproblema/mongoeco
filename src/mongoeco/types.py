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
type ArrayFilters = list[Filter]
type Projection = dict[str, Any]
type SortDirection = Literal[1, -1]
type SortSpec = list[tuple[str, SortDirection]]
type IndexKeySpec = SortSpec
type IndexDocument = dict[str, object]
type IndexInformationEntry = dict[str, object]
type IndexInformation = dict[str, IndexInformationEntry]
type DocumentScalarId = ObjectId | str | bytes | int | float | bool | None | UndefinedType
type DocumentId = DocumentScalarId | list[DocumentId] | dict[str, DocumentId]


class ReturnDocument(Enum):
    BEFORE = 'before'
    AFTER = 'after'


def _require_non_bool_int(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an int")
    return value


@dataclass(frozen=True, slots=True)
class WriteConcern:
    w: int | str | None = None
    j: bool | None = None
    wtimeout: int | None = None

    def __init__(
        self,
        w: int | str | None = None,
        *,
        j: bool | None = None,
        wtimeout: int | None = None,
    ):
        if w is not None:
            if isinstance(w, bool):
                raise TypeError("w must not be a bool")
            if isinstance(w, int):
                if w < 0:
                    raise ValueError("w must be >= 0")
            elif isinstance(w, str):
                if not w:
                    raise ValueError("w must be a non-empty string")
            else:
                raise TypeError("w must be an int, string, or None")
        if j is not None and not isinstance(j, bool):
            raise TypeError("j must be a bool or None")
        if wtimeout is not None:
            wtimeout = _require_non_bool_int(wtimeout, "wtimeout")
            if wtimeout < 0:
                raise ValueError("wtimeout must be >= 0")
        object.__setattr__(self, "w", w)
        object.__setattr__(self, "j", j)
        object.__setattr__(self, "wtimeout", wtimeout)

    @property
    def document(self) -> dict[str, object]:
        document: dict[str, object] = {}
        if self.w is not None:
            document["w"] = self.w
        if self.j is not None:
            document["j"] = self.j
        if self.wtimeout is not None:
            document["wtimeout"] = self.wtimeout
        return document


@dataclass(frozen=True, slots=True)
class ReadConcern:
    level: str | None = None

    def __init__(self, level: str | None = None):
        if level is not None and (not isinstance(level, str) or not level):
            raise ValueError("level must be a non-empty string or None")
        object.__setattr__(self, "level", level)

    @property
    def document(self) -> dict[str, object]:
        if self.level is None:
            return {}
        return {"level": self.level}


class ReadPreferenceMode(Enum):
    PRIMARY = "primary"
    PRIMARY_PREFERRED = "primaryPreferred"
    SECONDARY = "secondary"
    SECONDARY_PREFERRED = "secondaryPreferred"
    NEAREST = "nearest"


@dataclass(frozen=True, slots=True)
class ReadPreference:
    mode: ReadPreferenceMode = ReadPreferenceMode.PRIMARY
    tag_sets: tuple[dict[str, str], ...] | None = None
    max_staleness_seconds: int | None = None

    def __init__(
        self,
        mode: ReadPreferenceMode | str = ReadPreferenceMode.PRIMARY,
        *,
        tag_sets: Sequence[dict[str, str]] | None = None,
        max_staleness_seconds: int | None = None,
    ):
        if isinstance(mode, str):
            try:
                mode = ReadPreferenceMode(mode)
            except ValueError as exc:
                raise ValueError(f"unsupported read preference mode: {mode}") from exc
        elif not isinstance(mode, ReadPreferenceMode):
            raise TypeError("mode must be a ReadPreferenceMode or string")

        normalized_tag_sets: tuple[dict[str, str], ...] | None = None
        if tag_sets is not None:
            if isinstance(tag_sets, (str, bytes, bytearray)):
                raise TypeError("tag_sets must be a sequence of dictionaries")
            normalized_items: list[dict[str, str]] = []
            for tag_set in tag_sets:
                if not isinstance(tag_set, dict):
                    raise TypeError("tag_sets must contain only dictionaries")
                normalized_tag_set: dict[str, str] = {}
                for key, value in tag_set.items():
                    if not isinstance(key, str) or not key:
                        raise TypeError("tag set keys must be non-empty strings")
                    if not isinstance(value, str):
                        raise TypeError("tag set values must be strings")
                    normalized_tag_set[key] = value
                normalized_items.append(normalized_tag_set)
            normalized_tag_sets = tuple(normalized_items)

        if max_staleness_seconds is not None:
            max_staleness_seconds = _require_non_bool_int(
                max_staleness_seconds,
                "max_staleness_seconds",
            )
            if max_staleness_seconds <= 0:
                raise ValueError("max_staleness_seconds must be > 0")

        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "tag_sets", normalized_tag_sets)
        object.__setattr__(self, "max_staleness_seconds", max_staleness_seconds)

    @property
    def name(self) -> str:
        return self.mode.value

    @property
    def document(self) -> dict[str, object]:
        document: dict[str, object] = {"mode": self.mode.value}
        if self.tag_sets is not None:
            document["tag_sets"] = [dict(tag_set) for tag_set in self.tag_sets]
        if self.max_staleness_seconds is not None:
            document["maxStalenessSeconds"] = self.max_staleness_seconds
        return document


@dataclass(frozen=True, slots=True)
class CodecOptions:
    document_class: type[dict] = dict
    tz_aware: bool = False

    def __init__(
        self,
        document_class: type[dict] = dict,
        *,
        tz_aware: bool = False,
    ):
        if not isinstance(document_class, type):
            raise TypeError("document_class must be a type")
        if not issubclass(document_class, dict):
            raise TypeError("document_class must be a dict subclass")
        if not isinstance(tz_aware, bool):
            raise TypeError("tz_aware must be a bool")
        object.__setattr__(self, "document_class", document_class)
        object.__setattr__(self, "tz_aware", tz_aware)


@dataclass(frozen=True, slots=True)
class TransactionOptions:
    read_concern: ReadConcern = ReadConcern()
    write_concern: WriteConcern = WriteConcern()
    read_preference: ReadPreference = ReadPreference()
    max_commit_time_ms: int | None = None

    def __init__(
        self,
        *,
        read_concern: ReadConcern | None = None,
        write_concern: WriteConcern | None = None,
        read_preference: ReadPreference | None = None,
        max_commit_time_ms: int | None = None,
    ):
        if read_concern is None:
            read_concern = ReadConcern()
        elif not isinstance(read_concern, ReadConcern):
            raise TypeError("read_concern must be a ReadConcern")
        if write_concern is None:
            write_concern = WriteConcern()
        elif not isinstance(write_concern, WriteConcern):
            raise TypeError("write_concern must be a WriteConcern")
        if read_preference is None:
            read_preference = ReadPreference()
        elif not isinstance(read_preference, ReadPreference):
            raise TypeError("read_preference must be a ReadPreference")
        if max_commit_time_ms is not None:
            max_commit_time_ms = _require_non_bool_int(
                max_commit_time_ms,
                "max_commit_time_ms",
            )
            if max_commit_time_ms <= 0:
                raise ValueError("max_commit_time_ms must be > 0")
        object.__setattr__(self, "read_concern", read_concern)
        object.__setattr__(self, "write_concern", write_concern)
        object.__setattr__(self, "read_preference", read_preference)
        object.__setattr__(self, "max_commit_time_ms", max_commit_time_ms)


def normalize_write_concern(value: WriteConcern | None) -> WriteConcern:
    if value is None:
        return WriteConcern()
    if not isinstance(value, WriteConcern):
        raise TypeError("write_concern must be a WriteConcern")
    return value


def normalize_read_concern(value: ReadConcern | None) -> ReadConcern:
    if value is None:
        return ReadConcern()
    if not isinstance(value, ReadConcern):
        raise TypeError("read_concern must be a ReadConcern")
    return value


def normalize_read_preference(value: ReadPreference | None) -> ReadPreference:
    if value is None:
        return ReadPreference()
    if not isinstance(value, ReadPreference):
        raise TypeError("read_preference must be a ReadPreference")
    return value


def normalize_codec_options(value: CodecOptions | None) -> CodecOptions:
    if value is None:
        return CodecOptions()
    if not isinstance(value, CodecOptions):
        raise TypeError("codec_options must be a CodecOptions")
    return value


def normalize_transaction_options(value: TransactionOptions | None) -> TransactionOptions:
    if value is None:
        return TransactionOptions()
    if not isinstance(value, TransactionOptions):
        raise TypeError("transaction_options must be a TransactionOptions")
    return value


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


def default_id_index_information() -> IndexInformation:
    return default_id_index_definition().to_information_entry_map()


def default_id_index_document() -> IndexDocument:
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

    def to_list_document(self) -> IndexDocument:
        return {
            "name": self.name,
            "key": index_key_document(self.keys),
            "fields": self.fields,
            "unique": self.unique,
        }

    def to_model_document(self) -> IndexDocument:
        document: IndexDocument = {
            "name": self.name,
            "key": index_key_document(self.keys),
        }
        if self.unique:
            document["unique"] = True
        return document

    def to_information_entry(self) -> IndexInformationEntry:
        entry: IndexInformationEntry = {"key": list(self.keys)}
        if self.unique:
            entry["unique"] = True
        return entry

    def to_information_entry_map(self) -> IndexInformation:
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
    def definition(self) -> IndexDefinition:
        return IndexDefinition(self.keys, name=self.resolved_name, unique=self.unique)

    @property
    def document(self) -> IndexDocument:
        return self.definition.to_model_document()


@dataclass(frozen=True, slots=True)
class InsertOne:
    document: Document


@dataclass(frozen=True, slots=True)
class UpdateOne:
    filter: Filter
    update: Update
    upsert: bool = False
    sort: SortSpec | None = None
    array_filters: ArrayFilters | None = None
    hint: str | SortSpec | None = None
    comment: Any | None = None
    let: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class UpdateMany:
    filter: Filter
    update: Update
    upsert: bool = False
    array_filters: ArrayFilters | None = None
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
