import binascii
import os
import threading
import time
from dataclasses import dataclass
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
type DocumentScalarId = ObjectId | str | bytes | int | float | bool | None | UndefinedType
type DocumentId = DocumentScalarId | list[DocumentId] | dict[str, DocumentId]


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
