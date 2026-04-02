from __future__ import annotations

import binascii
import os
import threading
import time
from typing import Any, Self

try:
    from bson.errors import InvalidId as _PyMongoInvalidId
    from bson.objectid import ObjectId as _PyMongoObjectId
except Exception:  # pragma: no cover - optional dependency
    _PyMongoInvalidId = None
    _PyMongoObjectId = None


if _PyMongoObjectId is not None:
    class ObjectId(_PyMongoObjectId):
        """Wrapper compatible con PyMongo ObjectId y con el contrato historico local."""

        __slots__ = ()

        def __init__(self, oid: str | bytes | Self | _PyMongoObjectId | None = None):
            if oid is not None and not isinstance(oid, (str, bytes, ObjectId, _PyMongoObjectId)):
                raise TypeError(f"ID invalido tipo {type(oid)}: {oid}")
            try:
                super().__init__(oid)
            except TypeError as exc:
                if isinstance(oid, bytes):
                    raise ValueError(f"bytes de ObjectId deben ser de 12, no {len(oid)}") from exc
                raise
            except _PyMongoInvalidId as exc:
                text = str(oid)
                if isinstance(oid, str) and len(oid) != 24:
                    raise ValueError(f"string de ObjectId debe ser de 24 hex, no {len(oid)}") from exc
                raise ValueError(f"'{text}' no es un hexadecimal valido") from exc

        @classmethod
        def is_valid(cls, oid: Any) -> bool:
            if isinstance(oid, bytes):
                return len(oid) == 12
            return bool(_PyMongoObjectId.is_valid(oid))

        @property
        def generation_time(self) -> int:
            return int(super().generation_time.timestamp())
else:
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


OBJECT_ID_TYPES = (
    (ObjectId,)
    if _PyMongoObjectId is None
    else (ObjectId, _PyMongoObjectId)
)


def is_object_id_like(value: Any) -> bool:
    return isinstance(value, OBJECT_ID_TYPES)


def normalize_object_id(value: Any) -> ObjectId | Any:
    if isinstance(value, ObjectId):
        return value
    if is_object_id_like(value):
        return ObjectId(value)
    return value
