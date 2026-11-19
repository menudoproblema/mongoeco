from __future__ import annotations

import binascii
import os
import threading
import time
from typing import Any, Protocol, Self, runtime_checkable

try:
    from bson.objectid import ObjectId as _PyMongoObjectId
except Exception:  # pragma: no cover - optional dependency
    _PyMongoObjectId = None


@runtime_checkable
class ObjectIdLike(Protocol):
    @property
    def binary(self) -> bytes: ...


_OBJECT_ID_BYTE_LENGTH = 12


def _object_id_bytes(oid: str | bytes | ObjectIdLike) -> bytes:
    """Decode and validate the common input before either implementation stores it."""
    if isinstance(oid, str):
        if len(oid) != _OBJECT_ID_BYTE_LENGTH * 2:
            message = f"string de ObjectId debe ser de 24 hex, no {len(oid)}"
            raise ValueError(message)
        try:
            binary = binascii.unhexlify(oid)
        except (binascii.Error, ValueError) as exc:
            message = f"'{oid}' no es un hexadecimal valido"
            raise ValueError(message) from exc
    elif isinstance(oid, bytes):
        binary = oid
    elif isinstance(oid, ObjectIdLike):
        binary = bytes(oid.binary)
    else:
        message = f"ID invalido tipo {type(oid)}: {oid}"
        raise TypeError(message)
    if len(binary) != _OBJECT_ID_BYTE_LENGTH:
        message = f"bytes de ObjectId deben ser de 12, no {len(binary)}"
        raise ValueError(message)
    return binary


def _is_valid_object_id(oid: Any) -> bool:
    # The constructor also accepts the structural protocol. is_valid keeps
    # the SDK and fallback's historical domain of strings, bytes and ids.
    if not isinstance(oid, (str, bytes, *OBJECT_ID_TYPES)):
        return False
    try:
        _object_id_bytes(oid)
    except (TypeError, ValueError):
        return False
    return True


if _PyMongoObjectId is not None:

    class ObjectId(_PyMongoObjectId):
        """Wrapper compatible con PyMongo ObjectId y con el contrato historico local."""

        __slots__ = ()

        def __init__(
            self,
            oid: str | bytes | Self | ObjectIdLike | None = None,
        ):
            super().__init__(None if oid is None else _object_id_bytes(oid))

        @classmethod
        def is_valid(cls, oid: Any) -> bool:
            return _is_valid_object_id(oid)

        @property
        def binary(self) -> bytes:
            return bytes(super().binary)

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

        def __init__(
            self,
            oid: str | bytes | Self | ObjectIdLike | None = None,
        ):
            self._oid = self._generate() if oid is None else _object_id_bytes(oid)

        @classmethod
        def _generate(cls) -> bytes:
            timestamp = int(time.time())
            with cls._inc_lock:
                cls._counter = (cls._counter + 1) & 0xFFFFFF
                counter = cls._counter

            return (
                timestamp.to_bytes(4, "big") + cls._random + counter.to_bytes(3, "big")
            )

        @classmethod
        def is_valid(cls, oid: Any) -> bool:
            return _is_valid_object_id(oid)

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
    (ObjectId,) if _PyMongoObjectId is None else (ObjectId, _PyMongoObjectId)
)


def is_object_id_like(value: Any) -> bool:
    return isinstance(value, OBJECT_ID_TYPES)


def normalize_object_id(value: Any) -> ObjectId | Any:
    if isinstance(value, ObjectId):
        return value
    if is_object_id_like(value):
        return ObjectId(value)
    return value
