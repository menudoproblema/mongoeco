from __future__ import annotations

import datetime
import decimal
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Self

_DECIMAL128_CONTEXT = decimal.Context(prec=34, Emin=-6143, Emax=6144)


class UndefinedType:
    """Representa el tipo BSON `undefined` legado de MongoDB."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNDEFINED"

    def __hash__(self) -> int:
        return hash(UndefinedType)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, UndefinedType)


UNDEFINED = UndefinedType()


class Binary(bytes):
    """Representa BinData BSON con subtipo explícito."""

    def __new__(cls, data: bytes | bytearray | memoryview, subtype: int = 0) -> Self:
        instance = super().__new__(cls, bytes(data))
        if not 0 <= subtype <= 255:
            raise ValueError("binary subtype must be between 0 and 255")
        instance.subtype = subtype
        return instance

    def __repr__(self) -> str:
        return f"Binary({bytes(self)!r}, subtype={self.subtype})"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Binary)
            and bytes(self) == bytes(other)
            and self.subtype == other.subtype
        )

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash((bytes(self), self.subtype))


@dataclass(frozen=True, slots=True)
class Regex:
    pattern: str
    flags: str = ""

    def __post_init__(self) -> None:
        invalid = sorted(set(self.flags) - {"i", "m", "s", "x"})
        if invalid:
            joined = "".join(invalid)
            raise ValueError(f"unsupported regex flags: {joined}")
        canonical = "".join(flag for flag in "imsx" if flag in self.flags)
        object.__setattr__(self, "flags", canonical)

    def compile(self) -> re.Pattern[str]:
        compiled_flags = 0
        if "i" in self.flags:
            compiled_flags |= re.IGNORECASE
        if "m" in self.flags:
            compiled_flags |= re.MULTILINE
        if "s" in self.flags:
            compiled_flags |= re.DOTALL
        if "x" in self.flags:
            compiled_flags |= re.VERBOSE
        return re.compile(self.pattern, compiled_flags)


@dataclass(frozen=True, slots=True, order=True)
class Timestamp:
    time: int
    inc: int

    def __post_init__(self) -> None:
        if self.time < 0 or self.inc < 0:
            raise ValueError("timestamp components must be non-negative")

    def as_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.time, tz=datetime.UTC).replace(tzinfo=None)


@dataclass(frozen=True, slots=True)
class Decimal128:
    value: decimal.Decimal

    def __init__(self, value: decimal.Decimal | int | float | str) -> None:
        decimal_value = value if isinstance(value, decimal.Decimal) else decimal.Decimal(str(value))
        object.__setattr__(self, "value", _DECIMAL128_CONTEXT.create_decimal(decimal_value))

    def to_decimal(self) -> decimal.Decimal:
        return self.value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Decimal128):
            return NotImplemented
        if self.value.is_nan() and other.value.is_nan():
            return True
        return self.value == other.value

    def __hash__(self) -> int:
        if self.value.is_nan():
            return hash(("Decimal128", "NaN"))
        return hash(self.value)

    def __str__(self) -> str:
        return format(self.value, "f")

    def __repr__(self) -> str:
        return f"Decimal128('{self.value}')"


class SON(OrderedDict[str, Any]):
    """Ordered mapping compatible con bson.son.SON."""


@dataclass(frozen=True, slots=True)
class DBRef:
    collection: str
    id: Any
    database: str | None = None
    extras: dict[str, Any] = field(default_factory=dict)

    def as_document(self) -> SON:
        document: SON = SON([("$ref", self.collection), ("$id", self.id)])
        if self.database is not None:
            document["$db"] = self.database
        for key, value in self.extras.items():
            document[key] = value
        return document
