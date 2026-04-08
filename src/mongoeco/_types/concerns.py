from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import datetime
from enum import Enum
from typing import Callable, Sequence


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


class UuidRepresentation(Enum):
    STANDARD = "standard"
    PYTHON_LEGACY = "pythonLegacy"
    JAVA_LEGACY = "javaLegacy"
    CSHARP_LEGACY = "csharpLegacy"
    UNSPECIFIED = "unspecified"


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
            if max_staleness_seconds < 90:
                raise ValueError("max_staleness_seconds must be >= 90")

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
    tzinfo: datetime.tzinfo | None = None
    uuid_representation: UuidRepresentation = UuidRepresentation.STANDARD
    type_registry: tuple[tuple[type[object], Callable[[object], object]], ...] = ()

    def __init__(
        self,
        document_class: type[dict] = dict,
        *,
        tz_aware: bool = False,
        tzinfo: datetime.tzinfo | None = None,
        uuid_representation: UuidRepresentation | str = UuidRepresentation.STANDARD,
        type_registry: Mapping[type[object], Callable[[object], object]] | None = None,
    ):
        if not isinstance(document_class, type):
            raise TypeError("document_class must be a type")
        if not issubclass(document_class, dict):
            raise TypeError("document_class must be a dict subclass")
        if not isinstance(tz_aware, bool):
            raise TypeError("tz_aware must be a bool")
        if tzinfo is not None and not isinstance(tzinfo, datetime.tzinfo):
            raise TypeError("tzinfo must be a tzinfo or None")
        if tzinfo is not None and not tz_aware:
            raise ValueError("tzinfo requires tz_aware=True")
        if isinstance(uuid_representation, str):
            try:
                uuid_representation = UuidRepresentation(uuid_representation)
            except ValueError as exc:
                supported = ", ".join(member.value for member in UuidRepresentation)
                raise ValueError(
                    "uuid_representation must be one of: "
                    + supported
                ) from exc
        elif not isinstance(uuid_representation, UuidRepresentation):
            raise TypeError("uuid_representation must be a UuidRepresentation or string")
        normalized_type_registry: list[tuple[type[object], Callable[[object], object]]] = []
        if type_registry is not None:
            if not isinstance(type_registry, Mapping):
                raise TypeError("type_registry must be a mapping of type -> decoder callable")
            for value_type, decoder in type_registry.items():
                if not isinstance(value_type, type):
                    raise TypeError("type_registry keys must be types")
                if not callable(decoder):
                    raise TypeError("type_registry values must be callables")
                normalized_type_registry.append((value_type, decoder))
        object.__setattr__(self, "document_class", document_class)
        object.__setattr__(self, "tz_aware", tz_aware)
        object.__setattr__(self, "tzinfo", tzinfo)
        object.__setattr__(self, "uuid_representation", uuid_representation)
        object.__setattr__(self, "type_registry", tuple(normalized_type_registry))


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
