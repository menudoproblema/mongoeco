from __future__ import annotations

from collections.abc import Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


# BSON cstrings cannot contain NUL, so persisted documents cannot collide with
# this execution-only namespace.
RUNTIME_METADATA_FIELD = "\x00mongoeco_search_metadata"
VIRTUAL_FIELDS_KEY = "virtualFields"


class RuntimeMetadataKey(StrEnum):
    TEXT_SCORE = "textScore"
    VECTOR_SEARCH_SCORE = "vectorSearchScore"
    SEARCH_HIGHLIGHTS = "searchHighlights"


class RuntimeMaterializationPolicy(StrEnum):
    VIRTUAL = "virtual"
    EXPLICIT = "explicit"


@dataclass(frozen=True, slots=True)
class RuntimeMetadataEntry:
    key: RuntimeMetadataKey
    value: object

    def __post_init__(self) -> None:
        if not isinstance(self.key, RuntimeMetadataKey):
            message = "runtime metadata key must be RuntimeMetadataKey"
            raise TypeError(message)
        object.__setattr__(self, "value", deepcopy(self.value))


@dataclass(frozen=True, slots=True)
class RuntimeVirtualField:
    path: str
    value: object
    source: RuntimeMetadataKey
    policy: RuntimeMaterializationPolicy = RuntimeMaterializationPolicy.VIRTUAL

    def __post_init__(self) -> None:
        if not isinstance(self.path, str) or not self.path or self.path.startswith("$"):
            message = "runtime virtual field path must be a field path"
            raise ValueError(message)
        if not isinstance(self.source, RuntimeMetadataKey):
            message = "runtime virtual field source must be RuntimeMetadataKey"
            raise TypeError(message)
        if not isinstance(self.policy, RuntimeMaterializationPolicy):
            message = "runtime materialization policy is invalid"
            raise TypeError(message)
        object.__setattr__(self, "value", deepcopy(self.value))


@dataclass(frozen=True, slots=True)
class RuntimeMetadata:
    entries: tuple[RuntimeMetadataEntry, ...] = ()
    virtual_fields: tuple[RuntimeVirtualField, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.entries, tuple) or not all(
            isinstance(entry, RuntimeMetadataEntry) for entry in self.entries
        ):
            message = "runtime metadata entries must be a tuple"
            raise TypeError(message)
        if not isinstance(self.virtual_fields, tuple) or not all(
            isinstance(item, RuntimeVirtualField) for item in self.virtual_fields
        ):
            message = "runtime virtual fields must be a tuple"
            raise TypeError(message)
        keys = [entry.key for entry in self.entries]
        if len(keys) != len(set(keys)):
            message = "runtime metadata keys must be unique"
            raise ValueError(message)
        paths = [item.path for item in self.virtual_fields]
        if len(paths) != len(set(paths)):
            message = "runtime virtual field paths must be unique"
            raise ValueError(message)

    def value(self, key: RuntimeMetadataKey) -> tuple[bool, object]:
        for entry in self.entries:
            if entry.key is key:
                return True, deepcopy(entry.value)
        return False, None

    def with_value(self, key: RuntimeMetadataKey, value: object) -> RuntimeMetadata:
        entries = tuple(entry for entry in self.entries if entry.key is not key)
        return RuntimeMetadata(
            entries=(*entries, RuntimeMetadataEntry(key, value)),
            virtual_fields=self.virtual_fields,
        )

    def with_virtual_field(
        self,
        path: str,
        value: object,
        *,
        source: RuntimeMetadataKey,
        policy: RuntimeMaterializationPolicy = RuntimeMaterializationPolicy.VIRTUAL,
    ) -> RuntimeMetadata:
        fields = tuple(item for item in self.virtual_fields if item.path != path)
        return RuntimeMetadata(
            entries=self.entries,
            virtual_fields=(
                *fields,
                RuntimeVirtualField(path, value, source, policy),
            ),
        )

    def without_virtual_path(self, path: str) -> RuntimeMetadata:
        return RuntimeMetadata(
            entries=self.entries,
            virtual_fields=tuple(
                item
                for item in self.virtual_fields
                if not (
                    item.path == path
                    or item.path.startswith(path + ".")
                    or path.startswith(item.path + ".")
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class RuntimeDocumentState(Mapping[str, Any]):
    """Aggregation-owned document and execution metadata with separate lifetimes."""

    document: dict[str, Any]
    metadata: RuntimeMetadata = field(default_factory=RuntimeMetadata)

    def __post_init__(self) -> None:
        if not isinstance(self.document, dict):
            message = "runtime document state requires a document"
            raise TypeError(message)
        if not isinstance(self.metadata, RuntimeMetadata):
            message = "runtime document state requires RuntimeMetadata"
            raise TypeError(message)
        object.__setattr__(self, "document", deepcopy(self.document))

    def __deepcopy__(self, memo: dict[int, object]) -> RuntimeDocumentState:
        copied = RuntimeDocumentState(self.document, self.metadata)
        memo[id(self)] = copied
        return copied

    def __getitem__(self, key: str) -> Any:
        return self.document[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.document)

    def __len__(self) -> int:
        return len(self.document)

    def with_document(
        self,
        document: dict[str, Any],
        *,
        metadata: RuntimeMetadata | None = None,
    ) -> RuntimeDocumentState:
        return RuntimeDocumentState(
            document,
            self.metadata if metadata is None else metadata,
        )

    def with_metadata_value(
        self,
        key: RuntimeMetadataKey,
        value: object,
    ) -> RuntimeDocumentState:
        return self.with_document(
            self.document,
            metadata=self.metadata.with_value(key, value),
        )

    def with_virtual_field(
        self,
        path: str,
        value: object,
        *,
        source: RuntimeMetadataKey,
    ) -> RuntimeDocumentState:
        return self.with_document(
            self.document,
            metadata=self.metadata.with_virtual_field(
                path,
                value,
                source=source,
            ),
        )

    def metadata_value(self, key: RuntimeMetadataKey) -> tuple[bool, object]:
        return self.metadata.value(key)

    def resolve(self, path: str) -> tuple[bool, object]:
        legacy_metadata_paths = {
            f"{RUNTIME_METADATA_FIELD}.textScore": RuntimeMetadataKey.TEXT_SCORE,
            f"{RUNTIME_METADATA_FIELD}.vectorSearchScore": (
                RuntimeMetadataKey.VECTOR_SEARCH_SCORE
            ),
            f"{RUNTIME_METADATA_FIELD}.highlights": (
                RuntimeMetadataKey.SEARCH_HIGHLIGHTS
            ),
        }
        key = legacy_metadata_paths.get(path)
        if key is not None:
            return self.metadata_value(key)
        return _get_plain_path(self.public_document(), path)

    def public_document(self) -> dict[str, Any]:
        result = deepcopy(self.document)
        for virtual in sorted(
            self.metadata.virtual_fields,
            key=lambda item: (item.path.count("."), item.path),
        ):
            found, _value = _get_plain_path(result, virtual.path)
            if not found:
                _set_plain_path(result, virtual.path, deepcopy(virtual.value))
        return result

    def persistence_document(self) -> dict[str, Any]:
        return deepcopy(self.document)

    def without_virtual_path(self, path: str) -> RuntimeDocumentState:
        return self.with_document(
            self.document,
            metadata=self.metadata.without_virtual_path(path),
        )

    def materialize_virtual(
        self,
        source_path: str,
        *,
        destination_path: str | None = None,
    ) -> RuntimeDocumentState:
        found, value = self.resolve(source_path)
        if not found:
            return self
        materialized = deepcopy(self.document)
        destination = destination_path or source_path
        _set_plain_path(materialized, destination, deepcopy(value))
        metadata = self.metadata.without_virtual_path(destination)
        return RuntimeDocumentState(materialized, metadata)


def ensure_runtime_state(
    value: dict[str, Any] | RuntimeDocumentState,
) -> RuntimeDocumentState:
    if isinstance(value, RuntimeDocumentState):
        return deepcopy(value)
    return RuntimeDocumentState(value)


def prepare_public_document(
    value: dict[str, Any] | RuntimeDocumentState,
) -> dict[str, Any]:
    return (
        value.public_document()
        if isinstance(value, RuntimeDocumentState)
        else deepcopy(value)
    )


def prepare_persistence_document(
    value: dict[str, Any] | RuntimeDocumentState,
) -> dict[str, Any]:
    return (
        value.persistence_document()
        if isinstance(value, RuntimeDocumentState)
        else deepcopy(value)
    )


def runtime_state_from_legacy_document(
    document: dict[str, Any],
) -> RuntimeDocumentState:
    """Translate the private 4.5 sidecar at the compatibility boundary."""
    owned = deepcopy(document)
    raw_metadata = owned.pop(RUNTIME_METADATA_FIELD, None)
    metadata = RuntimeMetadata()
    if not isinstance(raw_metadata, dict):
        return RuntimeDocumentState(owned)

    key_mapping = {
        "textScore": RuntimeMetadataKey.TEXT_SCORE,
        "vectorSearchScore": RuntimeMetadataKey.VECTOR_SEARCH_SCORE,
        "highlights": RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
    }
    for legacy_name, key in key_mapping.items():
        if legacy_name in raw_metadata:
            metadata = metadata.with_value(key, raw_metadata[legacy_name])

    virtual_fields = raw_metadata.get(VIRTUAL_FIELDS_KEY)
    if isinstance(virtual_fields, dict):
        for path, value in virtual_fields.items():
            if not isinstance(path, str) or not path:
                continue
            metadata = metadata.with_virtual_field(
                path,
                value,
                source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
            )
    return RuntimeDocumentState(owned, metadata)


def legacy_document_from_runtime_state(state: RuntimeDocumentState) -> dict[str, Any]:
    """Serialize only for deprecated SPI v1 consumers during the 4.x window."""
    result = deepcopy(state.document)
    raw_metadata: dict[str, object] = {}
    legacy_names = {
        RuntimeMetadataKey.TEXT_SCORE: "textScore",
        RuntimeMetadataKey.VECTOR_SEARCH_SCORE: "vectorSearchScore",
        RuntimeMetadataKey.SEARCH_HIGHLIGHTS: "highlights",
    }
    for entry in state.metadata.entries:
        raw_metadata[legacy_names[entry.key]] = deepcopy(entry.value)
    if state.metadata.virtual_fields:
        raw_metadata[VIRTUAL_FIELDS_KEY] = {
            item.path: deepcopy(item.value) for item in state.metadata.virtual_fields
        }
    if raw_metadata:
        result[RUNTIME_METADATA_FIELD] = raw_metadata
    return result


def _get_plain_path(value: object, path: str) -> tuple[bool, object]:
    if not path:
        return True, value
    head, separator, tail = path.partition(".")
    if isinstance(value, dict):
        if head not in value:
            return False, None
        nested = value[head]
    elif isinstance(value, list) and head.isdigit():
        index = int(head)
        if index >= len(value):
            return False, None
        nested = value[index]
    else:
        return False, None
    return _get_plain_path(nested, tail) if separator else (True, nested)


def _set_plain_path(
    document: dict[str, Any] | list[Any],
    path: str,
    value: object,
) -> None:
    head, separator, tail = path.partition(".")
    if isinstance(document, list):
        if not head.isdigit():
            return
        index = int(head)
        if index >= len(document):
            document.extend([None] * (index - len(document) + 1))
        if not separator:
            document[index] = value
            return
        child = document[index]
        if not isinstance(child, (dict, list)):
            child = [] if tail.partition(".")[0].isdigit() else {}
            document[index] = child
        _set_plain_path(child, tail, value)
        return
    if not separator:
        document[head] = value
        return
    child = document.get(head)
    if not isinstance(child, (dict, list)):
        child = [] if tail.partition(".")[0].isdigit() else {}
        document[head] = child
    _set_plain_path(child, tail, value)
