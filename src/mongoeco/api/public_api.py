from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from mongoeco.compat.base import PyMongoProfile
from mongoeco.compat.operation_support import OPERATION_OPTION_SUPPORT


ARG_UNSET = object()


def _catalog_options(operation: str) -> frozenset[str]:
    return frozenset(OPERATION_OPTION_SUPPORT.get(operation, {}))


def _options(*parts: str | frozenset[str]) -> frozenset[str]:
    flattened: set[str] = set()
    for part in parts:
        if isinstance(part, str):
            flattened.add(part)
        else:
            flattened.update(part)
    return frozenset(flattened)


@dataclass(frozen=True, slots=True)
class PublicOperationSpec:
    name: str
    allowed_options: frozenset[str]
    aliases: Mapping[str, str] = field(default_factory=dict)
    required_arguments: frozenset[str] = frozenset()
    sort_depends_on_profile: bool = False


def unexpected_keyword_error(operation: str, option: str) -> TypeError:
    return TypeError(f"{operation}() got an unexpected keyword argument '{option}'")


def normalize_public_operation_arguments(
    spec: PublicOperationSpec,
    *,
    explicit: Mapping[str, object],
    extra_kwargs: Mapping[str, object],
    profile: PyMongoProfile | None = None,
) -> dict[str, object]:
    normalized: dict[str, object] = {}
    aliases = dict(spec.aliases)
    kwargs = {
        name: value
        for name, value in extra_kwargs.items()
        if value is not ARG_UNSET
    }

    for alias, canonical in aliases.items():
        if alias not in kwargs:
            continue
        if canonical in kwargs or explicit.get(canonical, ARG_UNSET) is not ARG_UNSET:
            raise TypeError(f"cannot pass both {alias} and {canonical}")
        kwargs[canonical] = kwargs.pop(alias)

    for option in kwargs:
        if option not in spec.allowed_options:
            raise unexpected_keyword_error(spec.name, option)

    for name, value in explicit.items():
        if value is not ARG_UNSET:
            normalized[name] = value

    normalized.update(kwargs)

    for required in spec.required_arguments:
        if normalized.get(required, ARG_UNSET) is ARG_UNSET:
            raise TypeError(f"{spec.name}() missing required argument: '{required}'")

    if (
        profile is not None
        and spec.sort_depends_on_profile
        and normalized.get("sort") is not None
        and not profile.supports_update_one_sort()
    ):
        raise TypeError(
            f"sort is not supported by PyMongo profile {profile.key} "
            f"for {spec.name}()"
        )

    return normalized


_FILTER_ALIAS = MappingProxyType({"filter": "filter_spec"})
_FILTER_UPDATE_ALIAS = MappingProxyType({"filter": "filter_spec", "update": "update_spec"})

_SESSION_OPTION = frozenset({"session"})
_READ_COMMAND_OPTIONS = _options("collation", "hint", "comment", "max_time_ms")
_READ_SELECTION_OPTIONS = _options(_READ_COMMAND_OPTIONS, "sort", "skip")
_WRITE_COMMAND_OPTIONS = _options("collation", "hint", "comment", "let", "session")
_SINGLE_WRITE_SELECTION_OPTIONS = _options(_WRITE_COMMAND_OPTIONS, "sort")
_FIND_PROJECTION_OPTIONS = _options(_READ_SELECTION_OPTIONS, "projection", "filter_spec")
_FIND_CURSOR_OPTIONS = _options(_FIND_PROJECTION_OPTIONS, "limit", "batch_size")
_FILTER_ONLY_OPTIONS = _options("filter_spec", _SESSION_OPTION)
_DELETE_OPTIONS = _options("filter_spec", _WRITE_COMMAND_OPTIONS)
_COUNT_OPTIONS = _options("filter_spec", _READ_COMMAND_OPTIONS, "skip", "limit", "session")


def _assert_catalog_alignment(operation: str, spec: PublicOperationSpec) -> None:
    missing = _catalog_options(operation) - spec.allowed_options
    if missing:
        raise AssertionError(
            f"public API spec {spec.name} is missing catalog options for {operation}: {sorted(missing)}"
        )


COLLECTION_FIND_ONE_SPEC = PublicOperationSpec(
    name="find_one",
    allowed_options=_options(_FIND_PROJECTION_OPTIONS, "session"),
    aliases=_FILTER_ALIAS,
)

COLLECTION_FIND_SPEC = PublicOperationSpec(
    name="find",
    allowed_options=_options(_FIND_CURSOR_OPTIONS, "session"),
    aliases=_FILTER_ALIAS,
)

COLLECTION_FIND_RAW_BATCHES_SPEC = PublicOperationSpec(
    name="find_raw_batches",
    allowed_options=COLLECTION_FIND_SPEC.allowed_options,
    aliases=COLLECTION_FIND_SPEC.aliases,
)

COLLECTION_UPDATE_ONE_SPEC = PublicOperationSpec(
    name="update_one",
    allowed_options=_options(
        "filter_spec",
        "update_spec",
        "upsert",
        "array_filters",
        "bypass_document_validation",
        _SINGLE_WRITE_SELECTION_OPTIONS,
    ),
    aliases=_FILTER_UPDATE_ALIAS,
    required_arguments=frozenset({"filter_spec", "update_spec"}),
    sort_depends_on_profile=True,
)

COLLECTION_UPDATE_MANY_SPEC = PublicOperationSpec(
    name="update_many",
    allowed_options=_options(
        "filter_spec",
        "update_spec",
        "upsert",
        "array_filters",
        "bypass_document_validation",
        _WRITE_COMMAND_OPTIONS,
    ),
    aliases=_FILTER_UPDATE_ALIAS,
    required_arguments=frozenset({"filter_spec", "update_spec"}),
)

COLLECTION_REPLACE_ONE_SPEC = PublicOperationSpec(
    name="replace_one",
    allowed_options=_options(
        "filter_spec",
        "replacement",
        "upsert",
        "bypass_document_validation",
        _SINGLE_WRITE_SELECTION_OPTIONS,
    ),
    aliases=_FILTER_ALIAS,
    required_arguments=frozenset({"filter_spec", "replacement"}),
    sort_depends_on_profile=True,
)

COLLECTION_FIND_ONE_AND_UPDATE_SPEC = PublicOperationSpec(
    name="find_one_and_update",
    allowed_options=_options(
        "filter_spec",
        "update_spec",
        "projection",
        "upsert",
        "return_document",
        "array_filters",
        "bypass_document_validation",
        _SINGLE_WRITE_SELECTION_OPTIONS,
        "max_time_ms",
    ),
    aliases=_FILTER_UPDATE_ALIAS,
    required_arguments=frozenset({"filter_spec", "update_spec"}),
)

COLLECTION_FIND_ONE_AND_REPLACE_SPEC = PublicOperationSpec(
    name="find_one_and_replace",
    allowed_options=_options(
        "filter_spec",
        "replacement",
        "projection",
        "upsert",
        "return_document",
        "bypass_document_validation",
        _SINGLE_WRITE_SELECTION_OPTIONS,
        "max_time_ms",
    ),
    aliases=_FILTER_ALIAS,
    required_arguments=frozenset({"filter_spec", "replacement"}),
)

COLLECTION_FIND_ONE_AND_DELETE_SPEC = PublicOperationSpec(
    name="find_one_and_delete",
    allowed_options=_options(
        "filter_spec",
        "projection",
        _SINGLE_WRITE_SELECTION_OPTIONS,
        "max_time_ms",
    ),
    aliases=_FILTER_ALIAS,
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_DELETE_ONE_SPEC = PublicOperationSpec(
    name="delete_one",
    allowed_options=_DELETE_OPTIONS,
    aliases=_FILTER_ALIAS,
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_DELETE_MANY_SPEC = PublicOperationSpec(
    name="delete_many",
    allowed_options=_DELETE_OPTIONS,
    aliases=_FILTER_ALIAS,
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_COUNT_DOCUMENTS_SPEC = PublicOperationSpec(
    name="count_documents",
    allowed_options=_COUNT_OPTIONS,
    aliases=_FILTER_ALIAS,
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_DISTINCT_SPEC = PublicOperationSpec(
    name="distinct",
    allowed_options=_options("key", "filter_spec", _READ_COMMAND_OPTIONS, "session"),
    aliases=_FILTER_ALIAS,
    required_arguments=frozenset({"key"}),
)

DATABASE_LIST_COLLECTION_NAMES_SPEC = PublicOperationSpec(
    name="list_collection_names",
    allowed_options=_FILTER_ONLY_OPTIONS,
    aliases=_FILTER_ALIAS,
)

DATABASE_LIST_COLLECTIONS_SPEC = PublicOperationSpec(
    name="list_collections",
    allowed_options=_FILTER_ONLY_OPTIONS,
    aliases=_FILTER_ALIAS,
)

for _operation, _spec in (
    ("find", COLLECTION_FIND_SPEC),
    ("update_one", COLLECTION_UPDATE_ONE_SPEC),
    ("update_many", COLLECTION_UPDATE_MANY_SPEC),
    ("replace_one", COLLECTION_REPLACE_ONE_SPEC),
    ("find_one_and_update", COLLECTION_FIND_ONE_AND_UPDATE_SPEC),
    ("find_one_and_replace", COLLECTION_FIND_ONE_AND_REPLACE_SPEC),
    ("find_one_and_delete", COLLECTION_FIND_ONE_AND_DELETE_SPEC),
    ("delete_one", COLLECTION_DELETE_ONE_SPEC),
    ("delete_many", COLLECTION_DELETE_MANY_SPEC),
    ("count_documents", COLLECTION_COUNT_DOCUMENTS_SPEC),
    ("distinct", COLLECTION_DISTINCT_SPEC),
):
    _assert_catalog_alignment(_operation, _spec)
