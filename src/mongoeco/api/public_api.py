from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping

from mongoeco.compat.base import PyMongoProfile
from mongoeco.compat.operation_support import OPERATION_OPTION_SUPPORT


ARG_UNSET = object()


def _catalog_options(operation: str) -> frozenset[str]:
    return frozenset(OPERATION_OPTION_SUPPORT.get(operation, {}))


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


COLLECTION_FIND_ONE_SPEC = PublicOperationSpec(
    name="find_one",
    allowed_options=frozenset(
        {
            "filter_spec",
            "projection",
            "collation",
            "sort",
            "skip",
            "hint",
            "comment",
            "max_time_ms",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
)

COLLECTION_FIND_SPEC = PublicOperationSpec(
    name="find",
    allowed_options=frozenset(
        {
            "filter_spec",
            "projection",
            "collation",
            "sort",
            "skip",
            "limit",
            "hint",
            "comment",
            "max_time_ms",
            "batch_size",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
)

COLLECTION_FIND_RAW_BATCHES_SPEC = PublicOperationSpec(
    name="find_raw_batches",
    allowed_options=COLLECTION_FIND_SPEC.allowed_options,
    aliases=COLLECTION_FIND_SPEC.aliases,
)

COLLECTION_UPDATE_ONE_SPEC = PublicOperationSpec(
    name="update_one",
    allowed_options=frozenset(
        {
            "filter_spec",
            "update_spec",
            "upsert",
            "collation",
            "sort",
            "array_filters",
            "hint",
            "comment",
            "let",
            "bypass_document_validation",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec", "update": "update_spec"}),
    required_arguments=frozenset({"filter_spec", "update_spec"}),
    sort_depends_on_profile=True,
)

COLLECTION_UPDATE_MANY_SPEC = PublicOperationSpec(
    name="update_many",
    allowed_options=frozenset(
        {
            "filter_spec",
            "update_spec",
            "upsert",
            "collation",
            "array_filters",
            "hint",
            "comment",
            "let",
            "bypass_document_validation",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec", "update": "update_spec"}),
    required_arguments=frozenset({"filter_spec", "update_spec"}),
)

COLLECTION_REPLACE_ONE_SPEC = PublicOperationSpec(
    name="replace_one",
    allowed_options=frozenset(
        {
            "filter_spec",
            "replacement",
            "upsert",
            "collation",
            "sort",
            "hint",
            "comment",
            "let",
            "bypass_document_validation",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
    required_arguments=frozenset({"filter_spec", "replacement"}),
    sort_depends_on_profile=True,
)

COLLECTION_FIND_ONE_AND_UPDATE_SPEC = PublicOperationSpec(
    name="find_one_and_update",
    allowed_options=frozenset(
        {
            "filter_spec",
            "update_spec",
            "projection",
            "collation",
            "sort",
            "upsert",
            "return_document",
            "array_filters",
            "hint",
            "comment",
            "max_time_ms",
            "let",
            "bypass_document_validation",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec", "update": "update_spec"}),
    required_arguments=frozenset({"filter_spec", "update_spec"}),
)

COLLECTION_FIND_ONE_AND_REPLACE_SPEC = PublicOperationSpec(
    name="find_one_and_replace",
    allowed_options=frozenset(
        {
            "filter_spec",
            "replacement",
            "projection",
            "collation",
            "sort",
            "upsert",
            "return_document",
            "hint",
            "comment",
            "max_time_ms",
            "let",
            "bypass_document_validation",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
    required_arguments=frozenset({"filter_spec", "replacement"}),
)

COLLECTION_FIND_ONE_AND_DELETE_SPEC = PublicOperationSpec(
    name="find_one_and_delete",
    allowed_options=frozenset(
        {
            "filter_spec",
            "projection",
            "collation",
            "sort",
            "hint",
            "comment",
            "max_time_ms",
            "let",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_DELETE_ONE_SPEC = PublicOperationSpec(
    name="delete_one",
    allowed_options=frozenset(
        {
            "filter_spec",
            "collation",
            "hint",
            "comment",
            "let",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_DELETE_MANY_SPEC = PublicOperationSpec(
    name="delete_many",
    allowed_options=frozenset(
        {
            "filter_spec",
            "collation",
            "hint",
            "comment",
            "let",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_COUNT_DOCUMENTS_SPEC = PublicOperationSpec(
    name="count_documents",
    allowed_options=frozenset(
        {
            "filter_spec",
            "collation",
            "hint",
            "comment",
            "max_time_ms",
            "skip",
            "limit",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
    required_arguments=frozenset({"filter_spec"}),
)

COLLECTION_DISTINCT_SPEC = PublicOperationSpec(
    name="distinct",
    allowed_options=frozenset(
        {
            "key",
            "filter_spec",
            "collation",
            "hint",
            "comment",
            "max_time_ms",
            "session",
        }
    ),
    aliases=MappingProxyType({"filter": "filter_spec"}),
    required_arguments=frozenset({"key"}),
)

DATABASE_LIST_COLLECTION_NAMES_SPEC = PublicOperationSpec(
    name="list_collection_names",
    allowed_options=frozenset({"filter_spec", "session"}),
    aliases=MappingProxyType({"filter": "filter_spec"}),
)

DATABASE_LIST_COLLECTIONS_SPEC = PublicOperationSpec(
    name="list_collections",
    allowed_options=frozenset({"filter_spec", "session"}),
    aliases=MappingProxyType({"filter": "filter_spec"}),
)
