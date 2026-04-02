from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class MongoDialectCatalogEntry:
    key: str
    server_version: str
    label: str
    aliases: tuple[str, ...]
    behavior_flags: MappingProxyType = MappingProxyType({})
    policy_spec: "MongoBehaviorPolicySpec" | None = None
    capabilities: frozenset[str] = frozenset()
    query_field_operators: frozenset[str] | None = None
    update_operators: frozenset[str] | None = None


@dataclass(frozen=True, slots=True)
class PyMongoProfileCatalogEntry:
    key: str
    driver_series: str
    label: str
    aliases: tuple[str, ...]
    behavior_flags: MappingProxyType = MappingProxyType({})
    capabilities: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class MongoBehaviorPolicySpec:
    null_query_matches_undefined: bool = True
    expression_truthiness: str = "mongo-default"
    projection_flag_mode: str = "bool-or-binary-int"
    update_path_sort_mode: str = "numeric-then-lex"
    equality_mode: str = "bson-structural"
    comparison_mode: str = "bson-total-order"


class OptionSupportStatus(Enum):
    EFFECTIVE = "effective"
    ACCEPTED_NOOP = "accepted-noop"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True, slots=True)
class OperationOptionSupport:
    status: OptionSupportStatus
    note: str | None = None


@dataclass(frozen=True, slots=True)
class DatabaseCommandSupport:
    family: str
    supports_wire: bool = True
    supports_explain: bool = False
    note: str | None = None
