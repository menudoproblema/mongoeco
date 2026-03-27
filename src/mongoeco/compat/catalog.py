from __future__ import annotations

import datetime
import decimal
import json
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any
import uuid

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.types import ObjectId, UndefinedType


@dataclass(frozen=True, slots=True)
class MongoDialectCatalogEntry:
    key: str
    server_version: str
    label: str
    aliases: tuple[str, ...]
    behavior_flags: MappingProxyType = MappingProxyType({})
    policy_spec: "MongoBehaviorPolicySpec" | None = None
    capabilities: frozenset[str] = frozenset()


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


MONGODB_DIALECT_HOOK_NAMES = (
    "null_query_matches_undefined",
)

PYMONGO_PROFILE_HOOK_NAMES = (
    "supports_update_one_sort",
)

DEFAULT_MONGODB_DIALECT = "7.0"
DEFAULT_PYMONGO_PROFILE = "4.9"
AUTO_INSTALLED_PYMONGO_PROFILE = "auto-installed"
STRICT_AUTO_INSTALLED_PYMONGO_PROFILE = "strict-auto-installed"

SUPPORTED_QUERY_FIELD_OPERATORS = frozenset(
    {
        "$eq",
        "$cmp",
        "$ne",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$in",
        "$nin",
        "$all",
        "$size",
        "$mod",
        "$regex",
        "$options",
        "$not",
        "$elemMatch",
        "$exists",
        "$type",
        "$bitsAllSet",
        "$bitsAnySet",
        "$bitsAllClear",
        "$bitsAnyClear",
    }
)

SUPPORTED_QUERY_TOP_LEVEL_OPERATORS = frozenset({"$and", "$or", "$nor", "$expr"})

SUPPORTED_UPDATE_OPERATORS = frozenset(
    {
        "$set",
        "$unset",
        "$inc",
        "$min",
        "$max",
        "$mul",
        "$bit",
        "$rename",
        "$currentDate",
        "$setOnInsert",
        "$push",
        "$addToSet",
        "$pull",
        "$pullAll",
        "$pop",
    }
)

SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS = frozenset(
    {
        "$literal",
        "$convert",
        "$eq",
        "$cmp",
        "$ne",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$and",
        "$or",
        "$in",
        "$ifNull",
        "$cond",
        "$switch",
        "$abs",
        "$add",
        "$multiply",
        "$subtract",
        "$divide",
        "$mod",
        "$exp",
        "$ln",
        "$log",
        "$log10",
        "$pow",
        "$round",
        "$sqrt",
        "$stdDevPop",
        "$stdDevSamp",
        "$median",
        "$percentile",
        "$floor",
        "$ceil",
        "$trunc",
        "$range",
        "$slice",
        "$firstN",
        "$lastN",
        "$maxN",
        "$minN",
        "$size",
        "$arrayElemAt",
        "$allElementsTrue",
        "$anyElementTrue",
        "$objectToArray",
        "$zip",
        "$isArray",
        "$bitAnd",
        "$bitNot",
        "$bitOr",
        "$bitXor",
        "$bsonSize",
        "$concat",
        "$ltrim",
        "$replaceOne",
        "$replaceAll",
        "$reverseArray",
        "$rtrim",
        "$setDifference",
        "$setEquals",
        "$setIntersection",
        "$setIsSubset",
        "$strcasecmp",
        "$substr",
        "$substrBytes",
        "$substrCP",
        "$strLenBytes",
        "$strLenCP",
        "$trim",
        "$split",
        "$toBool",
        "$toDate",
        "$toDecimal",
        "$toInt",
        "$toDouble",
        "$toLong",
        "$toObjectId",
        "$toUUID",
        "$toLower",
        "$toUpper",
        "$toString",
        "$let",
        "$first",
        "$concatArrays",
        "$setUnion",
        "$map",
        "$filter",
        "$reduce",
        "$arrayToObject",
        "$indexOfArray",
        "$indexOfBytes",
        "$indexOfCP",
        "$regexMatch",
        "$regexFind",
        "$regexFindAll",
        "$sortArray",
        "$dateTrunc",
        "$dateAdd",
        "$dateSubtract",
        "$dateDiff",
        "$dateFromString",
        "$dateFromParts",
        "$dateToParts",
        "$dateToString",
        "$year",
        "$month",
        "$dayOfMonth",
        "$dayOfWeek",
        "$dayOfYear",
        "$hour",
        "$minute",
        "$second",
        "$millisecond",
        "$isoDayOfWeek",
        "$rand",
        "$setField",
        "$unsetField",
        "$week",
        "$isoWeek",
        "$isoWeekYear",
        "$mergeObjects",
        "$getField",
        "$isNumber",
        "$type",
        "$binarySize",
    }
)

SUPPORTED_AGGREGATION_STAGES = frozenset(
    {
        "$match",
        "$project",
        "$unset",
        "$sample",
        "$sort",
        "$skip",
        "$limit",
        "$addFields",
        "$set",
        "$unwind",
        "$group",
        "$bucket",
        "$bucketAuto",
        "$lookup",
        "$unionWith",
        "$replaceRoot",
        "$replaceWith",
        "$facet",
        "$count",
        "$sortByCount",
        "$setWindowFields",
        "$documents",
    }
)

SUPPORTED_GROUP_ACCUMULATORS = frozenset(
    {
        "$sum",
        "$count",
        "$min",
        "$max",
        "$first",
        "$last",
        "$firstN",
        "$lastN",
        "$maxN",
        "$minN",
        "$top",
        "$bottom",
        "$topN",
        "$bottomN",
        "$avg",
        "$push",
        "$addToSet",
        "$mergeObjects",
        "$stdDevPop",
        "$stdDevSamp",
        "$median",
        "$percentile",
    }
)

SUPPORTED_WINDOW_ACCUMULATORS = frozenset(
    {
        "$sum",
        "$count",
        "$min",
        "$max",
        "$avg",
        "$push",
        "$first",
        "$last",
        "$firstN",
        "$lastN",
        "$maxN",
        "$minN",
        "$top",
        "$bottom",
        "$topN",
        "$bottomN",
        "$addToSet",
        "$stdDevPop",
        "$stdDevSamp",
        "$median",
        "$percentile",
        "$rank",
        "$denseRank",
        "$documentNumber",
    }
)

DEFAULT_BSON_TYPE_ORDER = MappingProxyType(
    {
        type(None): 1,
        UndefinedType: 1,
        int: 2,
        float: 2,
        decimal.Decimal: 2,
        BsonInt32: 2,
        BsonInt64: 2,
        BsonDouble: 2,
        BsonDecimal128: 2,
        str: 3,
        dict: 4,
        list: 5,
        bytes: 6,
        uuid.UUID: 6,
        ObjectId: 7,
        bool: 8,
        datetime.datetime: 9,
    }
)

MONGODB_DIALECT_CATALOG = MappingProxyType(
    {
        "7.0": MongoDialectCatalogEntry(
            key="7.0",
            server_version="7.0",
            label="MongoDB 7.0",
            aliases=("7", "7.0"),
            behavior_flags=MappingProxyType(
                {
                    "null_query_matches_undefined": True,
                }
            ),
            policy_spec=MongoBehaviorPolicySpec(
                null_query_matches_undefined=True,
            ),
        ),
        "8.0": MongoDialectCatalogEntry(
            key="8.0",
            server_version="8.0",
            label="MongoDB 8.0",
            aliases=("8", "8.0"),
            behavior_flags=MappingProxyType(
                {
                    "null_query_matches_undefined": False,
                }
            ),
            policy_spec=MongoBehaviorPolicySpec(
                null_query_matches_undefined=False,
            ),
        ),
    }
)

PYMONGO_PROFILE_CATALOG = MappingProxyType(
    {
        "4.9": PyMongoProfileCatalogEntry(
            key="4.9",
            driver_series="4.x",
            label="PyMongo 4.9",
            aliases=("4", "4.9"),
            behavior_flags=MappingProxyType(
                {
                    "supports_update_one_sort": False,
                }
            ),
        ),
        "4.11": PyMongoProfileCatalogEntry(
            key="4.11",
            driver_series="4.x",
            label="PyMongo 4.11",
            aliases=("4.11",),
            behavior_flags=MappingProxyType(
                {
                    "supports_update_one_sort": True,
                }
            ),
            capabilities=frozenset({"update_one.sort"}),
        ),
        "4.13": PyMongoProfileCatalogEntry(
            key="4.13",
            driver_series="4.x",
            label="PyMongo 4.13",
            aliases=("4.13",),
            behavior_flags=MappingProxyType(
                {
                    "supports_update_one_sort": True,
                }
            ),
            capabilities=frozenset({"update_one.sort"}),
        ),
    }
)

MONGODB_DIALECT_ALIASES = MappingProxyType(
    {
        alias: entry.key
        for entry in MONGODB_DIALECT_CATALOG.values()
        for alias in entry.aliases
    }
)

PYMONGO_PROFILE_ALIASES = MappingProxyType(
    {
        alias: entry.key
        for entry in PYMONGO_PROFILE_CATALOG.values()
        for alias in entry.aliases
    }
)

SUPPORTED_MONGODB_MAJORS = frozenset(
    int(key.split(".", 1)[0])
    for key in MONGODB_DIALECT_CATALOG
)

SUPPORTED_PYMONGO_MAJORS = frozenset(
    int(key.split(".", 1)[0])
    for key in PYMONGO_PROFILE_CATALOG
)

_EFFECTIVE = OptionSupportStatus.EFFECTIVE
_ACCEPTED_NOOP = OptionSupportStatus.ACCEPTED_NOOP

OPERATION_OPTION_SUPPORT_CATALOG = MappingProxyType(
    {
        "find": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Validated against existing indexes and applied to read planning/explain where engines can honor it."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata and surfaced by explain()."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced as a local deadline during read execution and explain()."),
                "batch_size": OperationOptionSupport(_EFFECTIVE, "Async and sync find cursors now fetch local batches before yielding results, even though engines remain in-process."),
            }
        ),
        "count_documents": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the underlying find() path used to count matching documents."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced through the underlying find() path used to count documents."),
            }
        ),
        "distinct": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the underlying find() path used to enumerate distinct values."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced through the underlying find() path used to enumerate distinct values."),
            }
        ),
        "estimated_document_count": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying full-collection read path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced through the underlying full-collection read path."),
            }
        ),
        "aggregate": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through the pushdown find() path used by aggregate() and surfaced in explain()."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata and propagated through aggregate explain/materialization."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Applied to referenced collection loads, pushdown reads and final pipeline materialization."),
                "batch_size": OperationOptionSupport(_EFFECTIVE, "Positive batch sizes trigger chunked execution for streamable aggregate pipelines; global stages still fall back to full materialization."),
                "let": OperationOptionSupport(_EFFECTIVE, "Propagated into aggregate expression evaluation and subpipelines."),
            }
        ),
        "update_one": MappingProxyType(
            {
                "array_filters": OperationOptionSupport(_EFFECTIVE, "Applied during update execution for supported filtered positional paths."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before single-document update execution."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented with profile-aware validation since PyMongo 4.11."),
            }
        ),
        "update_many": MappingProxyType(
            {
                "array_filters": OperationOptionSupport(_EFFECTIVE, "Applied during per-document update execution for supported filtered positional paths."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted _id preselection before per-document updates."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "replace_one": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before replacement."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented with profile-aware validation since PyMongo 4.11."),
            }
        ),
        "delete_one": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before delete."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "delete_many": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted _id preselection before per-document deletes."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "find_one_and_update": MappingProxyType(
            {
                "array_filters": OperationOptionSupport(_EFFECTIVE, "Propagated to the underlying update_one() execution for supported filtered positional paths."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection and post-update fetch."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and enforced there."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented through update_one()/find semantics with profile-aware validation."),
            }
        ),
        "find_one_and_replace": MappingProxyType(
            {
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection and post-replacement fetch."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and enforced there."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented through replace_one()/find semantics with profile-aware validation."),
            }
        ),
        "find_one_and_delete": MappingProxyType(
            {
                "sort": OperationOptionSupport(_EFFECTIVE, "Implemented through find() selection semantics before delete."),
                "hint": OperationOptionSupport(_EFFECTIVE, "Applied through hinted document selection before delete."),
                "comment": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and session metadata."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Propagated through the underlying read selection path and enforced there."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables are available through $expr in write filters and selection paths."),
            }
        ),
        "bulk_write": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for the batch write operation."),
                "let": OperationOptionSupport(_EFFECTIVE, "Command-level let variables flow into per-operation write filters through $expr when requests do not override them."),
            }
        ),
        "list_indexes": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
            }
        ),
        "create_index": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced as a local deadline during index build and multikey backfill."),
            }
        ),
        "create_indexes": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
                "max_time_ms": OperationOptionSupport(_EFFECTIVE, "Enforced as a local deadline across the whole index batch."),
            }
        ),
        "drop_index": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
            }
        ),
        "drop_indexes": MappingProxyType(
            {
                "comment": OperationOptionSupport(_EFFECTIVE, "Recorded in engine session metadata for index administration."),
            }
        ),
    }
)


def export_mongodb_dialect_catalog() -> dict[str, dict[str, object]]:
    return {
        key: {
            "server_version": entry.server_version,
            "label": entry.label,
            "aliases": list(entry.aliases),
            "behavior_flags": dict(entry.behavior_flags),
            "policy_spec": (
                {
                    "null_query_matches_undefined": entry.policy_spec.null_query_matches_undefined,
                    "expression_truthiness": entry.policy_spec.expression_truthiness,
                    "projection_flag_mode": entry.policy_spec.projection_flag_mode,
                    "update_path_sort_mode": entry.policy_spec.update_path_sort_mode,
                    "equality_mode": entry.policy_spec.equality_mode,
                    "comparison_mode": entry.policy_spec.comparison_mode,
                }
                if entry.policy_spec is not None
                else None
            ),
            "capabilities": sorted(entry.capabilities),
            "query_field_operators": sorted(SUPPORTED_QUERY_FIELD_OPERATORS),
            "query_top_level_operators": sorted(SUPPORTED_QUERY_TOP_LEVEL_OPERATORS),
            "update_operators": sorted(SUPPORTED_UPDATE_OPERATORS),
            "aggregation_expression_operators": sorted(SUPPORTED_AGGREGATION_EXPRESSION_OPERATORS),
            "aggregation_stages": sorted(SUPPORTED_AGGREGATION_STAGES),
            "group_accumulators": sorted(SUPPORTED_GROUP_ACCUMULATORS),
            "window_accumulators": sorted(SUPPORTED_WINDOW_ACCUMULATORS),
        }
        for key, entry in MONGODB_DIALECT_CATALOG.items()
    }


def export_pymongo_profile_catalog() -> dict[str, dict[str, object]]:
    return {
        key: {
            "driver_series": entry.driver_series,
            "label": entry.label,
            "aliases": list(entry.aliases),
            "behavior_flags": dict(entry.behavior_flags),
            "capabilities": sorted(entry.capabilities),
        }
        for key, entry in PYMONGO_PROFILE_CATALOG.items()
    }


def export_operation_option_catalog() -> dict[str, dict[str, dict[str, str | None]]]:
    return {
        operation: {
            option: {
                "status": support.status.value,
                "note": support.note,
            }
            for option, support in options.items()
        }
        for operation, options in OPERATION_OPTION_SUPPORT_CATALOG.items()
    }


def export_full_compat_catalog() -> dict[str, object]:
    return {
        "defaults": {
            "mongodb_dialect": DEFAULT_MONGODB_DIALECT,
            "pymongo_profile": DEFAULT_PYMONGO_PROFILE,
            "pymongo_auto_profile": AUTO_INSTALLED_PYMONGO_PROFILE,
            "pymongo_strict_auto_profile": STRICT_AUTO_INSTALLED_PYMONGO_PROFILE,
        },
        "hooks": {
            "mongodb_dialect": list(MONGODB_DIALECT_HOOK_NAMES),
            "pymongo_profile": list(PYMONGO_PROFILE_HOOK_NAMES),
        },
        "supported_majors": {
            "mongodb": sorted(SUPPORTED_MONGODB_MAJORS),
            "pymongo": sorted(SUPPORTED_PYMONGO_MAJORS),
        },
        "mongodb_dialects": export_mongodb_dialect_catalog(),
        "pymongo_profiles": export_pymongo_profile_catalog(),
        "operation_options": export_operation_option_catalog(),
    }


def export_full_compat_catalog_markdown() -> str:
    catalog = export_full_compat_catalog()
    lines: list[str] = ["# Compat Catalog", ""]

    defaults = catalog["defaults"]
    assert isinstance(defaults, dict)
    lines.append("## Defaults")
    for key, value in defaults.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")

    hooks = catalog["hooks"]
    assert isinstance(hooks, dict)
    lines.append("## Hooks")
    for key, values in hooks.items():
        rendered = ", ".join(f"`{value}`" for value in values) or "_none_"
        lines.append(f"- `{key}`: {rendered}")
    lines.append("")

    supported_majors = catalog["supported_majors"]
    assert isinstance(supported_majors, dict)
    lines.append("## Supported Majors")
    for key, values in supported_majors.items():
        rendered = ", ".join(str(value) for value in values)
        lines.append(f"- `{key}`: {rendered}")
    lines.append("")

    def _render_section(title: str, entries: dict[str, object]) -> None:
        lines.append(f"## {title}")
        for key, value in entries.items():
            assert isinstance(value, dict)
            lines.append(f"### `{key}`")
            for field_name, field_value in value.items():
                if isinstance(field_value, list):
                    rendered = ", ".join(f"`{item}`" for item in field_value) or "_empty_"
                else:
                    rendered = f"`{field_value}`"
                lines.append(f"- `{field_name}`: {rendered}")
            lines.append("")

    mongodb_dialects = catalog["mongodb_dialects"]
    assert isinstance(mongodb_dialects, dict)
    _render_section("MongoDB Dialects", mongodb_dialects)

    pymongo_profiles = catalog["pymongo_profiles"]
    assert isinstance(pymongo_profiles, dict)
    _render_section("PyMongo Profiles", pymongo_profiles)

    operation_options = catalog["operation_options"]
    assert isinstance(operation_options, dict)
    lines.append("## Operation Options")
    for operation, options in operation_options.items():
        assert isinstance(options, dict)
        lines.append(f"### `{operation}`")
        for option, support in options.items():
            assert isinstance(support, dict)
            rendered = ", ".join(
                f"`{field}`={json.dumps(value)}"
                for field, value in support.items()
            )
            lines.append(f"- `{option}`: {rendered}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
