"""Select the captured native average finalization before physical slicing.

This is numeric compatibility metadata, not a second execution planner. The
classic evaluator uses the double total; native slot-based groups finalize the
BSON sum first. Grammar support remains owned by the existing dialect catalog.
"""

from mongoeco.core.aggregation.extensions import (
    get_registered_aggregation_expression_operator,
    get_registered_aggregation_stage_registration,
)


# Native 8/9 expression lowering boundaries (see average-route-source-notes.md).
_CLASSIC_EXPRESSIONS = frozenset(
    {
        "$allElementsTrue",
        "$arrayElemAt",
        "$bitAnd",
        "$bitOr",
        "$bitXor",
        "$bitNot",
        "$first",
        "$last",
        "$filter",
        "$in",
        "$indexOfArray",
        "$log",
        "$map",
        "$meta",
        "$reduce",
        "$replaceAll",
        "$size",
        "$slice",
        "$strcasecmp",
        "$substr",
        "$substrBytes",
        "$substrCP",
        "$binarySize",
        "$strLenCP",
        "$type",
        "$zip",
        "$convert",
        "$avg",
        "$firstN",
        "$lastN",
        "$max",
        "$min",
        "$maxN",
        "$minN",
        "$median",
        "$percentile",
        "$stdDevPop",
        "$stdDevSamp",
        "$sum",
        "$mergeObjects",
        "$rand",
        "$toHashedIndexKey",
        "$getField",
        "$setField",
        "$unsetField",
        "$toBool",
        "$toDate",
        "$toDecimal",
        "$toInt",
        "$toDouble",
        "$toLong",
        "$toObjectId",
        "$toUUID",
        "$toString",
    }
)


def _expression_compatible(value, dialect):
    if isinstance(value, list):
        return all(_expression_compatible(item, dialect) for item in value)
    if not isinstance(value, dict):
        return True
    for name, operand in value.items():
        if name in {"$literal", "$const"}:
            continue
        if name.startswith("$") and (
            (
                name in _CLASSIC_EXPRESSIONS
                and not (dialect.server_version == "9.0" and name == "$in")
            )
            or not dialect.supports_aggregation_expression_operator(name)
            or get_registered_aggregation_expression_operator(name) is not None
        ):
            return False
        if not _expression_compatible(operand, dialect):
            return False
    return True


def group_average_uses_sum(spec, *, dialect):
    return (
        isinstance(spec, dict)
        and _expression_compatible(spec.get("_id"), dialect)
        and all(
            isinstance(value, dict) for name, value in spec.items() if name != "_id"
        )
        and all(
            _expression_compatible(operand, dialect)
            for name, accumulator in spec.items()
            if name != "_id"
            for operand in accumulator.values()
        )
    )


def group_average_prefix_compatible(operator, spec, *, dialect):
    if get_registered_aggregation_stage_registration(operator) is not None:
        return False
    if operator in {"$sort", "$skip", "$limit"}:
        return True
    if operator == "$match":
        return _match_compatible(spec, dialect)
    if dialect.server_version == "9.0" and operator in {
        "$project",
        "$set",
        "$addFields",
        "$unset",
        "$replaceRoot",
        "$replaceWith",
    }:
        return _expression_compatible(spec, dialect)
    if operator == "$project":
        return (
            any(
                name != "_id" and value not in (False, 0)
                for name, value in spec.items()
            )
            and not any(
                name != "_id" and value in (False, 0) for name, value in spec.items()
            )
            and _expression_compatible(spec, dialect)
        )
    return (
        operator == "$lookup"
        and "pipeline" not in spec
        and "let" not in spec
        and all(
            not any(part.isdigit() for part in spec.get(field, "").split("."))
            for field in ("localField", "foreignField")
        )
    )


def _match_compatible(spec, dialect):
    for name, value in spec.items():
        if name in {"$where", "$text", "$jsonSchema"}:
            return False
        if name == "$expr" and not _expression_compatible(value, dialect):
            return False
        if name in {"$and", "$or", "$nor"} and not all(
            _match_compatible(item, dialect) for item in value
        ):
            return False
    return True
