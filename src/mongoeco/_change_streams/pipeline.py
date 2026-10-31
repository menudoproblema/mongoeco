from __future__ import annotations

from dataclasses import replace

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.extensions import (
    get_registered_aggregation_expression_operator,
)
from mongoeco.core.aggregation.planning import _projection_flag
from mongoeco.core.aggregation.preparation import PreparedPipeline, prepare_pipeline
from mongoeco.core.bson_values import matches_bson_type
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.query_plan import (
    AndCondition,
    ExprCondition,
    MatchAll,
    NotCondition,
    OrCondition,
    compile_filter,
)
from mongoeco.core.server_errors import server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import (
    ChangeEventDocument,  # noqa: TC001 - introspectable helper signature
)


ALLOWED_CHANGE_STREAM_STAGES = frozenset(
    {
        "$match",
        "$project",
        "$addFields",
        "$set",
        "$unset",
        "$replaceRoot",
        "$replaceWith",
    }
)
ALLOWED_FULL_DOCUMENT_MODES = frozenset(
    {"default", "updateLookup", "whenAvailable", "required"}
)


def compile_change_stream_pipeline(
    pipeline: object | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collection: str | None = None,
) -> PreparedPipeline | None:
    if pipeline is None:
        return None
    if not isinstance(pipeline, list):
        raise TypeError("pipeline must be a list of stages")
    for stage in pipeline:
        if not isinstance(stage, dict):
            message = "pipeline stages must be documents"
            raise TypeError(message)
    prepared = prepare_pipeline(
        DocumentCodec.to_internal(pipeline),
        dialect=dialect,
        collection=collection,
        scope="watch",
    )
    for stage in prepared:
        operator = next(iter(stage))
        if operator not in ALLOWED_CHANGE_STREAM_STAGES:
            message = f"{operator} is not permitted in a change stream pipeline"
            raise server_error(message, code=20)
    return prepared


def normalize_full_document_mode(mode: str) -> str:
    if not isinstance(mode, str):
        raise TypeError("full_document must be a string")
    if mode not in ALLOWED_FULL_DOCUMENT_MODES:
        raise OperationFailure(
            "full_document must be one of default, updateLookup, whenAvailable, or required"
        )
    return mode


def apply_full_document_mode(
    document: ChangeEventDocument,
    mode: str,
    *,
    post_image_available: bool = False,
    lookup_document: dict | None = None,
) -> ChangeEventDocument:
    if document.get("operationType") != "update":
        return document
    normalized = dict(document)
    if mode == "default":
        normalized.pop("fullDocument", None)
    elif mode == "updateLookup":
        normalized["fullDocument"] = lookup_document
    else:
        image = document.get("fullDocument") if post_image_available else None
        if mode == "required" and image is None:
            message = "change stream fullDocument is required but was not available"
            raise server_error(message, code=47)
        normalized["fullDocument"] = image
    return normalized


_UNMOVABLE = object()


def pre_image_match(pipeline, dialect):
    """A necessary metadata guard; the original pipeline remains authoritative.

    Reuse query nodes/matcher and projection flags. Follow only proven field
    aliases through set/project/unset; full root replacement is a barrier.
    """
    guards = []
    preceding = []
    for stage in pipeline or ():
        if "$match" in stage:
            plan = compile_filter(stage["$match"], dialect=dialect)
            guard, _complete = _image_guard(plan, preceding, dialect)
            if guard is not None:
                guards.append(guard)
        preceding.append(stage)
    return AndCondition(tuple(guards)) if guards else None


def _image_guard(plan, preceding, dialect):
    if isinstance(plan, (AndCondition, OrCondition)):
        parts = [_image_guard(child, preceding, dialect) for child in plan.clauses]
        guards = tuple(guard for guard, _complete in parts if guard is not None)
        complete = len(guards) == len(parts) and all(done for _guard, done in parts)
        if not guards or (isinstance(plan, OrCondition) and len(guards) != len(parts)):
            return None, False
        return type(plan)(guards), complete
    if isinstance(plan, NotCondition):
        guard, complete = _image_guard(plan.clause, preceding, dialect)
        return (NotCondition(guard), True) if complete else (None, False)
    if isinstance(plan, MatchAll):
        return plan, True
    if isinstance(plan, ExprCondition):
        expression = _expression_before_image(plan.expression, preceding, dialect)
        return (
            (None, False)
            if expression is _UNMOVABLE
            else (ExprCondition(expression), True)
        )
    field = getattr(plan, "field", None)
    source = _source_path(field, preceding, dialect) if isinstance(field, str) else None
    return (None, False) if source is None else (replace(plan, field=source), True)


def _source_path(path, preceding, dialect):
    for stage in reversed(preceding):
        operator, spec = next(iter(stage.items()))
        if operator in {"$set", "$addFields"}:
            path = _path_before_assignments(path, spec)
        elif operator == "$project":
            path = _path_before_projection(path, spec, dialect)
        elif operator == "$unset":
            removed = [spec] if isinstance(spec, str) else spec
            if any(_overlap(path, field) for field in removed):
                return None
        elif operator != "$match":
            return None
        if path is None:
            return None
    return None if path.split(".", 1)[0] == "fullDocument" else path


def _overlap(left, right):
    return left == right or left.startswith(right + ".") or right.startswith(left + ".")


def _path_before_assignments(path, assignments):
    for target, expression in assignments.items():
        if path == target or path.startswith(target + "."):
            source = _alias_source(expression)
            return source + path[len(target) :] if source is not None else None
        if target.startswith(path + "."):
            return None
    return path


def _alias_source(expression):
    if not matches_bson_type(expression, "string"):
        return None
    for prefix in ("$$ROOT.", "$$CURRENT."):
        if expression.startswith(prefix):
            return expression[len(prefix) :]
    return (
        expression[1:]
        if expression.startswith("$") and not expression.startswith("$$")
        else None
    )


def _path_before_projection(path, spec, dialect):
    include = any(
        key != "_id" and _projection_flag(value, dialect=dialect) != 0
        for key, value in spec.items()
    ) or (set(spec) == {"_id"} and _projection_flag(spec["_id"], dialect=dialect) != 0)
    for target, expression in spec.items():
        if _overlap(path, target):
            flag = _projection_flag(expression, dialect=dialect)
            if flag == 1:
                return path if path == target or path.startswith(target + ".") else None
            if flag == 0:
                return None
            return _path_before_assignments(path, {target: expression})
    if path.split(".", 1)[0] == "_id":
        return path
    return None if include else path


def _expression_before_image(value, preceding, dialect):
    if isinstance(value, str):
        path = _alias_source(value)
        if path is not None:
            source = _source_path(path, preceding, dialect)
            return "$" + source if source is not None else _UNMOVABLE
        return (
            value
            if matches_bson_type(value, "string")
            and value not in {"$$ROOT", "$$CURRENT"}
            else _UNMOVABLE
        )
    if isinstance(value, list):
        children = [
            _expression_before_image(item, preceding, dialect) for item in value
        ]
        return _UNMOVABLE if any(item is _UNMOVABLE for item in children) else children
    if isinstance(value, dict):
        literal = len(value) == 1 and next(iter(value)) in {"$literal", "$const"}
        dynamic = any(
            key in {"$getField", "$setField", "$unsetField", "$rand"}
            or get_registered_aggregation_expression_operator(key) is not None
            for key in value
        )
        if literal or dynamic:
            return value if literal else _UNMOVABLE
        children = {
            key: _expression_before_image(item, preceding, dialect)
            for key, item in value.items()
        }
        return (
            _UNMOVABLE
            if any(item is _UNMOVABLE for item in children.values())
            else children
        )
    return value
