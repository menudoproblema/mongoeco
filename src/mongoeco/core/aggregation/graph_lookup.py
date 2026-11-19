"""Read-only traversal of a same-database collection, one breadth level at a time."""

from copy import deepcopy
from typing import TYPE_CHECKING

from mongoeco.core._filtering_support import query_array_members
from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.aggregation.runtime import (
    aggregation_equality_key,
    evaluate_expression,
)
from mongoeco.core.bson_ordering import bson_equality_key
from mongoeco.core.bson_scalars import (
    BsonInt64,
    exact_bson_int64,
    is_bson_numeric,
    truncate_bson_int64,
)
from mongoeco.core.bson_size import bson_document_size, bson_value_size
from mongoeco.core.error_documents import error_message
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.server_errors import server_error
from mongoeco.core.work_control import iter_with_deadline
from mongoeco.errors import OperationFailure


if TYPE_CHECKING:
    from mongoeco.core.aggregation.runtime import AggregationStageContext


_PATH_FIELDS = ("as", "connectFromField", "connectToField", "depthField")
_REQUIRED = ("from", "as", "connectFromField", "connectToField", "startWith")
_OPTIONS = frozenset((*_REQUIRED, "depthField", "maxDepth", "restrictSearchWithMatch"))
_MAX_DEPTH = (1 << 63) - 1
_MAX_MEMORY_BYTES = 100 * 1024 * 1024


def _validate_graph_path(path, name, modern):
    if not isinstance(path, str):
        message = f"$graphLookup '{name}' field must be a string"
        raise server_error(message, code=12109311 if modern else 40103)
    if not path and not modern and name != "depthField":
        message = "$graphLookup requires nonempty output and link paths"
        raise server_error(message, code=40105)
    if not path or path.endswith("."):
        message = f"Invalid $graphLookup field path '{path}'"
        raise server_error(message, code=40353 if path else 40352)
    for part in path.split("."):
        if not part:
            msg = "FieldPath field names may not be empty strings"
            raise server_error(msg, code=15998)
        if part.startswith("$"):
            msg = "FieldPath field names may not start with '$'"
            raise server_error(msg, code=16410)
        if "\x00" in part:
            msg = "FieldPath field names may not contain null bytes"
            raise server_error(msg, code=16411)


def _graph_max_depth(value, modern):
    if not is_bson_numeric(value):
        message = "$graphLookup 'maxDepth' must be numeric"
        raise server_error(message, code=12109307 if modern else 40100)
    truncated = truncate_bson_int64(value)
    if truncated is not None and truncated < 0:
        message = "$graphLookup 'maxDepth' must be nonnegative"
        raise server_error(message, code=12109308 if modern else 40101)
    depth = exact_bson_int64(value)
    if depth is None:
        message = (
            "$graphLookup 'maxDepth' must be an integer representable as a long long"
        )
        raise server_error(message, code=12109309 if modern else 40102)
    return depth


def graph_lookup_spec(raw, dialect):
    modern = dialect.key == "9.0"
    if not isinstance(raw, dict):
        message = "$graphLookup specification must be an object"
        raise server_error(message, code=9)
    for name in raw:
        if name not in _OPTIONS:
            message = f"unknown argument to $graphLookup: {name}"
            raise server_error(message, code=12109312 if modern else 40104)
    for name in _REQUIRED:
        if name not in raw:
            message = f"missing '{name}' option to $graphLookup"
            raise server_error(message, code=9 if modern or name == "from" else 40105)
    foreign = raw["from"]
    if not isinstance(foreign, str):
        msg = "$graphLookup 'from' field must be a string"
        raise server_error(msg, code=9)
    if not foreign or "\x00" in foreign:
        msg = "Invalid $graphLookup namespace"
        raise server_error(msg, code=73)
    for name in _PATH_FIELDS:
        if name in raw:
            _validate_graph_path(raw[name], name, modern)
    spec = dict(raw)
    if "maxDepth" in spec:
        spec["maxDepth"] = _graph_max_depth(spec["maxDepth"], modern)
    if "restrictSearchWithMatch" in spec and not isinstance(
        spec["restrictSearchWithMatch"], dict
    ):
        message = "$graphLookup 'restrictSearchWithMatch' must be an object"
        raise server_error(message, code=12109310 if modern else 40185)
    return spec


def _validate_graph_restriction(query):
    for key, value in query.items():
        if key in {"$text", "$where"}:
            message = f"{key} is not allowed in this context"
            raise server_error(message, code=2)
        if key in {"$and", "$or", "$nor"} and isinstance(value, list):
            for clause in value:
                if isinstance(clause, dict):
                    _validate_graph_restriction(clause)
        if (
            not key.startswith("$")
            and isinstance(value, dict)
            and (value.keys() & {"$near", "$nearSphere", "$geoNear"})
        ):
            message = "$geoNear, $near, and $nearSphere are not allowed in this context"
            raise server_error(message, code=5626500)


def compile_graph_restriction(spec, *, dialect, variables):
    restriction = spec.get("restrictSearchWithMatch", {})
    try:
        _validate_graph_restriction(restriction)
        return compile_filter(
            restriction,
            dialect=dialect,
            variables=variables,
        )
    except OperationFailure as error:
        message = (
            "Failed to parse 'restrictSearchWithMatch' option to $graphLookup"
            f" :: caused by :: {error_message(error)}"
        )
        raise server_error(message, code=error.code or 2) from error


def set_graph_output(document, path, value):
    """Assign through documents, replacing scalar or array intermediates."""
    *parents, leaf = path.split(".")
    for part in parents:
        child = document.get(part)
        if not isinstance(child, dict):
            child = {}
            document[part] = child
        document = child
    document[leaf] = value


def _add_frontier(frontier, values, size, visited_size, context):
    for value in iter_with_deadline(values, context.deadline):
        if value is MISSING:
            continue
        key = aggregation_equality_key(value, context.collation)
        if key in frontier:
            continue
        size += bson_value_size(value) + 32
        _check_memory(visited_size + size, context)
        frontier[key] = value
    return size


def _check_memory(size, context):
    if size >= _MAX_MEMORY_BYTES:
        message = "$graphLookup reached maximum memory consumption"
        raise server_error(message, code=292 if context.dialect.key == "9.0" else 40099)


def _matching_documents(foreign, frontier, spec, restriction, context):
    # Query membership expands terminal arrays and applies regexes. In this
    # stage a missing connectToField must not match an explicit null frontier.
    field = spec["connectToField"]
    values = list(frontier.values())
    predicate = {"$in": values}
    if any(value is None for value in values):
        predicate["$exists"] = True
    plan = compile_filter({field: predicate}, dialect=context.dialect)
    for candidate in iter_with_deadline(foreign, context.deadline):
        if QueryEngine.match_plan(
            candidate, plan, dialect=context.dialect, collation=context.collation
        ) and QueryEngine.match_plan(
            candidate,
            restriction,
            dialect=context.dialect,
            collation=context.collation,
            variables=context.variables,
        ):
            yield candidate


def _walk(start, foreign, spec, restriction, context):
    visited = set()
    matches = []
    output_identities = (
        set()
        if context.dialect.key == "9.0" and spec.get("depthField") == "_id"
        else None
    )
    visited_size = 0
    frontier = {}
    values = start if isinstance(start, list) else [start]
    frontier_size = _add_frontier(frontier, values, 0, 0, context)
    depth = 0
    max_depth = spec.get("maxDepth", _MAX_DEPTH)
    depth_field = spec.get("depthField")
    while frontier:
        next_frontier = {}
        next_size = 0
        for candidate in _matching_documents(
            foreign, frontier, spec, restriction, context
        ):
            if "_id" not in candidate:
                message = "Documents traversed by $graphLookup must contain an _id"
                raise server_error(message, code=40271)
            identity = bson_equality_key(candidate["_id"])
            if identity in visited:
                continue
            visited.add(identity)
            visited_size += bson_document_size(candidate) + 32
            _check_memory(visited_size + frontier_size + next_size, context)
            if depth < max_depth:
                values = query_array_members(candidate, spec["connectFromField"])
                next_size = _add_frontier(
                    next_frontier,
                    values,
                    next_size,
                    visited_size + frontier_size,
                    context,
                )
            result = deepcopy(candidate)
            if depth_field is not None:
                set_graph_output(result, depth_field, BsonInt64(depth))
            if output_identities is not None:
                identity = bson_equality_key(result["_id"])
                if identity in output_identities:
                    message = (
                        "duplicate keys are not supported in SpillableDocumentMapImpl"
                    )
                    raise server_error(message, code=2398003)
                output_identities.add(identity)
            matches.append(result)
        frontier = next_frontier
        frontier_size = next_size
        depth += 1
    return matches


def graph_lookup_results(documents, spec, context: "AggregationStageContext"):
    """Yield roots and their matches. Both aggregation runtimes use this walk."""
    if not documents:
        return
    if context.collection_resolver is None:
        msg = "$graphLookup requires collection resolver support"
        raise OperationFailure(msg)
    restriction = compile_graph_restriction(
        spec, dialect=context.dialect, variables=context.variables
    )
    foreign = context.collection_resolver(spec["from"]) or ()
    for document in iter_with_deadline(documents, context.deadline):
        start = evaluate_expression(
            document,
            spec["startWith"],
            context.variables,
            dialect=context.dialect,
            collation=context.collation,
        )
        yield document, _walk(start, foreign, spec, restriction, context)
