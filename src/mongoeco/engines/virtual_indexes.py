from __future__ import annotations

import re

from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core._filtering_support import query_field_values
from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.bson_scalars import _is_nan_numeric_value, unwrap_bson_numeric
from mongoeco.core.bson_values import document_mapping, matches_bson_type
from mongoeco.core.collation import (
    CollationSpec,
    compare_with_collation,
    normalize_collation,
    values_equal_with_collation,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_operators import DBREF_FIELDS
from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    ExistsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
    compile_filter,
)
from mongoeco.core.server_errors import server_error
from mongoeco.errors import WriteError
from mongoeco.types import (
    UNDEFINED,
    DBRef,
    Document,
    EngineIndexRecord,
    Filter,
    is_ordered_index_spec,
)


HASHED_ARRAY_ERROR = 16766
PARALLEL_ARRAY_ERROR = 171
INDEX_DOCUMENT_ERROR_CODES = frozenset({HASHED_ARRAY_ERROR, PARALLEL_ARRAY_ERROR})


def validate_index_definition(definition):
    """Server invariants shared by catalog construction in both engines."""
    if len(definition.keys) > 32:  # noqa: PLR2004 - server compound-key limit
        message = "Too many fields in compound key"
        raise server_error(message, code=13103)
    for field, _direction in definition.keys:
        parts = field.split(".")
        if any(
            not part or (
                part.startswith("$")
                and not (part == "$**" and position == len(parts) - 1)
                and not (position > 0 and part in DBREF_FIELDS)
            )
            for position, part in enumerate(parts)
        ):
            message = f"Invalid index field path: {field}"
            raise server_error(message, code=67)
    if definition.keys == [("_id", 1)] and (
        definition.sparse
        or definition.hidden
        or definition.partial_filter_expression is not None
        or definition.expire_after_seconds is not None
    ):
        message = "Invalid option for an _id index specification"
        raise server_error(message, code=197)
    if definition.expire_after_seconds is not None and len(definition.keys) != 1:
        message = "TTL indexes are single-field indexes"
        raise server_error(message, code=67)


def validate_index_document(document, index, *, building=False):
    message = None
    code = None
    for field, direction in index["key"]:
        if direction != "hashed":
            continue
        value = document
        for part in field.split("."):
            if isinstance(value, list):
                break
            mapping = document_mapping(value)
            value = MISSING if mapping is None else mapping.get(part, MISSING)
        if isinstance(value, list):
            message = "Hashed indexes do not currently support array values"
            code = HASHED_ARRAY_ERROR
            break
    if (
        message is None
        and len(compound_multikey_array_paths(document, index["fields"])) > 1
    ):
        message = "Cannot index parallel arrays"
        code = PARALLEL_ARRAY_ERROR
    if message is not None:
        if building:
            raise server_error(message, code=code)
        raise WriteError(message, code=code)


def _identity_collation(value):
    collation = normalize_collation(value)
    return None if collation is None or collation.locale == "simple" else collation


def collated_id_conflict(document, documents, options, *, exclude=None):
    collation = _identity_collation(options.get("collation"))
    if collation is None or "_id" not in document:
        return None
    try:
        for storage_key, existing in documents:
            if (
                storage_key != exclude
                and "_id" in existing
                and values_equal_with_collation(
                    document["_id"], existing["_id"], collation=collation
                )
            ):
                return {
                    "name": "_id_",
                    "key": [("_id", 1)],
                    "fields": ["_id"],
                    "collation": collation.to_document(),
                }
    finally:
        close = getattr(documents, "close", None)
        if callable(close):
            close()
    return None


def existing_index_for_definition(definition, indexes):
    """Return an identical named index, or reject conflicting catalog identity."""
    collation = _identity_collation(definition.collation)
    options = (
        "hidden",
        "expire_after_seconds",
        "weights",
        "default_language",
        "language_override",
        "min_value",
        "max_value",
        "bucket_size",
    )
    for index in indexes:
        existing = index.to_definition()
        same_identity = (
            existing.keys == definition.keys
            and existing.unique == definition.unique
            and existing.sparse == definition.sparse
            and _identity_collation(existing.collation) == collation
            and values_equal_with_collation(
                existing.partial_filter_expression,
                definition.partial_filter_expression,
                collation=collation,
            )
        )
        if existing.name == definition.name:
            if not same_identity:
                message = "Index name already has a different key specification"
                raise server_error(message, code=86)
            if any(
                getattr(existing, field) != getattr(definition, field)
                for field in options
            ):
                message = "Index already exists with different options"
                raise server_error(message, code=85)
            return existing.name
        if same_identity or (
            any(direction == "text" for _, direction in existing.keys)
            and any(direction == "text" for _, direction in definition.keys)
        ):
            message = "Index already exists with a different name"
            raise server_error(message, code=85)
    return None


def index_field_values(document, field):
    """Stored index keys expand terminal arrays once, independently of MQL matching."""
    if isinstance(document, list):
        document = {str(index): value for index, value in enumerate(document)}
    values = []
    for value, _traverse in query_field_values(document, field):
        if value is MISSING:
            values.append(None)
        elif isinstance(value, list):
            values.extend(value or [UNDEFINED])
        else:
            values.append(value)
    return values or [None]


def array_paths_for_index_field(
    document: Document,
    field: str,
) -> set[str]:
    array_paths: set[str] = set()
    parts = field.split(".")

    def collect(value: Any, position: int, path: str) -> None:
        if isinstance(value, DBRef):
            value = value.as_document()
        if isinstance(value, list):
            if position == len(parts):
                array_paths.add(path)
                return
            part = parts[position]
            if part.isdigit():
                item_index = int(part)
                if item_index < len(value):
                    next_path = f"{path}.{part}" if path else part
                    collect(value[item_index], position + 1, next_path)
                return
            array_paths.add(path)
            for item in value:
                collect(item, position, path)
            return
        if position == len(parts) or not isinstance(value, dict):
            return
        part = parts[position]
        if part not in value:
            return
        next_path = f"{path}.{part}" if path else part
        collect(value[part], position + 1, next_path)

    collect(document, 0, "")
    return array_paths


def compound_multikey_array_paths(
    document: Document,
    fields: list[str],
) -> set[str]:
    source_paths: set[str] = set()
    for field in fields:
        array_paths = array_paths_for_index_field(document, field)
        if array_paths:
            source_paths.add(
                min(array_paths, key=lambda path: len(path.split("."))),
            )
    return source_paths


def values_at_index_path(document: Document, path: str) -> list[Any]:
    values: list[Any] = [document]
    for part in path.split("."):
        next_values: list[Any] = []
        for source_value in values:
            value = (
                source_value.as_document()
                if isinstance(source_value, DBRef)
                else source_value
            )
            if isinstance(value, list):
                if part.isdigit():
                    item_index = int(part)
                    if item_index < len(value):
                        next_values.append(value[item_index])
                else:
                    for source_item in value:
                        item = (
                            source_item.as_document()
                            if isinstance(source_item, DBRef)
                            else source_item
                        )
                        if isinstance(item, dict) and part in item:
                            next_values.append(item[part])
            elif isinstance(value, dict) and part in value:
                next_values.append(value[part])
        values = next_values
    return values


def unique_index_values(
    document: Document,
    fields: list[str],
) -> list[tuple[Any, ...]]:
    if len(fields) == 1:
        values = index_field_values(document, fields[0])
        return [(value,) for value in values or [None]]

    array_source_paths = compound_multikey_array_paths(
        document,
        fields,
    )
    if len(array_source_paths) == 1:
        array_path = next(iter(array_source_paths))
        array_values = [
            value
            for value in values_at_index_path(document, array_path)
            if isinstance(value, list)
        ]
        if array_values:
            keys: list[tuple[Any, ...]] = []
            for array_value in array_values:
                for element in array_value:
                    candidates_per_field: list[list[Any]] = []
                    for field in fields:
                        if field == array_path:
                            candidates_per_field.append([element])
                        elif field.startswith(f"{array_path}."):
                            suffix = field.removeprefix(f"{array_path}.")
                            candidates_per_field.append(
                                index_field_values(element, suffix) or [None],
                            )
                        else:
                            candidates_per_field.append(
                                index_field_values(document, field) or [None],
                            )
                    element_keys: list[tuple[Any, ...]] = [()]
                    for candidates in candidates_per_field:
                        element_keys = [
                            (*key, candidate)
                            for key in element_keys
                            for candidate in candidates
                        ]
                    keys.extend(element_keys)
            if keys:
                return keys

    keys: list[tuple[Any, ...]] = [()]
    for field in fields:
        keys = [
            (*key, value)
            for key in keys
            for value in (index_field_values(document, field) or [None])
        ]
    return keys


def normalize_partial_filter_expression(
    partial_filter_expression: object | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> Filter | None:
    if partial_filter_expression is None:
        return None
    if not isinstance(partial_filter_expression, dict):
        raise TypeError("partial_filter_expression must be a dict or None")
    compile_filter(partial_filter_expression, dialect=dialect)
    return deepcopy(partial_filter_expression)


def is_virtual_index(index: EngineIndexRecord | dict[str, object]) -> bool:
    return bool(
        _index_sparse(index) or _index_partial_filter_expression(index) is not None
    )


def document_in_virtual_index(
    document: Document,
    index: EngineIndexRecord,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
) -> bool:
    fields = _index_fields(index)
    if _index_sparse(index) and not any(QueryEngine.extract_values(document, field) for field in fields):
        return False
    partial_filter_expression = _index_partial_filter_expression(index)
    if partial_filter_expression is not None:
        partial_plan = compile_filter(partial_filter_expression, dialect=dialect)
        if not QueryEngine.match_plan(
            document,
            partial_plan,
            dialect=dialect,
            collation=_index_collation(index),
        ):
            return False
    return True


def query_can_use_index(
    index: EngineIndexRecord | dict[str, object],
    query_plan: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | Filter | None = None,
) -> bool:
    if _index_hidden(index):
        return False
    keys = _index_key(index)
    if not isinstance(keys, list) or not is_ordered_index_spec(keys):
        return False
    if _index_sparse(index) and not any(
        _plan_implies_exists_true(query_plan, field)
        for field in _index_fields(index)
    ):
        return False
    partial_filter_expression = _index_partial_filter_expression(index)
    if partial_filter_expression is None:
        return True
    operation_collation = (
        normalize_collation(collation) if isinstance(collation, dict) else collation
    )
    index_collation = _index_collation(index)
    if operation_collation != index_collation:
        return False
    partial_plan = compile_filter(partial_filter_expression, dialect=dialect)
    return _plan_implies(
        query_plan,
        partial_plan,
        dialect=dialect,
        collation=index_collation,
    )


def query_can_use_index_order(
    index, query_plan, sort, *, dialect=MONGODB_DIALECT_70, collation=None
):
    """Prove an ordered prefix after equality-constrained index fields."""
    key = _index_key(index)
    if isinstance(key, dict):
        key = list(key.items())
    if not isinstance(key, list) or not is_ordered_index_spec(key):
        return False
    candidate = (
        index
        if isinstance(index, EngineIndexRecord)
        else {**index, "key": key, "fields": [field for field, _ in key]}
    )
    if not query_can_use_index(
        candidate, query_plan, dialect=dialect, collation=collation
    ):
        return False
    if _index_collation(candidate) != collation:
        return False

    def equalities(plan):
        if isinstance(plan, EqualsCondition) and not isinstance(
            plan.value, (list, dict)
        ):
            return {plan.field}
        if isinstance(plan, AndCondition):
            return set().union(*(equalities(clause) for clause in plan.clauses))
        return set()

    fields = equalities(query_plan)
    for start in range(len(key)):
        prefix = key[start : start + len(sort)]
        if len(prefix) == len(sort) and (
            all(a == b for a, b in zip(prefix, sort, strict=True))
            or all(
                a[0] == b[0] and a[1] == -b[1]
                for a, b in zip(prefix, sort, strict=True)
            )
        ):
            return True
        if key[start][0] not in fields:
            break
    return False


def collect_usable_virtual_index_names(
    indexes: list[EngineIndexRecord] | list[dict[str, object]],
    query_plan: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | Filter | None = None,
) -> list[str]:
    names: list[str] = []
    for index in indexes:
        if not is_virtual_index(index):
            continue
        if query_can_use_index(
            index,
            query_plan,
            dialect=dialect,
            collation=collation,
        ):
            name = _index_name(index)
            if name:
                names.append(name)
    return names


def describe_virtual_index_usage(
    indexes: list[EngineIndexRecord] | list[dict[str, object]],
    query_plan: QueryNode,
    *,
    hinted_index_name: str | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | Filter | None = None,
) -> dict[str, object] | None:
    usable = collect_usable_virtual_index_names(
        indexes,
        query_plan,
        dialect=dialect,
        collation=collation,
    )
    hinted_virtual: bool | None = None
    if hinted_index_name is not None:
        for index in indexes:
            if _index_name(index) != hinted_index_name:
                continue
            hinted_virtual = is_virtual_index(index)
            break
    if not usable and hinted_virtual is None:
        return None
    details: dict[str, object] = {}
    if usable:
        details["usableVirtualIndexes"] = usable
    if hinted_virtual is not None:
        details["hintedIndexIsVirtual"] = hinted_virtual
    return details


def _index_fields(index: EngineIndexRecord | dict[str, object]) -> list[str]:
    if isinstance(index, EngineIndexRecord):
        return index.fields
    return list(index.get("fields", []))


def _index_key(index: EngineIndexRecord | dict[str, object]) -> list[tuple[str, int]] | object:
    if isinstance(index, EngineIndexRecord):
        return index.key
    key = index.get("key")
    if key is not None:
        return key
    fields = _index_fields(index)
    if fields:
        return [(field, 1) for field in fields]
    return None


def _index_name(index: EngineIndexRecord | dict[str, object]) -> str | None:
    if isinstance(index, EngineIndexRecord):
        return index.name
    value = index.get("name")
    return value if isinstance(value, str) else None


def _index_sparse(index: EngineIndexRecord | dict[str, object]) -> bool:
    if isinstance(index, EngineIndexRecord):
        return index.sparse
    return bool(index.get("sparse", False))


def _index_hidden(index: EngineIndexRecord | dict[str, object]) -> bool:
    if isinstance(index, EngineIndexRecord):
        return index.hidden
    return bool(index.get("hidden", False))


def _index_collation(
    index: EngineIndexRecord | dict[str, object],
) -> CollationSpec | None:
    if isinstance(index, EngineIndexRecord):
        value = index.collation
    else:
        value = index.get("collation")
    return normalize_collation(value) if isinstance(value, dict) else None


def _index_partial_filter_expression(index: EngineIndexRecord | dict[str, object]) -> Filter | None:
    if isinstance(index, EngineIndexRecord):
        return index.partial_filter_expression
    value = index.get("partial_filter_expression")
    if value is None:
        value = index.get("partialFilterExpression")
    return value if isinstance(value, dict) else None


def _plan_implies(
    query_plan: QueryNode,
    requirement: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if isinstance(requirement, MatchAll):
        return True
    if isinstance(query_plan, OrCondition):
        return all(_plan_implies(clause, requirement, dialect=dialect, collation=collation) for clause in query_plan.clauses)
    if isinstance(requirement, OrCondition):
        return any(_plan_implies(query_plan, clause, dialect=dialect, collation=collation) for clause in requirement.clauses)
    if isinstance(requirement, AndCondition):
        return all(_plan_implies(query_plan, clause, dialect=dialect, collation=collation) for clause in requirement.clauses)
    if isinstance(query_plan, AndCondition):
        return any(_plan_implies(clause, requirement, dialect=dialect, collation=collation) for clause in query_plan.clauses)
    if _implies_same_field_condition(
        query_plan,
        requirement,
        dialect=dialect,
        collation=collation,
    ):
        return True
    if isinstance(requirement, ExistsCondition) and requirement.value:
        return _plan_implies_exists_true(query_plan, requirement.field)
    return False


def _plan_implies_exists_true(query_plan: QueryNode, field: str) -> bool:
    if isinstance(query_plan, ExistsCondition):
        return query_plan.field == field and query_plan.value
    if isinstance(query_plan, EqualsCondition):
        return query_plan.field == field and query_plan.value is not None
    if isinstance(query_plan, InCondition):
        return query_plan.field == field and bool(query_plan.values) and all(value is not None for value in query_plan.values)
    if isinstance(
        query_plan,
        GreaterThanCondition
        | GreaterThanOrEqualCondition
        | LessThanCondition
        | LessThanOrEqualCondition
        | RegexCondition
        | SizeCondition
        | ModCondition
        | TypeCondition,
    ):
        return query_plan.field == field
    if isinstance(query_plan, AndCondition):
        return any(_plan_implies_exists_true(clause, field) for clause in query_plan.clauses)
    if isinstance(query_plan, OrCondition):
        return all(_plan_implies_exists_true(clause, field) for clause in query_plan.clauses)
    return False


def _implies_same_field_condition(
    query_plan: QueryNode,
    requirement: QueryNode,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if isinstance(requirement, ExistsCondition):
        return requirement.value and _plan_implies_exists_true(query_plan, requirement.field)
    if isinstance(requirement, EqualsCondition):
        if isinstance(query_plan, EqualsCondition):
            return (
                query_plan.field == requirement.field
                and values_equal_with_collation(
                    query_plan.value,
                    requirement.value,
                    dialect=dialect,
                    collation=collation,
                )
            )
        if isinstance(query_plan, InCondition):
            return (
                query_plan.field == requirement.field
                and len(query_plan.values) == 1
                and values_equal_with_collation(
                    query_plan.values[0],
                    requirement.value,
                    dialect=dialect,
                    collation=collation,
                )
            )
        return False
    if isinstance(requirement, InCondition):
        if isinstance(query_plan, EqualsCondition):
            return query_plan.field == requirement.field and any(
                values_equal_with_collation(query_plan.value, candidate, dialect=dialect, collation=collation)
                for candidate in requirement.values
            )
        if isinstance(query_plan, InCondition):
            return query_plan.field == requirement.field and all(
                any(
                    values_equal_with_collation(query_value, required_value, dialect=dialect, collation=collation)
                    for required_value in requirement.values
                )
                for query_value in query_plan.values
            )
        return False
    if isinstance(requirement, TypeCondition):
        return _same_field_type_implies(query_plan, requirement.field, requirement.values)
    if isinstance(requirement, RegexCondition):
        return _same_field_regex_implies(query_plan, requirement)
    if isinstance(requirement, GreaterThanCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            minimum=requirement.value,
            inclusive=False,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(requirement, GreaterThanOrEqualCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            minimum=requirement.value,
            inclusive=True,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(requirement, LessThanCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            maximum=requirement.value,
            inclusive=False,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(requirement, LessThanOrEqualCondition):
        return _same_field_ordering_implies(
            query_plan,
            requirement.field,
            maximum=requirement.value,
            inclusive=True,
            dialect=dialect,
            collation=collation,
        )
    return False


def _same_field_type_implies(query_plan: QueryNode, field: str, type_spec: object) -> bool:
    aliases: set[str] = set()
    if isinstance(type_spec, tuple):
        for candidate in type_spec:
            aliases.update(QueryEngine._normalize_type_specifier(candidate))
    else:
        aliases.update(QueryEngine._normalize_type_specifier(type_spec))
    if isinstance(query_plan, EqualsCondition):
        return query_plan.field == field and any(
            matches_bson_type(query_plan.value, alias)
            for alias in aliases
        )
    if isinstance(query_plan, InCondition):
        return query_plan.field == field and bool(query_plan.values) and all(
            any(matches_bson_type(value, alias) for alias in aliases)
            for value in query_plan.values
        )
    if isinstance(query_plan, TypeCondition):
        query_aliases: set[str] = set()
        for candidate in query_plan.values:
            query_aliases.update(QueryEngine._normalize_type_specifier(candidate))
        return query_plan.field == field and query_aliases.issubset(aliases)
    return False


def _same_field_regex_implies(query_plan: QueryNode, requirement: RegexCondition) -> bool:
    if isinstance(query_plan, EqualsCondition):
        if query_plan.field != requirement.field or not isinstance(query_plan.value, str):
            return False
        return _compile_regex_condition(requirement).search(query_plan.value) is not None
    if isinstance(query_plan, InCondition):
        if query_plan.field != requirement.field or not query_plan.values:
            return False
        compiled = _compile_regex_condition(requirement)
        return all(isinstance(value, str) and compiled.search(value) is not None for value in query_plan.values)
    if isinstance(query_plan, RegexCondition):
        return (
            query_plan.field == requirement.field
            and query_plan.pattern == requirement.pattern
            and query_plan.options == requirement.options
        )
    return False


def _compile_regex_condition(condition: RegexCondition) -> re.Pattern[str]:
    flags = 0
    for option in condition.options:
        if option == "i":
            flags |= re.IGNORECASE
        elif option == "m":
            flags |= re.MULTILINE
        elif option == "s":
            flags |= re.DOTALL
        elif option == "a":
            flags |= re.ASCII
        elif option == "u":
            flags |= re.UNICODE
        elif option == "x":
            flags |= re.VERBOSE
    return re.compile(condition.pattern, flags)


def _same_field_ordering_implies(
    query_plan: QueryNode,
    field: str,
    *,
    minimum: object = MISSING,
    maximum: object = MISSING,
    inclusive: bool,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if isinstance(query_plan, EqualsCondition):
        if query_plan.field != field:
            return False
        return _value_satisfies_bounds(
            query_plan.value,
            minimum=minimum,
            maximum=maximum,
            inclusive=inclusive,
            dialect=dialect,
            collation=collation,
        )
    if isinstance(query_plan, InCondition):
        if query_plan.field != field or not query_plan.values:
            return False
        return all(
            _value_satisfies_bounds(
                value,
                minimum=minimum,
                maximum=maximum,
                inclusive=inclusive,
                dialect=dialect,
                collation=collation,
            )
            for value in query_plan.values
        )
    comparisons = {
        GreaterThanCondition: ("min", False),
        GreaterThanOrEqualCondition: ("min", True),
        LessThanCondition: ("max", False),
        LessThanOrEqualCondition: ("max", True),
    }
    for comparison_type, (kind, query_inclusive) in comparisons.items():
        if isinstance(query_plan, comparison_type):
            if query_plan.field != field:
                return False
            if kind == "min" and minimum is not MISSING:
                return _compare_bounds(
                    query_plan.value,
                    minimum,
                    query_inclusive,
                    inclusive,
                    dialect=dialect,
                    collation=collation,
                )
            if kind == "max" and maximum is not MISSING:
                return _compare_bounds(
                    query_plan.value,
                    maximum,
                    query_inclusive,
                    inclusive,
                    reverse=True,
                    dialect=dialect,
                    collation=collation,
                )
    return False


def _compare_bounds(
    left: object,
    right: object,
    left_inclusive: bool,
    right_inclusive: bool,
    *,
    reverse: bool = False,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    if not QueryEngine._comparison_types_match(left, right, dialect=dialect):
        return False
    if _is_nan_numeric_value(unwrap_bson_numeric(left)) != _is_nan_numeric_value(
        unwrap_bson_numeric(right)
    ):
        return False
    comparison = compare_with_collation(
        left,
        right,
        dialect=dialect,
        collation=collation,
    )
    if reverse:
        comparison = -comparison
    return comparison > 0 or (
        comparison == 0 and ((not left_inclusive) or right_inclusive)
    )


def _value_satisfies_bounds(
    value: object,
    *,
    minimum: object = MISSING,
    maximum: object = MISSING,
    inclusive: bool,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
) -> bool:
    return all(
        bound is MISSING
        or QueryEngine._comparison_matches_candidate(
            value, bound, operator, dialect=dialect, collation=collation
        )
        for bound, operator in (
            (minimum, "gte" if inclusive else "gt"),
            (maximum, "lte" if inclusive else "lt"),
        )
    )
