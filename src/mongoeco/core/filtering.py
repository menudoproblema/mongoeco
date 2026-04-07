import re
from typing import Any, assert_never

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core._filtering_matching import FilteringMatchingMixin
from mongoeco.core._filtering_specials import FilteringSpecialOperatorsMixin
from mongoeco.core._filtering_support import (
    compile_regex as _compile_regex,
    extract_values as _extract_values,
    get_field_value as _get_field_value,
    hashable_in_lookup_key as _hashable_in_lookup_key,
    path_mapping as _path_mapping,
)
from mongoeco.core.collation import CollationSpec, values_equal_with_collation
from mongoeco.core.geo import (
    geometry_intersects_geometry,
    geometry_within_geometry,
    parse_geo_geometry,
    planar_distance_to_geometry,
)
from mongoeco.errors import OperationFailure
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    DeferredQueryNode,
    ElemMatchCondition,
    EqualsCondition,
    ExistsCondition,
    ExprCondition,
    GeoIntersectsCondition,
    GeoWithinCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    JsonSchemaCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NearCondition,
    NotCondition,
    NotEqualsCondition,
    NotInCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
    WhereCondition,
    compile_filter,
    is_concrete_query_node,
)


HANDLED_QUERY_NODE_TYPES: tuple[type[QueryNode], ...] = (
    MatchAll,
    DeferredQueryNode,
    EqualsCondition,
    NotEqualsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    InCondition,
    NotInCondition,
    AllCondition,
    SizeCondition,
    ModCondition,
    RegexCondition,
    GeoWithinCondition,
    GeoIntersectsCondition,
    NearCondition,
    NotCondition,
    ElemMatchCondition,
    ExistsCondition,
    TypeCondition,
    BitwiseCondition,
    ExprCondition,
    WhereCondition,
    JsonSchemaCondition,
    AndCondition,
    OrCondition,
)


class _WhereDocumentView:
    def __init__(self, value: object):
        self._value = value

    @staticmethod
    def _wrap(value: object) -> object:
        if isinstance(value, dict | list | tuple):
            return _WhereDocumentView(value)
        return value

    def __getattr__(self, name: str) -> object:
        if isinstance(self._value, dict) and name in self._value:
            return self._wrap(self._value[name])
        raise AttributeError(name)

    def __getitem__(self, key: object) -> object:
        if isinstance(self._value, dict):
            return self._wrap(self._value[key])
        if isinstance(self._value, list | tuple):
            if not isinstance(key, int):
                raise TypeError("list/tuple $where indexing requires an integer")
            return self._wrap(self._value[key])
        raise TypeError("value does not support indexing")

    def __iter__(self):
        if isinstance(self._value, dict):
            for key in self._value:
                yield key
            return
        if isinstance(self._value, list | tuple):
            for item in self._value:
                yield self._wrap(item)
            return
        raise TypeError("value is not iterable")

    def __len__(self) -> int:
        if isinstance(self._value, dict | list | tuple):
            return len(self._value)
        raise TypeError("value has no length")

    def __bool__(self) -> bool:
        return bool(self._value)


class BSONComparator:
    """Reglas de comparacion de MongoDB (Type Brackets)."""

    TYPE_ORDER = MONGODB_DIALECT_70.bson_type_order

    @staticmethod
    def compare(
        a: Any,
        b: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> int:
        return dialect.policy.compare_values(a, b)


class QueryEngine(FilteringMatchingMixin, FilteringSpecialOperatorsMixin):
    """Motor central de filtrado de MongoDB."""

    @staticmethod
    def _hashable_in_lookup_key(
        value: Any,
        *,
        collation: CollationSpec | None = None,
    ) -> tuple[str, Any] | None:
        del collation
        return _hashable_in_lookup_key(value)

    @staticmethod
    def _path_mapping(value: Any) -> dict[str, Any] | None:
        return _path_mapping(value)

    @staticmethod
    def match(
        document: dict[str, Any],
        filter_spec: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        return QueryEngine.match_plan(
            document,
            compile_filter(filter_spec, dialect=dialect),
            dialect=dialect,
            collation=collation,
        )

    @staticmethod
    def match_plan(
        document: dict[str, Any],
        plan: QueryNode,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        if not is_concrete_query_node(plan):
            raise TypeError(f"Unsupported query plan node: {type(plan)!r}")
        match plan:
            case MatchAll():
                return True
            case DeferredQueryNode(issue=issue):
                raise OperationFailure(
                    f"query plan contains deferred validation issues: {issue.message}"
                )
            case EqualsCondition(field=field, value=value, null_matches_undefined=null_matches_undefined):
                return QueryEngine._evaluate_equals(
                    document,
                    field,
                    value,
                    null_matches_undefined=null_matches_undefined,
                    dialect=dialect,
                    collation=collation,
                )
            case NotEqualsCondition(field=field, value=value):
                return QueryEngine._evaluate_not_equals(
                    document,
                    field,
                    value,
                    dialect=dialect,
                    collation=collation,
                )
            case GreaterThanCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "gt",
                    dialect=dialect,
                    collation=collation,
                )
            case GreaterThanOrEqualCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "gte",
                    dialect=dialect,
                    collation=collation,
                )
            case LessThanCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "lt",
                    dialect=dialect,
                    collation=collation,
                )
            case LessThanOrEqualCondition(field=field, value=value):
                return QueryEngine._evaluate_comparison(
                    document,
                    field,
                    value,
                    "lte",
                    dialect=dialect,
                    collation=collation,
                )
            case InCondition(field=field, values=values, null_matches_undefined=null_matches_undefined):
                return QueryEngine._evaluate_in(
                    document,
                    field,
                    values,
                    null_matches_undefined=null_matches_undefined,
                    dialect=dialect,
                    collation=collation,
                )
            case NotInCondition(field=field, values=values, null_matches_undefined=null_matches_undefined):
                return QueryEngine._evaluate_not_in(
                    document,
                    field,
                    values,
                    null_matches_undefined=null_matches_undefined,
                    dialect=dialect,
                    collation=collation,
                )
            case AllCondition(field=field, values=values):
                return QueryEngine._evaluate_all(
                    document,
                    field,
                    values,
                    dialect=dialect,
                    collation=collation,
                )
            case SizeCondition(field=field, value=value):
                return QueryEngine._evaluate_size(document, field, value)
            case ModCondition(field=field, divisor=divisor, remainder=remainder):
                return QueryEngine._evaluate_mod(document, field, divisor, remainder)
            case RegexCondition(field=field, pattern=pattern, options=options):
                return QueryEngine._evaluate_regex(document, field, pattern, options)
            case GeoWithinCondition(field=field, geometry_kind=geometry_kind, geometry=geometry):
                return QueryEngine._evaluate_geo_within(document, field, geometry_kind, geometry)
            case GeoIntersectsCondition(field=field, geometry_kind=geometry_kind, geometry=geometry):
                return QueryEngine._evaluate_geo_intersects(document, field, geometry_kind, geometry)
            case NearCondition(
                field=field,
                point=point,
                min_distance=min_distance,
                max_distance=max_distance,
                spherical=spherical,
            ):
                return QueryEngine._evaluate_near(
                    document,
                    field,
                    point,
                    min_distance=min_distance,
                    max_distance=max_distance,
                    spherical=spherical,
                )
            case NotCondition(clause=clause):
                return not QueryEngine.match_plan(
                    document,
                    clause,
                    dialect=dialect,
                    collation=collation,
                )
            case ElemMatchCondition(
                field=field,
                condition=condition,
                compiled_plan=compiled_plan,
                compiled_dialect_key=compiled_dialect_key,
                wrap_value=wrap_value,
            ):
                return QueryEngine._evaluate_elem_match(
                    document,
                    field,
                    condition,
                    dialect=dialect,
                    collation=collation,
                    compiled_plan=compiled_plan,
                    compiled_dialect_key=compiled_dialect_key,
                    wrap_value=wrap_value,
                )
            case ExistsCondition(field=field, value=value):
                return QueryEngine._evaluate_exists(document, field, value)
            case TypeCondition(field=field, values=values, aliases=aliases):
                return QueryEngine._evaluate_type(document, field, values, aliases=aliases)
            case BitwiseCondition(field=field, operator=operator, operand=operand, mask=mask):
                return QueryEngine._evaluate_bitwise(document, field, operator, operand, mask=mask)
            case ExprCondition(expression=expression, variables=variables):
                from mongoeco.core.aggregation import _expression_truthy, evaluate_expression

                value = evaluate_expression(document, expression, variables, dialect=dialect)
                return _expression_truthy(value, dialect=dialect)
            case WhereCondition(
                expression=expression,
                predicate=predicate,
                compiled_expression=compiled_expression,
            ):
                try:
                    if predicate is not None:
                        return bool(predicate(document))
                    if compiled_expression is None:
                        raise ValueError("missing compiled expression")
                    wrapped = _WhereDocumentView(document)
                    return bool(
                        eval(  # noqa: S307 - bounded sandbox by AST restrictions in query_plan
                            compiled_expression,
                            {"__builtins__": {}},
                            {"this": wrapped, "doc": wrapped},
                        )
                    )
                except Exception as exc:  # pragma: no cover - covered by public tests
                    message = "$where evaluation failed"
                    if expression is not None:
                        message += f" for expression {expression!r}"
                    raise OperationFailure(f"{message}: {exc}") from exc
            case JsonSchemaCondition(schema=schema, compiled_schema=compiled_schema):
                from mongoeco.core.schema_validation import CompiledJsonSchema

                validator = compiled_schema if compiled_schema is not None else CompiledJsonSchema(schema)
                return validator.validate(document).valid
            case AndCondition(clauses=clauses):
                return all(
                    QueryEngine.match_plan(document, clause, dialect=dialect, collation=collation)
                    for clause in clauses
                )
            case OrCondition(clauses=clauses):
                return any(
                    QueryEngine.match_plan(document, clause, dialect=dialect, collation=collation)
                    for clause in clauses
                )
            case _:
                assert_never(plan)

    @staticmethod
    def _extract_values(doc: Any, path: str) -> list[Any]:
        return _extract_values(doc, path)

    @staticmethod
    def _get_field_value(doc: Any, path: str) -> tuple[bool, Any]:
        return _get_field_value(doc, path)

    @staticmethod
    def extract_values(doc: Any, path: str) -> list[Any]:
        """API publica para resolver valores observables por dot notation."""
        return QueryEngine._extract_values(doc, path)

    @staticmethod
    def _values_equal(
        left: Any,
        right: Any,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
    ) -> bool:
        return values_equal_with_collation(
            left,
            right,
            dialect=dialect,
            collation=collation,
        )

    @staticmethod
    def _geo_candidates(doc: dict[str, Any], field: str) -> list[object]:
        candidates: list[object] = []
        seen_wkb: set[bytes] = set()
        found, direct_value = QueryEngine._get_field_value(doc, field)
        if found:
            try:
                _geometry_kind, geometry = parse_geo_geometry(direct_value, label=field)
            except OperationFailure:
                pass
            else:
                if geometry.wkb not in seen_wkb:
                    seen_wkb.add(geometry.wkb)
                    candidates.append(geometry)
        for value in QueryEngine._extract_values(doc, field):
            try:
                _geometry_kind, geometry = parse_geo_geometry(value, label=field)
            except OperationFailure:
                continue
            if geometry.wkb not in seen_wkb:
                seen_wkb.add(geometry.wkb)
                candidates.append(geometry)
        return candidates

    @staticmethod
    def _evaluate_geo_within(
        doc: dict[str, Any],
        field: str,
        geometry_kind: str,
        geometry: object,
    ) -> bool:
        return any(
            geometry_within_geometry(candidate, geometry)
            for candidate in QueryEngine._geo_candidates(doc, field)
        )

    @staticmethod
    def _evaluate_geo_intersects(
        doc: dict[str, Any],
        field: str,
        geometry_kind: str,
        geometry: object,
    ) -> bool:
        return any(
            geometry_intersects_geometry(candidate, geometry)
            for candidate in QueryEngine._geo_candidates(doc, field)
        )

    @staticmethod
    def _evaluate_near(
        doc: dict[str, Any],
        field: str,
        point: tuple[float, float],
        *,
        min_distance: float | None,
        max_distance: float | None,
        spherical: bool,
    ) -> bool:
        del spherical
        for candidate in QueryEngine._geo_candidates(doc, field):
            distance = planar_distance_to_geometry(point, candidate)
            if min_distance is not None and distance < min_distance:
                continue
            if max_distance is not None and distance > max_distance:
                continue
            return True
        return False
