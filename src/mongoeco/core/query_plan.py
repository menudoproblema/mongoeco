from dataclasses import dataclass
import math
import re
import uuid
from typing import Any, TypeIs

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.errors import OperationFailure
from mongoeco.types import BsonBindings, BitwiseMaskOperand, BsonValue, Filter, PlanningIssue, PlanningMode, Regex


class QueryNode:
    """Nodo canónico de consulta para evaluación y futuras traducciones."""


@dataclass(frozen=True)
class MatchAll(QueryNode):
    pass


@dataclass(frozen=True)
class DeferredQueryNode(QueryNode):
    issue: PlanningIssue


@dataclass(frozen=True)
class EqualsCondition(QueryNode):
    field: str
    value: BsonValue
    null_matches_undefined: bool = False


@dataclass(frozen=True)
class NotEqualsCondition(QueryNode):
    field: str
    value: BsonValue


@dataclass(frozen=True)
class GreaterThanCondition(QueryNode):
    field: str
    value: BsonValue


@dataclass(frozen=True)
class GreaterThanOrEqualCondition(QueryNode):
    field: str
    value: BsonValue


@dataclass(frozen=True)
class LessThanCondition(QueryNode):
    field: str
    value: BsonValue


@dataclass(frozen=True)
class LessThanOrEqualCondition(QueryNode):
    field: str
    value: BsonValue


@dataclass(frozen=True)
class InCondition(QueryNode):
    field: str
    values: tuple[BsonValue, ...]
    null_matches_undefined: bool = False


@dataclass(frozen=True)
class NotInCondition(QueryNode):
    field: str
    values: tuple[BsonValue, ...]
    null_matches_undefined: bool = False


@dataclass(frozen=True)
class AllCondition(QueryNode):
    field: str
    values: tuple[BsonValue, ...]


@dataclass(frozen=True)
class SizeCondition(QueryNode):
    field: str
    value: int


@dataclass(frozen=True)
class ModCondition(QueryNode):
    field: str
    divisor: int | float
    remainder: int | float


@dataclass(frozen=True)
class RegexCondition(QueryNode):
    field: str
    pattern: str
    options: str = ""


@dataclass(frozen=True)
class NotCondition(QueryNode):
    clause: QueryNode


@dataclass(frozen=True)
class ElemMatchCondition(QueryNode):
    field: str
    condition: Filter
    compiled_plan: QueryNode | None = None
    compiled_dialect_key: str | None = None
    wrap_value: bool = False


@dataclass(frozen=True)
class ExistsCondition(QueryNode):
    field: str
    value: bool


@dataclass(frozen=True)
class TypeCondition(QueryNode):
    field: str
    values: tuple[BsonValue, ...]
    aliases: frozenset[str] = frozenset()


@dataclass(frozen=True)
class BitwiseCondition(QueryNode):
    field: str
    operator: str
    operand: BitwiseMaskOperand
    mask: int | None = None


@dataclass(frozen=True)
class ExprCondition(QueryNode):
    expression: Any
    variables: BsonBindings


@dataclass(frozen=True)
class JsonSchemaCondition(QueryNode):
    schema: Filter
    compiled_schema: object | None = None


@dataclass(frozen=True)
class AndCondition(QueryNode):
    clauses: tuple[QueryNode, ...]


@dataclass(frozen=True)
class OrCondition(QueryNode):
    clauses: tuple[QueryNode, ...]


type ConcreteQueryNode = (
    MatchAll
    | DeferredQueryNode
    | EqualsCondition
    | NotEqualsCondition
    | GreaterThanCondition
    | GreaterThanOrEqualCondition
    | LessThanCondition
    | LessThanOrEqualCondition
    | InCondition
    | NotInCondition
    | AllCondition
    | SizeCondition
    | ModCondition
    | RegexCondition
    | NotCondition
    | ElemMatchCondition
    | ExistsCondition
    | TypeCondition
    | BitwiseCondition
    | ExprCondition
    | JsonSchemaCondition
    | AndCondition
    | OrCondition
)


def is_concrete_query_node(node: QueryNode) -> TypeIs[ConcreteQueryNode]:
    return isinstance(
        node,
        (
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
            NotCondition,
            ElemMatchCondition,
            ExistsCondition,
            TypeCondition,
            BitwiseCondition,
            ExprCondition,
            JsonSchemaCondition,
            AndCondition,
            OrCondition,
        ),
    )


def _regex_options_from_pattern(pattern: re.Pattern[str]) -> str:
    options = ""
    if pattern.flags & re.IGNORECASE:
        options += "i"
    if pattern.flags & re.MULTILINE:
        options += "m"
    if pattern.flags & re.DOTALL:
        options += "s"
    if pattern.flags & re.VERBOSE:
        options += "x"
    return options


def _normalize_type_specifier(type_spec: Any) -> tuple[str, ...]:
    if isinstance(type_spec, bool):
        raise ValueError("$type no acepta booleanos como identificadores de tipo")
    if isinstance(type_spec, int):
        numeric_mapping = {
            -1: ("minKey",),
            1: ("double",),
            2: ("string",),
            3: ("object",),
            4: ("array",),
            5: ("binData",),
            6: ("dbPointer",),
            7: ("objectId",),
            8: ("bool",),
            9: ("date",),
            10: ("null",),
            11: ("regex",),
            13: ("javascript",),
            14: ("symbol",),
            15: ("javascriptWithScope",),
            16: ("int",),
            17: ("timestamp",),
            18: ("long",),
            19: ("decimal",),
            127: ("maxKey",),
        }
        if type_spec not in numeric_mapping:
            raise ValueError("$type usa un codigo BSON no soportado")
        return numeric_mapping[type_spec]
    if not isinstance(type_spec, str):
        raise ValueError("$type necesita alias string o codigo entero BSON")
    alias_mapping = {
        "double": ("double",),
        "string": ("string",),
        "object": ("object",),
        "array": ("array",),
        "bindata": ("binData",),
        "objectid": ("objectId",),
        "bool": ("bool",),
        "date": ("date",),
        "null": ("null",),
        "regex": ("regex",),
        "dbpointer": ("dbPointer",),
        "javascript": ("javascript",),
        "symbol": ("symbol",),
        "javascriptwithscope": ("javascriptWithScope",),
        "minkey": ("minKey",),
        "maxkey": ("maxKey",),
        "int": ("int",),
        "timestamp": ("timestamp",),
        "long": ("long",),
        "decimal": ("decimal",),
        "undefined": ("undefined",),
        "number": ("double", "int", "long", "decimal"),
    }
    normalized = type_spec.strip().casefold()
    if normalized not in alias_mapping:
        raise ValueError("$type usa un alias BSON no soportado")
    return alias_mapping[normalized]


def _normalize_type_aliases(type_specs: tuple[BsonValue, ...]) -> frozenset[str]:
    aliases: set[str] = set()
    for type_spec in type_specs:
        aliases.update(_normalize_type_specifier(type_spec))
    return frozenset(aliases)


def _coerce_bitwise_mask(operand: Any) -> int:
    int64_max = (1 << 63) - 1
    if isinstance(operand, bool):
        raise ValueError("bitwise query operators do not accept boolean masks")
    if isinstance(operand, int):
        if operand < 0 or operand > int64_max:
            raise ValueError("numeric bitmasks must be non-negative signed 64-bit integers")
        return operand
    if isinstance(operand, bytes):
        return int.from_bytes(operand, byteorder="little", signed=False)
    if isinstance(operand, uuid.UUID):
        return int.from_bytes(operand.bytes, byteorder="little", signed=False)
    if isinstance(operand, list):
        mask = 0
        for position in operand:
            if not isinstance(position, int) or isinstance(position, bool) or position < 0:
                raise ValueError("bit position lists must contain non-negative integers")
            if position > 63:
                raise ValueError("bit position lists must target signed 64-bit integers")
            mask |= 1 << position
        return mask
    raise ValueError("bitwise query operators require a numeric mask, BinData, or list of bit positions")


def _compile_field_condition(
    field: str,
    condition: Any,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    depth: int = 0,
) -> QueryNode:
    if isinstance(condition, Regex):
        return RegexCondition(
            field,
            condition.pattern,
            condition.flags,
        )
    if isinstance(condition, re.Pattern):
        return RegexCondition(
            field,
            condition.pattern,
            _regex_options_from_pattern(condition),
        )

    if not isinstance(condition, dict) or not any(isinstance(key, str) and key.startswith("$") for key in condition):
        return EqualsCondition(
            field,
            condition,
            null_matches_undefined=condition is None and dialect.policy.null_query_matches_undefined(),
        )

    clauses: list[QueryNode] = []
    regex_value: Any = None
    regex_options = ""
    for operator, value in condition.items():
        if not dialect.supports_query_field_operator(operator):
            raise OperationFailure(f"Unsupported query operator: {operator}")
        if operator == "$eq":
            if isinstance(value, Regex):
                clauses.append(RegexCondition(field, value.pattern, value.flags))
            elif isinstance(value, re.Pattern):
                clauses.append(RegexCondition(field, value.pattern, _regex_options_from_pattern(value)))
            else:
                clauses.append(
                    EqualsCondition(
                        field,
                        value,
                        null_matches_undefined=value is None and dialect.policy.null_query_matches_undefined(),
                    )
                )
        elif operator == "$ne":
            clauses.append(NotEqualsCondition(field, value))
        elif operator == "$gt":
            clauses.append(GreaterThanCondition(field, value))
        elif operator == "$gte":
            clauses.append(GreaterThanOrEqualCondition(field, value))
        elif operator == "$lt":
            clauses.append(LessThanCondition(field, value))
        elif operator == "$lte":
            clauses.append(LessThanOrEqualCondition(field, value))
        elif operator == "$in":
            if not isinstance(value, (list, tuple)):
                raise ValueError("$in necesita una lista")
            clauses.append(
                InCondition(
                    field,
                    tuple(value),
                    null_matches_undefined=any(item is None for item in value)
                    and dialect.policy.null_query_matches_undefined(),
                )
            )
        elif operator == "$nin":
            if not isinstance(value, (list, tuple)):
                raise ValueError("$nin necesita una lista")
            clauses.append(
                NotInCondition(
                    field,
                    tuple(value),
                    null_matches_undefined=any(item is None for item in value)
                    and dialect.policy.null_query_matches_undefined(),
                )
            )
        elif operator == "$all":
            if not isinstance(value, (list, tuple)):
                raise ValueError("$all necesita una lista")
            clauses.append(AllCondition(field, tuple(value)))
        elif operator == "$size":
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError("$size necesita un entero no negativo")
            clauses.append(SizeCondition(field, value))
        elif operator == "$mod":
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                raise ValueError("$mod necesita una lista de dos numeros")
            divisor, remainder = value
            if (
                not isinstance(divisor, (int, float))
                or isinstance(divisor, bool)
                or not math.isfinite(divisor)
                or divisor == 0
            ):
                raise ValueError("$mod necesita un divisor numerico distinto de cero")
            if not isinstance(remainder, (int, float)) or isinstance(remainder, bool):
                raise ValueError("$mod necesita un resto numerico")
            clauses.append(ModCondition(field, divisor, remainder))
        elif operator == "$regex":
            if isinstance(value, Regex):
                regex_value = value.pattern
                regex_options += value.flags
            elif isinstance(value, re.Pattern):
                regex_value = value.pattern
                regex_options += _regex_options_from_pattern(value)
            else:
                if not isinstance(value, str):
                    raise ValueError("$regex necesita un patron string o regex compilada")
                regex_value = value
        elif operator == "$options":
            if not isinstance(value, str):
                raise ValueError("$options necesita un string")
            regex_options += value
        elif operator == "$not":
            if not isinstance(value, dict) or not value or not all(isinstance(key, str) and key.startswith("$") for key in value):
                raise ValueError("$not necesita una expresion de operador")
            clauses.append(
                NotCondition(
                    _compile_field_condition(
                        field,
                        value,
                        dialect=dialect,
                        depth=depth + 1,
                    )
                )
            )
        elif operator == "$elemMatch":
            if not isinstance(value, dict):
                raise ValueError("$elemMatch necesita una expresion de filtro")
            operator_keys = [
                key
                for key in value
                if isinstance(key, str) and key.startswith("$")
            ]
            compiled_plan: QueryNode | None = None
            wrap_value = False
            try:
                if operator_keys and len(operator_keys) == len(value):
                    compiled_plan = _compile_field_condition("value", value, dialect=dialect, depth=depth + 1)
                    wrap_value = True
                elif not operator_keys:
                    compiled_plan = compile_filter(value, dialect=dialect, _depth=depth + 1)
            except (OperationFailure, ValueError, TypeError):
                compiled_plan = None
                wrap_value = False
            clauses.append(
                ElemMatchCondition(
                    field,
                    value,
                    compiled_plan=compiled_plan,
                    compiled_dialect_key=dialect.key if compiled_plan is not None else None,
                    wrap_value=wrap_value,
                )
            )
        elif operator == "$exists":
            clauses.append(ExistsCondition(field, bool(value)))
        elif operator == "$type":
            if isinstance(value, (list, tuple)):
                if not value:
                    raise ValueError("$type necesita al menos un tipo")
                compiled_values = tuple(value)
                clauses.append(TypeCondition(field, compiled_values, aliases=_normalize_type_aliases(compiled_values)))
            else:
                clauses.append(TypeCondition(field, (value,), aliases=_normalize_type_aliases((value,))))
        elif operator in {"$bitsAllSet", "$bitsAnySet", "$bitsAllClear", "$bitsAnyClear"}:
            clauses.append(BitwiseCondition(field, operator, value, mask=_coerce_bitwise_mask(value)))
        else:
            raise OperationFailure(f"Unsupported query operator: {operator}")

    if regex_options and regex_value is None:
        raise OperationFailure("$options requires $regex")
    if regex_value is not None:
        clauses.append(RegexCondition(field, regex_value, regex_options))

    if len(clauses) == 1:
        return clauses[0]
    return AndCondition(tuple(clauses))


def compile_filter(
    filter_spec: Filter,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables: BsonBindings | None = None,
    planning_mode: PlanningMode = PlanningMode.STRICT,
    _depth: int = 0,
) -> QueryNode:
    try:
        return _compile_filter_strict(
            filter_spec,
            dialect=dialect,
            variables=variables,
            planning_mode=planning_mode,
            depth=_depth,
        )
    except (OperationFailure, ValueError, TypeError) as exc:
        if planning_mode is PlanningMode.RELAXED:
            return DeferredQueryNode(PlanningIssue(scope="query", message=str(exc)))
        raise


def _compile_filter_strict(
    filter_spec: Filter,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables: BsonBindings | None = None,
    planning_mode: PlanningMode = PlanningMode.STRICT,
    depth: int = 0,
) -> QueryNode:
    if depth > 100:
        raise OperationFailure("query filter exceeds maximum nesting depth")
    if filter_spec is None:
        return MatchAll()
    if not isinstance(filter_spec, dict):
        raise ValueError("filter_spec must be a document")
    if not filter_spec:
        return MatchAll()

    clauses: list[QueryNode] = []
    for key, value in filter_spec.items():
        if isinstance(key, str) and key.startswith("$") and not dialect.supports_query_top_level_operator(key):
            raise OperationFailure(f"Unsupported top-level query operator: {key}")
        if key == "$comment":
            continue
        if key == "$expr":
            clauses.append(ExprCondition(value, {} if variables is None else dict(variables)))
            continue
        if key == "$jsonSchema":
            if not isinstance(value, dict):
                raise OperationFailure("$jsonSchema validator must be a document")
            from mongoeco.core.schema_validation import CompiledJsonSchema

            CompiledJsonSchema(value)
            clauses.append(JsonSchemaCondition(value, compiled_schema=CompiledJsonSchema(value)))
            continue
        if key == "$and":
            if not isinstance(value, list):
                raise ValueError("$and necesita una lista de filtros")
            if not value:
                raise ValueError("$and debe recibir una lista no vacia")
            clauses.append(
                AndCondition(
                    tuple(
                        compile_filter(
                            clause,
                            dialect=dialect,
                            variables=variables,
                            planning_mode=planning_mode,
                            _depth=depth + 1,
                        )
                        for clause in value
                    )
                )
            )
            continue
        if key == "$or":
            if not isinstance(value, list):
                raise ValueError("$or necesita una lista de filtros")
            if not value:
                raise ValueError("$or debe recibir una lista no vacia")
            clauses.append(
                OrCondition(
                    tuple(
                        compile_filter(
                            clause,
                            dialect=dialect,
                            variables=variables,
                            planning_mode=planning_mode,
                            _depth=depth + 1,
                        )
                        for clause in value
                    )
                )
            )
            continue
        if key == "$nor":
            if not isinstance(value, list):
                raise ValueError("$nor necesita una lista de filtros")
            if not value:
                raise ValueError("$nor debe recibir una lista no vacia")
            clauses.append(
                NotCondition(
                    OrCondition(
                        tuple(
                            compile_filter(
                                clause,
                                dialect=dialect,
                                variables=variables,
                                planning_mode=planning_mode,
                                _depth=depth + 1,
                            )
                            for clause in value
                        )
                    )
                )
            )
            continue
        if isinstance(key, str) and key.startswith("$"):
            raise OperationFailure(f"Unsupported top-level query operator: {key}")
        clauses.append(_compile_field_condition(key, value, dialect=dialect, depth=depth))

    if not clauses:
        return MatchAll()
    if len(clauses) == 1:
        return clauses[0]
    return AndCondition(tuple(clauses))


def ensure_query_plan(
    filter_spec: Filter | None = None,
    plan: QueryNode | None = None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    variables: BsonBindings | None = None,
    planning_mode: PlanningMode = PlanningMode.STRICT,
) -> QueryNode:
    if plan is not None:
        return plan
    if filter_spec is None:
        return MatchAll()
    return compile_filter(filter_spec, dialect=dialect, variables=variables, planning_mode=planning_mode)
