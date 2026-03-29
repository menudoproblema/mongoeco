import datetime
from copy import deepcopy
import re
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.accumulators import (
    _AverageAccumulator,
    _sum_accumulator_operand,
    _validate_accumulator_expression,
)
from mongoeco.core.aggregation.numeric_expressions import _require_numeric, _subtract_values
from mongoeco.core.aggregation.runtime import _aggregation_key, evaluate_expression
from mongoeco.core.bson_scalars import bson_add, bson_multiply, is_bson_numeric, unwrap_bson_numeric
from mongoeco.errors import OperationFailure
from mongoeco.types import Document


_SUPPORTED_GROUP_ACCUMULATORS = frozenset(
    {
        "$avg",
        "$count",
        "$first",
        "$last",
        "$max",
        "$min",
        "$sum",
    }
)


def _compiled_add(values: list[Any]) -> Any:
    if any(value is None for value in values):
        return None
    date_values = [value for value in values if isinstance(value, datetime.datetime)]
    if date_values:
        if len(date_values) != 1:
            raise OperationFailure("$add only supports a single date argument")
        result = date_values[0]
        for value in values:
            if isinstance(value, datetime.datetime):
                continue
            result += datetime.timedelta(milliseconds=unwrap_bson_numeric(_require_numeric("$add", value)))
        return result
    result = _require_numeric("$add", values[0])
    for value in values[1:]:
        result = bson_add(result, _require_numeric("$add", value))
    return result


def _compiled_multiply(values: list[Any]) -> Any:
    if any(value is None for value in values):
        return None
    result = _require_numeric("$multiply", values[0])
    for value in values[1:]:
        result = bson_multiply(result, _require_numeric("$multiply", value))
    return result


def _compiled_subtract(left: Any, right: Any) -> Any:
    if left is None or right is None:
        return None
    return _subtract_values(left, right)


class CompiledGroup:
    """Compile a subset of `$group` into a reusable Python callable."""
    _COMPILED_FUNCTION_CACHE: dict[tuple[Any, int], Any] = {}
    _COMPILED_FUNCTION_CACHE_MAXSIZE = 128

    def __init__(
        self,
        spec: dict[str, Any],
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
    ) -> None:
        self.spec = spec
        self.dialect = dialect
        self.id_expr = spec["_id"]
        self.accumulator_specs = {key: value for key, value in spec.items() if key != "_id"}
        self._compiled_accumulators = [
            (field, *next(iter(accumulator_spec.items())))
            for field, accumulator_spec in self.accumulator_specs.items()
        ]
        self._context_counter = 0
        self._context: dict[str, Any] = {
            "dialect": dialect,
            "bson_add": bson_add,
            "is_numeric": is_bson_numeric,
            "sum_operand": _sum_accumulator_operand,
            "unwrap": unwrap_bson_numeric,
            "evaluate_expression": evaluate_expression,
            "truthy": dialect.policy.expression_truthy,
            "_aggregation_key": _aggregation_key,
            "deepcopy": deepcopy,
            "_AverageAccumulator": _AverageAccumulator,
            "_compiled_add": _compiled_add,
            "_compiled_multiply": _compiled_multiply,
            "_compiled_subtract": _compiled_subtract,
        }
        cache_key = self._cache_key(spec, dialect)
        cached = self._COMPILED_FUNCTION_CACHE.get(cache_key)
        if cached is None:
            cached = self._compile()
            if len(self._COMPILED_FUNCTION_CACHE) >= self._COMPILED_FUNCTION_CACHE_MAXSIZE:
                self._COMPILED_FUNCTION_CACHE.pop(next(iter(self._COMPILED_FUNCTION_CACHE)))
        else:
            self._COMPILED_FUNCTION_CACHE.pop(cache_key)
        self._COMPILED_FUNCTION_CACHE[cache_key] = cached
        self._aggregate_func = cached

    @classmethod
    def supports(cls, spec: object) -> bool:
        if not isinstance(spec, dict) or "_id" not in spec:
            return False
        for field, accumulator_spec in spec.items():
            if field == "_id":
                continue
            if not isinstance(accumulator_spec, dict) or len(accumulator_spec) != 1:
                return False
            operator, expression = next(iter(accumulator_spec.items()))
            if operator not in _SUPPORTED_GROUP_ACCUMULATORS:
                return False
            try:
                _validate_accumulator_expression(operator, expression)
            except OperationFailure:
                return False
        return True

    def apply(
        self,
        documents: list[Document],
        variables: dict[str, Any] | None = None,
    ) -> list[Document]:
        return self._aggregate_func(documents, variables)

    @classmethod
    def _cache_key(cls, spec: dict[str, Any], dialect: MongoDialect) -> tuple[Any, int]:
        return cls._freeze_cache_value(spec), id(dialect)

    @classmethod
    def _freeze_cache_value(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return ("dict", tuple((key, cls._freeze_cache_value(item)) for key, item in value.items()))
        if isinstance(value, list):
            return ("list", tuple(cls._freeze_cache_value(item) for item in value))
        if isinstance(value, tuple):
            return ("tuple", tuple(cls._freeze_cache_value(item) for item in value))
        if isinstance(value, set | frozenset):
            return ("set", tuple(sorted(cls._freeze_cache_value(item) for item in value)))
        if isinstance(value, re.Pattern):
            return ("regex", value.pattern, value.flags)
        try:
            hash(value)
            return value
        except TypeError:
            return ("repr", repr(value))

    def _store_context_value(self, prefix: str, suffix: str, value: Any) -> str:
        key = f"{prefix}_{suffix}_{self._context_counter}"
        self._context_counter += 1
        self._context[key] = value
        return key

    def _compile(self) -> Any:
        lines = [
            "def aggregate_func(documents, variables):",
            "    _bson_add = bson_add",
            "    _is_numeric = is_numeric",
            "    _sum_operand = sum_operand",
            "    _unwrap = unwrap",
            "    _evaluate = evaluate_expression",
            "    _truthy = truthy",
            "    _agg_key = _aggregation_key",
            "    _deepcopy = deepcopy",
            "    _compare = dialect.policy.compare_values",
            "    _avg_acc = _AverageAccumulator",
            "    groups = {}",
            "    for doc in documents:",
        ]

        logic_lines = self._compile_logic_only()
        for line in logic_lines:
            lines.append(f"        {line}")

        final_lines = self._compile_finalization_only()
        lines.extend(final_lines)

        function_code = "\n".join(lines)
        local_vars: dict[str, Any] = {}
        exec(function_code, self._context, local_vars)
        return local_vars["aggregate_func"]

    def _compile_logic_only(self) -> list[str]:
        """Generate the body of the aggregation loop."""
        lines = []
        accumulator_fields = [field for field, _operator, _expression in self._compiled_accumulators]
        accumulator_count = len(accumulator_fields)

        if isinstance(self.id_expr, str) and self.id_expr.startswith("$") and not self.id_expr.startswith("$$") and "." not in self.id_expr:
            field_name = self.id_expr[1:]
            lines.append(f"group_id = doc.get({field_name!r})")
        else:
            id_expr_key = self._store_context_value("group", "id_expr", self.id_expr)
            lines.append(f"group_id = _evaluate(doc, {id_expr_key}, variables, dialect=dialect)")

        lines.append("group_key = _agg_key(group_id)")

        initial_states: list[str] = []
        for _field, operator, _expression in self._compiled_accumulators:
            if operator == "$avg":
                initial_states.append("_avg_acc()")
            elif operator in {"$sum", "$count"}:
                initial_states.append("0")
            else:
                initial_states.append("None")

        lines.append("if group_key not in groups:")
        lines.append(
            "    groups[group_key] = ["
            + ", ".join(initial_states)
            + ", group_id, {}]"
        )
        lines.append("state = groups[group_key]")

        for index, (field, operator, expression) in enumerate(self._compiled_accumulators):

            if operator == "$count":
                lines.append(f"state[{index}] += 1")
                continue

            expression_code = self._compile_expression(expression, f"acc_{index}")
            lines.append(f"value = {expression_code}")

            match operator:
                case "$sum":
                    if expression == 1:
                        lines.append(f"state[{index}] += 1")
                    else:
                        lines.append("operand = _sum_operand(value)")
                        lines.append("if operand is not None:")
                        lines.append(f"    state[{index}] = _bson_add(state[{index}], operand)")
                case "$min" | "$max":
                    comparison = "<" if operator == "$min" else ">"
                    lines.append("if value is not None:")
                    lines.append(
                        f"    if not state[{accumulator_count + 1}].get({field!r}) or "
                        f"_compare(value, state[{index}]) {comparison} 0:"
                    )
                    lines.append(
                        f"        state[{index}] = value if isinstance(value, (int, float, str, bool)) else _deepcopy(value)"
                    )
                    lines.append(f"        state[{accumulator_count + 1}][{field!r}] = True")
                case "$avg":
                    lines.append("if value is not None and _is_numeric(value):")
                    lines.append(f"    state[{index}].total = _bson_add(state[{index}].total, value)")
                    lines.append(f"    state[{index}].count += 1")
                case "$first":
                    lines.append(f"if not state[{accumulator_count + 1}].get({field!r}):")
                    lines.append(
                        f"    state[{index}] = value if isinstance(value, (int, float, str, bool)) else _deepcopy(value)"
                    )
                    lines.append(f"    state[{accumulator_count + 1}][{field!r}] = True")
                case "$last":
                    lines.append(f"state[{index}] = value if isinstance(value, (int, float, str, bool)) else _deepcopy(value)")

        return lines

    def _compile_finalization_only(self) -> list[str]:
        """Generate the group finalization logic."""
        accumulator_fields = [field for field, _operator, _expression in self._compiled_accumulators]
        accumulator_count = len(accumulator_fields)
        lines = [
            "    results = []",
            "    for state in groups.values():",
            f"        result = {{'_id': _deepcopy(state[{accumulator_count}])}}",
        ]
        for index, (field, operator, _expression) in enumerate(self._compiled_accumulators):
            if operator == "$avg":
                lines.append(f"        avg_state = state[{index}]")
                lines.append(
                    f"        result[{field!r}] = None if avg_state.count == 0 else _unwrap(avg_state.total) / avg_state.count"
                )
            else:
                lines.append(f"        result[{field!r}] = state[{index}]")
        lines.extend(["        results.append(result)", "    return results"])
        return lines

    def _compile_expression(self, expr: Any, prefix: str) -> str:
        """Translate a MongoDB expression into Python code when safe."""
        if isinstance(expr, str):
            if expr.startswith("$$"):
                key = self._store_context_value(prefix, "var", expr)
                return f"_evaluate(doc, {key}, variables, dialect=dialect)"
            if expr.startswith("$"):
                path = expr[1:]
                if "." not in path:
                    return f"doc.get({path!r})"
                key = self._store_context_value(prefix, "path", expr)
                return f"_evaluate(doc, {key}, variables, dialect=dialect)"
            return repr(expr)

        if not isinstance(expr, dict):
            return repr(expr)

        if len(expr) == 1:
            op, val = next(iter(expr.items()))
            if op.startswith("$"):
                match op:
                    case "$literal":
                        return repr(val)
                    case "$cond":
                        if isinstance(val, list) and len(val) == 3:
                            if_c = self._compile_expression(val[0], prefix)
                            then_c = self._compile_expression(val[1], prefix)
                            else_c = self._compile_expression(val[2], prefix)
                            return f"({then_c} if _truthy({if_c}) else {else_c})"
                        elif isinstance(val, dict) and len(val) == 3 and "if" in val and "then" in val and "else" in val:
                            if_c = self._compile_expression(val["if"], prefix)
                            then_c = self._compile_expression(val["then"], prefix)
                            else_c = self._compile_expression(val["else"], prefix)
                            return f"({then_c} if _truthy({if_c}) else {else_c})"
                    case "$ifNull" if isinstance(val, list) and len(val) == 2:
                        input_c = self._compile_expression(val[0], prefix)
                        fallback_c = self._compile_expression(val[1], prefix)
                        return (
                            f"(lambda __mongoeco_value: "
                            f"__mongoeco_value if __mongoeco_value is not None else {fallback_c})"
                            f"({input_c})"
                        )
                    case "$add" if isinstance(val, list) and len(val) >= 2:
                        return f"_compiled_add([{', '.join(self._compile_expression(item, prefix) for item in val)}])"
                    case "$multiply" if isinstance(val, list) and len(val) >= 2:
                        return f"_compiled_multiply([{', '.join(self._compile_expression(item, prefix) for item in val)}])"
                    case "$subtract" if isinstance(val, list) and len(val) == 2:
                        left_c = self._compile_expression(val[0], prefix)
                        right_c = self._compile_expression(val[1], prefix)
                        return f"_compiled_subtract({left_c}, {right_c})"

        key = self._store_context_value(prefix, "expr", expr)
        return f"_evaluate(doc, {key}, variables, dialect=dialect)"
