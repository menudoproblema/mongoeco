from copy import deepcopy
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.accumulators import (
    _AverageAccumulator,
    _sum_accumulator_operand,
    _validate_accumulator_expression,
)
from mongoeco.core.aggregation.runtime import _aggregation_key, evaluate_expression
from mongoeco.core.bson_scalars import bson_add, is_bson_numeric, unwrap_bson_numeric
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


class CompiledGroup:
    """Compile a subset of `$group` into a reusable Python callable."""

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
        self._context: dict[str, Any] = {
            "dialect": dialect,
            "bson_add": bson_add,
            "is_numeric": is_bson_numeric,
            "sum_operand": _sum_accumulator_operand,
            "unwrap": unwrap_bson_numeric,
            "evaluate_expression": evaluate_expression,
            "_aggregation_key": _aggregation_key,
            "deepcopy": deepcopy,
            "_AverageAccumulator": _AverageAccumulator,
        }
        self._aggregate_func = self._compile()

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

    def _compile(self) -> Any:
        lines = [
            "def aggregate_func(documents, variables):",
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
        """Generate only the body of the aggregation loop."""
        lines = []
        accumulator_fields = list(self.accumulator_specs.keys())
        accumulator_count = len(accumulator_fields)

        id_code = self._compile_expression(self.id_expr, "id_expr")
        lines.append(f"group_id = {id_code}")
        lines.append("group_key = _aggregation_key(group_id)")

        initial_states: list[str] = []
        for field in accumulator_fields:
            accumulator_spec = self.accumulator_specs[field]
            operator, _expression = next(iter(accumulator_spec.items()))
            if operator == "$avg":
                initial_states.append("_AverageAccumulator()")
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

        for index, field in enumerate(accumulator_fields):
            accumulator_spec = self.accumulator_specs[field]
            operator, expression = next(iter(accumulator_spec.items()))

            if operator == "$count":
                lines.append(f"state[{index}] += 1")
                continue

            expression_code = self._compile_expression(expression, f"acc_{index}")
            lines.append(f"value = {expression_code}")

            match operator:
                case "$sum":
                    lines.append("operand = sum_operand(value)")
                    lines.append("if operand is not None:")
                    lines.append(
                        f"    state[{index}] = bson_add(state[{index}], operand)"
                    )
                case "$min" | "$max":
                    comparison = "<" if operator == "$min" else ">"
                    lines.append("if value is not None:")
                    lines.append(
                        f"    if not state[{accumulator_count + 1}].get({field!r}) or "
                        f"dialect.policy.compare_values(value, state[{index}]) {comparison} 0:"
                    )
                    lines.append(
                        f"        state[{index}] = value if isinstance(value, (int, float, str, bool)) else deepcopy(value)"
                    )
                    lines.append(
                        f"        state[{accumulator_count + 1}][{field!r}] = True"
                    )
                case "$avg":
                    lines.append("if value is not None and is_numeric(value):")
                    lines.append(
                        f"    state[{index}].total = bson_add(state[{index}].total, value)"
                    )
                    lines.append(f"    state[{index}].count += 1")
                case "$first":
                    lines.append(
                        f"    if not state[{accumulator_count + 1}].get({field!r}):"
                    )
                    lines.append(
                        f"        state[{index}] = value if isinstance(value, (int, float, str, bool)) else deepcopy(value)"
                    )
                    lines.append(
                        f"        state[{accumulator_count + 1}][{field!r}] = True"
                    )
                case "$last":
                    lines.append(
                        f"    state[{index}] = value if isinstance(value, (int, float, str, bool)) else deepcopy(value)"
                    )
        return lines

    def _compile_finalization_only(self) -> list[str]:
        """Generate the group finalization logic."""
        accumulator_fields = list(self.accumulator_specs.keys())
        accumulator_count = len(accumulator_fields)
        lines = [
            "    results = []",
            "    for state in groups.values():",
            f"        result = {{'_id': deepcopy(state[{accumulator_count}])}}",
        ]
        for index, field in enumerate(accumulator_fields):
            operator, _expression = next(iter(self.accumulator_specs[field].items()))
            if operator == "$avg":
                lines.append(f"        avg_state = state[{index}]")
                lines.append(
                    f"        result[{field!r}] = None if avg_state.count == 0 else unwrap(avg_state.total) / avg_state.count"
                )
            else:
                lines.append(f"        result[{field!r}] = state[{index}]")
        lines.extend(
            [
                "        results.append(result)",
                "    return results",
            ]
        )
        return lines

    def _compile_expression(self, expr: Any, prefix: str) -> str:
        """Translate a MongoDB expression into Python code when safe."""
        if isinstance(expr, str):
            if expr.startswith("$$"):
                key = f"{prefix}_var"
                self._context[key] = expr
                return f"evaluate_expression(doc, {key}, variables, dialect=dialect)"
            if expr.startswith("$"):
                path = expr[1:]
                if "." not in path:
                    return f"doc.get({path!r})"
                key = f"{prefix}_path"
                self._context[key] = expr
                return f"evaluate_expression(doc, {key}, variables, dialect=dialect)"
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
                            return f"({then_c} if {if_c} else {else_c})"
                        elif isinstance(val, dict) and all(k in val for k in ("if", "then", "else")):
                            if_c = self._compile_expression(val["if"], prefix)
                            then_c = self._compile_expression(val["then"], prefix)
                            else_c = self._compile_expression(val["else"], prefix)
                            return f"({then_c} if {if_c} else {else_c})"
                    case "$ifNull" if isinstance(val, list) and len(val) == 2:
                        input_c = self._compile_expression(val[0], prefix)
                        fallback_c = self._compile_expression(val[1], prefix)
                        return f"({input_c} if {input_c} is not None else {fallback_c})"

        key = f"{prefix}_expr"
        self._context[key] = expr
        return f"evaluate_expression(doc, {key}, variables, dialect=dialect)"
