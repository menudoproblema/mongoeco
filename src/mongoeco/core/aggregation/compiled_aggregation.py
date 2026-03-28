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
        accumulator_fields = list(self.accumulator_specs.keys())
        accumulator_count = len(accumulator_fields)

        if (
            isinstance(self.id_expr, str)
            and self.id_expr.startswith("$")
            and not self.id_expr.startswith("$$")
            and "." not in self.id_expr
        ):
            lines.append(f"        group_id = doc.get({self.id_expr[1:]!r})")
        else:
            self._context["id_expr"] = self.id_expr
            lines.append(
                "        group_id = evaluate_expression(doc, id_expr, variables, dialect=dialect)"
            )

        lines.append("        group_key = _aggregation_key(group_id)")

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

        lines.append("        if group_key not in groups:")
        lines.append(
            "            groups[group_key] = ["
            + ", ".join(initial_states)
            + ", group_id, {}]"
        )
        lines.append("        state = groups[group_key]")

        for index, field in enumerate(accumulator_fields):
            accumulator_spec = self.accumulator_specs[field]
            operator, expression = next(iter(accumulator_spec.items()))
            expression_line = self._expression_code(
                depth=index,
                expression=expression,
            )
            if expression_line is not None:
                lines.append(f"        {expression_line}")

            match operator:
                case "$sum":
                    if expression == 1:
                        lines.append(f"        state[{index}] += 1")
                    else:
                        lines.append("        operand = sum_operand(value)")
                        lines.append("        if operand is not None:")
                        lines.append(
                            f"            state[{index}] = bson_add(state[{index}], operand)"
                        )
                case "$count":
                    lines.append(f"        state[{index}] += 1")
                case "$min" | "$max":
                    comparison = "<" if operator == "$min" else ">"
                    lines.append("        if value is not None:")
                    lines.append(
                        f"            if not state[{accumulator_count + 1}].get({field!r}) or "
                        f"dialect.policy.compare_values(value, state[{index}]) {comparison} 0:"
                    )
                    lines.append(
                        f"                state[{index}] = value if isinstance(value, (int, float, str, bool)) else deepcopy(value)"
                    )
                    lines.append(
                        f"                state[{accumulator_count + 1}][{field!r}] = True"
                    )
                case "$avg":
                    lines.append("        if value is not None and is_numeric(value):")
                    lines.append(
                        f"            state[{index}].total = bson_add(state[{index}].total, value)"
                    )
                    lines.append(f"            state[{index}].count += 1")
                case "$first":
                    lines.append(
                        f"        if not state[{accumulator_count + 1}].get({field!r}):"
                    )
                    lines.append(
                        f"            state[{index}] = value if isinstance(value, (int, float, str, bool)) else deepcopy(value)"
                    )
                    lines.append(
                        f"            state[{accumulator_count + 1}][{field!r}] = True"
                    )
                case "$last":
                    lines.append(
                        f"        state[{index}] = value if isinstance(value, (int, float, str, bool)) else deepcopy(value)"
                    )

        lines.extend(
            [
                "    results = []",
                "    for state in groups.values():",
                f"        result = {{'_id': deepcopy(state[{accumulator_count}])}}",
            ]
        )
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

        function_code = "\n".join(lines)
        local_vars: dict[str, Any] = {}
        exec(function_code, self._context, local_vars)
        return local_vars["aggregate_func"]

    def _expression_code(self, *, depth: int, expression: object) -> str | None:
        if (
            isinstance(expression, str)
            and expression.startswith("$")
            and not expression.startswith("$$")
            and "." not in expression
        ):
            return f"value = doc.get({expression[1:]!r})"

        key = f"expr_{depth}"
        self._context[key] = expression
        return f"value = evaluate_expression(doc, {key}, variables, dialect=dialect)"
