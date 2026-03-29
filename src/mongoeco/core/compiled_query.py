from typing import Any
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec, compare_with_collation, values_equal_with_collation
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExprCondition,
    ExistsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NotEqualsCondition,
    NotCondition,
    NotInCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
)
from mongoeco.types import Document


class CompiledQuery:
    """Compile a QueryNode into a reusable Python matcher."""

    def __init__(
        self,
        plan: QueryNode,
        *,
        dialect: MongoDialect = MONGODB_DIALECT_70,
        collation: CollationSpec | None = None,
        variable_prefix: str = "",
    ) -> None:
        self.plan = plan
        self.dialect = dialect
        self.collation = collation
        self._variable_prefix = variable_prefix
        self._context: dict[str, Any] = {
            "dialect": dialect,
            "collation": collation,
            "compare": compare_with_collation,
            "values_equal": values_equal_with_collation,
        }

        from mongoeco.core.filtering import QueryEngine
        self._context["extract"] = QueryEngine.extract_values
        self._context["eq_matches"] = QueryEngine._query_equality_matches
        self._context["top_eq"] = QueryEngine._match_top_level_equals
        self._context["top_compare"] = QueryEngine._match_top_level_comparison
        self._context["in_matches"] = QueryEngine._in_item_matches_candidate
        self._context["match_plan"] = QueryEngine.match_plan

        self._match_func = self._compile(plan)

    def match(self, document: Document) -> bool:
        return self._match_func(document)

    def get_inline_code(self, prefix: str | None = None) -> str:
        if prefix is not None:
            self._variable_prefix = prefix
        return self._node_to_code(self.plan, depth=0)

    def _compile(self, node: QueryNode) -> Any:
        expression = self.get_inline_code()
        # Local variable binding for performance
        function_code = (
            "def match_logic(doc):\n"
            "    _extract = extract\n"
            "    _eq_matches = eq_matches\n"
            "    _top_eq = top_eq\n"
            "    _top_compare = top_compare\n"
            "    _in_matches = in_matches\n"
            "    _compare = compare\n"
            "    _values_equal = values_equal\n"
            "    try:\n"
            f"        return {expression}\n"
            "    except (KeyError, TypeError, AttributeError):\n"
            "        return False"
        )

        local_vars: dict[str, Any] = {}
        exec(function_code, self._context, local_vars)
        return local_vars["match_logic"]

    def _node_to_code(self, node: QueryNode, depth: int) -> str:
        match node:
            case MatchAll():
                return "True"
            case EqualsCondition(field=field, value=value, null_matches_undefined=null_matches_undefined):
                field_key = self._store(depth, "field", field)
                value_key = self._store(depth, "value", value)
                if "." not in field:
                    return (
                        f"_top_eq(doc, {field_key}, {value_key}, "
                        f"null_matches_undefined={null_matches_undefined}, "
                        "dialect=dialect, collation=collation)"
                    )
                return (
                    "any("
                    f"_eq_matches(candidate, {value_key}, "
                    f"null_matches_undefined={null_matches_undefined}, "
                    "dialect=dialect, collation=collation)"
                    f" for candidate in (_extract(doc, {field_key}) or [None])"
                    ")"
                )
            case NotEqualsCondition(field=field, value=value):
                field_key = self._store(depth, "field", field)
                value_key = self._store(depth, "value", value)
                return (
                    "all("
                    f"not _values_equal(candidate, {value_key}, dialect=dialect, collation=collation)"
                    f" for candidate in (_extract(doc, {field_key}) or [None])"
                    ")"
                )
            case GreaterThanCondition(field=field, value=value):
                return self._comparison_code(depth, field, value, ">")
            case GreaterThanOrEqualCondition(field=field, value=value):
                return self._comparison_code(depth, field, value, ">=")
            case LessThanCondition(field=field, value=value):
                return self._comparison_code(depth, field, value, "<")
            case LessThanOrEqualCondition(field=field, value=value):
                return self._comparison_code(depth, field, value, "<=")
            case InCondition(field=field, values=values, null_matches_undefined=null_matches_undefined):
                field_key = self._store(depth, "field", field)
                value_key = self._store(depth, "values", values)
                return (
                    "any("
                    f"_in_matches(candidate, item, null_matches_undefined={null_matches_undefined}, "
                    "dialect=dialect, collation=collation)"
                    f" for candidate in (_extract(doc, {field_key}) or [None])"
                    f" for item in {value_key}"
                    ")"
                )
            case NotInCondition(field=field, values=values):
                field_key = self._store(depth, "field", field)
                value_key = self._store(depth, "values", values)
                return (
                    "not any("
                    "_in_matches(candidate, item, null_matches_undefined=False, "
                    "dialect=dialect, collation=collation)"
                    f" for candidate in (_extract(doc, {field_key}) or [None])"
                    f" for item in {value_key}"
                    ")"
                )
            case AndCondition(clauses=clauses):
                if not clauses:
                    return "True"
                return "(" + " and ".join(
                    self._node_to_code(clause, depth + index + 1)
                    for index, clause in enumerate(clauses)
                ) + ")"
            case OrCondition(clauses=clauses):
                if not clauses:
                    return "False"
                return "(" + " or ".join(
                    self._node_to_code(clause, depth + index + 1)
                    for index, clause in enumerate(clauses)
                ) + ")"
            case NotCondition(clause=clause):
                return f"not ({self._node_to_code(clause, depth + 1)})"
            case ExistsCondition(field=field, value=value):
                field_key = self._store(depth, "field", field)
                return f"bool(_extract(doc, {field_key})) == {value}"
            case _:
                node_key = self._store(depth, "node", node)
                return f"match_plan(doc, {node_key}, dialect=dialect, collation=collation)"

    def _comparison_code(self, depth: int, field: str, value: Any, operator: str) -> str:
        field_key = self._store(depth, "field", field)
        value_key = self._store(depth, "value", value)
        if "." not in field:
            return (
                f"_top_compare(doc, {field_key}, {value_key}, {operator!r}, "
                "dialect=dialect, collation=collation)"
            )
        return (
            "any("
            f"_compare(candidate, {value_key}, dialect=dialect, collation=collation) {operator} 0"
            f" for candidate in _extract(doc, {field_key})"
            ")"
        )

    def _store(self, depth: int, prefix: str, value: Any) -> str:
        key = f"{self._variable_prefix}{prefix}_{depth}"
        self._context[key] = value
        return key
