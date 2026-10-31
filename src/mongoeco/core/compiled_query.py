from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec
from mongoeco.core.query_plan import (
    AndCondition,
    MatchAll,
    NotCondition,
    OrCondition,
    QueryNode,
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
    ) -> None:
        self.plan = plan
        self.dialect = dialect
        self.collation = collation
        self._values: list[Any] = []
        self._store_counter = 0

        self._match_func = self._compile(plan)

    def match(
        self,
        document: Document,
        *,
        variables: Mapping[str, Any] | None = None,
    ) -> bool:
        return self._match_func(
            document,
            self._values,
            self.dialect,
            self.collation,
            variables,
        )

    def get_inline_code(self) -> str:
        self._store_counter = 0
        self._values = []
        return self._node_to_code(self.plan, depth=0)

    def _compile(self, node: QueryNode) -> Any:
        expression = self.get_inline_code()
        return self._compile_expression(expression)

    @staticmethod
    @lru_cache(maxsize=2048)
    def _compile_expression(expression: str) -> Any:
        from mongoeco.core.filtering import QueryEngine

        context: dict[str, Any] = {"query_engine": QueryEngine}
        function_code = (
            "def match_logic(doc, values, dialect, collation, variables):\n"
            f"    return {expression}\n"
        )

        local_vars: dict[str, Any] = {}
        exec(function_code, context, local_vars)
        return local_vars["match_logic"]

    def _node_to_code(self, node: QueryNode, depth: int) -> str:
        match node:
            case MatchAll():
                return "True"
            case AndCondition(clauses=clauses):
                if not clauses:
                    return "True"
                return (
                    "("
                    + " and ".join(
                        self._node_to_code(clause, depth + index + 1)
                        for index, clause in enumerate(clauses)
                    )
                    + ")"
                )
            case OrCondition(clauses=clauses):
                if not clauses:
                    return "False"
                return (
                    "("
                    + " or ".join(
                        self._node_to_code(clause, depth + index + 1)
                        for index, clause in enumerate(clauses)
                    )
                    + ")"
                )
            case NotCondition(clause=clause):
                return f"not ({self._node_to_code(clause, depth + 1)})"
            case _:
                node_key = self._store("node", node)
                return (
                    f"query_engine.match_plan(doc, {node_key}, dialect=dialect, "
                    "collation=collation, variables=variables)"
                )

    def _store(self, prefix: str, value: Any) -> str:
        del prefix
        key = f"values[{self._store_counter}]"
        self._store_counter += 1
        self._values.append(value)
        return key
