from functools import lru_cache
from typing import Any
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.collation import CollationSpec, compare_with_collation
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
        variable_prefix: str = '',
    ) -> None:
        self.plan = plan
        self.dialect = dialect
        self.collation = collation
        self._variable_prefix = variable_prefix
        self._values: list[Any] = []
        self._store_counter = 0
        self._local_counter = 0

        self._match_func = self._compile(plan)

    def match(self, document: Document) -> bool:
        return self._match_func(document, self._values, self.dialect, self.collation)

    def get_inline_code(self, prefix: str | None = None) -> str:
        if prefix is not None:
            self._variable_prefix = prefix
        self._store_counter = 0
        self._local_counter = 0
        self._values = []
        return self._node_to_code(self.plan, depth=0)

    def _compile(self, node: QueryNode) -> Any:
        expression = self.get_inline_code()
        return self._compile_expression(expression)

    @staticmethod
    @lru_cache(maxsize=2048)
    def _compile_expression(expression: str) -> Any:
        from mongoeco.core.filtering import QueryEngine

        context: dict[str, Any] = {
            'compare': compare_with_collation,
            'extract': QueryEngine.extract_values,
            'eq_matches': QueryEngine._query_equality_matches,
            'top_eq': QueryEngine._match_top_level_equals,
            'top_compare': QueryEngine._match_top_level_comparison,
            'in_matches': QueryEngine._in_item_matches_candidate,
            'match_plan': QueryEngine.match_plan,
        }
        # Local variable binding for performance
        function_code = (
            'def match_logic(doc, values, dialect, collation):\n'
            '    _extract = extract\n'
            '    _eq_matches = eq_matches\n'
            '    _top_eq = top_eq\n'
            '    _top_compare = top_compare\n'
            '    _in_matches = in_matches\n'
            '    _compare = compare\n'
            '    try:\n'
            f'        return {expression}\n'
            '    except (KeyError, TypeError, AttributeError):\n'
            '        return False'
        )

        local_vars: dict[str, Any] = {}
        exec(function_code, context, local_vars)
        return local_vars['match_logic']

    def _node_to_code(self, node: QueryNode, depth: int) -> str:
        match node:
            case MatchAll():
                return 'True'
            case EqualsCondition(
                field=field,
                value=value,
                null_matches_undefined=null_matches_undefined,
            ):
                field_key = self._field(field)
                value_key = self._store('value', value)
                if '.' not in field:
                    return (
                        f'_top_eq(doc, {field_key}, {value_key}, '
                        f'null_matches_undefined={null_matches_undefined}, '
                        'dialect=dialect, collation=collation)'
                    )
                return (
                    'any('
                    f'_eq_matches(candidate, {value_key}, '
                    f'null_matches_undefined={null_matches_undefined}, '
                    'dialect=dialect, collation=collation)'
                    f' for candidate in (_extract(doc, {field_key}) or [None])'
                    ')'
                )
            case NotEqualsCondition(field=field, value=value):
                field_key = self._field(field)
                value_key = self._store('value', value)
                values_name = self._local('ne_values')
                return (
                    '(all('
                    f'not _eq_matches(candidate, {value_key}, '
                    'null_matches_undefined=dialect.policy.null_query_matches_undefined(), '
                    'dialect=dialect, collation=collation)'
                    f' for candidate in {values_name}) if ({values_name} := _extract(doc, {field_key})) '
                    f'else not ({value_key} is None and dialect.policy.null_query_matches_undefined()))'
                )
            case GreaterThanCondition(field=field, value=value):
                return self._comparison_code(field, value, '>')
            case GreaterThanOrEqualCondition(field=field, value=value):
                return self._comparison_code(field, value, '>=')
            case LessThanCondition(field=field, value=value):
                return self._comparison_code(field, value, '<')
            case LessThanOrEqualCondition(field=field, value=value):
                return self._comparison_code(field, value, '<=')
            case InCondition(
                field=field,
                values=values,
                null_matches_undefined=null_matches_undefined,
            ):
                field_key = self._field(field)
                value_key = self._store('values', values)
                return (
                    'any('
                    f'_in_matches(candidate, item, null_matches_undefined={null_matches_undefined}, '
                    'dialect=dialect, collation=collation)'
                    f' for candidate in (_extract(doc, {field_key}) or [None])'
                    f' for item in {value_key}'
                    ')'
                )
            case NotInCondition(
                field=field,
                values=values,
                null_matches_undefined=null_matches_undefined,
            ):
                field_key = self._field(field)
                value_key = self._store('values', values)
                values_name = self._local('nin_values')
                return (
                    '(not any('
                    f'_in_matches(candidate, item, null_matches_undefined={null_matches_undefined}, '
                    'dialect=dialect, collation=collation)'
                    f' for candidate in {values_name}'
                    f' for item in {value_key}'
                    f') if ({values_name} := _extract(doc, {field_key})) '
                    f'else not (any(item is None for item in {value_key}) and {null_matches_undefined}))'
                )
            case AndCondition(clauses=clauses):
                if not clauses:
                    return 'True'
                return (
                    '('
                    + ' and '.join(
                        self._node_to_code(clause, depth + index + 1)
                        for index, clause in enumerate(clauses)
                    )
                    + ')'
                )
            case OrCondition(clauses=clauses):
                if not clauses:
                    return 'False'
                return (
                    '('
                    + ' or '.join(
                        self._node_to_code(clause, depth + index + 1)
                        for index, clause in enumerate(clauses)
                    )
                    + ')'
                )
            case NotCondition(clause=clause):
                return f'not ({self._node_to_code(clause, depth + 1)})'
            case ExistsCondition(field=field, value=value):
                field_key = self._field(field)
                return f'bool(_extract(doc, {field_key})) == {value}'
            case _:
                node_key = self._store('node', node)
                return f'match_plan(doc, {node_key}, dialect=dialect, collation=collation)'

    def _comparison_code(self, field: str, value: Any, operator: str) -> str:
        field_key = self._field(field)
        value_key = self._store('value', value)
        if '.' not in field:
            return (
                f'_top_compare(doc, {field_key}, {value_key}, {operator!r}, '
                'dialect=dialect, collation=collation)'
            )
        return (
            'any('
            f'_compare(candidate, {value_key}, dialect=dialect, collation=collation) {operator} 0'
            f' for candidate in _extract(doc, {field_key})'
            ')'
        )

    def _store(self, prefix: str, value: Any) -> str:
        del prefix
        key = f'values[{self._store_counter}]'
        self._store_counter += 1
        self._values.append(value)
        return key

    @staticmethod
    def _field(field: str) -> str:
        return repr(field)

    def _local(self, prefix: str) -> str:
        key = f'_{self._variable_prefix}{prefix}_{self._local_counter}'
        self._local_counter += 1
        return key
