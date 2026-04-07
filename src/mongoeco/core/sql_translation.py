from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from mongoeco.core.query_plan import QueryNode
from mongoeco.types import SortSpec


type SqlFragment = tuple[str, list[object]]


@dataclass(frozen=True, slots=True)
class SQLSelectStatement:
    sql: str
    params: tuple[object, ...]


class BaseSQLTranslator(ABC):
    @abstractmethod
    def translate_query_plan(self, plan: QueryNode) -> SqlFragment: ...

    @abstractmethod
    def translate_sort_spec(self, sort: SortSpec | None) -> str: ...

    def build_select_statement(
        self,
        *,
        select_clause: str,
        from_clause: str,
        namespace_sql: str,
        namespace_params: tuple[object, ...],
        plan: QueryNode | None = None,
        where_fragment: SqlFragment | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
    ) -> SQLSelectStatement:
        if where_fragment is None:
            if plan is None:
                raise ValueError("build_select_statement requires plan or where_fragment")
            where_sql, where_params = self.translate_query_plan(plan)
        else:
            where_sql, where_params = where_fragment
        order_sql = self.translate_sort_spec(sort)
        sql = f"""
            SELECT {select_clause}
            FROM {from_clause}
            WHERE {namespace_sql} AND ({where_sql})
            {order_sql}
        """
        params: list[object] = [*namespace_params, *where_params]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        elif skip:
            sql += " LIMIT -1"
        if skip:
            sql += " OFFSET ?"
            params.append(skip)
        return SQLSelectStatement(
            sql=sql,
            params=tuple(params),
        )
