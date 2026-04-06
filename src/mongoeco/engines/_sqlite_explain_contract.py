from __future__ import annotations

from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    ElemMatchCondition,
    ExprCondition,
    GeoIntersectsCondition,
    GeoWithinCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    JsonSchemaCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    ModCondition,
    NearCondition,
    NotCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
)
from mongoeco.engines.sqlite_planner import SQLiteReadExecutionPlan
from mongoeco.types import PlanningIssue


def sqlite_pushdown_details(execution_plan: object) -> dict[str, object]:
    use_sql = isinstance(execution_plan, SQLiteReadExecutionPlan) and execution_plan.use_sql
    apply_python_sort = (
        isinstance(execution_plan, SQLiteReadExecutionPlan)
        and execution_plan.apply_python_sort
    )
    strategy = getattr(execution_plan, "strategy")
    fallback_reason = getattr(execution_plan, "fallback_reason")
    return {
        "mode": strategy,
        "usesSqlRuntime": bool(use_sql),
        "pythonSort": bool(apply_python_sort),
        "fallbackReason": fallback_reason,
    }


def sqlite_planning_issues(fallback_reason: str | None) -> tuple[PlanningIssue, ...]:
    if fallback_reason is None:
        return ()
    return (PlanningIssue(scope="engine", message=fallback_reason),)


def sqlite_pushdown_followup_hints(
    plan: QueryNode,
    fallback_reason: str | None = None,
) -> list[dict[str, object]]:
    operator_counts: dict[str, int] = {}

    def visit(node: QueryNode) -> None:
        if isinstance(node, RegexCondition):
            operator_counts["$regex"] = operator_counts.get("$regex", 0) + 1
            return
        if isinstance(node, GeoWithinCondition):
            operator_counts["$geoWithin"] = operator_counts.get("$geoWithin", 0) + 1
            return
        if isinstance(node, GeoIntersectsCondition):
            operator_counts["$geoIntersects"] = operator_counts.get("$geoIntersects", 0) + 1
            return
        if isinstance(node, NearCondition):
            operator_counts["$nearSphere" if node.spherical else "$near"] = (
                operator_counts.get("$nearSphere" if node.spherical else "$near", 0) + 1
            )
            return
        if isinstance(node, ModCondition):
            operator_counts["$mod"] = operator_counts.get("$mod", 0) + 1
            return
        if isinstance(node, SizeCondition):
            operator_counts["$size"] = operator_counts.get("$size", 0) + 1
            return
        if isinstance(node, AllCondition):
            operator_counts["$all"] = operator_counts.get("$all", 0) + 1
            return
        if isinstance(node, ElemMatchCondition):
            operator_counts["$elemMatch"] = operator_counts.get("$elemMatch", 0) + 1
            return
        if isinstance(node, TypeCondition):
            operator_counts["$type"] = operator_counts.get("$type", 0) + 1
            return
        if isinstance(node, BitwiseCondition):
            operator_counts[node.operator] = operator_counts.get(node.operator, 0) + 1
            return
        if isinstance(node, ExprCondition):
            operator_counts["$expr"] = operator_counts.get("$expr", 0) + 1
            return
        if isinstance(node, JsonSchemaCondition):
            operator_counts["$jsonSchema"] = operator_counts.get("$jsonSchema", 0) + 1
            return
        if isinstance(node, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)):
            operator_counts["range-comparison"] = operator_counts.get("range-comparison", 0) + 1
            return
        if isinstance(node, NotCondition):
            visit(node.clause)
            return
        if isinstance(node, (AndCondition, OrCondition)):
            for clause in node.clauses:
                visit(clause)

    visit(plan)
    hints: list[dict[str, object]] = []
    if "$regex" in operator_counts:
        hints.append(
            {
                "operator": "$regex",
                "priority": "high",
                "occurrences": operator_counts["$regex"],
                "currentSupport": "literal-contains-prefix-suffix-exact on top-level scalar strings, plus ASCII-only option i",
                "nextStep": "broaden regex pushdown beyond literal-safe patterns or support non-ASCII ignore-case semantics",
            }
        )
    for geo_operator in ("$geoWithin", "$geoIntersects", "$near", "$nearSphere"):
        if geo_operator in operator_counts:
            hints.append(
                {
                    "operator": geo_operator,
                    "priority": "high",
                    "occurrences": operator_counts[geo_operator],
                    "currentSupport": "evaluated in Python on broad planar local geo semantics",
                    "nextStep": "add SQL-side prefilters for safe planar local geo subsets without faking geodesic behaviour",
                }
            )
    if "$mod" in operator_counts:
        hints.append(
            {
                "operator": "$mod",
                "priority": "high",
                "occurrences": operator_counts["$mod"],
                "currentSupport": "integer scalar top-level fields without arrays or real values",
                "nextStep": "broaden mod pushdown to mixed numeric or array-aware semantics",
            }
        )
    if "$size" in operator_counts:
        hints.append(
            {
                "operator": "$size",
                "priority": "medium",
                "occurrences": operator_counts["$size"],
                "currentSupport": "simple scalar-array size checks on SQL-safe paths",
                "nextStep": "broaden size pushdown to more nested or structurally mixed paths",
            }
        )
    if "range-comparison" in operator_counts:
        hints.append(
            {
                "operator": "range-comparison",
                "priority": "medium",
                "occurrences": operator_counts["range-comparison"],
                "currentSupport": "same-type scalar comparisons plus homogeneous top-level scalar-or-array paths with explicit fallback guards",
                "nextStep": "broaden comparison pushdown across more BSON-tagged and structurally mixed array cases",
            }
        )
    if "$all" in operator_counts:
        hints.append(
            {
                "operator": "$all",
                "priority": "high",
                "occurrences": operator_counts["$all"],
                "currentSupport": "simple top-level scalar arrays with scalar literal membership",
                "nextStep": "add broader all-elements pushdown for operator values and more complex array shapes",
            }
        )
    if "$elemMatch" in operator_counts:
        hints.append(
            {
                "operator": "$elemMatch",
                "priority": "high",
                "occurrences": operator_counts["$elemMatch"],
                "currentSupport": "top-level scalar arrays with constrained scalar predicates",
                "nextStep": "add array-subdocument matching pushdown for constrained elemMatch shapes",
            }
        )
    if "$type" in operator_counts:
        hints.append(
            {
                "operator": "$type",
                "priority": "medium",
                "occurrences": operator_counts["$type"],
                "currentSupport": "partial SQL support only through broader translated predicates",
                "nextStep": "broaden explicit BSON type pushdown and explain coverage",
            }
        )
    for bitwise_operator in ("$bitsAllSet", "$bitsAnySet", "$bitsAllClear", "$bitsAnyClear"):
        if bitwise_operator in operator_counts:
            hints.append(
                {
                    "operator": bitwise_operator,
                    "priority": "high",
                    "occurrences": operator_counts[bitwise_operator],
                    "currentSupport": "no dedicated SQL pushdown yet",
                    "nextStep": "add bounded 64-bit bitmask pushdown for SQL-safe scalar integer cases",
                }
            )
    if "$expr" in operator_counts:
        hints.append(
            {
                "operator": "$expr",
                "priority": "high",
                "occurrences": operator_counts["$expr"],
                "currentSupport": "no dedicated SQL pushdown yet",
                "nextStep": "compile selected expression trees into SQL where semantics are stable",
            }
        )
    if "$jsonSchema" in operator_counts:
        hints.append(
            {
                "operator": "$jsonSchema",
                "priority": "medium",
                "occurrences": operator_counts["$jsonSchema"],
                "currentSupport": "evaluated in core/runtime rather than SQL pushdown",
                "nextStep": "add SQL prefilters for the subset of schema predicates with stable translation",
            }
        )
    reason_hints = {
        "classic $text local runtime executes in Python fallback": {
            "operator": "$text",
            "priority": "high",
            "currentSupport": "local text index over one or more text fields with local tokenization, textScore projection and score sort in Python fallback",
            "nextStep": "add SQL-side prefilters or dedicated local text indexing if classic text becomes a planner priority",
        },
        "Sort requires Python fallback": {
            "operator": "sort",
            "priority": "high",
            "currentSupport": "SQL sort plus hybrid fallback on unsupported paths and BSON ordering edges",
            "nextStep": "broaden SQL sorting across more BSON brackets and structural paths",
        },
        "Collation requires Python fallback": {
            "operator": "collation",
            "priority": "high",
            "currentSupport": "SQLite pushdown stays on simple binary semantics; richer collation runs in Python",
            "nextStep": "broaden SQL-side collation support or pre-normalized compare keys",
        },
        "Array traversal requires Python fallback": {
            "operator": "array-traversal",
            "priority": "high",
            "currentSupport": "top-level scalar and selected array-safe paths only",
            "nextStep": "broaden SQL pushdown for traversing nested arrays safely",
        },
        "DBRef subfield access requires Python fallback": {
            "operator": "dbref-subfield",
            "priority": "medium",
            "currentSupport": "DBRef subfield semantics stay in Python",
            "nextStep": "broaden SQL-aware handling for tagged DBRef subfield access",
        },
        "Top-level array comparisons require Python fallback": {
            "operator": "array-comparison",
            "priority": "high",
            "currentSupport": "scalar comparisons push down; array comparison semantics stay in Python",
            "nextStep": "broaden array-aware comparison pushdown where semantics are stable",
        },
        "Tagged undefined requires Python fallback": {
            "operator": "tagged-undefined",
            "priority": "medium",
            "currentSupport": "tagged undefined semantics stay in Python",
            "nextStep": "broaden pushdown with explicit undefined-aware SQL handling",
        },
        "Tagged bytes require Python fallback": {
            "operator": "tagged-bytes",
            "priority": "medium",
            "currentSupport": "tagged binary ordering and comparison semantics stay in Python",
            "nextStep": "broaden pushdown with byte-aware SQL compare keys",
        },
        "Geospatial operators require Python query fallback": {
            "operator": "geo-runtime",
            "priority": "high",
            "currentSupport": "broad planar local geo evaluation runs in Python/runtime",
            "nextStep": "add SQL prefilters for safe planar local geospatial subsets",
        },
    }
    if fallback_reason in reason_hints:
        hints.append(reason_hints[fallback_reason])
    return hints
