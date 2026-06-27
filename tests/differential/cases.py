import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


RealParityAction = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class RealParityCase:
    name: str
    seed_documents: list[dict[str, Any]]
    action: RealParityAction
    min_version: tuple[int, int] = (7, 0)

    def supports(self, target_version: tuple[int, int]) -> bool:
        return target_version >= self.min_version


def _write_error_code(action: Callable[[], Any]) -> tuple[str, int | None]:
    try:
        action()
    except Exception as exc:
        return ("error", getattr(exc, "code", None))
    return ("ok", None)


def _write_error_flag(action: Callable[[], Any]) -> str:
    try:
        action()
    except Exception:
        return "error"
    return "ok"


REAL_PARITY_CASES: tuple[RealParityCase, ...] = (
    RealParityCase(
        name="find_expr_compare_fields",
        seed_documents=[
            {"_id": "1", "spent": 12, "budget": 10},
            {"_id": "2", "spent": 8, "budget": 10},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find({"$expr": {"$gt": ["$spent", "$budget"]}}, sort=[("_id", 1)])
        ],
    ),
    RealParityCase(
        name="find_subdocument_order_sensitive_equality",
        seed_documents=[
            {"_id": "ordered", "value": {"a": 1, "b": 2}},
            {"_id": "reordered", "value": {"b": 2, "a": 1}},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find({"value": {"a": 1, "b": 2}}, sort=[("_id", 1)])
        ],
    ),
    RealParityCase(
        name="find_all_with_multiple_elem_match",
        seed_documents=[
            {
                "_id": "1",
                "items": [
                    {"kind": "a", "qty": 1},
                    {"kind": "b", "qty": 2},
                    {"kind": "b", "qty": 5},
                ],
            },
            {"_id": "2", "items": [{"kind": "a", "qty": 1}, {"kind": "b", "qty": 2}]},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {
                    "items": {
                        "$all": [
                            {"$elemMatch": {"kind": "a"}},
                            {"$elemMatch": {"kind": "b", "qty": {"$gte": 5}}},
                        ]
                    }
                },
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="find_implicit_regex_literal",
        seed_documents=[
            {"_id": "1", "name": "MongoDB"},
            {"_id": "2", "name": "Postgres"},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find({"name": re.compile("^mongo", re.IGNORECASE)}, sort=[("_id", 1)])
        ],
    ),
    RealParityCase(
        name="find_in_with_regex_literals",
        seed_documents=[
            {"_id": "1", "tags": ["beta", "stable"]},
            {"_id": "2", "tags": ["alpha", "stable"]},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {"tags": {"$in": [re.compile("^be"), re.compile("^zz")]}},
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="update_add_to_set_document_order_sensitive",
        seed_documents=[{"_id": "1", "items": [{"kind": "a", "qty": 1}]}],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                collection.find_one({"_id": "1"}),
            )
        )(
            collection.update_one(
                {"_id": "1"},
                {"$addToSet": {"items": {"qty": 1, "kind": "a"}}},
            )
        ),
    ),
    RealParityCase(
        name="update_set_on_insert_id_matches_filter",
        seed_documents=[],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                result.upserted_id,
                collection.find_one({"_id": "seed"}),
            )
        )(
            collection.update_one(
                {"_id": "seed"},
                {"$setOnInsert": {"_id": "seed", "state": "new"}},
                upsert=True,
            )
        ),
    ),
    RealParityCase(
        name="update_pipeline_project_preserves_id",
        seed_documents=[{"_id": "1", "name": "Ada", "legacy": True}],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                collection.find_one({"_id": "1"}),
            )
        )(
            collection.update_one(
                {"_id": "1"},
                [{"$project": {"_id": 0, "name": 1}}],
            )
        ),
    ),
    RealParityCase(
        name="update_pipeline_rejects_id_change",
        seed_documents=[{"_id": "1", "name": "Ada"}],
        action=lambda collection: _write_error_code(
            lambda: collection.update_one(
                {"_id": "1"},
                [{"$set": {"_id": "2"}}],
            )
        ),
    ),
    RealParityCase(
        name="insert_rejects_root_array_id",
        seed_documents=[],
        action=lambda collection: _write_error_flag(lambda: collection.insert_one({"_id": [1]})),
    ),
    RealParityCase(
        name="find_expr_truthiness_array",
        seed_documents=[
            {"_id": "array", "flag": []},
            {"_id": "zero", "flag": 0},
            {"_id": "false", "flag": False},
            {"_id": "string", "flag": ""},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find({"$expr": "$flag"}, sort=[("_id", 1)])
        ],
    ),
    RealParityCase(
        name="aggregate_project_array_traversal",
        seed_documents=[
            {"_id": "1", "items": [{"kind": "a"}, {"kind": "b"}]},
            {"_id": "2", "items": [{"kind": "c"}]},
        ],
        action=lambda collection: list(
            collection.aggregate(
                [
                    {"$project": {"_id": 1, "kinds": "$items.kind"}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
    ),
    RealParityCase(
        name="aggregate_get_field_literal_name",
        seed_documents=[{"_id": "1", "a.b.c": 1, "$price": 2, "x..y": 3}],
        action=lambda collection: list(
            collection.aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "dotted": {"$getField": {"field": "a.b.c"}},
                            "dollar": {"$getField": {"field": {"$literal": "$price"}}},
                            "double_dot": {"$getField": {"field": "x..y"}},
                        }
                    }
                ]
            )
        ),
    ),
)


def get_real_parity_case(name: str) -> RealParityCase:
    for case in REAL_PARITY_CASES:
        if case.name == name:
            return case
    raise KeyError(name)
