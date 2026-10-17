"""Finite preparation corpus: context, selection, replacement and partial writes."""

from pymongo import ReturnDocument, UpdateOne

from tests.differential.error_phase_cases import case


PREPARATION_CASES = [
    case(
        "prep_update_let_selector",
        "update_many",
        [{"$expr": {"$eq": ["$key", "$$target"]}}, [{"$set": {"n": "$$value"}}]],
        {"let": {"target": 2, "value": 8}},
    ),
    case("prep_update_partial", "update_many", [{}, {"$inc": {"n": 1}}]),
    case(
        "prep_pipeline_partial",
        "update_many",
        [{}, [{"$set": {"n": {"$add": ["$n", 1]}}}]],
    ),
    case("prep_delete_selector", "delete_many", [{"key": {"$gte": 2}}]),
    case(
        "prep_delete_let",
        "delete_many",
        [{"$expr": {"$eq": ["$key", "$$target"]}}],
        {"let": {"target": 2}},
    ),
    case(
        "prep_replacement_seed",
        "replace_one",
        [{"_id": "new", "tenant": 7}, {"n": 8}],
        {"upsert": True},
    ),
    case(
        "prep_update_null_upsert",
        "update_one",
        [{"_id": None}, {"$set": {"n": 8}}],
        {"upsert": True},
    ),
    case(
        "prep_fam_null_upsert",
        "find_one_and_update",
        [{"_id": None}, [{"$set": {"n": 8}}]],
        {"upsert": True, "return_document": ReturnDocument.AFTER},
    ),
    case(
        "prep_replacement_collision",
        "find_one_and_replace",
        [{"_id": "b"}, {"key": 1}],
        indexes=[{"keys": [("key", 1)], "unique": True}],
    ),
    case(
        "prep_delete_projection",
        "find_one_and_delete",
        [{"key": 2}],
        {"projection": {"key": 1}},
    ),
    case(
        "prep_update_projection_invalid",
        "find_one_and_update",
        [{"key": 2}, {"$set": {"n": 8}}],
        {"projection": {"n": 1, "key": 0}},
    ),
    case(
        "prep_no_match_dynamic",
        "update_many",
        [{"_id": "missing"}, [{"$set": {"n": {"$divide": [1, 0]}}}]],
    ),
    case(
        "prep_aggregate_context",
        "aggregate",
        [
            [
                {"$match": {"$expr": {"$eq": ["$key", "$$target"]}}},
                {"$project": {"n": 1, "sameNow": {"$eq": ["$$NOW", "$$NOW"]}}},
            ]
        ],
        {"let": {"target": 2}},
    ),
    case("prep_count_selector", "count_documents", [{"key": {"$gte": 2}}]),
]

PREPARATION_CASES.extend(
    case(
        f"prep_bulk_partial_{ordered}",
        "bulk_write",
        [
            [
                UpdateOne({"_id": "a"}, {"$set": {"before": True}}),
                UpdateOne({"_id": "b"}, {"$set": {"key": 1}}),
                UpdateOne({"_id": "a"}, {"$set": {"after": True}}),
            ]
        ],
        {"ordered": ordered},
        indexes=[{"keys": [("key", 1)], "unique": True}],
    )
    for ordered in (True, False)
)
