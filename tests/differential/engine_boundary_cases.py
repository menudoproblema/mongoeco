"""Finite command equivalents of identity plus conditional SPI selection."""

from pymongo import ReturnDocument, UpdateOne

from tests.differential.error_phase_cases import case


BOUNDARY_CASES = []
for name, selector in (
    ("absent", None),
    ("empty", {}),
    ("match", {"key": 2}),
    ("rejected", {"key": 1}),
    ("other_identity", {"_id": "a"}),
):
    predicate = {"_id": "b"}
    if selector is not None:
        predicate = {"$and": [predicate, selector]}
    for method in (
        "update_one",
        "delete_one",
        "find_one_and_update",
        "find_one_and_delete",
    ):
        args = [predicate]
        options = {}
        if method in {"update_one", "find_one_and_update"}:
            args.append({"$set": {"selected": True}})
        if method == "find_one_and_update":
            options["return_document"] = ReturnDocument.AFTER
        BOUNDARY_CASES.append(case(f"boundary_{method}_{name}", method, args, options))

BOUNDARY_CASES.extend(
    [
        case(
            "boundary_conditional_update_image",
            "find_one_and_update",
            [{"$and": [{"_id": "b"}, {"key": 2}]}, {"$set": {"key": 4}}],
            {"return_document": ReturnDocument.BEFORE},
        ),
        case(
            "boundary_duplicate",
            "find_one_and_update",
            [{"$and": [{"_id": "b"}, {"key": 2}]}, {"$set": {"key": 1}}],
            indexes=[{"keys": [("key", 1)], "unique": True}],
        ),
        case(
            "boundary_replacement_identity",
            "find_one_and_replace",
            [{"$and": [{"_id": "b"}, {"key": 2}]}, {"_id": "a", "key": 4}],
        ),
        case(
            "boundary_update_many_partial",
            "update_many",
            [{"key": {"$gte": 2}}, {"$inc": {"n": 1}}],
        ),
    ]
)
BOUNDARY_CASES.extend(
    case(
        f"boundary_bulk_partial_{ordered}",
        "bulk_write",
        [
            [
                UpdateOne({"_id": "b", "key": 2}, {"$set": {"selected": True}}),
                UpdateOne({"_id": "b", "key": 2}, {"$set": {"key": 1}}),
                UpdateOne({"_id": "b", "key": 2}, {"$set": {"after": True}}),
            ]
        ],
        {"ordered": ordered},
        indexes=[{"keys": [("key", 1)], "unique": True}],
    )
    for ordered in (True, False)
)
