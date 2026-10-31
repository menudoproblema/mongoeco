"""Document order compares value type, binary field name, then field value."""

from functools import partial

from bson import Decimal128


VALUES = (
    {"z": None},
    {"a": 1},
    {"z": 1},
    {"a": 2},
    {"a": True},
    {},
    {"a": [1]},
    {"a": {"z": None}},
    {"a": {"a": 1}},
    {"a": "A"},
    {"a": "a"},
    {"A": 1},
    {"a": 1, "b": 0},
    {"b": 0, "a": 1},
    {"a": [{"z": None}]},
    {"a": [{"a": 1}]},
    {"z": Decimal128("0.1")},
    {"a": 0.1},
)
SEED = [{"_id": i, "v": value} for i, value in enumerate(VALUES)]


def comparisons(collection, *, collated):
    options = {"collation": {"locale": "en", "strength": 2}} if collated else {}
    return list(
        collection.aggregate(
            [
                {"$sort": {"_id": 1}},
                {
                    "$project": {
                        "comparisons": [
                            {"$cmp": ["$v", {"$literal": value}]} for value in VALUES
                        ]
                    }
                },
            ],
            **options,
        )
    )


def sorted_documents(collection, *, collated):
    options = {"collation": {"locale": "en", "strength": 2}} if collated else {}
    sort = [("v", 1), ("_id", 1)]
    selector = {"v": {"$gte": {"a": 1}}}
    collection.create_index(sort, **options)
    return {
        "find": [d["_id"] for d in collection.find({}, sort=sort, **options)],
        "aggregate": [
            d["_id"] for d in collection.aggregate([{"$sort": dict(sort)}], **options)
        ],
        "filter": [d["_id"] for d in collection.find(selector, sort=sort, **options)],
    }


def partial_document_index(collection):
    collection.update_many({}, {"$set": {"rank": 1}})
    collection.database.command(
        {
            "createIndexes": collection.name,
            "indexes": [
                {
                    "key": {"rank": 1},
                    "name": "partial_docs",
                    "partialFilterExpression": {"v": {"$gte": {"a": 1}}},
                }
            ],
        }
    )
    return [
        [
            doc["_id"]
            for doc in collection.find(
                {"rank": 1, "v": {"$gte": value}},
                sort=[("_id", 1)],
                **({"hint": "partial_docs"} if hinted else {}),
            )
        ]
        for hinted in (False, True)
        for value in ({"a": 1}, {"z": 1}, {"a": True})
    ]


SEMANTIC_DOCUMENT_ORDER_CASES = (
    ("semantic_document_compare_simple", partial(comparisons, collated=False)),
    ("semantic_document_compare_collation", partial(comparisons, collated=True)),
    ("semantic_document_sort_simple", partial(sorted_documents, collated=False)),
    ("semantic_document_sort_collation", partial(sorted_documents, collated=True)),
    ("semantic_document_partial_index", partial_document_index),
)
