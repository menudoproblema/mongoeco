"""BSON-sensitive graph traversal through the standard differential runner."""

from bson import Regex

from tests.support_write_parity import SEMANTIC_ERRORS


SEED = [{"_id": 1, "obsolete": 1}]
GRAPH = [
    {"_id": 1, "parent_id": 3},
    {"_id": 2, "parent_id": [1, 3]},
    {"_id": 3, "parent_id": 1},
    {"_id": 4, "parent_id": [2, 3], "active": False},
    {"_id": 5, "parent_id": 4},
]
GRAPH_SPEC = {
    "from": "graph_foreign",
    "startWith": "$_id",
    "connectFromField": "_id",
    "connectToField": "parent_id",
    "as": "children",
    "depthField": "depth",
}


def traversal(collection):
    collection.database["graph_foreign"].insert_many(GRAPH)
    cases = {
        "cycle_diamond_array": {},
        "max_depth_zero": {"maxDepth": 0},
        "integral_decimal_depth": {"maxDepth": 1.0},
        "array_seeds": {"startWith": [1, 4]},
        "restriction_prunes": {"restrictSearchWithMatch": {"active": {"$ne": False}}},
        "foreign_expression": {
            "restrictSearchWithMatch": {"$expr": {"$eq": ["$_id", "$$wanted"]}}
        },
        "empty_collection": {"from": "missing_graph_collection"},
    }
    results = {}
    for name, changes in cases.items():
        pipeline = [{"$graphLookup": GRAPH_SPEC | changes}]
        options = {"let": {"wanted": 2}} if name == "foreign_expression" else {}
        (document,) = collection.aggregate(pipeline, **options)
        document["children"].sort(key=lambda child: child["_id"])
        results[name] = document
    pipeline = [
        {
            "$facet": {
                "nested": [
                    {
                        "$graphLookup": GRAPH_SPEC
                        | {"as": "obsolete.children", "maxDepth": 0}
                    }
                ]
            }
        }
    ]
    reply = collection.database.command(
        "aggregate", collection.name, pipeline=pipeline, cursor={}
    )
    (document,) = reply["cursor"]["firstBatch"]
    document["nested"][0]["obsolete"]["children"].sort(key=lambda child: child["_id"])
    results["nested_command"] = document
    pipeline = [
        {
            "$lookup": {
                "from": collection.name,
                "let": {"seed": "$_id"},
                "pipeline": [
                    {
                        "$graphLookup": GRAPH_SPEC
                        | {"startWith": "$$seed", "maxDepth": 0}
                    }
                ],
                "as": "nested",
            }
        }
    ]
    (document,) = collection.aggregate(pipeline)
    document["nested"][0]["children"].sort(key=lambda child: child["_id"])
    results["nested_lookup"] = document
    return results


def matching(collection):
    collection.database["graph_foreign"].insert_many(
        [
            {"_id": 1, "key": None},
            {"_id": 2},
            {"_id": 3, "key": [None]},
            {"_id": 4, "key": "ab"},
            {"_id": 5, "key": Regex("^a")},
            {"_id": 6, "key": [1, 2]},
            {"_id": 7, "key": "café", "next": {"links": [{"value": 7}, {"value": 8}]}},
            {"_id": 8, "key": 7},
            {"_id": 9, "key": 8},
            {"_id": "A", "key": 8},
            {"_id": "a", "key": 8},
        ]
    )
    spec = GRAPH_SPEC | {
        "connectFromField": "next.links.value",
        "connectToField": "key",
    }
    results = {}
    for name, start, collation in (
        ("missing", "$absent", None),
        ("null", None, None),
        ("empty_array", [], None),
        ("array_link", [[1, 2]], None),
        ("regex", Regex("^a"), None),
        ("collation", "CAFE", {"locale": "en", "strength": 1}),
    ):
        pipeline = [{"$graphLookup": spec | {"startWith": start}}]
        (document,) = collection.aggregate(
            pipeline, **({"collation": collation} if collation else {})
        )
        results[name] = sorted(
            document["children"], key=lambda child: str(child["_id"])
        )
    return results


def invalid(collection):
    collection.delete_many({})
    results = {}
    for name, changes in {
        "negative_depth": {"maxDepth": -1},
        "fractional_depth": {"maxDepth": 1.5},
        "boolean_depth": {"maxDepth": True},
        "negative_fractional_depth": {"maxDepth": -0.5},
        "filter_text": {"restrictSearchWithMatch": {"$text": {"$search": "x"}}},
        "filter_where": {"restrictSearchWithMatch": {"$where": "true"}},
        "filter_near": {"restrictSearchWithMatch": {"p": {"$near": [0, 0]}}},
        "empty_namespace": {"from": ""},
        "empty_output_path": {"as": ""},
        "bad_path_type": {"as": 1},
        "bad_filter": {"restrictSearchWithMatch": 1},
        "missing_variable_start": {"startWith": "$$absent"},
        "missing_variable_filter": {"restrictSearchWithMatch": {"$expr": "$$absent"}},
        "unknown_argument": {"unknown": 1},
    }.items():
        try:
            list(collection.aggregate([{"$graphLookup": GRAPH_SPEC | changes}]))
        except SEMANTIC_ERRORS as error:
            results[name] = {"code": error.code}
        else:
            msg = f"{name}: accepted invalid $graphLookup"
            raise AssertionError(msg)
    return results


SEMANTIC_GRAPH_LOOKUP_CASES = (
    ("semantic_graph_lookup_traversal", traversal),
    ("semantic_graph_lookup_matching", matching),
    ("semantic_graph_lookup_invalid", invalid),
)
