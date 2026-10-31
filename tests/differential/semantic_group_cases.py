"""Independent public witnesses for group routing and literal conservation."""

from bson import Decimal128, ObjectId


def group_routes(collection):
    """Adding a consumer or a separate accumulator preserves the sum."""
    group = {"$group": {"_id": None, "value": {"$sum": True}}}
    return {
        "top": list(collection.aggregate([group])),
        "facet": list(collection.aggregate([{"$facet": {"result": [group]}}])),
        "extra": list(
            collection.aggregate(
                [
                    {
                        "$facet": {
                            "result": [
                                {
                                    "$group": {
                                        "_id": None,
                                        "value": {"$sum": True},
                                        "extra": {"$push": "$_id"},
                                    }
                                }
                            ]
                        }
                    }
                ]
            )
        ),
    }


def _literal_groups(collection, values):
    return [
        list(
            collection.aggregate(
                [
                    {
                        "$facet": {
                            "result": [
                                {
                                    "$group": {
                                        "_id": None,
                                        "value": {"$first": {"$literal": value}},
                                    }
                                }
                            ]
                        }
                    }
                ]
            )
        )
        for value in values
    ]


def literal_sequence(collection):
    """Queries retain literals regardless of previous queries."""
    return _literal_groups(collection, (True, 1, 1.0, False, 0, 0.0, 1.0, 1, True))


def bson_literals(collection):
    """BSON literals are values, not executable Python representations."""
    return _literal_groups(
        collection, (Decimal128("1.25"), ObjectId("000000000000000000000001"))
    )


SEMANTIC_GROUP_CASES = (
    ("semantic_fix_group_route_boolean", group_routes),
    ("semantic_fix_cache_identity_literals", literal_sequence),
    ("semantic_fix_compiled_literal_bson", bson_literals),
)
