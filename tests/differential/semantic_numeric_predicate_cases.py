"""Independent query witnesses around double, long and decimal boundaries."""

from functools import partial

from bson import Decimal128, Int64


VALUES = (
    0.1,
    Decimal128("0.1"),
    0.5,
    Decimal128("0.5"),
    float(1000000000000000128),
    Int64(1000000000000000128),
    Decimal128("1000000000000000128"),
    Decimal128("1000000000000000100"),
    1000000000000000128,
    Int64(2**53 + 1),
    float(2**53 + 1),
)
SEED = [
    {
        "_id": i,
        "v": value,
        "a": [value],
        **({"d": value, "b": [value]} if isinstance(value, (int, float)) else {}),
        **(
            {"plain": value, "plainArray": [value]}
            if type(value) in (int, float)
            else {}
        ),
    }
    for i, value in enumerate(VALUES)
]


def query_observations(collection, *, indexed):
    if indexed:
        for field in ("v", "a", "d", "b", "plain", "plainArray"):
            collection.create_index(field)
    observations = []
    for field in ("v", "a", "d", "b", "plain", "plainArray"):
        for value in VALUES:
            for operator in ("$eq", "$lt", "$lte", "$gt", "$gte", "$in", "$nin"):
                operand = [value] if operator in {"$in", "$nin"} else value
                selector = {field: {operator: operand}}
                options = {"hint": f"{field}_1"} if indexed else {}
                observations.append(
                    sorted(
                        doc["_id"]
                        for doc in collection.find(selector, {"_id": 1}, **options)
                    )
                )
    return observations


SEMANTIC_NUMERIC_PREDICATE_CASES = (
    ("semantic_numeric_predicate_scan", partial(query_observations, indexed=False)),
    ("semantic_numeric_predicate_index", partial(query_observations, indexed=True)),
)
