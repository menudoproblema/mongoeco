"""Native read order and MQL eligibility for the two bounded P2 workloads."""

import hashlib

from bson import BSON, Binary


SEED = [{"_id": 0}]
_SUBTYPE_BOUNDARY = 32


def consumption_cost_contract(collection):
    collection.delete_many({})
    collection.insert_many(
        [{"_id": n, "n": n, "p": "x" * 4096} for n in reversed(range(200))]
    )
    output = {}
    for label, query in (("all", {}), ("late", {"n": {"$gte": 150}})):
        for direction in (1, -1):
            for skip, limit in ((0, 1), (3, 7), (0, 0)):
                cursor = collection.find(
                    query,
                    sort=[("_id", direction)],
                    skip=skip,
                    **({"limit": limit} if limit else {}),
                )
                try:
                    first = cursor.to_list(1)
                    rows = first + list(cursor)
                    output[f"{label}/{direction}/{skip}/{limit}"] = {
                        "first": [row["_id"] for row in first],
                        "ids": [row["_id"] for row in rows],
                        "bson_sha256": hashlib.sha256(
                            b"".join(BSON.encode(row) for row in rows)
                        ).hexdigest(),
                    }
                finally:
                    cursor.close()
    return output


def vector_filter_eligibility(collection):
    output = {}
    for scenario in ("interleaved", "late"):
        collection.delete_many({})
        collection.insert_many(
            [
                {
                    "_id": f"{n:03}",
                    "embedding": [1.0, n / 64],
                    "v": Binary(
                        b"x",
                        5
                        if (
                            n % 2 == 0
                            if scenario == "interleaved"
                            else n >= _SUBTYPE_BOUNDARY
                        )
                        else 0,
                    ),
                }
                for n in range(64)
            ]
        )
        output[scenario] = {}
        for subtype in (0, 5):
            cursor = collection.find({"v": Binary(b"x", subtype)}, {"_id": 1}).sort(
                "_id"
            )
            try:
                output[scenario][str(subtype)] = [row["_id"] for row in cursor]
            finally:
                cursor.close()
    return output


SEMANTIC_P2_READ_COST_CASES = (
    ("semantic_p2_consumption_cost", consumption_cost_contract),
    ("semantic_p2_vector_filter_eligibility", vector_filter_eligibility),
)
