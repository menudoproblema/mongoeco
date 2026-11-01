"""Text metadata remains a public sort document until BSON admission."""

SEED = [
    {"_id": 2, "title": "alpha"},
    {"_id": 1, "title": "alpha"},
    {"_id": 3, "title": "beta"},
]


def text_sort_admission(collection):
    collection.create_index([("title", "text")])
    query = {"$text": {"$search": "alpha"}}
    projection = {"_id": 1, "rank": {"$meta": "textScore"}}
    order = {"sortScore": {"$meta": "textScore"}, "_id": 1}
    output = {}
    for route in ("find-argument", "find-method", "find-limited", "find-one"):
        phase = "invoke"
        try:
            if route == "find-one":
                rows = [collection.find_one(query, projection, sort=order)]
            else:
                if route == "find-limited":
                    cursor = collection.find(query, projection, sort=order, limit=1)
                elif route == "find-argument":
                    cursor = collection.find(query, projection, sort=order)
                else:
                    cursor = collection.find(query, projection).sort(order)
                phase = "iterate"
                rows = list(cursor)
            output[route] = {
                "rows": [
                    {
                        "id": row["_id"],
                        "keys": list(row),
                        "positiveScore": row["rank"] > 0,
                    }
                    for row in rows
                ],
            }
        except Exception as error:
            output[route] = {"error": type(error).__name__, "phase": phase}
    for route in ("filter", "sort"):
        phase = "invoke"
        try:
            cursor = (
                collection.find({"invalid\x00key": 1})
                if route == "filter"
                else collection.find().sort({"invalid\x00key": 1})
            )
            phase = "iterate"
            output["invalid-" + route] = {"rows": list(cursor)}
        except Exception as error:
            output["invalid-" + route] = {
                "error": type(error).__name__,
                "phase": phase,
            }
    output["recovery"] = [row["_id"] for row in collection.find().sort("_id")]
    return output


SEMANTIC_TEXT_SORT_ADMISSION_CASES = (
    ("semantic_text_sort_admission", text_sort_admission),
)
