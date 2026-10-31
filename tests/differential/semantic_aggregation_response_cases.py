"""Response boundaries after blocking and expanding stages, through public cursors."""

from functools import partial


SEED = [{"_id": 0}]


def _pipeline(kind, error_at, limit):
    prefix = [{"$sort": {"_id": 1}}, {"$set": {"pre": {"$add": ["$_id", 0]}}}]
    group = {"$group": {"_id": "$_id", "pre": {"$first": "$pre"}}}
    if kind in {"group", "group_twice"}:
        prefix.append(group)
    if kind == "group_twice":
        prefix.extend([{"$sort": {"pre": 1}}, group])
    if kind not in {"unwind", "single_unwind"}:
        prefix.append({"$sort": {"pre": 1}})
    expanding = kind in {"unwind", "sorted_unwind", "single_unwind"}
    if expanding:
        prefix.append({"$unwind": {"path": "$items", "includeArrayIndex": "copy"}})
    condition = (
        {
            "$and": [
                {"$eq": ["$pre", (error_at + 1) // 2]},
                {"$eq": ["$copy", (error_at - 1) % 2]},
            ]
        }
        if expanding and error_at is not None
        else {"$eq": ["$pre", error_at]}
    )
    if kind == "single_unwind":
        condition = {"$eq": ["$copy", None if error_at is None else error_at - 1]}
    prefix.append(
        {
            "$project": {
                "copy": 1,
                "v": {"$divide": [1, {"$cond": [condition, 0, 1]}]},
            }
        }
    )
    if limit is not None:
        prefix.append({"$limit": limit})
    return prefix


def response_suffix(collection, *, kind):
    collection.delete_many({})
    collection.insert_many(
        [{"_id": 1, "items": list(range(226))}]
        if kind == "single_unwind"
        else [{"_id": n, "items": [0, 1]} for n in range(1, 114)]
    )
    result = {}
    configurations = [
        (batch, error_at, None)
        for batch in (None, 0, 1, 2)
        for error_at in (None, 1, 3, 103)
    ] + [(None, 3, 1), (None, 103, 102)]
    for disk in (False, True):
        for batch, error_at, limit in configurations:
            options = {"allowDiskUse": disk}
            if batch is not None:
                options["batchSize"] = batch
            phase = "open"
            received = []
            cursor = None
            try:
                cursor = collection.aggregate(
                    _pipeline(kind, error_at, limit), **options
                )
                phase = "iterate"
                received.extend([row["_id"], row.get("copy")] for row in cursor)
                observation = {"ok": True}
            except Exception as error:
                observation = {
                    "ok": False,
                    "exception": type(error).__name__,
                    "code": getattr(error, "code", None),
                    "codeName": (getattr(error, "details", None) or {}).get("codeName"),
                    "phase": phase,
                }
            finally:
                if cursor is not None:
                    cursor.close()
            result[f"{disk}-{batch}-{error_at}-{limit}"] = {
                **observation,
                "received": received,
            }
    return result


SEMANTIC_AGGREGATION_RESPONSE_CASES = tuple(
    (f"semantic_aggregation_response_{kind}", partial(response_suffix, kind=kind))
    for kind in (
        "sort",
        "group",
        "group_twice",
        "unwind",
        "sorted_unwind",
        "single_unwind",
    )
)


def prefix_types(collection):
    collection.delete_many({})
    collection.insert_many([{"_id": n, "n": n} for n in range(1, 4)])
    pipeline = [
        {"$set": {"pre": {"$toLong": "$n"}}},
        {"$sort": {"pre": 1}},
        {"$project": {"kind": {"$type": "$pre"}, "value": "$pre"}},
    ]
    result = {}
    for disk in (False, True):
        for batch in (None, 0, 1, 2):
            options = {"allowDiskUse": disk}
            if batch is not None:
                options["batchSize"] = batch
            cursor = collection.aggregate(pipeline, **options)
            try:
                result[f"{disk}-{batch}"] = list(cursor)
            finally:
                cursor.close()
    return result


SEMANTIC_AGGREGATION_RESPONSE_CASES += (
    ("semantic_aggregation_response_prefix_types", prefix_types),
)
