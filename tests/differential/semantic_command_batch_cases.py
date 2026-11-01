"""Protocol batch boundaries, independent of a driver's cursor iteration."""

SEED = [{"_id": n, "v": n} for n in (1, 2, 3)]
MAX_BATCHES = 8
KINDS = (
    "find",
    "find-value",
    "aggregate",
    "project",
    "sort",
    "sort-value",
    "group",
    "group-single",
    "group-rows",
)


def _request(collection, kind, size, *, limit=None, single_batch=False):
    if kind in {"find", "find-value"}:
        request = {"find": collection.name, "batchSize": size}
        if kind == "find-value":
            request["sort"] = {"v": 1}
        if limit is not None:
            request["limit"] = limit
        if single_batch:
            request["singleBatch"] = True
        return request
    pipeline = {
        "aggregate": [],
        "project": [{"$project": {"v": 1}}],
        "sort": [{"$sort": {"_id": 1}}],
        "sort-value": [{"$sort": {"v": 1}}],
        "group-single": [{"$group": {"_id": None, "v": {"$sum": "$v"}}}],
        "group-rows": [
            {"$group": {"_id": "$_id"}},
            {"$project": {"_id": 0, "v": {"$literal": 1}}},
        ],
        "group": [
            {"$group": {"_id": "$_id", "v": {"$sum": "$v"}}},
            {"$sort": {"_id": 1}},
        ],
    }[kind]
    if limit is not None:
        pipeline = [*pipeline, {"$limit": limit}]
    return {
        "aggregate": collection.name,
        "pipeline": pipeline,
        "cursor": {"batchSize": size},
    }


def _batches(collection, request, size):
    frames = []
    reply = collection.database.command(request)
    cursor_id = reply["cursor"]["id"]
    try:
        for index in range(MAX_BATCHES):
            cursor = reply["cursor"]
            cursor_id = cursor["id"]
            field = "firstBatch" if not index else "nextBatch"
            frames.append(
                {
                    "keys": list(reply),
                    "cursor_keys": list(cursor),
                    "documents": cursor[field],
                    "open": bool(cursor_id),
                    "cursor_id_type": type(cursor_id).__name__,
                    "namespace_matches": cursor["ns"] == collection.full_name,
                    "ok": reply["ok"],
                }
            )
            if not cursor_id:
                break
            reply = collection.database.command(
                {
                    "getMore": cursor_id,
                    "collection": collection.name,
                    "batchSize": size or 1,
                }
            )
        return {"frames": frames, "unfinished": bool(cursor_id)}
    finally:
        if cursor_id:
            collection.database.command(
                {
                    "killCursors": collection.name,
                    "cursors": [cursor_id],
                }
            )


def batch_shapes(collection):
    result = {}
    for count in (0, 1, 3):
        collection.delete_many({})
        if count:
            collection.insert_many(SEED[:count])
        for kind in KINDS:
            for size in (0, 1, 2, 3, 4):
                result[f"{count}/{kind}/{size}"] = _batches(
                    collection, _request(collection, kind, size), size
                )
    return result


def batch_limits(collection):
    result = {}
    for kind in KINDS:
        for limit in (1, 2, 3, 4):
            for size in (1, 2, 3):
                result[f"{kind}/{limit}/{size}"] = _batches(
                    collection, _request(collection, kind, size, limit=limit), size
                )
    return result


def batch_single(collection):
    result = {}
    for count in (0, 1, 3):
        collection.delete_many({})
        if count:
            collection.insert_many(SEED[:count])
        for size in (0, 1, 2, 3, 4):
            result[f"{count}/{size}"] = _batches(
                collection,
                _request(collection, "find", size, single_batch=True),
                size,
            )
    return result


def batch_single_types(collection):
    result = {}
    for label, value in (
        ("false", False),
        ("true", True),
        ("zero", 0),
        ("one", 1),
        ("string", "yes"),
        ("null", None),
        ("object", {}),
        ("array", []),
    ):
        request = _request(collection, "find", 1)
        request["singleBatch"] = value
        try:
            result[label] = {"ok": True, "batches": _batches(collection, request, 1)}
        except Exception as error:
            result[label] = {
                "ok": False,
                "exception": type(error).__name__,
                "code": getattr(error, "code", None),
            }
    return result


def batch_index_order(collection):
    result = {}
    for compound in (False, True):
        index = collection.create_index(
            [("v", 1)] if not compound else [("tag", 1), ("v", -1)]
        )
        collection.update_many({}, {"$set": {"tag": "fixed"}})
        try:
            for reverse in (False, True):
                for kind in ("find-value", "sort-value"):
                    for size in (1, 3):
                        request = _request(collection, kind, size, limit=4)
                        direction = -1 if reverse else 1
                        if kind == "find-value":
                            request["sort"] = {"v": direction}
                            if compound:
                                request["filter"] = {"tag": "fixed"}
                        else:
                            request["pipeline"][0] = {"$sort": {"v": direction}}
                            if compound:
                                request["pipeline"].insert(
                                    0, {"$match": {"tag": "fixed"}}
                                )
                        result[f"{compound}/{reverse}/{kind}/{size}"] = _batches(
                            collection, request, size
                        )
        finally:
            collection.drop_index(index)
    return result


def batch_projection_errors(collection):
    collection.delete_many({})
    last = SEED[-1]["_id"]
    collection.insert_many(
        [{"_id": n, "v": n, "zero": 0 if n == last else 1} for n in (1, 2, 3)]
    )
    result = {}
    for kind in ("find-value", "sort-value"):
        projection = {"x": {"$divide": [1, "$zero"]}}
        request = _request(collection, kind, 1, limit=4)
        if kind == "find-value":
            request["projection"] = projection
        else:
            request["pipeline"].insert(1, {"$project": projection})
        cursor_id = 0
        frames = []
        try:
            reply = collection.database.command(request)
            for index in range(MAX_BATCHES):
                cursor_id = reply["cursor"]["id"]
                field = "firstBatch" if not index else "nextBatch"
                frames.append(reply["cursor"][field])
                if not cursor_id:
                    break
                reply = collection.database.command(
                    {
                        "getMore": cursor_id,
                        "collection": collection.name,
                        "batchSize": 1,
                    }
                )
            result[kind] = {"frames": frames, "error": None}
        except Exception as error:
            result[kind] = {
                "frames": frames,
                "error": type(error).__name__,
                "code": getattr(error, "code", None),
            }
        finally:
            if cursor_id:
                collection.database.command(
                    {"killCursors": collection.name, "cursors": [cursor_id]}
                )
    return result


SEMANTIC_COMMAND_BATCH_CASES = (
    ("semantic_command_batch_shapes", batch_shapes),
    ("semantic_command_batch_limits", batch_limits),
    ("semantic_command_batch_single", batch_single),
    ("semantic_command_batch_single_types", batch_single_types),
    ("semantic_command_batch_index_order", batch_index_order),
    ("semantic_command_batch_projection_errors", batch_projection_errors),
)
