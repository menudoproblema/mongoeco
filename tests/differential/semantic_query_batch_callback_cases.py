"""Driver response formation and public/raw observation of bounded reads."""

from bson import decode_all

from tests.differential.semantic_bson_dispatch_cases import _options


SEED = [{"_id": n, "v": n} for n in (1, 2, 3)]


def _observed_collection(collection, calls):
    class ObserveReply(dict):
        def __del__(self):
            # Observe the completed envelope without overriding lookup semantics.
            if "id" in self:
                calls.append(bool(dict.__getitem__(self, "id")))

    return _options(collection, document_class=ObserveReply)


def query_batch_callbacks(collection):
    result = {}
    for limit in (1, 2, 4):
        for size in (None, 1, 2, 3):
            calls = []
            selected = _observed_collection(collection, calls)
            kwargs = {} if size is None else {"batch_size": size}
            cursor = selected.find({}, sort=[("v", 1)], limit=limit, **kwargs)
            try:
                rows = list(cursor)
                result[f"{limit}/{size}"] = {
                    "ids": [row["_id"] for row in rows],
                    "reply_cursor_ids": calls,
                    "alive": cursor.alive,
                }
            finally:
                cursor.close()
    return result


def aggregate_batch_callbacks(collection):
    result = {}
    for limit in (1, 2, 4):
        for size in (None, 1, 2, 3):
            calls = []
            selected = _observed_collection(collection, calls)
            kwargs = {} if size is None else {"batchSize": size}
            cursor = selected.aggregate(
                [{"$sort": {"v": 1}}, {"$limit": limit}], **kwargs
            )
            try:
                rows = list(cursor)
                result[f"{limit}/{size}"] = {
                    "ids": [row["_id"] for row in rows],
                    "reply_cursor_ids": calls,
                }
            finally:
                cursor.close()
    return result


def raw_query_batch_limits(collection):
    result = {}
    for limit in (1, 2, 4):
        for size in (None, 1, 2, 3):
            calls = []
            selected = _observed_collection(collection, calls)
            kwargs = {} if size is None else {"batch_size": size}
            cursor = selected.find_raw_batches(
                {}, sort=[("v", 1)], limit=limit, **kwargs
            )
            frames = []
            try:
                for batch in cursor:
                    frames.append(  # noqa: PERF401 - liveness is observed after each pull
                        {
                            "ids": [row["_id"] for row in decode_all(batch)],
                            "alive": cursor.alive,
                        }
                    )
                result[f"{limit}/{size}"] = {"frames": frames, "calls": calls}
            finally:
                cursor.close()
    return result


SEMANTIC_QUERY_BATCH_CALLBACK_CASES = (
    ("semantic_query_batch_callbacks", query_batch_callbacks),
    ("semantic_aggregate_batch_callbacks", aggregate_batch_callbacks),
    ("semantic_raw_query_batch_limits", raw_query_batch_limits),
)
