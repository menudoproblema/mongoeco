"""One public cursor position across list reads, iteration and rewind."""

SEED = [{"_id": 0}]


def _rows(count):
    return [{"_id": n, "n": n} for n in range(1, count + 1)]


def _reset(collection, count):
    collection.delete_many({})
    if count:
        collection.insert_many(_rows(count))


def _with_decoders(collection, decoders):
    options = collection.codec_options
    if isinstance(options.type_registry, tuple):
        return collection.with_options(
            codec_options=type(options)(
                type_registry=dict(decoders), uuid_representation="unspecified"
            )
        )
    from tests.differential.semantic_bson_dispatch_cases import (  # noqa: PLC0415
        _options,
    )

    return _options(collection, decoders)


def _open(collection, *, kind, size, limit):
    if kind == "aggregate":
        pipeline = [{"$sort": {"_id": 1}}]
        if limit is not None:
            pipeline.append({"$limit": limit})
        return collection.aggregate(pipeline, batchSize=size)
    options = {"sort": [("_id", 1)], "batch_size": size}
    if limit is not None:
        options["limit"] = limit
    return collection.find({}, **options)


def _consume(cursor, operations, *, observe_alive):
    iterator = None
    observations = []
    for operation, value in operations:
        result = {"operation": operation}
        try:
            if operation == "list":
                result["rows"] = cursor.to_list(value)
            elif operation == "iterate":
                result["rows"] = list(cursor)
            elif operation == "iter":
                iterator = iter(cursor)
            elif operation == "next":
                if iterator is None:
                    iterator = iter(cursor)
                result["row"] = next(iterator)
            elif operation == "temporary-next":
                result["row"] = next(iter(cursor))
            elif operation == "rewind":
                cursor.rewind()
                iterator = None
            else:
                message = f"unknown cursor consumption step: {operation}"
                raise AssertionError(message)
        except StopIteration:
            result["end"] = True
        except Exception as error:
            result.update(exception=type(error).__name__, message=str(error))
        if observe_alive:
            result["alive"] = cursor.alive
        observations.append(result)
    return observations


def cursor_list_consumption(collection):
    result = {}
    for count in (0, 1, 3, 5):
        _reset(collection, count)
        for limit in (None, 1, 2):
            for size in (0, 1, 2):
                for length in (None, 1, 2, 5):
                    cursor = _open(collection, kind="find", size=size, limit=limit)
                    try:
                        result[f"{count}-{limit}-{size}-{length}"] = _consume(
                            cursor,
                            [
                                ("list", length),
                                ("list", None),
                                ("iterate", None),
                                ("list", 1),
                                ("next", None),
                            ],
                            observe_alive=True,
                        )
                    finally:
                        cursor.close()
    return result


def cursor_iterator_consumption(collection):
    _reset(collection, 5)
    result = {}
    for limit in (None, 1, 2):
        for size in (0, 1, 2):
            cursor = _open(collection, kind="find", size=size, limit=limit)
            try:
                result[f"{limit}-{size}"] = _consume(
                    cursor,
                    [
                        ("iter", None),
                        ("next", None),
                        ("list", 1),
                        ("next", None),
                        ("list", None),
                        ("iterate", None),
                        ("rewind", None),
                        ("list", 1),
                        ("list", None),
                    ],
                    observe_alive=True,
                )
                clone = cursor.clone()
                try:
                    result[f"{limit}-{size}-clone"] = _consume(
                        clone,
                        [("list", 1), ("list", None), ("iterate", None)],
                        observe_alive=True,
                    )
                finally:
                    clone.close()
            finally:
                cursor.close()
    return result


def cursor_decoder_consumption(collection):
    _reset(collection, 5)
    result = {}
    for mode in ("identity", "expand", "empty-first", "none"):
        for limit in (None, 1, 3):
            for size in (1, 2):
                calls = []

                def batch(rows, *, calls=calls, mode=mode):
                    calls.append(len(rows))
                    if mode == "expand":
                        return [row for row in rows for _ in range(2)]
                    return [] if mode == "empty-first" and len(calls) == 1 else rows

                decoded = _with_decoders(
                    collection,
                    [(dict, lambda _row: None)] if mode == "none" else [(list, batch)],
                )
                cursor = _open(decoded, kind="find", size=size, limit=limit)
                try:
                    result[f"{mode}-{limit}-{size}"] = {
                        "observations": _consume(
                            cursor,
                            [
                                ("list", 1),
                                ("next", None),
                                ("list", 2),
                                ("iterate", None),
                                ("list", None),
                            ],
                            observe_alive=True,
                        ),
                        "batch_calls": calls,
                    }
                finally:
                    cursor.close()
    return result


def command_cursor_consumption(collection):
    _reset(collection, 5)
    result = {}
    for limit in (None, 1, 3):
        for size in (0, 1, 2):
            cursor = _open(collection, kind="aggregate", size=size, limit=limit)
            try:
                result[f"{limit}-{size}"] = _consume(
                    cursor,
                    [
                        ("list", 1),
                        ("next", None),
                        ("list", 2),
                        ("iterate", None),
                        ("list", None),
                    ],
                    observe_alive=False,
                )
            finally:
                cursor.close()
    return result


def cursor_temporary_iterator_consumption(collection):
    _reset(collection, 5)
    result = {}
    for kind in ("find", "aggregate"):
        for limit in (None, 1, 3):
            for size in (0, 1, 2):
                cursor = _open(collection, kind=kind, size=size, limit=limit)
                try:
                    result[f"{kind}-{limit}-{size}"] = _consume(
                        cursor,
                        [
                            ("temporary-next", None),
                            ("list", 1),
                            ("temporary-next", None),
                            ("list", None),
                            ("iterate", None),
                        ],
                        observe_alive=kind == "find",
                    )
                finally:
                    cursor.close()
    return result


SEMANTIC_CURSOR_CONSUMPTION_CASES = (
    ("semantic_cursor_list_consumption", cursor_list_consumption),
    ("semantic_cursor_iterator_consumption", cursor_iterator_consumption),
    ("semantic_cursor_decoder_consumption", cursor_decoder_consumption),
    ("semantic_command_cursor_consumption", command_cursor_consumption),
    (
        "semantic_cursor_temporary_iterator_consumption",
        cursor_temporary_iterator_consumption,
    ),
)
