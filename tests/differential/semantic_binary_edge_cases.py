"""Binary boundaries observed through public APIs, without production keys."""

from uuid import UUID

from bson import Binary


SEED = [{"_id": "initial"}]
PAYLOAD = bytes(range(16))
VALUES = (
    b"",
    Binary(b"", 0),
    Binary(b"\xff", 0),
    Binary(b"\0\0", 0),
    Binary(b"", 2),
    Binary(b"\0", 2),
    Binary(b"\xff", 128),
    Binary(PAYLOAD, 3),
    Binary(PAYLOAD, 4),
    Binary(PAYLOAD, 5),
    Binary(b"\xff" * 17, 0),
    Binary(b"", 128),
)


def _view(value):
    if isinstance(value, UUID):
        return {"subtype": 4, "hex": value.bytes.hex()}
    if isinstance(value, bytes):
        return {"subtype": getattr(value, "subtype", 0), "hex": bytes(value).hex()}
    if isinstance(value, list):
        return [_view(item) for item in value]
    if isinstance(value, dict):
        return {key: _view(item) for key, item in value.items()}
    return value


def _reset(collection, values=VALUES):
    collection.delete_many({})
    collection.insert_many(
        [
            {"_id": i, "v": value, "a": [value], "n": [{"v": value}]}
            for i, value in enumerate(values, 1)
        ]
    )


def ordering(collection):
    _reset(collection)
    output = {
        str(direction): [
            row["_id"]
            for row in collection.find({}, sort=[("v", direction), ("_id", 1)])
        ]
        for direction in (1, -1)
    }
    for i, operand in enumerate(VALUES):
        output[f"cmp{i}"] = list(
            collection.aggregate(
                [
                    {"$project": {"_id": 1, "cmp": {"$cmp": ["$v", operand]}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )
    output["extrema"] = _view(
        list(
            collection.aggregate(
                [{"$group": {"_id": None, "lo": {"$min": "$v"}, "hi": {"$max": "$v"}}}]
            )
        )
    )
    return output


def matching(collection):
    _reset(collection)
    output = {}
    for i in (0, 2, 5, 8, 9, 6):
        value = VALUES[i]
        for path in ("v", "a", "n.v", "n.0.v"):
            for operator in ("$eq", "$ne", "$in", "$nin", "$gt", "$gte", "$lt", "$lte"):
                operand = [value] if operator in {"$in", "$nin"} else value
                query = {path: {operator: operand}}
                output[f"{i}-{path}-{operator}"] = [
                    row["_id"] for row in collection.find(query, sort=[("_id", 1)])
                ]
        output[f"count{i}"] = collection.count_documents({"n.v": {"$in": [value]}})
        result = collection.update_many({"n.v": value}, {"$set": {f"m{i}": True}})
        output[f"write{i}"] = [result.matched_count, result.modified_count]
    return output


def materialization(collection):
    output = {}
    for label, values in (("forward", VALUES), ("reverse", tuple(reversed(VALUES)))):
        _reset(collection, values)
        for path in ("v", "a", "n.v"):
            items = [_view(value) for value in collection.distinct(path)]
            output[f"{label}-{path}"] = sorted(
                items, key=lambda x: (x["subtype"], x["hex"])
            )
        first = collection.find_one({"_id": 1})
        first["n"][0]["v"] = b"changed"
        first["a"].append(b"changed")
        output[f"{label}-reread"] = _view(collection.find_one({"_id": 1}))
    return output


def literal_grammar(collection):
    output = {}
    for populated in (False, True):
        collection.delete_many({})
        if populated:
            collection.insert_one({"_id": 1, "v": Binary(b"ab", 5)})
        for subtype in (3, 5):
            for length in (0, 2, 16):
                value = Binary(b"a" * length, subtype)
                forms = {
                    "scalar": value,
                    "literal": {"$literal": value},
                    "const": {"$const": value},
                    "object": {"$literal": {"v": value}},
                    "array": {"$literal": [value]},
                }
                for label, expression in forms.items():
                    key = f"{populated}-{subtype}-{length}-{label}"
                    phase = "open"
                    try:
                        cursor = collection.aggregate(
                            [{"$project": {"_id": 0, "v": expression}}]
                        )
                        phase = "iterate"
                        output[key] = {"ok": True, "documents": _view(list(cursor))}
                    except Exception as error:
                        output[key] = {
                            "ok": False,
                            "exception": type(error).__name__,
                            "code": getattr(error, "code", None),
                            "phase": phase,
                        }
        output[f"{populated}-query"] = [
            row["_id"] for row in collection.find({"v": Binary(b"ab", 5)})
        ]
        output[f"{populated}-field"] = _view(
            list(collection.aggregate([{"$project": {"_id": 0, "v": "$v"}}]))
        )
    return output


def root_aliases(collection):
    output = {}
    for i, (first, alias) in enumerate(
        ((b"a", Binary(b"a", 0)), (Binary(b"a", 0), b"a"))
    ):
        collection.delete_many({})
        collection.insert_one({"_id": first, "v": 1})
        output[f"{i}-find"] = _view(collection.find_one({"_id": alias}))
        output[f"{i}-count"] = collection.count_documents({"_id": alias})
        changed = collection.update_one({"_id": alias}, {"$set": {"v": 2}})
        output[f"{i}-update"] = [changed.matched_count, changed.modified_count]
        try:
            collection.insert_one({"_id": alias, "v": 3})
            output[f"{i}-duplicate"] = None
        except Exception as error:
            output[f"{i}-duplicate"] = getattr(error, "code", None)
        collection.insert_one({"_id": Binary(b"a", 128), "v": 4})
        output[f"{i}-state"] = sorted(d["v"] for d in collection.find({}))
    return output


SEMANTIC_BINARY_EDGE_CASES = (
    ("semantic_binary_edges_ordering", ordering),
    ("semantic_binary_edges_matching", matching),
    ("semantic_binary_edges_materialization", materialization),
    ("semantic_binary_edges_literal_grammar", literal_grammar),
    ("semantic_binary_edges_root_aliases", root_aliases),
)
