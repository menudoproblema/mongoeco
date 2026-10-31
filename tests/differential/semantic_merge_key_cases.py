"""Array merge keys fail before selecting or mutating a target document."""

SEED = [{"_id": "initial"}]


def array_merge_keys(collection):
    output = {}
    target = collection.database["target"]
    for matched in ("merge", "replace", "keepExisting", "fail"):
        for unmatched in ("insert", "discard", "fail"):
            for present in (False, True):
                collection.delete_many({})
                target.delete_many({})
                collection.insert_one({"_id": 1, "v": "source"})
                if present:
                    target.insert_one({"_id": 1, "v": "target"})
                pipeline = [
                    {"$set": {"_id": {"$literal": [1]}}},
                    {
                        "$merge": {
                            "into": "target",
                            "whenMatched": matched,
                            "whenNotMatched": unmatched,
                        }
                    },
                ]
                try:
                    list(collection.aggregate(pipeline))
                    result = {"ok": True}
                except Exception as error:
                    if getattr(error, "code", None) is None:
                        raise
                    result = {
                        "error": type(error).__name__,
                        "code": error.code,
                        "code_name": (getattr(error, "details", None) or {}).get(
                            "codeName"
                        ),
                    }
                result["target"] = list(target.find({}))
                output[f"{matched}/{unmatched}/{present}"] = result
    return output


def invalid_merge_modes(collection):
    output = {}
    for matched in ("keepExisting", "fail"):
        for unmatched in ("discard", "fail"):
            for populated in (False, True):
                for prefix, expression in (
                    ("plain", None),
                    ("constant_divide", {"$divide": [1, 0]}),
                    ("unknown_expression", {"$mergeKeyUnknown": 1}),
                    ("wrong_arity", {"$divide": [1]}),
                    ("undefined_variable", "$$mergeKeyUndefined"),
                ):
                    collection.delete_many({})
                    target = collection.database["target"]
                    target.delete_many({})
                    if populated:
                        collection.insert_one({"_id": 1})
                    pipeline = [
                        {
                            "$merge": {
                                "into": "target",
                                "whenMatched": matched,
                                "whenNotMatched": unmatched,
                            }
                        }
                    ]
                    if expression is not None:
                        pipeline.insert(0, {"$set": {"bad": expression}})
                    phase = "open"
                    try:
                        cursor = collection.aggregate(pipeline)
                        phase = "read"
                        list(cursor)
                        result = {"ok": True}
                    except Exception as error:
                        if getattr(error, "code", None) is None:
                            raise
                        result = {
                            "phase": phase,
                            "error": type(error).__name__,
                            "code": error.code,
                            "code_name": (getattr(error, "details", None) or {}).get(
                                "codeName"
                            ),
                        }
                    result["target"] = list(target.find({}))
                    output[f"{matched}/{unmatched}/{populated}/{prefix}"] = result
    return output


SEMANTIC_MERGE_KEY_CASES = (
    ("semantic_array_merge_keys", array_merge_keys),
    ("semantic_invalid_merge_modes", invalid_merge_modes),
)
