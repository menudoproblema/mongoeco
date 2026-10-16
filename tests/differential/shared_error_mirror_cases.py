"""Finite regression witnesses for shared error compilation and adaptation."""

from pymongo import UpdateOne

from tests.differential.write_cases import WriteCase, WriteRequest


def cases():
    seed = [{"_id": "a", "k": 1, "items": [1]}, {"_id": "b", "k": 2, "items": [2]}]
    recovery = WriteRequest("update_one", [{"_id": "a"}, {"$set": {"recovered": True}}])
    variants = {
        "missing_filter": ({"$set": {"items.$[i]": 3}}, {}),
        "unused_filter": ({"$set": {"x": 3}}, {"array_filters": [{"i": 1}]}),
        "empty_filter": ({"$set": {"items.$[i]": 3}}, {"array_filters": [{}]}),
        "two_filter_roots": (
            {"$set": {"items.$[i]": 3}},
            {"array_filters": [{"i": 1, "j": 2}]},
        ),
        "invalid_filter_name": (
            {"$set": {"items.$[I]": 3}},
            {"array_filters": [{"I": 1}]},
        ),
        "unknown_operator": ({"$invalid": {"x": 3}}, {}),
        "operator_scalar": ({"$set": 3}, {}),
        "sqrt": ([{"$set": {"x": {"$sqrt": -1}}}], {}),
        "numeric_type": ([{"$set": {"x": {"$divide": ["bad", 2]}}}], {}),
        "cluster_time": ([{"$set": {"x": "$$CLUSTER_TIME"}}], {}),
        "duplicate": ({"$set": {"k": 1}}, {}),
    }
    for tag, (update, opts) in variants.items():
        indexes = [{"keys": [("k", 1)], "unique": True}] if tag == "duplicate" else []
        for method in ["update_one", "update_many", "find_one_and_update"]:
            yield WriteCase(
                method + "_" + tag,
                seed,
                [WriteRequest(method, [{"_id": "b"}, update], opts), recovery],
                indexes=indexes,
            )
        for ordered in [True, False]:
            models = [
                UpdateOne({"_id": "a"}, {"$set": {"before": True}}),
                UpdateOne({"_id": "b"}, update, **opts),
                UpdateOne({"_id": "a"}, {"$set": {"after": True}}),
            ]
            yield WriteCase(
                "bulk_" + str(ordered) + "_" + tag,
                seed,
                [WriteRequest("bulk_write", [models], {"ordered": ordered}), recovery],
                indexes=indexes,
            )
        yield WriteCase(
            "raw_" + tag,
            seed,
            [
                WriteRequest(
                    "command",
                    [
                        {
                            "update": "items",
                            "updates": [
                                {
                                    "q": {"_id": "b"},
                                    "u": update,
                                    **(
                                        {"arrayFilters": opts["array_filters"]}
                                        if opts
                                        else {}
                                    ),
                                }
                            ],
                        }
                    ],
                ),
                recovery,
            ],
            indexes=indexes,
        )
    for tag, pipeline in [
        ("cluster_time", [{"$set": {"x": "$$CLUSTER_TIME"}}]),
        ("window_output", [{"$setWindowFields": {"output": 1}}]),
        ("sqrt", [{"$set": {"x": {"$sqrt": -1}}}]),
        ("numeric_type", [{"$set": {"x": {"$divide": ["bad", 2]}}}]),
    ]:
        yield WriteCase(
            "aggregate_" + tag, seed, [WriteRequest("aggregate", [pipeline]), recovery]
        )
    yield WriteCase(
        "replace_duplicate",
        seed,
        [WriteRequest("replace_one", [{"_id": "b"}, {"k": 1}]), recovery],
        indexes=[{"keys": [("k", 1)], "unique": True}],
    )


MIRROR_ERROR_CASES = list(cases())
for _case in MIRROR_ERROR_CASES:
    _case.id = "mirror_" + _case.id
