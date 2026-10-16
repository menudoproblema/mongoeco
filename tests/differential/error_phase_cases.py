"""Anonymous finite probes spanning validation, execution and error transport."""

from bson import Code, Int64
from pymongo import DeleteMany, DeleteOne, UpdateMany, UpdateOne

from tests.differential.write_cases import WriteCase, WriteRequest


R = WriteRequest
SEED = [
    {"_id": "a", "n": 1, "arr": [1, 2], "obj": {"x": 1}, "key": 1},
    {"_id": "b", "n": 2, "arr": [2, 3], "obj": {"x": 2}, "key": 2},
    {"_id": "c", "n": "bad", "arr": 5, "obj": {"x": 3}, "key": 3},
]


def recovery():
    return R("update_one", [{"_id": "a"}, {"$set": {"recovered": True}}])


def case(name, method, args, kwargs=None, *, seed=None, indexes=None, options=None):  # noqa: PLR0913 - declarative request and state specimen
    return WriteCase(
        name,
        SEED if seed is None else seed,
        [R(method, args, kwargs or {}), recovery()],
        indexes=indexes or [],
        options=options or {},
    )


def bulk(name, change, **opts):
    return case(
        name,
        "bulk_write",
        [
            [
                UpdateOne({"_id": "a"}, {"$set": {"before": True}}),
                UpdateOne({"_id": "b"}, change, **opts),
                UpdateOne({"_id": "a"}, {"$set": {"after": True}}),
            ]
        ],
        {"ordered": False},
    )


CASES = []
# Operators sharing numeric type/domain guards, with both constant and field operands.
expr = {}
for op in ["$abs", "$exp", "$ln", "$log10", "$sqrt", "$floor", "$ceil"]:
    expr[op[1:] + "_type"] = {op: "bad"}
for op in ["$add", "$multiply", "$subtract", "$divide", "$mod", "$pow", "$log"]:
    expr[op[1:] + "_left_type"] = {op: ["bad", 2]}
    expr[op[1:] + "_right_type"] = {op: [2, "bad"]}
for op in ["$round", "$trunc"]:
    expr[op[1:] + "_type"] = {op: ["bad", 0]}
    expr[op[1:] + "_place_type"] = {op: [1.2, "bad"]}
    expr[op[1:] + "_place_range"] = {op: [1.2, 101]}
expr.update(
    {
        "abs_min": {"$abs": Int64(-(2**63))},
        "exp_overflow": {"$exp": 1000},
        "ln_zero": {"$ln": 0},
        "log10_negative": {"$log10": -1},
        "sqrt_negative": {"$sqrt": -1},
        "pow_negative_fraction": {"$pow": [-1, 0.5]},
        "pow_zero_negative": {"$pow": [0, -1]},
        "log_base_one": {"$log": [10, 1]},
        "range_zero_step": {"$range": [0, 3, 0]},
        "range_start_type": {"$range": ["bad", 3]},
        "range_end_type": {"$range": [0, "bad"]},
        "range_step_type": {"$range": [0, 3, "bad"]},
        "divide_field_type": {"$divide": ["$n", 2]},
        "add_field_type": {"$add": ["$n", 2]},
    }
)
expr.update(
    {
        "toint": {"$toInt": "bad"},
        "convert_onerror": {
            "$convert": {"input": "bad", "to": "int", "onError": "safe"}
        },
        "size": {"$size": 1},
        "split": {"$split": [1, ","]},
        "array_elem_at": {"$arrayElemAt": [[1], "bad"]},
        "filter_input": {"$filter": {"input": 1, "cond": True}},
        "map_input": {"$map": {"input": 1, "in": "$$this"}},
        "reduce_input": {"$reduce": {"input": 1, "initialValue": 0, "in": "$$this"}},
        "concat": {"$concat": [1, "a"]},
        "concat_arrays": {"$concatArrays": [1, []]},
        "in_type": {"$in": [1, 1]},
        "eq_arity": {"$eq": [1]},
        "regex_invalid": {"$regexMatch": {"input": "x", "regex": "["}},
        "sqrt_valid": {"$sqrt": 4},
    }
)
for name, value in expr.items():
    pipeline = [{"$set": {"value": value}}]
    CASES.append(case("agg_" + name, "aggregate", [pipeline]))
    CASES.append(
        case(
            "update_" + name,
            "update_one",
            [{"_id": "c" if name.endswith("field_type") else "b"}, pipeline],
        )
    )
for name in [
    "ln_zero",
    "mod_left_type",
    "toint",
    "regex_invalid",
    "abs_min",
    "exp_overflow",
    "pow_zero_negative",
    "divide_field_type",
]:
    change = [{"$set": {"value": expr[name]}}]
    CASES.append(
        case(
            "fam_" + name,
            "find_one_and_update",
            [{"_id": "c" if name.endswith("field_type") else "b"}, change],
        )
    )
    CASES.append(bulk("bulk_" + name, change))
# Projection syntax must be checked before writes and deletes.
projections = {
    "mixed": {"n": 1, "arr": 0},
    "path_collision": {"obj": 1, "obj.x": 1},
    "slice_type": {"arr": {"$slice": "bad"}},
    "slice_zero": {"arr": {"$slice": [0, 0]}},
    "elem_scalar": {"arr": {"$elemMatch": 1}},
    "meta_invalid": {"x": {"$meta": "bad"}},
    "positional_twice": {"arr.$": 1, "other.$": 1},
    "unknown_op": {"n": {"$bad": 1}},
}
for name, projection in projections.items():
    for method in [
        "find_one",
        "find_one_and_update",
        "find_one_and_replace",
        "find_one_and_delete",
    ]:
        args = [{"_id": "a"}] + (
            [{"$set": {"touched": True}}]
            if method == "find_one_and_update"
            else [{"touched": True}]
            if method == "find_one_and_replace"
            else []
        )
        CASES.append(
            case(
                method + "_projection_" + name, method, args, {"projection": projection}
            )
        )
    CASES.append(
        case(
            "fam_projection_empty_" + name,
            "find_one_and_update",
            [{"_id": "missing"}, {"$set": {"touched": True}}],
            {"projection": projection},
        )
    )
# Invalid query trees and expression failures, including deletion families.
queries = {
    "unknown": {"n": {"$bad": 1}},
    "in_type": {"n": {"$in": 1}},
    "and_empty": {"$and": []},
    "and_scalar": {"$and": [1]},
    "options_without_regex": {"n": {"$options": "i"}},
    "expr_divzero": {"$expr": {"$divide": [1, 0]}},
    "expr_variable": {"$expr": {"$eq": ["$$missing", 1]}},
    "expr_cluster": {"$expr": {"$eq": ["$$CLUSTER_TIME", 1]}},
    "expr_badpath": {"$expr": {"$eq": ["$$ROOT..x", 1]}},
}
for name, query in queries.items():
    for method in [
        "find_one",
        "count_documents",
        "distinct",
        "update_one",
        "delete_one",
        "delete_many",
        "find_one_and_delete",
    ]:
        args = (
            ["n", query]
            if method == "distinct"
            else [query]
            + ([{"$set": {"touched": True}}] if method == "update_one" else [])
        )
        CASES.append(case(method + "_query_" + name, method, args))
    for model in [DeleteOne, DeleteMany]:
        CASES.append(  # noqa: PERF401 - explicit finite scenario inventory
            case(
                "bulk_" + model.__name__ + "_query_" + name,
                "bulk_write",
                [
                    [
                        UpdateOne({"_id": "a"}, {"$set": {"before": True}}),
                        model(query),
                        UpdateOne({"_id": "a"}, {"$set": {"after": True}}),
                    ]
                ],
                {"ordered": False},
            )
        )
# Array filter paths, compiler arity and classical operator execution.
updates = {
    "filter_logical": (
        {"$set": {"arr.$[i]": 7}},
        {"array_filters": [{"$and": [{"i": {"$gt": 1}}, {"i": {"$lt": 4}}]}]},
    ),
    "filter_expr": (
        {"$set": {"arr.$[i]": 7}},
        {"array_filters": [{"i": {"$expr": {"$eq": [1, 1]}}}]},
    ),
    "filter_scalar": ({"$set": {"arr.$[i]": 7}}, {"array_filters": [1]}),
    "filter_upper_path": ({"$set": {"arr.$[I]": 7}}, {"array_filters": [{"i": 1}]}),
    "positional_no_match": ({"$set": {"arr.$": 7}}, {}),
    "rename_array": ({"$rename": {"arr.0": "renamed"}}, {}),
    "rename_target": ({"$rename": {"n": 3}}, {}),
    "push_each": ({"$push": {"arr": {"$each": 3}}}, {}),
    "push_sort": ({"$push": {"arr": {"$each": [4], "$sort": 3}}}, {}),
    "push_sort_scalar": ({"$push": {"arr": {"$each": [4], "$sort": {"x": 1}}}}, {}),
    "addset_each": ({"$addToSet": {"arr": {"$each": 3}}}, {}),
    "pullall_type": ({"$pullAll": {"arr": 3}}, {}),
    "pop_value": ({"$pop": {"arr": 2}}, {}),
    "bit_operand": ({"$bit": {"n": {"and": "bad"}}}, {}),
    "inc_field_type": ({"$inc": {"n": 1}}, {}),
    "currentdate_type": ({"$currentDate": {"n": {"$type": "bad"}}}, {}),
    "pipeline_two_keys": ([{"$set": {"n": 1}, "$unset": "key"}], {}),
    "pipeline_scalar_stage": ([1], {}),
    "pipeline_stage_type": ([{"$set": 3}], {}),
    "pipeline_unknown": ([{"$bad": {}}], {}),
}
for name, (change, opts) in updates.items():
    for method in ["update_one", "find_one_and_update"]:
        CASES.append(  # noqa: PERF401 - explicit finite scenario inventory
            case(
                method + "_" + name,
                method,
                [{"_id": "c" if name == "inc_field_type" else "b"}, change],
                opts,
            )
        )
    for ordered in [True, False]:
        c = bulk("bulk_" + str(ordered) + "_" + name, change, **opts)
        c.requests[0].kwargs["ordered"] = ordered
        CASES.append(c)
# Administrative/index command errors, including post-conflict state.
for method, args, opts in [
    ("create_index", ["key"], {"unique": True, "name": "already"}),
    ("create_index", ["arr"], {"expireAfterSeconds": -1}),
    ("drop_index", ["missing"], {}),
    ("create_index", [{"key": 3}], {}),
    ("create_index", [("key", 1)], {"name": "same"}),
    ("create_index", [("arr", 1)], {"name": "same"}),
]:
    name = "admin_" + str(len(CASES))
    indexes = (
        [{"keys": [("key", 1)], "name": "already", "unique": False}]
        if opts.get("name") == "already"
        else [{"keys": [("key", 1)], "name": "same"}]
        if opts.get("name") == "same"
        else []
    )
    CASES.append(case(name, method, args, opts, indexes=indexes))
CASES.append(
    case(
        "create_unique_duplicates",
        "create_index",
        ["key"],
        {"unique": True},
        seed=[{"_id": "a", "key": 1}, {"_id": "b", "key": 1}],
    )
)
CASES.append(
    case(
        "create_unique_code_duplicates",
        "create_index",
        ["key"],
        {"unique": True},
        seed=[{"_id": "a", "key": Code("x")}, {"_id": "b", "key": Code("x")}],
    )
)
# Validator details and unordered prefix effects.
validator = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["n"],
            "properties": {"n": {"bsonType": "int"}},
        }
    }
}
for method, args in [
    ("insert_one", [{"_id": "bad"}]),
    ("update_one", [{"_id": "b"}, {"$set": {"n": "bad"}}]),
    ("find_one_and_update", [{"_id": "b"}, {"$set": {"n": "bad"}}]),
    ("replace_one", [{"_id": "b"}, {"n": "bad"}]),
    ("find_one_and_replace", [{"_id": "b"}, {"n": "bad"}]),
]:
    CASES.append(
        case(method + "_validation", method, args, seed=SEED[:2], options=validator)
    )
# Whole-document multi-write failures vs acknowledged partial counters.
for ordered in [True, False]:
    CASES.append(  # noqa: PERF401 - explicit finite scenario inventory
        case(
            "multi_partial_" + str(ordered),
            "bulk_write",
            [
                [
                    UpdateOne({"_id": "a"}, {"$set": {"before": True}}),
                    UpdateMany({}, {"$inc": {"n": 1}}),
                    UpdateOne({"_id": "a"}, {"$set": {"after": True}}),
                ]
            ],
            {"ordered": ordered},
        )
    )
# Upsert duplicate identity should retain the same diagnostics as a secondary key.
for method in [
    "update_one",
    "replace_one",
    "find_one_and_update",
    "find_one_and_replace",
]:
    change = {"_id": "a", "n": 7}
    change = {"$set": change} if "update" in method else change
    CASES.append(
        case(
            method + "_upsert_id_duplicate",
            method,
            [{"_id": "missing"}, change],
            {"upsert": True},
        )
    )
assert len({c.id for c in CASES}) == len(CASES)
# Raw counterparts distinguish local prevalidation from server-command semantics.
for name, (change, opts) in updates.items():
    entry = {"q": {"_id": "b"}, "u": change}
    if opts:
        entry["arrayFilters"] = opts["array_filters"]
    CASES.append(
        case(
            "raw_update_" + name,
            "command",
            [
                {
                    "update": "items",
                    "updates": [
                        {"q": {"_id": "a"}, "u": {"$set": {"before": True}}},
                        entry,
                        {"q": {"_id": "a"}, "u": {"$set": {"after": True}}},
                    ],
                    "ordered": False,
                }
            ],
        )
    )
for name, query in queries.items():
    for command in ["update", "delete", "findAndModify"]:
        spec = (
            {
                "update": "items",
                "updates": [{"q": query, "u": {"$set": {"touched": True}}}],
            }
            if command == "update"
            else {"delete": "items", "deletes": [{"q": query, "limit": 1}]}
            if command == "delete"
            else {"findAndModify": "items", "query": query, "remove": True}
        )
        CASES.append(case("raw_" + command + "_query_" + name, "command", [spec]))
for name, projection in projections.items():
    for remove in [True, False]:
        spec = {
            "findAndModify": "items",
            "query": {"_id": "a"},
            "fields": projection,
            **({"remove": True} if remove else {"update": {"$set": {"touched": True}}}),
        }
        CASES.append(
            case("raw_fam_" + str(remove) + "_projection_" + name, "command", [spec])
        )
for c in CASES:
    if c.id.startswith("admin_") and c.requests[0].args in [[("key", 1)], [("arr", 1)]]:
        c.requests[0].args = [c.requests[0].args]
CASES.append(case("ttl_zero_control", "create_index", ["n"], {"expireAfterSeconds": 0}))
assert len({c.id for c in CASES}) == len(CASES)

CASES = [c for c in CASES if c.id not in {"admin_328", "admin_330", "ttl_zero_control"}]
