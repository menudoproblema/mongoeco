# ruff: noqa: PERF401 - declarative specimens keep each request next to its state
"""Composed expressions, BSON command grammar and multiple-error regressions."""

from dataclasses import replace

from bson import Decimal128, Int64
from pymongo import UpdateOne

from tests.differential.error_phase_cases import SEED, case


C = []
exprs = {
    "nested_div": {"$eq": [{"$divide": [1, 0]}, 0]},
    "nested_sqrt": {"$add": [{"$sqrt": -1}, 1]},
    "unknown": {"$unknown": 1},
    "multi_expr": {"$abs": 1, "$sqrt": 1},
    "add_arity": {"$subtract": [1]},
    "concat_type": {"$concat": [1, "a"]},
    "and_dead": {"$and": [False, {"$divide": [1, 0]}]},
    "cond_dead": {"$cond": [False, {"$divide": [1, 0]}, 1]},
    "map_as": {"$map": {"input": [], "as": 7, "in": 1}},
    "map_unknown": {"$map": {"input": [], "in": {"$unknown": 1}}},
    "filter_unknown": {"$filter": {"input": [], "cond": {"$unknown": 1}}},
    "pow_large": {"$pow": [1e308, 2]},
    "pow_neg_large": {"$pow": [-1e308, 3]},
    "bad_convert": {"$convert": {"input": 1, "to": "bad"}},
    "round_null": {"$round": [1, None]},
    "size_missing": {"$size": "$absent"},
    "literal": {"$literal": {"$unknown": "$$missing"}},
}
for n, e in exprs.items():
    for empty in (False, True):
        seed = [] if empty else SEED[:2]
        for method, args in [
            ("aggregate", [[{"$match": {"_id": "missing"}}, {"$set": {"v": e}}]]),
            ("find_one", [{"$expr": e}]),
            ("update_one", [{"_id": "missing"}, [{"$set": {"v": e}}]]),
            ("find_one_and_update", [{"_id": "missing"}, [{"$set": {"v": e}}]]),
        ]:
            C.append(case(f"{n}_{empty}_{method}", method, args, seed=seed))
# Two command failures within one physical update batch.
changes = {
    "two": [{"$set": {}, "$unset": "n"}],
    "unknown": [{"$bad": {}}],
    "scalar": [1],
    "valid": {"$set": {"yes": True}},
}
for left in ("two", "unknown", "scalar"):
    for right in ("two", "unknown", "scalar"):
        for ordered in (True, False):
            C.append(
                case(
                    f"pair_{left}_{right}_{ordered}",
                    "bulk_write",
                    [
                        [
                            UpdateOne({"_id": "a"}, changes["valid"]),
                            UpdateOne({"_id": "b"}, changes[left]),
                            UpdateOne({"_id": "b"}, changes[right]),
                            UpdateOne({"_id": "a"}, {"$set": {"after": True}}),
                        ]
                    ],
                    {"ordered": ordered},
                )
            )
for key, value in [
    ("arrayFilters", None),
    ("arrayFilters", []),
    ("let", None),
    ("let", []),
    ("upsert", None),
    ("multi", None),
]:
    for pipeline in (False, True):
        change = [{"$set": {"n": 4}}] if pipeline else {"$set": {"n": 4}}
        for command in ("update", "findAndModify"):
            spec = (
                {
                    "update": "items",
                    "updates": [{"q": {"_id": "a"}, "u": change, key: value}],
                }
                if command == "update"
                else {
                    "findAndModify": "items",
                    "query": {"_id": "a"},
                    "update": change,
                    key: value,
                }
            )
            C.append(
                case(f"null_{command}_{key}_{value}_{pipeline}", "command", [spec])
            )
for n, schema, doc in [
    ("multi_missing", {"bsonType": "int", "minimum": 4}, "a"),
    ("description", {"bsonType": "int", "description": "number"}, "a"),
    ("title", {"bsonType": "int", "title": "number"}, "a"),
]:
    for method, args in [
        ("insert_one", [{"_id": "x", "n": doc}]),
        ("find_one_and_update", [{"_id": "a"}, {"$set": {"n": doc}}]),
    ]:
        C.append(
            case(
                "schema_" + n + "_" + method,
                method,
                args,
                seed=[{"_id": "a", "n": 5}],
                options={"validator": {"$jsonSchema": {"properties": {"n": schema}}}},
            )
        )
# Shared per-document guards and array filters, including valid no-match cases.
for n, change, opts in [
    ("pipeline_filters", [{"$set": {"n": 1}}], {"array_filters": []}),
    (
        "push_mixed",
        {"$push": {"arr": {"$each": [{"x": 2}, 1, {"x": 1}], "$sort": {"x": 1}}}},
        {},
    ),
    (
        "logical_or",
        {"$set": {"arr.$[i]": 9}},
        {"array_filters": [{"$or": [{"i": 1}, {"i": 2}]}]},
    ),
    (
        "logical_nor",
        {"$set": {"arr.$[i]": 9}},
        {"array_filters": [{"$nor": [{"i": 1}, {"i": 2}]}]},
    ),
    ("pullall_bad", {"$pullAll": {"arr": None}}, {}),
    ("unset_bad", [{"$unset": 1}], {}),
    ("replacewith_bad", [{"$replaceWith": 1}], {}),
    ("project_bad", [{"$project": {"n": 1, "arr": 0}}], {}),
]:
    for method in ("update_one", "find_one_and_update"):
        for match in (True, False):
            C.append(
                case(
                    f"{n}_{method}_{match}",
                    method,
                    [{"_id": "a" if match else "missing"}, change],
                    opts,
                )
            )
assert len({c.id for c in C}) == len(C)
for field in ("let", "unknownField"):
    C.append(
        case(
            "raw_update_entry_" + field,
            "command",
            [
                {
                    "update": "items",
                    "updates": [
                        {"q": {"_id": "a"}, "u": {"$set": {"changed": True}}, field: {}}
                    ],
                }
            ],
        )
    )
for command in ("findAndModify", "update", "delete"):
    for field in ("unknownField", "bypassDocumentValidation", "let"):
        for value in (None, [], 0, {}):
            if command == "findAndModify":
                spec = {
                    "findAndModify": "items",
                    "query": {"_id": "a"},
                    "update": {"$set": {"changed": True}},
                }
            elif command == "update":
                spec = {
                    "update": "items",
                    "updates": [{"q": {"_id": "a"}, "u": {"$set": {"changed": True}}}],
                }
            else:
                spec = {"delete": "items", "deletes": [{"q": {"_id": "a"}, "limit": 1}]}
            spec[field] = value
            C.append(
                case(f"command_shape_{command}_{field}_{value}", "command", [spec])
            )
for n, e in exprs.items():
    if n in ("literal", "pow_large", "pow_neg_large"):
        continue
    for method in ("update_one", "find_one_and_update"):
        C.append(
            case(f"match_{n}_{method}", method, [{"_id": "a"}, [{"$set": {"v": e}}]])
        )
for filters in ([], [{"i": 1}]):
    for method in ("update_one", "find_one_and_update"):
        if not filters:
            continue
        C.append(
            case(
                "pipeline_nonempty_filters_" + method,
                method,
                [{"_id": "a"}, [{"$set": {"n": 4}}]],
                {"array_filters": filters},
            )
        )

# A raw physical batch must retain its first command failure and both valid effects.
for left, right in (("two", "unknown"), ("unknown", "two")):
    for ordered in (True, False):
        C.append(
            case(
                f"raw_pair_{left}_{right}_{ordered}",
                "command",
                [
                    {
                        "update": "items",
                        "ordered": ordered,
                        "updates": [
                            {"q": {"_id": "a"}, "u": {"$set": {"before": True}}},
                            {"q": {"_id": "b"}, "u": changes[left]},
                            {"q": {"_id": "b"}, "u": changes[right]},
                            {"q": {"_id": "a"}, "u": {"$set": {"after": True}}},
                        ],
                    }
                ],
            )
        )

# Mirror the dynamic null semantics across reads, single/multiple writes and FAM.
for name, expression in {
    "size_null": {"$size": None},
    "trunc_null": {"$trunc": [1, None]},
    "round_missing_place": {"$round": [1, "$absent"]},
    "trunc_missing_place": {"$trunc": [1, "$absent"]},
}.items():
    for empty in (False, True):
        for method in (
            "aggregate",
            "find_one",
            "update_one",
            "update_many",
            "find_one_and_update",
        ):
            args = (
                [[{"$set": {"v": expression}}]]
                if method == "aggregate"
                else [{"$expr": expression}]
                if method == "find_one"
                else [{"_id": "a"}, [{"$set": {"v": expression}}]]
            )
            C.append(
                case(
                    f"{name}_{empty}_{method}", method, args, seed=[] if empty else SEED
                )
            )

# Native IDL booleans differ from bool-or-number command flags.
for command in ("update", "delete", "findAndModify"):
    fields = (
        ("bypassDocumentValidation", "upsert", "multi")
        if command == "update"
        else ("bypassDocumentValidation", "upsert", "new", "remove")
        if command == "findAndModify"
        else ("bypassDocumentValidation",)
    )
    for field in fields:
        for number, value in enumerate((None, False, True, 0, 1, "", [], {})):
            spec = (
                {
                    "update": "items",
                    "updates": [{"q": {"_id": "missing"}, "u": {"$set": {"n": 4}}}],
                }
                if command == "update"
                else {
                    "delete": "items",
                    "deletes": [{"q": {"_id": "missing"}, "limit": 1}],
                }
                if command == "delete"
                else {
                    "findAndModify": "items",
                    "query": {"_id": "missing"},
                    "update": {"$set": {"n": 4}},
                }
            )
            (
                spec["updates"][0]
                if command == "update" and field != "bypassDocumentValidation"
                else spec
            )[field] = value
            C.append(case(f"flag_{command}_{field}_{number}", "command", [spec]))

# Shape validation of the whole command precedes a valid earlier write.
for command in ("update", "delete"):
    entries = (
        [
            {"q": {"_id": "a"}, "u": {"$set": {"changed": True}}},
            {"q": {"_id": "b"}, "u": {"$set": {"changed": True}}, "unknown": None},
        ]
        if command == "update"
        else [
            {"q": {"_id": "a"}, "limit": 1},
            {"q": {"_id": "b"}, "limit": 1, "unknown": None},
        ]
    )
    for ordered in (True, False):
        C.append(
            case(
                f"unknown_late_{command}_{ordered}",
                "command",
                [
                    {
                        command: "items",
                        command + "s" if command == "update" else "deletes": entries,
                        "ordered": ordered,
                    }
                ],
            )
        )

for name, change in {
    "project": [{"$project": {"n": 1, "arr": 0}}],
    "unset": [{"$unset": 1}],
    "replace": [{"$replaceWith": 1}],
}.items():
    for ordered in (True, False):
        for method in ("command", "bulk_write"):
            args = (
                [
                    {
                        "update": "items",
                        "ordered": ordered,
                        "updates": [
                            {"q": {"_id": "a"}, "u": {"$set": {"before": True}}},
                            {"q": {"_id": "b"}, "u": change},
                            {"q": {"_id": "a"}, "u": {"$set": {"after": True}}},
                        ],
                    }
                ]
                if method == "command"
                else [
                    [
                        UpdateOne({"_id": "a"}, {"$set": {"before": True}}),
                        UpdateOne({"_id": "b"}, change),
                        UpdateOne({"_id": "a"}, {"$set": {"after": True}}),
                    ]
                ]
            )
            C.append(
                case(
                    f"stage_batch_{name}_{method}_{ordered}",
                    method,
                    args,
                    {} if method == "command" else {"ordered": ordered},
                )
            )

# Keep the seed valid so recovery proves reuse after validation failure.
for method, args in (
    ("insert_one", [{"_id": "x", "obj": {"n": "bad"}}]),
    ("find_one_and_update", [{"_id": "a"}, {"$set": {"obj.n": "bad"}}]),
):
    C.append(
        case(
            "schema_nested_metadata_" + method,
            method,
            args,
            seed=[{"_id": "a", "obj": {"n": 5}}],
            options={
                "validator": {
                    "$jsonSchema": {
                        "properties": {
                            "obj": {
                                "title": "object title",
                                "description": "object description",
                                "properties": {
                                    "n": {
                                        "title": "number title",
                                        "description": "number description",
                                        "bsonType": "int",
                                    }
                                },
                            }
                        }
                    }
                }
            },
        )
    )

# Adjacent stage guards retain their individual codes and execution phase.
for name, change in {
    "unset_empty": [{"$unset": []}],
    "unset_empty_path": [{"$unset": [""]}],
    "unset_nonstrings": [{"$unset": [1]}],
    "project_exclusion_inclusion": [{"$project": {"n": 0, "arr": 1}}],
    "project_exclusion_expression": [{"$project": {"n": 0, "v": {"$add": [1, 2]}}}],
    "project_expression_exclusion": [{"$project": {"v": {"$add": [1, 2]}, "n": 0}}],
    "project_id_exclusion": [{"$project": {"_id": 1, "n": 0}}],
    "replace_root_scalar": [{"$replaceRoot": {"newRoot": 1}}],
    "const_literal": [{"$set": {"v": {"$const": {"$unknown": "$$absent"}}}}],
}.items():
    for match in (True, False):
        for method in ("update_one", "find_one_and_update"):
            C.append(
                case(
                    f"adjacent_{name}_{match}_{method}",
                    method,
                    [{"_id": "a" if match else "missing"}, change],
                )
            )

for command in ("update", "delete", "findAndModify"):
    for number, flag in enumerate((Int64(0), 1.5, Decimal128("1"))):
        spec = (
            {
                "update": "items",
                "updates": [{"q": {"_id": "a"}, "u": {"$set": {"changed": True}}}],
            }
            if command == "update"
            else {"delete": "items", "deletes": [{"q": {"_id": "b"}, "limit": 1}]}
            if command == "delete"
            else {
                "findAndModify": "items",
                "query": {"_id": "a"},
                "update": {"$set": {"changed": True}},
            }
        )
        spec["bypassDocumentValidation"] = flag
        C.append(case(f"bson_flag_{command}_{number}", "command", [spec]))

# Special numeric results must not bypass null propagation or place validation.
for operator in ("$round", "$trunc"):
    for label, value in (("inf", float("inf")), ("nan", float("nan"))):
        for place_index, place in enumerate((0, None, "bad", 101)):
            expression = {operator: [value, place]}
            for empty in (False, True):
                for method in (
                    "aggregate",
                    "find_one",
                    "update_one",
                    "find_one_and_update",
                ):
                    args = (
                        [[{"$set": {"v": expression}}]]
                        if method == "aggregate"
                        else [{"$expr": expression}]
                        if method == "find_one"
                        else [{"_id": "a"}, [{"$set": {"v": expression}}]]
                    )
                    C.append(
                        case(
                            f"special_{operator[1:]}_{label}_{place_index}_{empty}_{method}",
                            method,
                            args,
                            seed=[] if empty else SEED,
                        )
                    )

# Prefix all identifiers to keep this corpus independent of the older specimens.
COMPOSITION_CASES = [
    replace(specimen, id="composition_" + specimen.id) for specimen in C
]
assert len({specimen.id for specimen in COMPOSITION_CASES}) == len(COMPOSITION_CASES)
