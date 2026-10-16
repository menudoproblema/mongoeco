"""Native specimens for identity errors and findAndModify validation boundaries."""

from copy import deepcopy

from bson import Decimal128, Int64, Regex
from pymongo import InsertOne, ReplaceOne, ReturnDocument, UpdateMany, UpdateOne

from tests.differential.write_cases import WriteCase, WriteRequest


VALIDATOR = {
    "$jsonSchema": {
        "required": ["name"],
        "properties": {"name": {"bsonType": "string"}},
    }
}


def insert_case(method, ordered, position):
    documents = [{"name": "Ada"}, {"name": "Bea"}, {"name": "Cid"}]
    documents[position]["_id"] = ["invalid"]
    payload = (
        documents
        if method == "insert_many"
        else [InsertOne(document) for document in documents]
    )
    return WriteCase(
        f"{method}_{ordered}_{position}",
        [],
        [WriteRequest(method, [payload], {"ordered": ordered})],
    )


def mixed_insert_case(ordered, bypass):
    return WriteCase(
        f"mixed_insert_{ordered}_{bypass}",
        [],
        [
            WriteRequest(
                "insert_many",
                [
                    [
                        {"_id": "a", "name": "Ada"},
                        {"_id": ["invalid"]},
                        {"_id": "a", "name": "Duplicate"},
                        {"_id": "schema"},
                        {"_id": "c", "name": "Cid"},
                    ]
                ],
                {"ordered": ordered, "bypass_document_validation": bypass},
            )
        ],
        options={"validator": deepcopy(VALIDATOR)},
    )


def modify_case(method, mode):
    seed = [{"_id": "a", "lookup": 1, "name": "Ada"}]
    query = {"lookup": 1}
    replacement = {"_id": "b", "name": "New"}
    options = {}
    indexes = []
    kwargs = {}
    if mode.endswith("upsert"):
        seed = []
        kwargs["upsert"] = True
    if mode in ("array", "array_upsert", "array_no_match"):
        replacement["_id"] = ["invalid"]
        if mode == "array_no_match":
            seed = []
    if mode == "embedded_upsert":
        replacement["_id"] = {"nested": ["valid"]}
    if mode.startswith(("schema", "bypass")):
        options["validator"] = deepcopy(VALIDATOR)
        replacement = {"name": 3, "changed": True}
        if mode.startswith("bypass"):
            kwargs["bypassDocumentValidation"] = True
    if mode.startswith("duplicate"):
        seed.append({"_id": "other", "name": "Bea"})
        replacement = {"name": "Bea"}
        indexes = [{"keys": [["name", 1]], "unique": True}]
    update = {"$set": replacement} if "update" in method else replacement
    if method.startswith("bulk_"):
        model = UpdateOne if method == "bulk_update" else ReplaceOne
        request = WriteRequest("bulk_write", [[model(query, update, **kwargs)]])
    else:
        request = WriteRequest(method, [query, update], kwargs)
    return WriteCase(
        f"{method}_{mode}", seed, [request], options=options, indexes=indexes
    )


def replacement_id_filter_case(method, equality_operator, mode):
    query = {"_id": {"$eq": "a"} if equality_operator else "a", "lookup": 1}
    replacement = {"name": "New"}
    seed = []
    if mode.startswith("array"):
        replacement["_id"] = ["invalid"]
        if mode == "array_existing":
            seed = [{"_id": "a", "lookup": 1, "name": "Ada"}]
    if mode == "conflict_upsert":
        replacement["_id"] = "b"
    request = (
        WriteRequest("bulk_write", [[ReplaceOne(query, replacement, upsert=True)]])
        if method == "bulk_replace"
        else WriteRequest(method, [query, replacement], {"upsert": True})
    )
    return WriteCase(f"id_filter_{method}_{equality_operator}_{mode}", seed, [request])


IDENTITY_CASES = [
    insert_case(method, ordered, position)
    for method in ("insert_many", "bulk_write")
    for ordered in (True, False)
    for position in range(3)
]
IDENTITY_CASES += [
    mixed_insert_case(ordered, bypass)
    for ordered in (True, False)
    for bypass in (True, False)
]
IDENTITY_CASES += [
    modify_case(method, mode)
    for method in ("find_one_and_update", "find_one_and_replace")
    for mode in (
        "immutable",
        "array",
        "array_upsert",
        "array_no_match",
        "embedded_upsert",
        "schema",
        "schema_upsert",
        "bypass",
        "bypass_upsert",
        "duplicate",
        "duplicate_upsert",
    )
]
IDENTITY_CASES += [
    replacement_id_filter_case(method, equality_operator, mode)
    for method in ("replace_one", "find_one_and_replace", "bulk_replace")
    for equality_operator in (True, False)
    for mode in ("array_existing", "array_upsert", "conflict_upsert", "inherit_upsert")
]
IDENTITY_CASES += [
    modify_case(method, mode)
    for method in ("replace_one", "bulk_replace", "update_one", "bulk_update")
    for mode in (
        "immutable",
        "array",
        "array_upsert",
        "array_no_match",
        "embedded_upsert",
    )
]


def null_identity_cases():
    cases = []
    for method in (
        "update_one",
        "update_many",
        "replace_one",
        "find_one_and_update",
        "find_one_and_replace",
    ):
        returns = (False, True) if method.startswith("find_") else (None,)
        for after in returns:
            for mode in ("upsert", "match", "no_match"):
                kwargs = {"upsert": mode != "no_match"}
                suffix = "" if after is None else f"_{after}"
                if after is not None:
                    kwargs["return_document"] = (
                        ReturnDocument.AFTER if after else ReturnDocument.BEFORE
                    )
                update = {"value": 1} if "replace" in method else {"$set": {"value": 1}}
                cases.append(
                    WriteCase(
                        f"null_id_{method}{suffix}_{mode}",
                        [{"_id": None, "value": 0}] if mode == "match" else [],
                        [WriteRequest(method, [{"_id": None}, update], kwargs)],
                    )
                )
    for identifier in (0, "", False):
        for method in ("update_one", "replace_one"):
            update = {"value": 1} if method == "replace_one" else {"$set": {"value": 1}}
            cases.append(
                WriteCase(
                    f"falsy_id_{method}_{type(identifier).__name__}",
                    [],
                    [
                        WriteRequest(
                            method, [{"_id": identifier}, update], {"upsert": True}
                        )
                    ],
                )
            )
    for model in (UpdateOne, UpdateMany, ReplaceOne):
        for ordered in (True, False):
            for fail in (True, False):
                requests = [
                    InsertOne({"_id": "before"}),
                    model(
                        {"_id": None},
                        {"value": 1} if model is ReplaceOne else {"$set": {"value": 1}},
                        upsert=True,
                    ),
                ]
                if fail:
                    requests.append(InsertOne({"_id": "before"}))
                requests.append(InsertOne({"_id": "after"}))
                cases.append(
                    WriteCase(
                        f"null_id_bulk_{model.__name__}_{ordered}_{fail}",
                        [],
                        [WriteRequest("bulk_write", [requests], {"ordered": ordered})],
                    )
                )
    return cases


def regex_identity_cases():
    invalid = Regex("x", "i")
    cases = [
        WriteCase(
            "regex_id_insert_one", [], [WriteRequest("insert_one", [{"_id": invalid}])]
        )
    ]
    for method in ("insert_many", "bulk_write"):
        for ordered in (True, False):
            for position in range(3):
                documents = [{"_id": "a"}, {"_id": "b"}, {"_id": "c"}]
                documents[position]["_id"] = invalid
                payload = (
                    documents
                    if method == "insert_many"
                    else [InsertOne(doc) for doc in documents]
                )
                cases.append(
                    WriteCase(
                        f"regex_id_{method}_{ordered}_{position}",
                        [],
                        [WriteRequest(method, [payload], {"ordered": ordered})],
                    )
                )
    for method in (
        "update_one",
        "update_many",
        "replace_one",
        "find_one_and_update",
        "find_one_and_replace",
        "bulk_update",
        "bulk_replace",
    ):
        query = {"lookup": "new"}
        update = {"_id": invalid, "value": 1}
        if "update" in method:
            update = {"$setOnInsert": update}
        if method.startswith("bulk_"):
            model = UpdateOne if method == "bulk_update" else ReplaceOne
            request = WriteRequest("bulk_write", [[model(query, update, upsert=True)]])
        else:
            request = WriteRequest(method, [query, update], {"upsert": True})
        cases.append(WriteCase(f"regex_id_{method}_upsert", [], [request]))
    # A regex nested inside a document ID is not a root regex ID.
    for method in ("insert_one", "replace_one"):
        document = {"_id": {"pattern": invalid}, "value": 1}
        args = [document] if method == "insert_one" else [{"lookup": "new"}, document]
        cases.append(
            WriteCase(
                f"embedded_regex_id_{method}",
                [],
                [
                    WriteRequest(
                        method, args, {} if method == "insert_one" else {"upsert": True}
                    )
                ],
            )
        )
    return cases


def numeric_identity_cases():
    cases = []
    pairs = [
        (1, 1.0),
        (1, Int64(1)),
        (1, Decimal128("1")),
        (Int64(1), 1),
        (Decimal128("1"), Int64(1)),
        (1.0, Decimal128("1")),
        ({"n": 1}, {"n": Int64(1)}),
    ]
    for position, (original, equivalent) in enumerate(pairs):
        for method in ("insert_one", "insert_many", "bulk_write"):
            for ordered in (True,) if method == "insert_one" else (True, False):
                documents = [
                    {"_id": "before"},
                    {"_id": equivalent, "value": 1},
                    {"_id": "after"},
                ]
                if method == "insert_one":
                    request = WriteRequest(method, [documents[1]])
                else:
                    payload = (
                        documents
                        if method == "insert_many"
                        else [InsertOne(doc) for doc in documents]
                    )
                    request = WriteRequest(method, [payload], {"ordered": ordered})
                cases.append(
                    WriteCase(
                        f"numeric_id_{position}_{method}_{ordered}",
                        [{"_id": original, "value": 0}],
                        [request],
                    )
                )
        for method in (
            "find_one",
            "count_documents",
            "update_one",
            "update_many",
            "replace_one",
            "find_one_and_update",
            "find_one_and_replace",
            "delete_one",
            "delete_many",
        ):
            query = {"_id": equivalent}
            args = [query]
            kwargs = {}
            if "update" in method or "replace" in method:
                args.append(
                    {"value": 1} if "replace" in method else {"$set": {"value": 1}}
                )
                kwargs["upsert"] = True
            if method.startswith("find_one_and"):
                kwargs["return_document"] = ReturnDocument.AFTER
            cases.append(
                WriteCase(
                    f"numeric_lookup_{position}_{method}",
                    [{"_id": original, "value": 0}],
                    [WriteRequest(method, args, kwargs)],
                )
            )
    for position, (original, different) in enumerate(
        (
            (1, 2.0),
            (1, False),
            (1, "1"),
            (Int64(9007199254740993), 9007199254740992.0),
            (None, 0),
        )
    ):
        cases.append(
            WriteCase(
                f"numeric_distinct_{position}",
                [{"_id": original}],
                [WriteRequest("insert_one", [{"_id": different}])],
            )
        )
    return cases


def explicit_id_conversion_cases():
    cases = []
    pairs = [
        (1, 1.0),
        (1.0, 1),
        (1, Int64(1)),
        (Int64(1), 1),
        (1, Decimal128("1")),
        (Decimal128("1"), 1),
        ({"n": 1}, {"n": Int64(1)}),
        ({"n": Int64(1)}, {"n": 1}),
    ]
    for position, (original, converted) in enumerate(pairs):
        for method in (
            "replace_one",
            "update_one",
            "update_many",
            "find_one_and_replace",
            "find_one_and_update",
        ):
            kwargs = (
                {"return_document": ReturnDocument.AFTER}
                if method.startswith("find_one_and")
                else {}
            )
            replacement = "replace" in method

            def mutation(value, replacement=replacement):
                return (
                    {"_id": value, "value": 0}
                    if replacement
                    else [{"$set": {"_id": {"$literal": value}}}]
                )

            cases.append(
                WriteCase(
                    f"id_conversion_{position}_{method}",
                    [{"_id": original, "value": 0}],
                    [
                        WriteRequest(
                            method, [{"_id": original}, mutation(converted)], kwargs
                        ),
                        WriteRequest(
                            method, [{"_id": original}, mutation(converted)], kwargs
                        ),
                        WriteRequest(
                            "update_one",
                            [{"_id": original}, {"$set": {"value": 1}}],
                            {"upsert": True},
                        ),
                        WriteRequest(
                            "replace_one",
                            [{"_id": converted}, {"_id": original, "value": 1}],
                        ),
                        WriteRequest("delete_one", [{"_id": converted}]),
                    ],
                )
            )
    for shape, update in (
        ("set", [{"$set": {"_id": {"$toLong": "$_id"}}}]),
        (
            "replace",
            [{"$replaceWith": {"_id": {"$toLong": "$_id"}, "value": "$value"}}],
        ),
        ("unset", [{"$unset": "_id"}]),
        ("double", [{"$set": {"_id": {"$toDouble": "$_id"}}}]),
        ("decimal", [{"$set": {"_id": {"$toDecimal": "$_id"}}}]),
        ("int", [{"$set": {"_id": {"$toInt": "$_id"}}}]),
        ("convert", [{"$set": {"_id": {"$convert": {"input": 1, "to": "long"}}}}]),
        (
            "let",
            [
                {
                    "$set": {
                        "_id": {"$let": {"vars": {"id": 1}, "in": {"$toLong": "$$id"}}}
                    }
                }
            ],
        ),
        ("bool", [{"$set": {"_id": {"$toLong": True}}}]),
        (
            "on_error",
            [
                {
                    "$set": {
                        "_id": {
                            "$convert": {"input": "bad", "to": "long", "onError": 1}
                        }
                    }
                }
            ],
        ),
    ):
        cases.append(
            WriteCase(
                f"id_conversion_expression_{shape}",
                [{"_id": 1, "value": 0}],
                [WriteRequest("update_one", [{}, update])],
            )
        )
    for method in (
        "replace_one",
        "update_one",
        "find_one_and_replace",
        "find_one_and_update",
    ):
        update = {"_id": 1.0} if "replace" in method else [{"$set": {"_id": 1.0}}]
        kwargs = {"upsert": True}
        if method.startswith("find_one_and"):
            kwargs["return_document"] = ReturnDocument.AFTER
        cases.append(
            WriteCase(
                f"id_conversion_upsert_{method}",
                [],
                [WriteRequest(method, [{"_id": 1}, update], kwargs)],
            )
        )
    for method in (
        "replace_one",
        "update_one",
        "find_one_and_replace",
        "find_one_and_update",
    ):
        for invalid in (2, False, "1"):
            update = (
                {"_id": invalid, "value": 1}
                if "replace" in method
                else [{"$set": {"_id": invalid, "value": 1}}]
            )
            cases.append(
                WriteCase(
                    f"id_conversion_invalid_{method}_{invalid}",
                    [{"_id": 1, "value": 0}],
                    [WriteRequest(method, [{}, update])],
                )
            )
    cases.extend(
        WriteCase(
            f"id_conversion_classic_{type(value).__name__}",
            [{"_id": 1, "value": 0}],
            [WriteRequest("update_one", [{}, {"$set": {"_id": value}}])],
        )
        for value in (1.0, Int64(1), Decimal128("1"))
    )
    for ordered in (True, False):
        for replacement in (True, False):
            first = (
                ReplaceOne({"_id": 1}, {"_id": 1.0})
                if replacement
                else UpdateOne({"_id": 1}, [{"$set": {"_id": 1.0}}])
            )
            cases.append(
                WriteCase(
                    f"id_conversion_bulk_{replacement}_{ordered}",
                    [{"_id": 1}, {"_id": 2}],
                    [
                        WriteRequest(
                            "bulk_write",
                            [
                                [
                                    first,
                                    UpdateOne({"_id": 2}, [{"$set": {"_id": 200}}]),
                                    InsertOne({"_id": "after"}),
                                ]
                            ],
                            {"ordered": ordered},
                        )
                    ],
                )
            )
    partial = [
        {"$set": {"_id": {"$cond": [{"$eq": ["$_id", 2]}, 200, {"$toLong": "$_id"}]}}}
    ]
    cases.append(
        WriteCase(
            "id_conversion_many_partial",
            [{"_id": n} for n in (1, 2, 3)],
            [WriteRequest("update_many", [{}, partial])],
        )
    )
    cases.append(
        WriteCase(
            "id_conversion_bulk_many_partial",
            [{"_id": n} for n in (1, 2, 3)],
            [
                WriteRequest(
                    "bulk_write",
                    [[UpdateMany({}, partial), InsertOne({"_id": "after"})]],
                    {"ordered": False},
                )
            ],
        )
    )
    return cases


IDENTITY_CASES += (
    null_identity_cases()
    + regex_identity_cases()
    + numeric_identity_cases()
    + explicit_id_conversion_cases()
)
RAW_IDENTITY_CASE_IDS = tuple(
    case.id
    for case in IDENTITY_CASES
    if (
        (
            case.id.startswith(("numeric_id_", "numeric_lookup_"))
            and case.requests[0].method not in ("find_one", "count_documents")
        )
        or (
            case.id.startswith("id_conversion_")
            and case.requests[0].method != "bulk_write"
        )
        or (case.id.startswith("null_id_") and "bulk" not in case.id)
        or case.id
        in (
            "regex_id_insert_one",
            "regex_id_insert_many_True_1",
            "regex_id_insert_many_False_1",
            "regex_id_update_one_upsert",
            "regex_id_replace_one_upsert",
        )
    )
)

# A failed write must leave the collection usable. This same follow-up is
# observed on the native server, including collections with a validator/index.
for case in IDENTITY_CASES:
    case.requests.append(
        WriteRequest("insert_one", [{"_id": "recovery", "name": "Recovery"}])
    )
