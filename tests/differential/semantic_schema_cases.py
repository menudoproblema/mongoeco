"""H40-H44: native schema decisions, preparation and rejected-write state."""

from copy import deepcopy
from functools import partial

from bson import BSON, Code, DBRef, Decimal128, Int64
from bson.raw_bson import RawBSONDocument
from pymongo import InsertOne, UpdateOne
from pymongo.errors import OperationFailure

from tests.differential.write_cases import WriteCase, WriteRequest
from tests.support_write_parity import BSONObservation, observe_write_case


SEED = [{"_id": 0}]
VALUES = [
    None,
    False,
    True,
    0,
    1,
    1.0,
    Decimal128("1.0"),
    0.1,
    Decimal128("0.1"),
    0.3,
    float("nan"),
    float("inf"),
    float("-inf"),
    Decimal128("NaN"),
    Decimal128("Infinity"),
    "a",
    "A",
    [1],
    [True],
    [1, 1.0],
    [1, True],
    {"x": 1, "y": [2]},
    {"y": [2], "x": 1},
    {"x": True, "y": [2]},
    {"extra": True},
    [0.1, Decimal128("0.1")],
    [{"x": 1, "y": 2}, {"y": 2, "x": 1}],
    [{"x": 1}, {"x": True}],
]
RULES = {
    "enum_typed": {"bsonType": "int", "enum": [1]},
    "enum_number": {"enum": [1]},
    "enum_array": {"enum": [[1]]},
    "enum_object": {"enum": [{"x": 1, "y": [2]}]},
    "enum_binary": {"enum": ["a"]},
    "minimum": {"minimum": Decimal128("0.100000000000000002")},
    "maximum": {"maximum": Decimal128("0.1")},
    "nan_minimum": {"minimum": float("nan")},
    "exclusive": {
        "minimum": 1,
        "exclusiveMinimum": True,
        "maximum": 2,
        "exclusiveMaximum": True,
    },
    "multiple_double": {"multipleOf": 0.1},
    "multiple_decimal": {"multipleOf": Decimal128("0.1")},
    "unique": {"uniqueItems": True},
    "additional": {"additionalProperties": {"bsonType": "int"}},
}
INVALID = {
    "duplicate_type": {"type": ["number", "number"]},
    "infinite_multiple": {"multipleOf": float("inf")},
    "nan_multiple": {"multipleOf": float("nan")},
    "mutual_types": {"type": "string", "bsonType": "string"},
    "bson_json_alias": {"type": "int"},
    "empty_enum": {"enum": []},
    "duplicate_enum": {"enum": [1, 1.0]},
    "nested_duplicate": {"enum": [{"x": 1, "y": 2}, {"y": 2, "x": 1}]},
    "duplicate_required": {"required": ["a", "a"]},
    "unknown": {"unknownKeyword": True},
    "bad_multiple": {"multipleOf": 0},
    "exclusive_type": {"minimum": 0, "exclusiveMinimum": 1},
    "empty_type": {"type": []},
    "bad_pattern": {"properties": {"a": {"pattern": "["}}},
}


def decisions(collection, *, surface):
    collection.delete_many({})
    collection.database.command(
        {
            "insert": collection.name,
            "documents": [
                {"_id": index, "a": value} for index, value in enumerate(VALUES)
            ],
        }
    )
    output = {}
    for label, rule in RULES.items():
        query = {"$jsonSchema": {"properties": {"a": rule}}}
        if surface == "aggregate":
            rows = collection.aggregate([{"$match": query}, {"$sort": {"_id": 1}}])
        else:
            rows = collection.find(query, sort=[("_id", 1)])
        output[label] = [row["_id"] for row in rows]
    # Schema enum is binary even when the surrounding operation has collation.
    output["collation"] = [
        row["_id"]
        for row in collection.aggregate(
            [
                {"$match": {"$jsonSchema": {"properties": {"a": {"enum": ["a"]}}}}},
                {"$sort": {"_id": 1}},
            ],
            collation={"locale": "en", "strength": 2},
        )
    ]
    return output


def preparation(collection):
    collection.delete_many({})
    output = {}
    for name, schema in INVALID.items():
        try:
            list(collection.aggregate([{"$match": {"$jsonSchema": schema}}]))
            output[name] = {"ok": True}
        except OperationFailure as error:
            output[name] = {
                "ok": False,
                "code": error.code,
                "message": error.details.get("errmsg"),
            }
    return output


def writes(collection, *, family):
    output = {}
    for label, rule in RULES.items():
        if (family == "equality") != (label.startswith("enum") or label == "unique"):
            continue
        name = collection.name + "_" + label
        # A fresh action namespace; native capture gives every action a private DB.
        case = WriteCase(
            label,
            [],
            [
                WriteRequest("insert_one", [{"_id": i, "a": deepcopy(value)}])
                for i, value in enumerate(VALUES)
            ],
            options={"validator": {"$jsonSchema": {"properties": {"a": rule}}}},
        )
        output[label] = observe_write_case(
            collection.database, name, case, error_contract=True
        )
    return output


def lifecycle(collection):
    output = {}
    for level, action in (
        ("strict", "error"),
        ("moderate", "error"),
        ("off", "error"),
        ("strict", "warn"),
    ):
        name = collection.name + "_" + level + "_" + action
        case = WriteCase(
            name,
            [{"_id": 1, "a": 1}, {"_id": 2, "a": True}],
            [
                WriteRequest("update_one", [{"_id": 1}, {"$set": {"a": True}}]),
                WriteRequest("update_one", [{"_id": 2}, {"$set": {"a": False}}]),
                WriteRequest("replace_one", [{"_id": 1}, {"_id": 1, "a": True}]),
                WriteRequest("update_one", [{"_id": 1}, [{"$set": {"a": True}}]]),
                WriteRequest(
                    "find_one_and_update", [{"_id": 1}, {"$set": {"a": True}}]
                ),
                WriteRequest(
                    "update_one", [{"_id": 3}, {"$set": {"a": True}}], {"upsert": True}
                ),
                WriteRequest(
                    "insert_many",
                    [[{"_id": 4, "a": 1}, {"_id": 5, "a": True}, {"_id": 6, "a": 1}]],
                ),
                WriteRequest(
                    "bulk_write",
                    [
                        [
                            InsertOne({"_id": 7, "a": 1}),
                            UpdateOne({"_id": 1}, {"$set": {"a": True}}),
                            InsertOne({"_id": 8, "a": 1}),
                        ]
                    ],
                    {"ordered": False},
                ),
                WriteRequest("update_many", [{}, {"$set": {"a": True}}]),
                WriteRequest(
                    "update_one",
                    [{"_id": 1}, {"$set": {"a": True}}],
                    {"bypass_document_validation": True},
                ),
            ],
            options={
                "validator": {"$jsonSchema": {"properties": {"a": {"enum": [1]}}}},
                "validationLevel": level,
                "validationAction": action,
            },
        )
        output[level + "_" + action] = observe_write_case(
            collection.database, name, case, error_contract=True
        )
    return output


def validator_changes(collection):
    name = collection.name + "_changes"
    collection.database.create_collection(
        name, validator={"$jsonSchema": {"properties": {"a": {"enum": [1]}}}}
    )
    case = WriteCase(
        name,
        [],
        [
            WriteRequest("insert_one", [{"_id": 1, "a": True}]),
            WriteRequest(
                "command",
                [
                    {
                        "collMod": name,
                        "validator": {
                            "$jsonSchema": {"properties": {"a": {"enum": [True]}}}
                        },
                    }
                ],
            ),
            WriteRequest("insert_one", [{"_id": 1, "a": True}]),
            WriteRequest(
                "command",
                [{"collMod": name, "validator": {"$jsonSchema": {"enum": []}}}],
            ),
            WriteRequest("insert_one", [{"_id": 2, "a": True}]),
            WriteRequest("command", [{"collMod": name, "validationLevel": "off"}]),
            WriteRequest("insert_one", [{"_id": 3, "a": 1}]),
            WriteRequest("command", [{"collMod": name, "validationLevel": "strict"}]),
            WriteRequest("insert_one", [{"_id": 4, "a": 1}]),
        ],
    )
    return observe_write_case(
        collection.database, name, case, existing=True, error_contract=True
    )


def moderate_undefined(collection):
    # Raw BSON is required: PyMongo's default decoder maps undefined to None.
    encoded = BSON.encode({"_id": 1})
    raw = encoded[:-1] + b"\x06a\0\0"
    raw = len(raw).to_bytes(4, "little", signed=True) + raw[4:]
    output = {}
    for level in ("strict", "moderate"):
        name = collection.name + "_undefined_" + level
        collection.database.create_collection(
            name, validator={"a": {"$ne": None}}, validationLevel=level
        )
        target = collection.database[name]
        target.database.command(
            {
                "insert": name,
                "documents": [RawBSONDocument(raw)],
                "bypassDocumentValidation": True,
            }
        )
        before = list(target.aggregate([{"$project": {"kind": {"$type": "$a"}}}]))
        try:
            result = target.update_one({"_id": 1}, {"$set": {"a": None}})
            status = {"ok": True, "modified": result.modified_count}
        except OperationFailure as error:
            status = {"ok": False, "code": error.code}
        output[level] = {
            "before": before,
            "status": status,
            "after": list(target.aggregate([{"$project": {"kind": {"$type": "$a"}}}])),
            "recovery": target.count_documents({}),
        }
    return output


def writeback(collection):
    output = {}
    for mode in ("replace", "merge"):
        name = collection.name + "_merge_" + mode
        case = WriteCase(
            mode,
            [{"_id": 1, "a": 1}, {"_id": 2, "a": True}],
            [
                WriteRequest(
                    "aggregate",
                    [
                        [
                            {"$sort": {"_id": 1}},
                            {"$merge": {"into": name, "whenMatched": mode}},
                        ]
                    ],
                ),
                WriteRequest("insert_one", [{"_id": 3, "a": 1}]),
            ],
            options={
                "validator": {"$jsonSchema": {"properties": {"a": {"enum": [1]}}}}
            },
        )
        output[mode] = observe_write_case(
            collection.database, name, case, error_contract=True
        )
    return output


def configuration(collection):
    output = {}
    for label, validator, values in (
        (
            "decimal",
            {"$jsonSchema": {"properties": {"a": {"minimum": Decimal128("0.1")}}}},
            [Decimal128("0.1"), 0.1],
        ),
        ("object_order", {"a": {"y": 1, "x": 2}}, [{"y": 1, "x": 2}, {"x": 2, "y": 1}]),
        ("mql_collation", {"a": "a"}, ["A", "B"]),
        (
            "schema_collation",
            {"$jsonSchema": {"properties": {"a": {"enum": ["a"]}}}},
            ["A", "a"],
        ),
    ):
        options = {"validator": validator}
        if "collation" in label:
            options["collation"] = {"locale": "en", "strength": 2}
        name = collection.name + "_configuration_" + label
        target = collection.database.create_collection(name, **options)
        statuses = []
        for i, value in enumerate(values):
            try:
                target.insert_one({"_id": i, "a": value})
                statuses.append({"ok": True})
            except OperationFailure as error:
                statuses.append({"ok": False, "code": error.code})
        output[label] = {
            "validator": BSONObservation([validator]).value(
                target.options()["validator"]
            ),
            "statuses": statuses,
            "ids": [doc["_id"] for doc in target.find({}, sort=[("_id", 1)])],
        }
    return output


REPRESENTATION_VALUES = [
    Code("abc"),
    Code("abc", {"x": 1}),
    DBRef("items", 1),
    {"$ref": "items", "$id": 1},
    "abc",
    {"x": 1},
    Code("abc", {"a": 1, "b": 2}),
    Code("abc", {"b": 2, "a": 1}),
    [Code("abc", {"a": 1, "b": 2}), Code("abc", {"b": 2, "a": 1})],
    [DBRef("items", 1), {"$ref": "items", "$id": 1}],
    Code("abc", {}),
]
REPRESENTATION_RULES = {
    "string": {"bsonType": "string"},
    "string_length": {"minLength": 4},
    "pattern": {"pattern": "z"},
    "object": {"bsonType": "object", "required": ["x"]},
    "reference_property": {"properties": {"$id": {"minimum": 2}}},
    "reference_enum": {"enum": [{"$ref": "items", "$id": 1}]},
    "code_enum": {"enum": [Code("abc", {"a": 1, "b": 2})]},
    "plain_code_enum": {"enum": [Code("abc")]},
    "empty_scope_enum": {"enum": [Code("abc", {})]},
    "unique": {"uniqueItems": True},
    "additional": {"additionalProperties": False},
}


def representation_decisions(collection):
    collection.delete_many({})
    collection.insert_many(
        [
            {"_id": i, "a": deepcopy(value)}
            for i, value in enumerate(REPRESENTATION_VALUES)
        ]
    )
    return {
        label: [
            r["_id"]
            for r in collection.find(
                {"$jsonSchema": {"properties": {"a": rule}}}, sort=[("_id", 1)]
            )
        ]
        for label, rule in REPRESENTATION_RULES.items()
    }


def representation_writes(collection):
    # Keep the strict state observer on the canonical SDK DBRef representation;
    # the decision case also tests the BSON-equivalent plain document.
    values = deepcopy(REPRESENTATION_VALUES)
    values[3] = DBRef("items", 1)
    values[9] = [DBRef("items", 1), DBRef("items", 1)]
    rules = deepcopy(REPRESENTATION_RULES)
    rules["reference_enum"] = {"enum": [DBRef("items", 1)]}
    return {
        label: observe_write_case(
            collection.database,
            collection.name + "_" + label,
            WriteCase(
                label,
                [],
                [
                    WriteRequest("insert_one", [{"_id": i, "a": deepcopy(value)}])
                    for i, value in enumerate(values)
                ],
                options={"validator": {"$jsonSchema": {"properties": {"a": rule}}}},
            ),
            error_contract=True,
        )
        for label, rule in rules.items()
    }


def bson_preparation(collection):
    output = {}
    rules = {
        "code_type": {"type": Code("string")},
        "code_bson_type": {"bsonType": [Code("string")]},
        "code_title": {"title": Code("title")},
        "code_description": {"description": Code("description")},
        "code_pattern": {"pattern": Code("abc")},
        "code_required": {"required": [Code("a")]},
    }
    for keyword in ("minLength", "maxLength", "minItems", "maxItems"):
        for label, bound in (
            ("int64", Int64(2**63 - 1)),
            ("double_inside", float(2**63) - 1024),
            ("double_outside", float(2**63)),
            ("decimal_outside", Decimal128("9223372036854775808")),
            ("fraction", 0.5),
            ("decimal_fraction", Decimal128("0.5")),
            ("negative", -1),
            ("bool", True),
            ("null", None),
            ("string", "one"),
            ("nan", float("nan")),
            ("infinite", float("inf")),
        ):
            rules[keyword + "_" + label] = {keyword: bound}
    for populated in (False, True):
        collection.delete_many({})
        if populated:
            collection.insert_one({"_id": 1, "a": 1})
        for label, schema in rules.items():
            try:
                list(collection.aggregate([{"$match": {"$jsonSchema": schema}}]))
                result = {"ok": True}
            except OperationFailure as error:
                result = {
                    "ok": False,
                    "code": error.code,
                    "message": error.details.get("errmsg"),
                }
            output[f"{populated}_{label}"] = result
    return output


SEMANTIC_SCHEMA_CASES = (
    ("semantic_schema_bson_preparation", bson_preparation),
    ("semantic_schema_bson_representations", representation_decisions),
    ("semantic_schema_bson_representation_writes", representation_writes),
    ("semantic_schema_configuration", configuration),
    ("semantic_schema_writeback", writeback),
    ("semantic_schema_moderate_undefined", moderate_undefined),
    ("semantic_schema_lifecycle", lifecycle),
    ("semantic_schema_validator_changes", validator_changes),
    ("semantic_schema_find", partial(decisions, surface="find")),
    ("semantic_schema_aggregate", partial(decisions, surface="aggregate")),
    ("semantic_schema_preparation", preparation),
    ("semantic_schema_equality_writes", partial(writes, family="equality")),
    ("semantic_schema_numeric_writes", partial(writes, family="numeric")),
)
