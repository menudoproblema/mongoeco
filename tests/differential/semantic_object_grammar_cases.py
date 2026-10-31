"""Structured field expressions and unary accumulator grammar, native witnesses."""

from copy import deepcopy
from functools import partial

from bson import Decimal128

from tests.differential.semantic_expression_grammar_cases import (
    _invoke as _invoke_expression,
    _observe,
    _reset,
)


SEED = [{"_id": 1, "a": 5, "name": "a", "object": {"a": 3}}]

FORMS = {
    "get_empty": {"$getField": {}},
    "child_before_unknown_key": {
        "$getField": {"field": {"$unknown": 1}, "other": 1, "input": {}}
    },
    "unknown_key_before_child": {
        "$getField": {"other": 1, "field": {"$unknown": 1}, "input": {}}
    },
    "child_before_required_field": {"$getField": {"input": {"$unknown": 1}}},
    "undefined_before_unknown_key": {
        "$getField": {"field": "a", "input": "$$unknown", "other": 1}
    },
    "set_child_before_required_field": {
        "$setField": {"input": {"$unknown": 1}, "value": 1}
    },
    "get_unknown_expression": {"$getField": {"field": {"$unknown": 1}, "input": {}}},
    "set_unknown_expression": {
        "$setField": {"field": {"$unknown": 1}, "input": {}, "value": 1}
    },
    "get_no_input": {"$getField": {"field": "a"}},
    "get_no_field": {"$getField": {"input": {}}},
    "get_unknown": {"$getField": {"field": "a", "input": {}, "other": 1}},
    "get_field_null": {"$getField": {"field": None, "input": {}}},
    "get_field_number": {"$getField": {"field": 1, "input": {}}},
    "get_input_scalar": {"$getField": {"field": "a", "input": 1}},
    "get_input_array": {"$getField": {"field": "a", "input": []}},
    "get_input_missing": {"$getField": {"field": "a", "input": "$absent"}},
    "get_dynamic": {"$getField": {"field": "$name", "input": "$$CURRENT"}},
    "get_shorthand": {"$getField": "a"},
    "get_current": {"$let": {"vars": {"CURRENT": {"a": 9}}, "in": {"$getField": "a"}}},
    "set_empty": {"$setField": {}},
    "set_no_field": {"$setField": {"input": {}, "value": 1}},
    "set_no_input": {"$setField": {"field": "a", "value": 1}},
    "set_no_value": {"$setField": {"field": "a", "input": {}}},
    "set_unknown": {"$setField": {"field": "a", "input": {}, "value": 1, "other": 1}},
    "set_field_number": {"$setField": {"field": 1, "input": {}, "value": 1}},
    "set_dynamic": {"$setField": {"field": "$name", "input": {}, "value": 1}},
    "set_input_scalar": {"$setField": {"field": "a", "input": 1, "value": 1}},
    "set_constant": {
        "$setField": {"field": {"$concat": ["a", "b"]}, "input": {}, "value": 1}
    },
    "unset_empty": {"$unsetField": {}},
    "unset_no_field": {"$unsetField": {"input": {}}},
    "unset_no_input": {"$unsetField": {"field": "a"}},
    "unset_value": {"$unsetField": {"field": "a", "input": {}, "value": 1}},
    "unset_unknown": {"$unsetField": {"field": "a", "input": {}, "other": 1}},
    "unset_field_number": {"$unsetField": {"field": 1, "input": {}}},
    "unset_input_scalar": {"$unsetField": {"field": "a", "input": 1}},
    "getField_null": {"$getField": None},
    "getField_number": {"$getField": 5},
    "getField_array": {"$getField": []},
    "setField_null": {"$setField": None},
    "setField_number": {"$setField": 5},
    "setField_array": {"$setField": []},
    "unsetField_null": {"$unsetField": None},
    "unsetField_number": {"$unsetField": 5},
    "unsetField_array": {"$unsetField": []},
    "get_field_concat": {"$getField": {"field": {"$concat": ["a", "b"]}, "input": {}}},
    "get_field_array": {"$getField": {"field": [], "input": {}}},
    "set_field_array": {"$setField": {"field": [], "input": {}, "value": 1}},
    "get_literal_name": {
        "$getField": {"field": {"$literal": "$a"}, "input": {"$literal": {"$a": 5}}}
    },
    "set_literal_name": {
        "$setField": {"field": {"$literal": "$a"}, "input": {}, "value": 1}
    },
}


def _invoke(collection, context, expression):
    if context == "find_one":
        return collection.find_one({"$expr": expression})
    if context == "distinct":
        return collection.distinct("_id", {"$expr": expression})
    return _invoke_expression(collection, context, expression)


def fields(collection, *, context):
    output = {}
    namespace = f"{collection.database.name}.{collection.name}"
    for state, documents in (("empty", []), ("rows", SEED)):
        for name, expression in FORMS.items():
            _reset(collection, documents)
            value = _observe(
                partial(_invoke, collection, context, deepcopy(expression)), namespace
            )
            distinct_prefix = "Executor error during distinct command on namespace: "
            prefixes = (
                (
                    f"Executor error during find command: {namespace} ::",
                    "Executor error during find command: <namespace> ::",
                ),
                (
                    distinct_prefix + namespace + " ::",
                    distinct_prefix + "<namespace> ::",
                ),
            )
            for prefix, replacement in prefixes:
                if not value["ok"] and value["message"].startswith(prefix):
                    value["message"] = value["message"].replace(prefix, replacement, 1)
            value["state"] = list(collection.find({}, sort=[("_id", 1)]))
            value["recovery"] = collection.count_documents({})
            output[f"{state}_{name}"] = value
    return output


def unary_accumulators(collection, *, context):
    output = {}
    namespace = f"{collection.database.name}.{collection.name}"
    for state, documents in (("empty", []), ("rows", SEED)):
        for operator in (
            "$sum",
            "$avg",
            "$first",
            "$last",
            "$min",
            "$max",
            "$push",
            "$addToSet",
        ):
            for label, operand in (
                ("empty", []),
                ("one", [1]),
                ("two", [1, 2]),
                ("literal", {"$literal": [1, 2]}),
            ):
                _reset(collection, documents)
                if context == "group":
                    pipeline = [{"$group": {"_id": None, "v": {operator: operand}}}]
                elif context == "bucket":
                    pipeline = [
                        {
                            "$bucket": {
                                "groupBy": "$a",
                                "boundaries": [0, 10],
                                "output": {"v": {operator: operand}},
                            }
                        }
                    ]
                else:
                    pipeline = [
                        {
                            "$setWindowFields": {
                                "sortBy": {"_id": 1},
                                "output": {
                                    "v": {
                                        operator: operand,
                                        "window": {
                                            "documents": ["unbounded", "current"]
                                        },
                                    }
                                },
                            }
                        }
                    ]
                value = _observe(
                    lambda pipeline=pipeline: list(collection.aggregate(pipeline)),
                    namespace,
                )
                value["state"] = list(collection.find({}, sort=[("_id", 1)]))
                value["recovery"] = collection.count_documents({})
                output[f"{state}_{operator}_{label}"] = value
    return output


def command_fields(collection):
    output = {}
    namespace = f"{collection.database.name}.{collection.name}"
    for name, bindings in {
        "implicit_current": {"x": {"$getField": "a"}},
        "closed_current": {"CURRENT": {"a": 9}, "x": {"$getField": "a"}},
        "explicit_input": {"x": {"$getField": {"field": "a", "input": {"a": 9}}}},
    }.items():
        _reset(collection, SEED)
        output[name] = _observe(
            lambda bindings=bindings: list(
                collection.aggregate([{"$project": {"v": "$$x"}}], let=bindings)
            ),
            namespace,
        )
    return output


def numeric_group_identity(collection, **options):
    # Observe cardinality/values, not a server's choice of BSON representative
    # among equal numbers. Other numeric witnesses retain raw types/cohorts.
    values = [
        0,
        2,
        -0.0,
        3,
        Decimal128("-0"),
        0.5,
        Decimal128("0.500"),
        Decimal128("5E-1"),
        1000,
        Decimal128("1E+3"),
        1000.0,
    ]
    _reset(collection, [{"_id": i, "value": v} for i, v in enumerate(values)])
    return list(
        collection.aggregate(
            [
                {"$group": {"_id": "$value", "n": {"$sum": 1}}},
                {"$replaceWith": {"value": {"$toDouble": "$_id"}, "count": "$n"}},
                {"$sort": {"value": 1}},
            ],
            **options,
        )
    )


SEMANTIC_OBJECT_GRAMMAR_CASES = (
    ("semantic_object_numeric_group_identity", numeric_group_identity),
    ("semantic_object_command_fields", command_fields),
    *(
        (f"semantic_object_fields_{context}", partial(fields, context=context))
        for context in (
            "project",
            "set",
            "expr",
            "find",
            "find_one",
            "distinct",
            "count",
            "update",
        )
    ),
    *(
        (
            f"semantic_object_unary_{context}",
            partial(unary_accumulators, context=context),
        )
        for context in ("group", "bucket", "window")
    ),
)
