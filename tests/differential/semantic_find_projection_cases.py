"""Find and aggregation projection grammar with independent native references."""

# ruff: noqa: B023 - each witness is observed within its iteration

from datetime import datetime

from bson import Binary, Code, Decimal128

from tests.differential.semantic_bson_admission_cases import _observe


SEED = [
    {
        "_id": 1,
        "v": 7,
        "name": "Ada",
        "o": {"x": 9, "y": 4},
        "a": [{"x": 2}, {"x": 3}, {}],
        "array": [1, 2, 3],
    },
    {"_id": 2, "v": None, "name": "ada", "o": {"y": 8}, "a": [], "array": []},
    {"_id": 3},
]
ROUTES = ("find", "find-one", "aggregate", "command")


def _invoke(collection, route, projection, **kwargs):
    if route == "find":
        return collection.find({}, projection, **kwargs)
    if route == "find-one":
        return collection.find_one({}, projection, **kwargs)
    if route == "aggregate":
        return collection.aggregate([{"$project": projection}], **kwargs)
    command = {"find": collection.name, "projection": projection, **kwargs}
    return collection.database.command(command)["cursor"]["firstBatch"]


def projection_constants(collection):
    output = {}
    for label, value in (
        ("double-zero", 0.0),
        ("double-two", 2.5),
        ("int-negative", -2),
        ("decimal-zero", Decimal128("0")),
        ("decimal-two", Decimal128("2")),
        ("decimal-nan", Decimal128("NaN")),
        ("decimal-snan", Decimal128("sNaN")),
        ("null", None),
        ("string", "hello"),
        ("field", "$o.x"),
        ("code", Code("$o.x")),
        ("date", datetime(1970, 1, 1, 0, 0, 1, 1000)),  # noqa: DTZ001 - default BSON UTC read
        ("binary0", Binary(b"ab", 0)),
        ("binary5-short", Binary(b"ab", 5)),
        ("binary5-valid", Binary(bytes(range(16)), 5)),
        ("array", [1, "$v", "$missing", {"$add": [1, 2]}]),
        ("literal-document", {"$literal": {"x": 1, "y": 0}}),
        ("object-array", {"x": [True, None, "a", 1.5]}),
        ("add", {"$add": [1, 2]}),
    ):
        for route in ROUTES:
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, {"v": value}),
                cursor=route in {"find", "aggregate"},
            )
    return output


def projection_shapes(collection):
    output = {}
    for label, projection in (
        ("nested-include", {"o": {"x": 1}}),
        ("nested-exclude", {"o": {"x": 0}}),
        ("nested-computed", {"o": {"x": {"$add": [1, 2]}, "y": 1}}),
        ("include-computed", {"o.x": 1, "value": "$v"}),
        ("nested-missing", {"o": {"x": "$absent"}}),
        ("dotted-missing", {"o.x": "$absent"}),
        ("dotted-remove", {"a.x": "$$REMOVE"}),
        ("array-computed", {"a.x": {"$add": [1, 2]}}),
        ("computed-id", {"_id": {"$literal": "changed"}, "v": 1}),
        ("id-zero-computed", {"_id": 0, "x": "$v"}),
        ("exclude-computed", {"v": 0, "x": "$name"}),
        ("computed-exclude", {"x": "$name", "v": 0}),
        ("empty-object", {"o": {}}),
        ("unknown-op", {"x": {"$notAnOperator": 1}}),
        ("slice-computed", {"array": {"$slice": 1}, "x": "$v"}),
        ("elem-computed", {"a": {"$elemMatch": {"x": 2}}, "x": "$v"}),
        ("meta-computed", {"x": {"$meta": "textScore"}, "v": "$name"}),
    ):
        for route in ROUTES:
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, projection),
                cursor=route in {"find", "aggregate"},
            )
    return output


def projection_context(collection):
    output = {}
    for route in ROUTES:
        for label, projection, kwargs in (
            ("let", {"v": "$$value"}, {"let": {"value": 11}}),
            ("root", {"x": "$$ROOT.o.x"}, {}),
            ("clock", {"x": {"$eq": ["$$NOW", "$$NOW"]}}, {}),
            (
                "collation",
                {"x": {"$eq": ["$name", "ada"]}},
                {"collation": {"locale": "en", "strength": 2}},
            ),
        ):
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, projection, **kwargs),
                cursor=route in {"find", "aggregate"},
            )
    return output


SEMANTIC_FIND_PROJECTION_CASES = (
    ("semantic_find_projection_constants", projection_constants),
    ("semantic_find_projection_shapes", projection_shapes),
    ("semantic_find_projection_context", projection_context),
)


def projection_array_shapes(collection):
    output = {}
    rows = [{"_id": 1, "a": [1, None, {}, {"x": 8}, [{"x": 9}]]}, {"_id": 2, "a": None}]
    collection.drop()
    collection.insert_many(rows)
    for label, projection in (
        ("include", {"a.x": 1}),
        ("computed", {"a.x": {"$literal": 3}}),
        ("nested-computed", {"a": {"x": {"$literal": 3}}}),
    ):
        for route in ROUTES:
            output[f"{label}/{route}"] = _observe(
                collection,
                lambda: _invoke(collection, route, projection),
                cursor=route in {"find", "aggregate"},
            )
    return output


def projection_modify(collection):
    output = {}
    for route in ("update", "replace", "delete"):
        for label, projection, kwargs in (
            ("computed", {"value": {"$add": ["$v", 1]}}, {}),
            ("after", {"value": {"$add": ["$v", 1]}}, {"return_document": True}),
            ("let", {"value": "$$value"}, {"let": {"value": 11}}),
            (
                "collation",
                {"value": {"$eq": ["$name", "ada"]}},
                {"collation": {"locale": "en", "strength": 2}},
            ),
            ("dynamic-error", {"value": {"$divide": [1, "$zero"]}}, {}),
            ("constant-error", {"value": {"$divide": [1, 0]}}, {}),
        ):
            if route == "delete" and label == "after":
                continue
            collection.drop()
            collection.insert_one({"_id": 1, "v": 7, "zero": 0, "name": "Ada"})

            def invoke():
                if route == "update":
                    return collection.find_one_and_update(
                        {"_id": 1}, {"$set": {"v": 8}}, projection=projection, **kwargs
                    )
                if route == "replace":
                    return collection.find_one_and_replace(
                        {"_id": 1},
                        {"_id": 1, "v": 8, "zero": 0, "name": "Ada"},
                        projection=projection,
                        **kwargs,
                    )
                return collection.find_one_and_delete(
                    {"_id": 1}, projection=projection, **kwargs
                )

            output[f"{label}/{route}"] = _observe(collection, invoke)
    return output


def projection_empty_and_id(collection):
    output = {}
    for populated in (False, True):
        collection.drop()
        if populated:
            collection.insert_many(SEED)
        for route in ROUTES:
            for label, projection in (
                ("meta", {"x": {"$meta": "textScore"}}),
                ("nested-meta", {"x": {"$gt": [{"$meta": "textScore"}, 0]}}),
                ("literal-meta", {"x": {"$literal": {"$meta": "textScore"}}}),
                ("binary", {"x": Binary(b"ab", 5)}),
                ("let", {"x": "$$value"}),
                ("id-zero", {"_id": Decimal128("0"), "v": 1}),
            ):
                output[f"{populated}/{label}/{route}"] = _observe(
                    collection,
                    lambda: _invoke(collection, route, projection, let={"value": 11}),
                    cursor=route in {"find", "aggregate"},
                )
        if populated:
            output["find-one-id/let"] = _observe(
                collection,
                lambda: collection.find_one(
                    {"_id": 1}, {"x": "$$value"}, let={"value": 11}
                ),
            )
    return output


SEMANTIC_FIND_PROJECTION_CASES += (
    ("semantic_find_projection_array_shapes", projection_array_shapes),
    ("semantic_find_projection_modify", projection_modify),
    ("semantic_find_projection_empty_and_id", projection_empty_and_id),
)


def projection_metadata_sources(collection):
    output = {}
    project = {"$project": {"available": {"$gt": [{"$meta": "textScore"}, 0]}}}
    text = {"$match": {"$text": {"$search": "Ada"}}}
    for populated in (False, True):
        collection.drop()
        collection.create_index([("name", "text")])
        if populated:
            collection.insert_many(SEED)
        for label, pipeline in (
            ("absent", [project]),
            ("text", [text, {"$sort": {"_id": 1}}, project]),
            (
                "text-project",
                [text, {"$sort": {"_id": 1}}, {"$project": {"name": 1}}, project],
            ),
            ("text-count", [text, {"$count": "n"}, project]),
            ("facet-absent", [{"$facet": {"rows": [project]}}]),
            (
                "facet-text",
                [text, {"$sort": {"_id": 1}}, {"$facet": {"rows": [project]}}],
            ),
            ("facet-outer", [text, {"$facet": {"rows": []}}, project]),
        ):
            output[f"{populated}/{label}"] = _observe(
                collection, lambda: collection.aggregate(pipeline), cursor=True
            )
    return output


SEMANTIC_FIND_PROJECTION_CASES += (
    ("semantic_find_projection_metadata_sources", projection_metadata_sources),
)
