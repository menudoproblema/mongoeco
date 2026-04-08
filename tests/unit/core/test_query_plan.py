import re
import unittest
import uuid

from shapely.geometry import MultiPoint, Point, box as shapely_box

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MongoDialect
from mongoeco.errors import OperationFailure
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    DeferredQueryNode,
    ElemMatchCondition,
    EqualsCondition,
    ExistsCondition,
    GeoIntersectsCondition,
    GeoWithinCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    JsonSchemaCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NearCondition,
    NotCondition,
    NotInCondition,
    OrCondition,
    RegexCondition,
    SizeCondition,
    TypeCondition,
    WhereCondition,
    _coerce_bitwise_mask,
    _normalize_type_specifier,
    _regex_options_from_pattern,
    compile_filter,
    ensure_query_plan,
)
from mongoeco.types import PlanningMode, Regex


class QueryPlanTests(unittest.TestCase):
    def test_compile_filter_can_use_custom_dialect_operator_catalog(self):
        class _NoRegexDialect(MongoDialect):
            def supports_query_field_operator(self, name: str) -> bool:
                return False if name == "$regex" else super().supports_query_field_operator(name)

        with self.assertRaises(OperationFailure):
            compile_filter(
                {"name": {"$regex": "^ad"}},
                dialect=_NoRegexDialect(key="test", server_version="test", label="No Regex"),
            )

    def test_compile_filter_rejects_custom_supported_but_unimplemented_field_operator(self):
        class _FutureFieldDialect(MongoDialect):
            def supports_query_field_operator(self, name: str) -> bool:
                return True if name == "$futureField" else super().supports_query_field_operator(name)

        with self.assertRaises(OperationFailure):
            compile_filter(
                {"name": {"$futureField": "Ada"}},
                dialect=_FutureFieldDialect(
                    key="test",
                    server_version="test",
                    label="Future Field",
                ),
            )

    def test_compile_filter_rejects_custom_supported_but_unimplemented_top_level_operator(self):
        class _FutureTopLevelDialect(MongoDialect):
            def supports_query_top_level_operator(self, name: str) -> bool:
                return True if name == "$futureTopLevel" else super().supports_query_top_level_operator(name)

        with self.assertRaises(OperationFailure):
            compile_filter(
                {"$futureTopLevel": {"name": "Ada"}},
                dialect=_FutureTopLevelDialect(
                    key="test",
                    server_version="test",
                    label="Future Top Level",
                ),
            )

    def test_compile_filter_relaxed_mode_defers_invalid_where_failures(self):
        plan = compile_filter(
            {"$where": "len(this.a) > 1"},
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertIsInstance(plan, DeferredQueryNode)
        self.assertEqual(plan.issue.scope, "query")

    def test_compile_filter_supports_safe_where_expressions_and_callables(self):
        where_expression_plan = compile_filter({"$where": "this.age >= 18 and this.role == 'admin'"})
        self.assertIsInstance(where_expression_plan, WhereCondition)
        self.assertEqual(where_expression_plan.expression, "this.age >= 18 and this.role == 'admin'")
        self.assertIsNotNone(where_expression_plan.compiled_expression)
        self.assertIsNone(where_expression_plan.predicate)

        where_callable = lambda doc: doc.get("age", 0) >= 18
        where_callable_plan = compile_filter({"$where": where_callable})
        self.assertIsInstance(where_callable_plan, WhereCondition)
        self.assertIs(where_callable_plan.predicate, where_callable)
        self.assertIsNone(where_callable_plan.expression)
        self.assertIsNone(where_callable_plan.compiled_expression)

    def test_compile_filter_rejects_unsafe_where_expressions(self):
        with self.assertRaisesRegex(ValueError, "safe local expression subset"):
            compile_filter({"$where": "len(this.tags) > 0"})
        with self.assertRaisesRegex(ValueError, "only supports names"):
            compile_filter({"$where": "other_name"})
        with self.assertRaisesRegex(ValueError, "must be a non-empty string or callable"):
            compile_filter({"$where": ""})

    def test_geo_query_plan_rejects_invalid_local_runtime_shapes(self):
        with self.assertRaisesRegex(ValueError, "\\$geoWithin requires a document specification"):
            compile_filter({"location": {"$geoWithin": "bad"}})
        with self.assertRaisesRegex(OperationFailure, "only supports Polygon or MultiPolygon"):
            compile_filter(
                {
                    "location": {
                        "$geoWithin": {
                            "$geometry": {"type": "Point", "coordinates": [0, 0]},
                        }
                    }
                }
            )
        with self.assertRaisesRegex(ValueError, "\\$geoIntersects requires a \\$geometry document"):
            compile_filter({"location": {"$geoIntersects": {}}})
        with self.assertRaisesRegex(ValueError, "\\$near requires a \\$geometry point"):
            compile_filter({"location": {"$near": {}}})
        with self.assertRaisesRegex(OperationFailure, "\\$nearSphere.\\$minDistance must be a non-negative finite number"):
            compile_filter(
                {
                    "location": {
                        "$nearSphere": {
                            "$geometry": {"type": "Point", "coordinates": [0, 0]},
                            "$minDistance": -1,
                        }
                    }
                }
            )

    def test_compile_filter_produces_typed_field_conditions(self):
        plan = compile_filter({"name": "Ada", "age": {"$gte": 18}})

        self.assertIsInstance(plan, AndCondition)
        self.assertEqual(
            plan.clauses,
            (
                EqualsCondition("name", "Ada"),
                GreaterThanOrEqualCondition("age", 18),
            ),
        )

    def test_compile_filter_produces_logical_conditions(self):
        plan = compile_filter({"$or": [{"name": "Ada"}, {"role": "admin"}]})

        self.assertIsInstance(plan, OrCondition)
        self.assertEqual(
            plan.clauses,
            (
                EqualsCondition("name", "Ada"),
                EqualsCondition("role", "admin"),
            ),
        )

    def test_compile_filter_supports_nor(self):
        plan = compile_filter({"$nor": [{"name": "Ada"}, {"role": "admin"}]})

        self.assertEqual(
            plan,
            NotCondition(
                OrCondition(
                    (
                        EqualsCondition("name", "Ada"),
                        EqualsCondition("role", "admin"),
                    )
                )
            ),
        )

    def test_compile_filter_rejects_excessive_logical_nesting_depth(self):
        nested = {"name": "Ada"}
        for _ in range(101):
            nested = {"$and": [nested]}

        with self.assertRaisesRegex(OperationFailure, "maximum nesting depth"):
            compile_filter(nested)

    def test_compile_filter_expands_in_operator_to_typed_leaf(self):
        plan = compile_filter({"role": {"$in": ["admin", "staff"]}})

        self.assertEqual(plan, InCondition("role", ("admin", "staff")))

    def test_compile_filter_carries_null_undefined_behavior_in_equals_and_in(self):
        self.assertEqual(
            compile_filter({"role": None}, dialect=MONGODB_DIALECT_70),
            EqualsCondition("role", None, null_matches_undefined=True),
        )
        self.assertEqual(
            compile_filter({"role": None}, dialect=MONGODB_DIALECT_80),
            EqualsCondition("role", None, null_matches_undefined=False),
        )
        self.assertEqual(
            compile_filter({"role": {"$in": [None]}}, dialect=MONGODB_DIALECT_70),
            InCondition("role", (None,), null_matches_undefined=True),
        )
        self.assertEqual(
            compile_filter({"role": {"$nin": [None]}}, dialect=MONGODB_DIALECT_70),
            NotInCondition("role", (None,), null_matches_undefined=True),
        )
        self.assertEqual(
            compile_filter({"role": {"$nin": [None]}}, dialect=MONGODB_DIALECT_80),
            NotInCondition("role", (None,), null_matches_undefined=False),
        )

    def test_compile_filter_returns_and_condition_for_multiple_field_operators(self):
        plan = compile_filter({"age": {"$gte": 18, "$in": [18, 21, 30]}})

        self.assertIsInstance(plan, AndCondition)

    def test_compile_filter_supports_all_size_and_regex(self):
        self.assertEqual(
            compile_filter({"tags": {"$all": ["python", "mongodb"]}}),
            AllCondition("tags", ("python", "mongodb")),
        )
        self.assertEqual(
            compile_filter({"tags": {"$size": 2}}),
            SizeCondition("tags", 2),
        )
        self.assertEqual(
            compile_filter({"count": {"$lte": 2}}),
            LessThanOrEqualCondition("count", 2),
        )
        self.assertEqual(
            compile_filter({"count": {"$mod": [3, 1]}}),
            ModCondition("count", 3, 1),
        )
        self.assertEqual(
            compile_filter({"name": {"$regex": "^ad", "$options": "i"}}),
            RegexCondition("name", "^ad", "i"),
        )
        self.assertEqual(
            compile_filter({"name": {"$regex": re.compile("^ad", re.IGNORECASE)}}),
            RegexCondition("name", "^ad", "i"),
        )
        self.assertEqual(
            compile_filter({"name": {"$regex": Regex("^ad", "i"), "$options": "m"}}),
            RegexCondition("name", "^ad", "im"),
        )
        self.assertEqual(
            compile_filter({"name": re.compile("^ad", re.IGNORECASE)}),
            RegexCondition("name", "^ad", "i"),
        )
        self.assertEqual(
            compile_filter({"name": {"$not": {"$regex": "^ad", "$options": "i"}}}),
            NotCondition(RegexCondition("name", "^ad", "i")),
        )
        elem_match = compile_filter({"items": {"$elemMatch": {"kind": "a"}}})
        self.assertIsInstance(elem_match, ElemMatchCondition)
        self.assertEqual(elem_match.field, "items")
        self.assertEqual(elem_match.condition, {"kind": "a"})
        self.assertFalse(elem_match.wrap_value)
        self.assertIsNotNone(elem_match.compiled_plan)
        self.assertEqual(elem_match.compiled_dialect_key, MONGODB_DIALECT_70.key)
        self.assertEqual(
            compile_filter({"flag": {"$exists": False}}),
            ExistsCondition("flag", False),
        )
        self.assertEqual(
            compile_filter({"flag": {"$exists": 1}}),
            ExistsCondition("flag", True),
        )
        self.assertEqual(
            compile_filter({"flag": {"$exists": 0}}),
            ExistsCondition("flag", False),
        )
        self.assertEqual(
            compile_filter({"flag": {"$exists": "yes"}}),
            ExistsCondition("flag", True),
        )
        self.assertEqual(
            compile_filter({"flag": {"$type": ["number", "string"]}}),
            TypeCondition("flag", ("number", "string"), aliases=frozenset({"double", "int", "long", "decimal", "string"})),
        )
        self.assertEqual(
            compile_filter({"flag": {"$type": [13, 14, 15]}}),
            TypeCondition(
                "flag",
                (13, 14, 15),
                aliases=frozenset({"javascript", "symbol", "javascriptWithScope"}),
            ),
        )
        self.assertIsInstance(compile_filter({"$comment": "trace-only"}), MatchAll)

    def test_compile_filter_rejects_invalid_all_size_and_regex_payloads(self):
        with self.assertRaises(ValueError):
            compile_filter({"tags": {"$all": "python"}})
        with self.assertRaises(ValueError):
            compile_filter({"tags": {"$size": -1}})
        with self.assertRaises(ValueError):
            compile_filter({"tags": {"$size": 1 << 40}})
        with self.assertRaises(ValueError):
            compile_filter({"count": {"$mod": 3}})
        with self.assertRaises(ValueError):
            compile_filter({"count": {"$mod": [0, 1]}})
        with self.assertRaises(ValueError):
            compile_filter({"count": {"$mod": [3, "x"]}})
        with self.assertRaises(ValueError):
            compile_filter({"name": {"$regex": 123}})
        with self.assertRaises(ValueError):
            compile_filter({"name": {"$options": 1}})
        with self.assertRaises(ValueError):
            compile_filter({"name": {"$not": 1}})
        with self.assertRaises(ValueError):
            compile_filter({"name": {"$not": {}}})
        with self.assertRaises(ValueError):
            compile_filter({"name": {"$not": {"x": 1}}})
        with self.assertRaises(ValueError):
            compile_filter({"name": {"$not": {"$regex": "^ad", "x": 1}}})
        with self.assertRaises(ValueError):
            compile_filter({"items": {"$elemMatch": 1}})

    def test_compile_filter_rejects_invalid_field_names(self):
        for filter_spec in (
            {"": 1},
            {"a.": 1},
            {".a": 1},
            {"a..b": 1},
            {"field\x00name": 1},
        ):
            with self.subTest(filter_spec=filter_spec):
                with self.assertRaises(OperationFailure):
                    compile_filter(filter_spec)

    def test_query_plan_private_helpers_cover_regex_type_and_bitwise_branches(self):
        compiled = re.compile("^ad", re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE)
        self.assertEqual(_regex_options_from_pattern(compiled), "imsx")

        self.assertEqual(_normalize_type_specifier(127), ("maxKey",))
        self.assertEqual(_normalize_type_specifier(" number "), ("double", "int", "long", "decimal"))
        with self.assertRaises(ValueError):
            _normalize_type_specifier(999)
        with self.assertRaises(ValueError):
            _normalize_type_specifier(object())
        with self.assertRaises(ValueError):
            _normalize_type_specifier("future")

        self.assertEqual(_coerce_bitwise_mask(uuid.UUID("12345678-1234-5678-1234-567812345678")), int.from_bytes(uuid.UUID("12345678-1234-5678-1234-567812345678").bytes, byteorder="little", signed=False))
        with self.assertRaises(ValueError):
            _coerce_bitwise_mask(True)
        with self.assertRaises(ValueError):
            _coerce_bitwise_mask([64])

    def test_compile_filter_private_branches_cover_regex_equals_and_elem_match_fallback(self):
        self.assertEqual(
            compile_filter({"name": {"$eq": Regex("^ad", "i")}}),
            RegexCondition("name", "^ad", "i"),
        )
        self.assertEqual(
            compile_filter({"name": {"$eq": re.compile("^ad", re.IGNORECASE | re.MULTILINE)}}),
            RegexCondition("name", "^ad", "im"),
        )

        plan = compile_filter({"items": {"$elemMatch": {"$future": 1}}}, planning_mode=PlanningMode.STRICT)
        self.assertIsInstance(plan, ElemMatchCondition)
        self.assertIsNone(plan.compiled_plan)
        self.assertFalse(plan.wrap_value)

    def test_compile_filter_rejects_options_without_regex(self):
        with self.assertRaises(Exception):
            compile_filter({"name": {"$options": "i"}})

    def test_compile_filter_supports_expr_with_optional_bound_variables(self):
        plan = compile_filter({"$expr": {"$gt": ["$a", 1]}})
        bound = compile_filter(
            {"$expr": {"$eq": ["$tenant", "$$tenant"]}},
            variables={"tenant": "a"},
        )

        self.assertEqual(type(plan).__name__, "ExprCondition")
        self.assertEqual(type(bound).__name__, "ExprCondition")
        self.assertEqual(bound.variables, {"tenant": "a"})

    def test_compile_filter_rejects_unknown_top_level_operators(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"$foo": 1})
    def test_compile_filter_accepts_top_level_json_schema(self):
        compiled = compile_filter({"$jsonSchema": {"required": ["name"]}})

        self.assertEqual(compiled.schema, {"required": ["name"]})
        self.assertIsNotNone(compiled.compiled_schema)

    def test_compile_filter_accepts_richer_top_level_json_schema(self):
        compiled = compile_filter(
            {
                "$jsonSchema": {
                    "required": ["profile", "score"],
                    "additionalProperties": False,
                    "properties": {
                        "_id": {"bsonType": "string"},
                        "score": {"bsonType": "int", "minimum": 5, "maximum": 10},
                        "profile": {
                            "bsonType": "object",
                            "required": ["name", "tags"],
                            "additionalProperties": False,
                            "properties": {
                                "name": {"bsonType": "string", "minLength": 3, "maxLength": 5},
                                "tags": {
                                    "bsonType": "array",
                                    "items": {"bsonType": "string", "minLength": 2},
                                },
                            },
                        },
                    },
                }
            }
        )

        self.assertEqual(compiled.schema["properties"]["score"]["minimum"], 5)
        self.assertEqual(
            compiled.schema["properties"]["profile"]["properties"]["tags"]["items"]["minLength"],
            2,
        )
        self.assertIsNotNone(compiled.compiled_schema)

    def test_compile_filter_rejects_invalid_json_schema_payloads(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"$jsonSchema": 1})
        with self.assertRaises(OperationFailure):
            compile_filter({"$jsonSchema": {"required": "name"}})

    def test_compile_filter_supports_local_geo_subset(self):
        box_plan = compile_filter(
            {
                "location": {
                    "$geoWithin": {
                        "$box": [[-1, -1], [1, 1]],
                    }
                }
            }
        )
        self.assertEqual(box_plan.field, "location")
        self.assertEqual(box_plan.geometry_kind, "box")
        self.assertTrue(box_plan.geometry.equals(shapely_box(-1.0, -1.0, 1.0, 1.0)))

        intersects_plan = compile_filter(
            {
                "location": {
                    "$geoIntersects": {
                        "$geometry": {"type": "Point", "coordinates": [1, 2]},
                    }
                }
            }
        )
        self.assertEqual(intersects_plan.field, "location")
        self.assertEqual(intersects_plan.geometry_kind, "point")
        self.assertTrue(intersects_plan.geometry.equals(Point(1.0, 2.0)))
        self.assertEqual(
            compile_filter(
                {
                    "location": {
                        "$near": {
                            "$geometry": {"type": "Point", "coordinates": [0, 0]},
                            "$maxDistance": 3,
                        }
                    }
                }
            ),
            NearCondition("location", (0.0, 0.0), max_distance=3.0),
        )
        self.assertEqual(
            compile_filter({"location": {"$nearSphere": [0, 0]}}),
            NearCondition("location", (0.0, 0.0), spherical=True),
        )

    def test_compile_filter_rejects_unsupported_geo_shapes(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"location": {"$geoWithin": {"$geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}}}})
        with self.assertRaises(OperationFailure):
            compile_filter({"location": {"$geoIntersects": {"$geometry": {"type": "Circle", "coordinates": [0, 0]}}}})
        with self.assertRaises(OperationFailure):
            compile_filter({"location": {"$near": {"$geometry": {"type": "Polygon", "coordinates": []}}}})
        with self.assertRaisesRegex(OperationFailure, "supports only \\$geometry Polygon or legacy \\$box"):
            compile_filter({"location": {"$geoWithin": {"$center": [[0, 0], 2]}}})

    def test_compile_filter_supports_broader_local_geo_shapes(self):
        within_plan = compile_filter(
            {
                "location": {
                    "$geoWithin": {
                        "$geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]],
                                [[[9, 9], [11, 9], [11, 11], [9, 11], [9, 9]]],
                            ],
                        }
                    }
                }
            }
        )
        self.assertEqual(within_plan.geometry_kind, "multipolygon")

        intersects_plan = compile_filter(
            {
                "location": {
                    "$geoIntersects": {
                        "$geometry": {
                            "type": "MultiPoint",
                            "coordinates": [[1, 2], [3, 4]],
                        }
                    }
                }
            }
        )
        self.assertEqual(intersects_plan.geometry_kind, "multipoint")
        self.assertTrue(intersects_plan.geometry.equals(MultiPoint([(1, 2), (3, 4)])))

    def test_compile_filter_accepts_bits_all_set_with_dedicated_test(self):
        self.assertEqual(
            compile_filter({"value": {"$bitsAllSet": [0, 2]}}),
            BitwiseCondition("value", "$bitsAllSet", [0, 2], mask=5),
        )

    def test_compile_filter_accepts_bits_any_set_with_dedicated_test(self):
        self.assertEqual(
            compile_filter({"value": {"$bitsAnySet": 1}}),
            BitwiseCondition("value", "$bitsAnySet", 1, mask=1),
        )

    def test_compile_filter_rejects_invalid_nor_payload(self):
        with self.assertRaises(ValueError):
            compile_filter({"$nor": {"name": "Ada"}})
        with self.assertRaises(ValueError):
            compile_filter({"$and": []})
        with self.assertRaises(ValueError):
            compile_filter({"$or": []})
        with self.assertRaises(ValueError):
            compile_filter({"$nor": []})
        with self.assertRaises(ValueError):
            compile_filter({"$and": [[]]})  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            compile_filter({"$or": [[]]})  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            compile_filter({"$nor": [[]]})  # type: ignore[list-item]

    def test_compile_filter_rejects_non_document_top_level_payload(self):
        with self.assertRaises(ValueError):
            compile_filter([])  # type: ignore[arg-type]

    def test_compile_filter_accepts_none_as_match_all(self):
        self.assertIsInstance(compile_filter(None), MatchAll)  # type: ignore[arg-type]

    def test_ensure_query_plan_prefers_explicit_plan_and_defaults_to_match_all(self):
        explicit = EqualsCondition("kind", "view")

        self.assertIs(ensure_query_plan({"kind": "click"}, explicit), explicit)
        self.assertIsInstance(ensure_query_plan(), MatchAll)
