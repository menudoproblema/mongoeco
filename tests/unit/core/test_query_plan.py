import re
import unittest

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
    GreaterThanOrEqualCondition,
    InCondition,
    JsonSchemaCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NotCondition,
    OrCondition,
    RegexCondition,
    SizeCondition,
    TypeCondition,
    compile_filter,
    ensure_query_plan,
)
from mongoeco.types import PlanningMode


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

    def test_compile_filter_relaxed_mode_defers_validation_failures(self):
        plan = compile_filter(
            {"$where": "this.a > 1"},
            planning_mode=PlanningMode.RELAXED,
        )

        self.assertIsInstance(plan, DeferredQueryNode)
        self.assertEqual(plan.issue.scope, "query")

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

    def test_compile_filter_rejects_invalid_all_size_and_regex_payloads(self):
        with self.assertRaises(ValueError):
            compile_filter({"tags": {"$all": "python"}})
        with self.assertRaises(ValueError):
            compile_filter({"tags": {"$size": -1}})
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
            compile_filter({"items": {"$elemMatch": 1}})

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
        with self.assertRaises(OperationFailure):
            compile_filter({"$where": "this.a > 1"})
    def test_compile_filter_accepts_top_level_json_schema(self):
        self.assertEqual(
            compile_filter({"$jsonSchema": {"required": ["name"]}}),
            JsonSchemaCondition({"required": ["name"]}),
        )

    def test_compile_filter_rejects_invalid_json_schema_payloads(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"$jsonSchema": 1})
        with self.assertRaises(OperationFailure):
            compile_filter({"$jsonSchema": {"required": "name"}})

    def test_compile_filter_rejects_where_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"$where": "this.a > 1"})

    def test_compile_filter_rejects_unsupported_field_level_query_operators_explicitly(self):
        unsupported_filters = [
            {"location": {"$geoIntersects": {"$geometry": {}}}},
            {"location": {"$geoWithin": {"$geometry": {}}}},
            {"location": {"$near": [0, 0]}},
            {"location": {"$nearSphere": [0, 0]}},
        ]

        for filter_spec in unsupported_filters:
            with self.subTest(filter_spec=filter_spec):
                with self.assertRaises(OperationFailure):
                    compile_filter(filter_spec)

    def test_compile_filter_rejects_geo_within_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"location": {"$geoWithin": {"$geometry": {}}}})

    def test_compile_filter_rejects_geo_intersects_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"location": {"$geoIntersects": {"$geometry": {}}}})

    def test_compile_filter_rejects_near_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"location": {"$near": [0, 0]}})

    def test_compile_filter_rejects_near_sphere_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            compile_filter({"location": {"$nearSphere": [0, 0]}})

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
