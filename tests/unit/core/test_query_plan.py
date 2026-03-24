import unittest

from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    OrCondition,
    compile_filter,
)


class QueryPlanTests(unittest.TestCase):
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

    def test_compile_filter_expands_in_operator_to_typed_leaf(self):
        plan = compile_filter({"role": {"$in": ["admin", "staff"]}})

        self.assertEqual(plan, InCondition("role", ("admin", "staff")))

    def test_compile_filter_returns_and_condition_for_multiple_field_operators(self):
        plan = compile_filter({"age": {"$gte": 18, "$in": [18, 21, 30]}})

        self.assertIsInstance(plan, AndCondition)
