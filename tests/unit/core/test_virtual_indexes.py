import unittest

from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    ExistsCondition,
    GreaterThanCondition,
    InCondition,
    LessThanOrEqualCondition,
    LessThanCondition,
    MatchAll,
    RegexCondition,
    TypeCondition,
    OrCondition,
    compile_filter,
)
from mongoeco.engines.virtual_indexes import (
    _compare_bounds,
    _compile_regex_condition,
    _index_key,
    _implies_same_field_condition,
    _plan_implies,
    _plan_implies_exists_true,
    _same_field_ordering_implies,
    _same_field_regex_implies,
    _same_field_type_implies,
    _value_satisfies_bounds,
    collect_usable_virtual_index_names,
    describe_virtual_index_usage,
    document_in_virtual_index,
    is_virtual_index,
    normalize_partial_filter_expression,
    query_can_use_index,
)
from mongoeco.types import EngineIndexRecord


class VirtualIndexTests(unittest.TestCase):
    def test_normalize_partial_filter_expression_validates_input(self):
        original = {"active": True}
        self.assertIsNone(normalize_partial_filter_expression(None))
        self.assertEqual(
            normalize_partial_filter_expression(original),
            {"active": True},
        )
        normalized = normalize_partial_filter_expression(original)
        self.assertIsNot(normalized, original)
        with self.assertRaises(TypeError):
            normalize_partial_filter_expression(["bad"])  # type: ignore[arg-type]

    def test_sparse_and_partial_document_membership_is_shared(self):
        sparse_index = EngineIndexRecord(
            name="email_1",
            fields=["email"],
            key=[("email", 1)],
            unique=False,
            sparse=True,
        )
        partial_index = EngineIndexRecord(
            name="email_1",
            fields=["email"],
            key=[("email", 1)],
            unique=False,
            partial_filter_expression={"active": True},
        )

        self.assertFalse(document_in_virtual_index({"_id": "1"}, sparse_index))
        self.assertTrue(document_in_virtual_index({"_id": "2", "email": None}, sparse_index))
        self.assertFalse(document_in_virtual_index({"_id": "1", "active": False}, partial_index))
        self.assertTrue(document_in_virtual_index({"_id": "2", "active": True}, partial_index))
        self.assertTrue(is_virtual_index(sparse_index))
        self.assertTrue(is_virtual_index(partial_index))
        self.assertFalse(
            is_virtual_index(
                EngineIndexRecord(
                    name="plain",
                    fields=["email"],
                    key=[("email", 1)],
                    unique=False,
                )
            )
        )

    def test_query_can_use_sparse_and_partial_index_conservatively(self):
        sparse_index = EngineIndexRecord(
            name="email_1",
            fields=["email"],
            key=[("email", 1)],
            unique=False,
            sparse=True,
        )
        partial_index = EngineIndexRecord(
            name="email_1",
            fields=["email"],
            key=[("email", 1)],
            unique=False,
            partial_filter_expression={"active": True},
        )

        self.assertFalse(query_can_use_index(sparse_index, MatchAll()))
        self.assertTrue(query_can_use_index(sparse_index, compile_filter({"email": "a@example.com"})))
        self.assertFalse(query_can_use_index(sparse_index, compile_filter({"email": None})))
        self.assertTrue(query_can_use_index(sparse_index, compile_filter({"email": {"$in": ["a@example.com"]}})))
        self.assertFalse(query_can_use_index(partial_index, compile_filter({"email": "a@example.com"})))
        self.assertTrue(
            query_can_use_index(
                partial_index,
                AndCondition(
                    (
                        EqualsCondition("active", True),
                        EqualsCondition("email", "a@example.com"),
                    )
                ),
            )
        )

    def test_partial_index_implication_handles_subsets_and_ranges(self):
        partial_index = EngineIndexRecord(
            name="score_active_idx",
            fields=["score"],
            key=[("score", 1)],
            unique=False,
            partial_filter_expression={"score": {"$gte": 10}},
        )

        self.assertTrue(query_can_use_index(partial_index, compile_filter({"score": {"$gt": 20}})))
        self.assertTrue(query_can_use_index(partial_index, InCondition("score", (10, 12, 15))))
        self.assertFalse(query_can_use_index(partial_index, compile_filter({"score": {"$lt": 20}})))

    def test_partial_index_implication_uses_bson_type_order_for_mixed_type_bounds(self):
        partial_index = EngineIndexRecord(
            name="score_gte_idx",
            fields=["score"],
            key=[("score", 1)],
            unique=False,
            partial_filter_expression={"score": {"$gte": 10}},
        )

        self.assertTrue(query_can_use_index(partial_index, compile_filter({"score": {"$gt": "Ada"}})))
        self.assertFalse(query_can_use_index(partial_index, compile_filter({"score": {"$lt": "Ada"}})))

    def test_partial_index_implication_handles_type_and_regex_requirements(self):
        typed_index = EngineIndexRecord(
            name="typed_idx",
            fields=["value"],
            key=[("value", 1)],
            unique=False,
            partial_filter_expression={"value": {"$type": "string"}},
        )
        regex_index = EngineIndexRecord(
            name="regex_idx",
            fields=["name"],
            key=[("name", 1)],
            unique=False,
            partial_filter_expression={"name": {"$regex": "^ad", "$options": "i"}},
        )

        self.assertTrue(query_can_use_index(typed_index, compile_filter({"value": "ada"})))
        self.assertFalse(query_can_use_index(typed_index, compile_filter({"value": 3})))
        self.assertTrue(query_can_use_index(regex_index, compile_filter({"name": "Ada"})))
        self.assertTrue(query_can_use_index(regex_index, compile_filter({"name": {"$in": ["Ada", "admin"]}})))
        self.assertFalse(query_can_use_index(regex_index, compile_filter({"name": {"$in": ["Ada", 3]}})))

    def test_virtual_index_usage_description_surfaces_usable_and_hinted_indexes(self):
        indexes = [
            EngineIndexRecord(
                name="email_sparse",
                fields=["email"],
                key=[("email", 1)],
                unique=False,
                sparse=True,
            ),
            EngineIndexRecord(
                name="active_partial",
                fields=["email"],
                key=[("email", 1)],
                unique=False,
                partial_filter_expression={"active": True},
            ),
        ]

        plan = compile_filter({"active": True, "email": {"$in": ["a@example.com"]}})

        self.assertEqual(
            collect_usable_virtual_index_names(indexes, plan),
            ["email_sparse", "active_partial"],
        )
        self.assertEqual(
            describe_virtual_index_usage(indexes, plan, hinted_index_name="active_partial"),
            {
                "usableVirtualIndexes": ["email_sparse", "active_partial"],
                "hintedIndexIsVirtual": True,
            },
        )

    def test_virtual_index_usage_description_handles_plain_hints_and_dict_indexes(self):
        indexes = [
            {
                "name": "plain_name",
                "fields": ["name"],
                "sparse": False,
            },
            {
                "name": "email_sparse",
                "fields": ["email"],
                "sparse": True,
            },
        ]

        details = describe_virtual_index_usage(
            indexes,
            compile_filter({"email": "a@example.com"}),
            hinted_index_name="plain_name",
        )

        self.assertEqual(
            details,
            {
                "usableVirtualIndexes": ["email_sparse"],
                "hintedIndexIsVirtual": False,
            },
        )

    def test_virtual_index_helpers_cover_hidden_invalid_and_empty_usage_paths(self):
        hidden_index = EngineIndexRecord(
            name="hidden_email",
            fields=["email"],
            key=[("email", 1)],
            unique=False,
            hidden=True,
            sparse=True,
        )

        self.assertFalse(query_can_use_index(hidden_index, compile_filter({"email": "a@example.com"})))
        self.assertFalse(
            query_can_use_index(
                {"name": "bad_key", "fields": ["email"], "key": [("email", "hashed")]},
                compile_filter({"email": "a@example.com"}),
            )
        )
        self.assertIsNone(describe_virtual_index_usage([], MatchAll()))
        self.assertEqual(_index_key({"fields": ["email"]}), [("email", 1)])
        self.assertIsNone(_index_key({}))

    def test_partial_index_implication_handles_or_and_exists_requirements(self):
        partial_index = EngineIndexRecord(
            name="active_or_premium",
            fields=["tier"],
            key=[("tier", 1)],
            unique=False,
            partial_filter_expression={"$or": [{"active": True}, {"tier": "premium"}]},
        )

        self.assertTrue(query_can_use_index(partial_index, compile_filter({"active": True})))
        self.assertTrue(query_can_use_index(partial_index, compile_filter({"tier": "premium"})))
        self.assertFalse(query_can_use_index(partial_index, compile_filter({"tier": "basic"})))

        sparse_dict_index = {
            "name": "profile_sparse",
            "fields": ["profile.name"],
            "sparse": True,
        }
        self.assertTrue(query_can_use_index(sparse_dict_index, compile_filter({"profile.name": {"$exists": True}})))
        self.assertFalse(query_can_use_index(sparse_dict_index, compile_filter({"profile.name": None})))

    def test_partial_index_implication_respects_inclusive_and_type_alias_bounds(self):
        partial_index = EngineIndexRecord(
            name="score_lte_idx",
            fields=["score"],
            key=[("score", 1)],
            unique=False,
            partial_filter_expression={"score": {"$lte": 10}},
        )
        typed_index = EngineIndexRecord(
            name="typed_idx",
            fields=["value"],
            key=[("value", 1)],
            unique=False,
            partial_filter_expression={"value": {"$type": ["string", "null"]}},
        )

        self.assertTrue(query_can_use_index(partial_index, compile_filter({"score": {"$lt": 5}})))
        self.assertTrue(query_can_use_index(partial_index, compile_filter({"score": {"$lte": 10}})))
        self.assertFalse(query_can_use_index(partial_index, compile_filter({"score": {"$gt": 10}})))
        self.assertTrue(query_can_use_index(typed_index, compile_filter({"value": None})))
        self.assertTrue(query_can_use_index(typed_index, compile_filter({"value": "ada"})))
        self.assertFalse(query_can_use_index(typed_index, compile_filter({"value": 2})))

    def test_plan_implication_helpers_cover_match_all_and_boolean_composition(self):
        self.assertTrue(_plan_implies(EqualsCondition("email", "a@example.com"), MatchAll()))
        self.assertTrue(
            _plan_implies(
                AndCondition((EqualsCondition("tier", "premium"), EqualsCondition("active", True))),
                EqualsCondition("active", True),
            )
        )
        self.assertTrue(
            _plan_implies(
                OrCondition(
                    (
                        ExistsCondition("email", True),
                        EqualsCondition("email", "a@example.com"),
                    )
                ),
                ExistsCondition("email", True),
            )
        )
        self.assertFalse(
            _plan_implies(
                EqualsCondition("email", "a@example.com"),
                ExistsCondition("email", False),
            )
        )
        self.assertFalse(
            _plan_implies(
                EqualsCondition("email", "a@example.com"),
                AndCondition((EqualsCondition("email", "a@example.com"), EqualsCondition("active", True))),
            )
        )
        self.assertFalse(
            _plan_implies(
                EqualsCondition("other", 1),
                ExistsCondition("email", True),
            )
        )

    def test_exists_and_type_regex_helpers_cover_remaining_implication_paths(self):
        self.assertTrue(_plan_implies_exists_true(GreaterThanCondition("score", 10), "score"))
        self.assertTrue(
            _plan_implies_exists_true(
                AndCondition((EqualsCondition("score", 1), EqualsCondition("other", 2))),
                "score",
            )
        )
        self.assertFalse(
            _plan_implies_exists_true(
                OrCondition((EqualsCondition("score", 1), EqualsCondition("score", None))),
                "score",
            )
        )
        self.assertTrue(
            _same_field_type_implies(
                TypeCondition("value", ("string",)),
                "value",
                ("string", "null"),
            )
        )
        self.assertFalse(
            _same_field_type_implies(
                TypeCondition("value", ("int",)),
                "value",
                "string",
            )
        )
        self.assertTrue(
            _same_field_type_implies(
                InCondition("value", ("ada", "grace")),
                "value",
                "string",
            )
        )
        self.assertFalse(_same_field_type_implies(MatchAll(), "value", "string"))
        regex = RegexCondition("name", "^ad", "i")
        self.assertTrue(_same_field_regex_implies(RegexCondition("name", "^ad", "i"), regex))
        self.assertFalse(_same_field_regex_implies(EqualsCondition("name", 3), regex))
        self.assertFalse(_same_field_regex_implies(InCondition("name", (1,)), regex))
        self.assertFalse(_same_field_regex_implies(InCondition("name", ()), regex))
        self.assertFalse(_same_field_regex_implies(MatchAll(), regex))
        self.assertIsNotNone(_compile_regex_condition(RegexCondition("body", "^ada$", "aimsx")).search("Ada\n"))
        self.assertIsNotNone(_compile_regex_condition(RegexCondition("body", "^ada$", "iumsx")).search("Ada\n"))

    def test_ordering_and_bound_helpers_cover_edge_comparisons(self):
        self.assertTrue(
            _same_field_ordering_implies(
                InCondition("score", (5, 10)),
                "score",
                minimum=5,
                inclusive=True,
            )
        )
        self.assertFalse(
            _same_field_ordering_implies(
                InCondition("score", (4, 10)),
                "score",
                minimum=5,
                inclusive=True,
            )
        )
        self.assertTrue(
            _same_field_ordering_implies(
                GreaterThanCondition("score", 11),
                "score",
                minimum=10,
                inclusive=True,
            )
        )
        self.assertTrue(
            _same_field_ordering_implies(
                LessThanOrEqualCondition("score", 10),
                "score",
                maximum=10,
                inclusive=True,
            )
        )
        self.assertFalse(
            _same_field_ordering_implies(
                EqualsCondition("other", 10),
                "score",
                minimum=5,
                inclusive=True,
            )
        )
        self.assertFalse(
            _same_field_ordering_implies(
                InCondition("score", ()),
                "score",
                minimum=5,
                inclusive=True,
            )
        )
        self.assertFalse(
            _same_field_ordering_implies(
                GreaterThanCondition("other", 10),
                "score",
                minimum=5,
                inclusive=True,
            )
        )
        self.assertFalse(
            _implies_same_field_condition(
                MatchAll(),
                LessThanCondition("score", 10),
            )
        )
        self.assertTrue(
            _implies_same_field_condition(
                InCondition("score", (5,)),
                EqualsCondition("score", 5),
            )
        )
        self.assertFalse(
            _implies_same_field_condition(
                MatchAll(),
                EqualsCondition("score", 5),
            )
        )
        self.assertFalse(
            _implies_same_field_condition(
                InCondition("other", (5,)),
                InCondition("score", (5, 6)),
            )
        )
        self.assertTrue(
            _implies_same_field_condition(
                EqualsCondition("score", 7),
                GreaterThanCondition("score", 5),
            )
        )
        self.assertFalse(
            _implies_same_field_condition(
                MatchAll(),
                MatchAll(),
            )
        )
        self.assertTrue(
            _implies_same_field_condition(
                EqualsCondition("score", 5),
                InCondition("score", (5, 6)),
            )
        )
        self.assertFalse(
            _implies_same_field_condition(
                MatchAll(),
                InCondition("score", (5, 6)),
            )
        )
        self.assertFalse(_compare_bounds(10, 10, False, True))
        self.assertTrue(_compare_bounds(10, 10, False, False, reverse=True))
        self.assertFalse(_compare_bounds(11, 10, False, False, reverse=True))
        self.assertFalse(_compare_bounds(9, 10, False, True))
        self.assertFalse(_value_satisfies_bounds(10, minimum=10, inclusive=False))
        self.assertTrue(_value_satisfies_bounds(10, maximum=10, inclusive=True))
        self.assertFalse(_value_satisfies_bounds(11, maximum=10, inclusive=True))
