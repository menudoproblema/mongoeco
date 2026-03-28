import unittest

from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    InCondition,
    MatchAll,
    compile_filter,
)
from mongoeco.engines.virtual_indexes import (
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
        self.assertIsNone(normalize_partial_filter_expression(None))
        self.assertEqual(
            normalize_partial_filter_expression({"active": True}),
            {"active": True},
        )
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
