import unittest

from mongoeco.core.query_plan import AndCondition, EqualsCondition, MatchAll, compile_filter
from mongoeco.engines.virtual_indexes import (
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
