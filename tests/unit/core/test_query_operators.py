import unittest

from mongoeco.core.query_operators import (
    has_query_top_level_operator,
    is_non_empty_document_clause_list,
    is_query_operator_key,
    query_operator_keys,
    require_non_empty_document_clause_list,
)
from mongoeco.errors import OperationFailure


class QueryOperatorsTests(unittest.TestCase):
    def test_is_query_operator_key_only_accepts_string_operator_keys(self):
        self.assertTrue(is_query_operator_key('$or'))
        self.assertFalse(is_query_operator_key('field'))
        self.assertFalse(is_query_operator_key(1))

    def test_query_operator_keys_only_returns_string_operator_keys(self):
        self.assertEqual(
            query_operator_keys({'$or': [], 'field': 1, 1: '$and'}),
            ('$or',),
        )

    def test_has_query_top_level_operator_only_matches_query_operators(self):
        self.assertTrue(has_query_top_level_operator({'$or': []}))
        self.assertTrue(
            has_query_top_level_operator({'field': 1, '$expr': {}})
        )
        self.assertFalse(has_query_top_level_operator({'$in': [1, 2]}))
        self.assertFalse(has_query_top_level_operator({'field': 1}))

    def test_is_non_empty_document_clause_list_validates_boolean_clauses(self):
        self.assertTrue(is_non_empty_document_clause_list([{'a': 1}]))
        self.assertFalse(is_non_empty_document_clause_list([]))
        self.assertFalse(is_non_empty_document_clause_list(['bad']))
        self.assertFalse(is_non_empty_document_clause_list({'a': 1}))

    def test_require_non_empty_document_clause_list_reports_invalid_shapes(
        self,
    ):
        self.assertEqual(
            require_non_empty_document_clause_list(
                [{'a': 1}], operator='$or', context='$match'
            ),
            [{'a': 1}],
        )
        with self.assertRaisesRegex(
            OperationFailure, r'\$or in \$match requires a non-empty list'
        ):
            require_non_empty_document_clause_list(
                [], operator='$or', context='$match'
            )
        with self.assertRaisesRegex(
            OperationFailure, r'\$or in \$match requires document clauses'
        ):
            require_non_empty_document_clause_list(
                ['bad'], operator='$or', context='$match'
            )
