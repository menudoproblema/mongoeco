from __future__ import annotations

import unittest

from mongoeco.core.upserts import _iter_seedable_filters


class UpsertHelperTests(unittest.TestCase):
    def test_iter_seedable_filters_skips_non_list_and_unknown_operator_shapes(self):
        self.assertEqual(
            list(
                _iter_seedable_filters(
                    {
                        "$and": "bad",
                        "$or": [{"tenant": {"$eq": "ignored"}}],
                        "tenant": {"$eq": "a"},
                        "kind": {"$in": ["note"]},
                    }
                )
            ),
            [("tenant", "a"), ("kind", "note")],
        )
