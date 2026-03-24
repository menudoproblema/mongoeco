import unittest

from mongoeco.core.identity import canonical_document_id


class IdentityTests(unittest.TestCase):
    def test_canonical_document_id_supports_dicts_lists_and_unhashable_values(self):
        self.assertEqual(
            canonical_document_id({"tenant": [1, 2]}),
            ("dict", (("tenant", ("list", (1, 2))),)),
        )

        class UnhashableValue:
            __hash__ = None

            def __repr__(self) -> str:
                return "unhashable"

        self.assertEqual(canonical_document_id(UnhashableValue()), ("repr", "unhashable"))
