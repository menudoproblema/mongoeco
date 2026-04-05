from __future__ import annotations

from dataclasses import dataclass
import unittest

from mongoeco._types.documents import _serialize_document_value


class DocumentSerializationInternalTests(unittest.TestCase):
    def test_serialize_document_value_prefers_to_document_and_recurses_dataclasses(self):
        class WithToDocument:
            def to_document(self):
                return {"kind": "custom"}

        @dataclass
        class Payload:
            name: str
            tags: tuple[str, ...]

        self.assertEqual(
            _serialize_document_value(WithToDocument()),
            {"kind": "custom"},
        )
        self.assertEqual(
            _serialize_document_value(Payload(name="Ada", tags=("python", "math"))),
            {"name": "Ada", "tags": ["python", "math"]},
        )
