from __future__ import annotations

from dataclasses import dataclass
import unittest

from mongoeco._types.documents import WriteErrorEntry, _serialize_document_value


class DocumentSerializationInternalTests(unittest.TestCase):
    def test_serialize_document_value_prefers_to_document_and_recurses_dataclasses(
        self,
    ):
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

    def test_write_error_envelope_order_keeps_explicit_field_precedence(self):
        details = {"index": 99, "code": 14, "errmsg": "context", "errInfo": {"x": 1}}
        result = WriteErrorEntry(0, "overflow", code=2, details=details).to_document()
        self.assertEqual(list(result)[:3], ["index", "code", "errmsg"])
        self.assertEqual(
            result, {"index": 0, "code": 2, "errmsg": "overflow", "errInfo": {"x": 1}}
        )
        result["errInfo"]["x"] = 2
        self.assertEqual(details["errInfo"], {"x": 1})
        inherited = WriteErrorEntry(0, "failure", details={"code": 14}).to_document()
        self.assertEqual(inherited, {"index": 0, "code": 14, "errmsg": "failure"})
