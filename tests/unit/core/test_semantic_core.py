import unittest
from unittest.mock import patch

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.query_plan import compile_filter
from mongoeco.engines import semantic_core
from mongoeco.engines.semantic_core import (
    EngineFindSemantics,
    build_query_plan_explanation,
    compile_find_semantics,
    compile_update_semantics,
    enforce_collection_document_validation,
    finalize_documents,
    iter_filtered_documents,
    stream_finalize_documents,
    validate_collection_document,
)
from mongoeco.errors import DocumentValidationFailure
from mongoeco.types import Document


class _RenderedIssue:
    def __init__(self, message: str):
        self._message = message

    def render(self) -> dict[str, object]:
        return {"description": self._message}


class _ValidationResult:
    def __init__(self, *, valid: bool, first_message: str = "", issues: list[object] | None = None):
        self.valid = valid
        self.first_message = first_message
        self.issues = issues or []


class _ValidatorStub:
    def __init__(self, result, *, validation_action: str = "error"):
        self._result = result
        self.validation_action = validation_action
        self.calls = []

    def validate_document(self, document, **kwargs):
        self.calls.append((document, kwargs))
        return self._result


class SemanticCoreUnitTests(unittest.TestCase):
    def test_validate_collection_document_returns_valid_result_when_no_validator_exists(self):
        with patch.object(semantic_core, "compile_collection_validation_semantics", return_value=None):
            result = validate_collection_document({"_id": "1"}, options=None)

        self.assertTrue(result.valid)

    def test_validate_collection_document_delegates_to_compiled_validator(self):
        validator = _ValidatorStub(_ValidationResult(valid=True))

        with patch.object(semantic_core, "compile_collection_validation_semantics", return_value=validator):
            result = validate_collection_document(
                {"_id": "1"},
                options={"validator": {}},
                original_document={"_id": "1", "done": False},
                is_upsert_insert=True,
            )

        self.assertTrue(result.valid)
        self.assertEqual(
            validator.calls,
            [
                (
                    {"_id": "1"},
                    {
                        "original_document": {"_id": "1", "done": False},
                        "is_upsert_insert": True,
                        "dialect": None,
                    },
                )
            ],
        )

    def test_enforce_collection_document_validation_respects_warn_mode_and_raises_with_details(self):
        warn_validator = _ValidatorStub(_ValidationResult(valid=False, first_message="warned"), validation_action="warn")
        with patch.object(semantic_core, "compile_collection_validation_semantics", return_value=warn_validator):
            enforce_collection_document_validation({"_id": "1"}, options={"validator": {}})

        failing_validator = _ValidatorStub(
            _ValidationResult(
                valid=False,
                first_message="missing field",
                issues=[_RenderedIssue("field is required")],
            )
        )
        with patch.object(semantic_core, "compile_collection_validation_semantics", return_value=failing_validator):
            with self.assertRaises(DocumentValidationFailure) as context:
                enforce_collection_document_validation({"_id": "1"}, options={"validator": {}})

        self.assertEqual(context.exception.details["failingDocumentId"], "1")
        self.assertEqual(
            context.exception.details["schemaRulesNotSatisfied"],
            [{"description": "field is required"}],
        )
        self.assertEqual(context.exception.details["codeName"], "DocumentValidationFailure")

    def test_compile_update_semantics_requires_compiled_plans(self):
        operation = compile_update_operation({"name": "Ada"})

        with self.assertRaisesRegex(ValueError, "compiled update plans"):
            compile_update_semantics(operation)

    def test_iter_filtered_documents_uses_query_engine_when_compiled_query_is_missing_and_deadline_present(self):
        semantics = EngineFindSemantics(
            filter_spec={"name": "Ada"},
            query_plan=compile_filter({"name": "Ada"}),
            projection=None,
            collation=None,
            sort=None,
            skip=0,
            limit=None,
            hint=None,
            comment=None,
            max_time_ms=1,
            dialect=MONGODB_DIALECT_70,
            compiled_query=None,
        )
        documents: list[Document] = [{"_id": "1"}, {"_id": "2"}]

        with (
            patch.object(semantic_core, "enforce_deadline") as enforce_deadline,
            patch.object(semantic_core.QueryEngine, "match_plan", side_effect=[True, False]) as match_plan,
        ):
            result = list(iter_filtered_documents(documents, semantics))

        self.assertEqual(result, [{"_id": "1"}])
        self.assertEqual(match_plan.call_count, 2)
        self.assertEqual(enforce_deadline.call_count, 3)

    def test_finalize_documents_applies_skip_and_limit_without_sort_phase(self):
        semantics = compile_find_semantics({}, skip=1, limit=1)
        documents = [{"_id": "1"}, {"_id": "2"}, {"_id": "3"}]

        result = finalize_documents(
            documents,
            semantics,
            apply_sort_phase=False,
            apply_skip_limit_phase=True,
            emit_public_documents=False,
        )

        self.assertEqual(result, [{"_id": "2"}])

    def test_compile_find_semantics_validates_negative_skip_and_limit_and_respects_explicit_compiled_query(self):
        compiled_query = object()

        with self.assertRaisesRegex(ValueError, "skip must be >= 0"):
            compile_find_semantics({}, skip=-1)
        with self.assertRaisesRegex(ValueError, "limit must be >= 0"):
            compile_find_semantics({}, limit=-1)

        semantics = compile_find_semantics({}, compiled_query=compiled_query)

        self.assertIs(semantics.compiled_query, compiled_query)

    def test_iter_filtered_documents_covers_match_all_and_compiled_query_deadline_paths(self):
        match_all_semantics = compile_find_semantics({}, max_time_ms=1)
        compiled = type("Compiled", (), {"match": lambda self, document: document["_id"] == "2"})()
        compiled_semantics = EngineFindSemantics(
            filter_spec={"name": "Ada"},
            query_plan=compile_filter({"name": "Ada"}),
            projection=None,
            collation=None,
            sort=None,
            skip=0,
            limit=None,
            hint=None,
            comment=None,
            max_time_ms=1,
            dialect=MONGODB_DIALECT_70,
            compiled_query=compiled,
        )
        documents: list[Document] = [{"_id": "1"}, {"_id": "2"}]

        with patch.object(semantic_core, "enforce_deadline") as enforce_deadline:
            match_all_result = list(iter_filtered_documents(documents, match_all_semantics))

        self.assertEqual(match_all_result, documents)
        self.assertEqual(enforce_deadline.call_count, 3)

        with patch.object(semantic_core, "enforce_deadline") as enforce_deadline:
            compiled_result = list(iter_filtered_documents(documents, compiled_semantics))

        self.assertEqual(compiled_result, [{"_id": "2"}])
        self.assertEqual(enforce_deadline.call_count, 3)

    def test_stream_finalize_documents_covers_skip_limit_and_deadline_paths(self):
        documents = [{"_id": "1"}, {"_id": "2"}, {"_id": "3"}]

        without_deadline = list(
            stream_finalize_documents(
                documents,
                compile_find_semantics({}, skip=1, limit=1),
                emit_public_documents=False,
            )
        )
        with patch.object(semantic_core, "enforce_deadline") as enforce_deadline:
            with_deadline = list(
                stream_finalize_documents(
                    documents,
                    compile_find_semantics({}, skip=1, limit=1, max_time_ms=1),
                    emit_public_documents=False,
                )
            )

        self.assertEqual(without_deadline, [{"_id": "2"}])
        self.assertEqual(with_deadline, [{"_id": "2"}])
        self.assertEqual(enforce_deadline.call_count, 2)

    def test_build_query_plan_explanation_uses_semantics_snapshot(self):
        semantics = compile_find_semantics(
            {"kind": "view"},
            sort=[("rank", 1)],
            skip=2,
            limit=3,
            hint="rank_idx",
            comment="trace",
            max_time_ms=10,
        )

        explanation = build_query_plan_explanation(
            engine="memory",
            strategy="scan",
            semantics=semantics,
            hinted_index="rank_idx",
            details={"stage": "COLLSCAN"},
            indexes=[{"name": "rank_idx"}],
            fallback_reason="unsupported pushdown",
        )

        self.assertEqual(explanation.engine, "memory")
        self.assertEqual(explanation.strategy, "scan")
        self.assertEqual(explanation.sort, [("rank", 1)])
        self.assertEqual(explanation.skip, 2)
        self.assertEqual(explanation.limit, 3)
        self.assertEqual(explanation.hint, "rank_idx")
        self.assertEqual(explanation.hinted_index, "rank_idx")
        self.assertEqual(explanation.comment, "trace")
        self.assertEqual(explanation.max_time_ms, 10)
        self.assertEqual(explanation.fallback_reason, "unsupported pushdown")


if __name__ == "__main__":
    unittest.main()
