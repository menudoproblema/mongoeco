"""Internal invariants prevent incomplete server errors reaching public adapters."""

import asyncio

from copy import deepcopy

import pytest

from mongoeco.api._async._write_errors import find_modify_errors, write_command_errors
from mongoeco.api._async.cursor import _AsyncCursorIterator
from mongoeco.compat import resolve_mongodb_dialect
from mongoeco.core.aggregation import evaluate_expression
from mongoeco.core.aggregation.runtime import _resolve_variable_expression
from mongoeco.core.error_documents import command_error_document
from mongoeco.core.server_errors import ErrorScope, ServerDiagnostic, server_error
from mongoeco.errors import OperationFailure, WriteError

from tests.support_error_phases import comparable


@pytest.mark.parametrize("code", [None, True, "2"])
def test_internal_diagnostic_requires_integer_code(code):
    with pytest.raises(TypeError):
        ServerDiagnostic(code, "failure")


@pytest.mark.parametrize("message", ["", None, 1])
def test_internal_diagnostic_requires_nonempty_message(message):
    with pytest.raises(ValueError, match=r"nonempty message|message must be nonempty"):
        ServerDiagnostic(2, message)


def test_returned_command_and_item_documents_own_their_metadata():
    context = {
        "errInfo": {"failingDocumentId": "a", "details": {"missingProperties": ["x"]}}
    }
    diagnostic = ServerDiagnostic(121, "Document failed validation", context=context)
    context["errInfo"]["failingDocumentId"] = "changed"
    command = diagnostic.command_document()
    item_index = 3
    item = diagnostic.item_document(item_index)
    assert (
        command["errInfo"]["failingDocumentId"]
        == item["errInfo"]["failingDocumentId"]
        == "a"
    )
    command["errInfo"]["details"]["missingProperties"].append("changed")
    assert diagnostic.item_document()["errInfo"]["details"]["missingProperties"] == [
        "x"
    ]
    assert {"ok", "code", "codeName", "errmsg"} <= command.keys()
    assert set(item) == {"index", "code", "errmsg", "errInfo"}
    assert item["index"] == item_index


@pytest.mark.parametrize("scope", [ErrorScope.ITEM, ErrorScope.COMMAND])
def test_scope_controls_write_family_and_command_envelope(scope):
    @write_command_errors
    async def invoke():
        message = "unknown future coded error"
        raise server_error(message, code=987654, scope=scope)

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(invoke())
    assert isinstance(caught.value, WriteError) == (scope is ErrorScope.ITEM)
    if scope is ErrorScope.ITEM:
        assert caught.value.details == {
            "index": 0,
            "code": 987654,
            "errmsg": "unknown future coded error",
        }
    else:
        assert command_error_document(caught.value)["codeName"] == "Location987654"


@pytest.mark.parametrize("adapter", [write_command_errors, find_modify_errors])
def test_adapters_preserve_structured_validation_context(adapter):
    @adapter
    async def invoke():
        message = "validation"
        raise server_error(
            message, code=121, context={"errInfo": {"failingDocumentId": "a"}}
        )

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(invoke())
    assert caught.value.details["errInfo"] == {"failingDocumentId": "a"}
    assert caught.value.details["errmsg"] == "validation"
    assert caught.value.details["code"] == caught.value.code


def test_observer_keeps_application_fields_named_like_diagnostics():
    original = [
        {
            "method": "find_one",
            "result": {
                "message": "first",
                "module": "domain",
                "errmsg": "one",
                "name": "_id_",
                "key": {},
                "unique": True,
            },
            "documents": [{"message": "original"}],
            "indexes": [],
        }
    ]
    for field in ["message", "module", "errmsg", "unique"]:
        changed = deepcopy(original)
        changed[0]["result"][field] = "different"
        assert comparable(original) != comparable(changed)
    changed = deepcopy(original)
    changed[0]["documents"][0]["message"] = "different"
    assert comparable(original) != comparable(changed)


def test_observer_detects_missing_codes_and_details_but_ignores_driver_module_names():
    original = [
        {
            "method": "update_one",
            "error": {
                "family": "WriteError",
                "code": 2,
                "module": "driver",
                "message": "rendered",
                "details": {"index": 0, "code": 2, "errmsg": "bad value"},
            },
        }
    ]
    changed = deepcopy(original)
    changed[0]["error"]["module"] = "other"
    assert comparable(original) == comparable(changed)
    for field in ["code", "errmsg"]:
        changed = deepcopy(original)
        changed[0]["error"]["details"].pop(field)
        assert comparable(original) != comparable(changed)


def test_internal_context_and_scope_are_immutable():
    diagnostic = ServerDiagnostic(2, "failure", context={"errInfo": {"paths": ["a"]}})
    with pytest.raises(TypeError):
        diagnostic.context["errInfo"]["paths"] = ("changed",)
    with pytest.raises(TypeError):
        ServerDiagnostic(2, "failure", scope="item")


def test_cleanup_failure_preserves_the_primary_exception_and_note():

    class Cursor:
        async def close(self):
            message = "secondary failure"
            raise RuntimeError(message)

    primary = OperationFailure("primary failure", code=2)

    async def exercise():
        try:
            raise primary
        except OperationFailure:
            await _AsyncCursorIterator._close_after_failure(Cursor())
            raise

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(exercise())
    assert caught.value is primary
    assert "secondary failure" in primary.__notes__[0]


@pytest.mark.parametrize("adapter", [write_command_errors, find_modify_errors])
def test_adapters_preserve_primary_exception_notes(adapter):
    original = server_error("invalid field", code=14)
    original.add_note("cleanup retained the primary diagnostic")

    @adapter
    async def fail():
        raise original

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(fail())
    assert caught.value.__notes__ == original.__notes__
    assert caught.value.details["code"] == original.code


def test_cleanup_error_without_primary_failure_is_not_suppressed():
    class Cursor:
        async def close(self):
            message = "cleanup failed"
            raise RuntimeError(message)

    with pytest.raises(RuntimeError, match="cleanup failed"):
        asyncio.run(_AsyncCursorIterator._close_after_failure(Cursor()))


@pytest.mark.parametrize(
    ["target", "code"], [("7.0", 51144), ("8.0", 51144), ("9.0", 10071200)]
)
def test_runtime_cluster_time_retains_semantic_reason(target, code):
    with pytest.raises(OperationFailure) as caught:
        _resolve_variable_expression(
            "$$CLUSTER_TIME", {}, dialect=resolve_mongodb_dialect(target)
        )
    assert caught.value.code == code
    assert caught.value._server_diagnostic.reason == "cluster_time_unavailable"


@pytest.mark.parametrize(
    ["base", "power", "expected"],
    [(1e308, 2, float("inf")), (-1e308, 3, float("-inf"))],
)
def test_pow_overflow_preserves_sign_and_double_type(base, power, expected):
    value = evaluate_expression({}, {"$pow": [base, power]})
    assert isinstance(value, float)
    assert value == expected
