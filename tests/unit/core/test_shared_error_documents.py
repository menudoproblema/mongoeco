"""Phase markers, raw diagnostics and BSON index keys survive shared adapters."""

import asyncio

import pytest

from bson import Code

from mongoeco.api._async._write_errors import find_modify_errors, write_command_errors
from mongoeco.core.bson_ordering import bson_engine_key, bson_equality_key
from mongoeco.core.bson_scalars import bson_divide, bson_mod
from mongoeco.core.error_documents import (
    command_errors,
    error_message,
    sync_command_errors,
)
from mongoeco.core.server_errors import ErrorScope, server_error
from mongoeco.engines.results import _RejectedInsertOutcome
from mongoeco.error_catalog import EMPTY_GROUP_FIELD_ERROR
from mongoeco.errors import (
    DuplicateKeyError,
    ExecutionTimeout,
    OperationFailure,
    WriteError,
)


@pytest.mark.parametrize("code", [9, 17276, 16610, 999])
def test_write_family_is_selected_by_phase_for_additional_server_codes(code):
    original = OperationFailure("original", code=code, details={"codeName": "Example"})

    def compile_item():
        raise original

    @write_command_errors
    async def fail():
        compile_item()

    with pytest.raises(WriteError) as caught:
        asyncio.run(fail())
    assert caught.value.code == code
    assert caught.value.details == {"index": 0, "code": code, "errmsg": "original"}
    assert "full error:" not in error_message(caught.value)


def test_global_command_failure_keeps_its_family():
    original = server_error("global", code=40415, scope=ErrorScope.COMMAND)

    @write_command_errors
    async def fail():
        raise original

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(fail())
    assert caught.value is original
    assert not isinstance(caught.value, WriteError)


@pytest.mark.parametrize("adapter", [command_errors, find_modify_errors])
def test_additional_command_codes_retain_complete_details_and_labels(adapter):
    source = {"codeName": "Location17276", "errorLabels": ["Example"]}

    @adapter
    async def fail():
        message = "original"
        raise OperationFailure(message, code=17276, details=source)

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(fail())
    error = caught.value
    assert error.details == {**source, "ok": 0.0, "code": 17276, "errmsg": "original"}
    assert error.has_error_label("Example")
    assert source == {"codeName": "Location17276", "errorLabels": ["Example"]}


@pytest.mark.parametrize(["operation", "code"], [(bson_divide, 2), (bson_mod, 16610)])
def test_zero_divisors_retain_real_server_codes(operation, code):
    with pytest.raises(OperationFailure) as caught:
        operation(1, 0)
    assert caught.value.code == code


@pytest.mark.parametrize("key", [bson_engine_key, bson_equality_key])
def test_javascript_index_key_is_hashable_and_preserves_scope_and_bson_type(key):
    variants = ["x", Code("x"), Code("x", {}), Code("x", {"a": 1}), Code("x", {"a": 2})]
    assert len({key(value) for value in variants}) == len(variants)
    assert key(Code("x", {"a": 1})) == key(Code("x", {"a": 1}))


def test_per_item_server_document_does_not_acquire_command_code_name():
    details = {"index": 0, "code": 11000, "errmsg": "native diagnostic"}
    error = DuplicateKeyError._from_server_document(
        details["errmsg"], code=details["code"], details=details
    )
    assert error.details == details
    assert error.details is not details
    assert str(error).count("full error:") == 1
    assert error_message(error) == "native diagnostic"


def test_rejected_outcome_copies_private_diagnostic_context():
    context = {"keyValue": {"key": {"nested": 1}}}
    outcome = _RejectedInsertOutcome(applied=False, _duplicate_key_context=context)
    context["keyValue"]["key"]["nested"] = 2
    assert not outcome
    assert outcome._duplicate_key_context["keyValue"]["key"]["nested"] == 1
    with pytest.raises(ValueError, match="cannot be applied"):
        _RejectedInsertOutcome(applied=True)


TIMEOUT_CODE = 50


def test_find_modify_preserves_timeout_family_while_completing_its_document():
    @find_modify_errors
    async def fail():
        message = "operation exceeded time limit"
        raise ExecutionTimeout(message)

    with pytest.raises(ExecutionTimeout) as caught:
        asyncio.run(fail())
    assert caught.value.code == TIMEOUT_CODE
    assert caught.value.details == {
        "ok": 0.0,
        "code": 50,
        "codeName": "MaxTimeMSExpired",
        "errmsg": "operation exceeded time limit",
    }


@pytest.mark.parametrize("synchronous", [False, True])
def test_command_completion_preserves_descriptor_name_without_details(synchronous):
    original = OperationFailure("original", descriptor=EMPTY_GROUP_FIELD_ERROR)
    if synchronous:

        @sync_command_errors
        def fail():
            raise original
    else:

        @command_errors
        async def fail_async():
            raise original

        def fail():
            return asyncio.run(fail_async())

    with pytest.raises(OperationFailure) as caught:
        fail()
    assert caught.value.code == original.code
    assert caught.value.code_name == original.code_name
    assert caught.value.details == {
        "ok": 0.0,
        "code": original.code,
        "codeName": original.code_name,
        "errmsg": "original",
    }
