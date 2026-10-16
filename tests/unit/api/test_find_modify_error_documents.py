"""Command adaptation preserves the raw diagnostic and completes its envelope."""

import asyncio

import pytest

from mongoeco.api._async._write_errors import find_modify_errors
from mongoeco.errors import DuplicateKeyError, OperationFailure


@pytest.mark.parametrize("error_type", [OperationFailure, DuplicateKeyError])
def test_find_modify_completes_details_without_mutating_or_reformatting_them(
    error_type,
):
    source = {"errmsg": "original server diagnostic", "errorLabels": ["ExampleLabel"]}
    original = error_type("outer diagnostic", code=14, details=source)

    @find_modify_errors
    async def fail():
        raise original

    with pytest.raises(error_type) as caught:
        asyncio.run(fail())
    error = caught.value
    assert error.details == {
        **source,
        "ok": 0.0,
        "code": 14,
        "codeName": "TypeMismatch",
    }
    assert source == {
        "errmsg": "original server diagnostic",
        "errorLabels": ["ExampleLabel"],
    }
    assert str(error).startswith("original server diagnostic, full error:")
    assert error.has_error_label("ExampleLabel")


def test_find_modify_completes_an_unknown_server_code():
    original = OperationFailure("unknown", code=999)

    @find_modify_errors
    async def fail():
        raise original

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(fail())
    assert caught.value.code == original.code
    assert caught.value.details == {
        "ok": 0.0,
        "code": 999,
        "codeName": "Location999",
        "errmsg": "unknown",
    }


def test_find_modify_uses_the_raw_diagnostic_when_existing_details_lack_errmsg():
    original = OperationFailure(
        "invalid field", code=14, details={"codeName": "TypeMismatch"}
    )

    @find_modify_errors
    async def fail():
        raise original

    with pytest.raises(OperationFailure) as caught:
        asyncio.run(fail())
    assert caught.value.details["errmsg"] == "invalid field"
    assert str(caught.value).count("full error:") == 1
