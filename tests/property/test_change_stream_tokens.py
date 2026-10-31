"""Local token roundtrips and malformed-input admission do not leak Python errors."""

import base64
import json

from copy import deepcopy
from types import MappingProxyType

import pytest

from hypothesis import given, settings, strategies as st

from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
    _parse_resume_token,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import encode_change_stream_token


PARSE_ERROR = 9


def _encoded(document):
    raw = json.dumps(document).encode()
    return {"_data": base64.urlsafe_b64encode(raw).decode()}


@settings(max_examples=100, deadline=None)
@given(position=st.integers(min_value=0, max_value=2**63 - 1))
def test_legacy_decimal_and_v1_tokens_preserve_their_position(position):
    assert _parse_resume_token({"_data": str(position)}) == position
    encoded = encode_change_stream_token(position)
    assert _parse_resume_token({"_data": encoded}) == position


@pytest.mark.parametrize(
    "raw", ["42", "00042", "eyJ2IjoxLCJ0Ijo0Mn0", "eyJ2IjoxLCJ0Ijo0Mn0="]
)
def test_preexisting_token_bytes_and_mapping_inputs_remain_readable(raw):
    legacy_position = 42
    assert _parse_resume_token(MappingProxyType({"_data": raw})) == legacy_position


@settings(max_examples=150, deadline=None)
@given(raw=st.text(max_size=256))
def test_arbitrary_data_strings_return_a_position_or_a_public_diagnostic(raw):
    code = None
    position = None
    try:
        position = _parse_resume_token({"_data": raw})
    except OperationFailure as error:
        code = error.code
    if code is not None:
        assert code in (PARSE_ERROR, 40649)
    else:
        assert isinstance(position, int)
        assert not isinstance(position, bool)
        assert position >= 0


@pytest.mark.parametrize(
    "document",
    [
        None, [], True, 1, "v1", {}, {"v": 1}, {"t": 1},
        {"v": True, "t": 1}, {"v": 1.0, "t": 1}, {"v": "1", "t": 1},
        {"v": 0, "t": 1}, {"v": 2, "t": 1}, {"v": 1, "t": True},
        {"v": 1, "t": 1.0}, {"v": 1, "t": "1"}, {"v": 1, "t": -1},
        {"v": 1, "t": 1, "i": 1}, {"v": 1, "t": 1, "i": None},
    ],
)
def test_malformed_local_payload_is_rejected_before_watcher_registration(document):
    hub = ChangeStreamHub()
    hub.publish(
        operation_type="insert", db_name="db", coll_name="c", document_key={"_id": 1},
    )
    before = hub.state.to_document()
    token = _encoded(document)
    supplied = deepcopy(token)
    try:
        for option in ("resume_after", "start_after"):
            with pytest.raises(OperationFailure) as error:
                AsyncChangeStreamCursor(
                    hub, scope=ChangeStreamScope(), **{option: token}
                )
            assert error.value.code == PARSE_ERROR
            assert hub.watcher_count == 0
            assert hub.state.to_document() == before
            assert token == supplied
    finally:
        hub.close()


@pytest.mark.parametrize(
    "raw", ["²", "\N{ARABIC-INDIC DIGIT ONE}", "=", "!!!!", "W10=!", "1" * 5000]
)
def test_invalid_numeric_or_base64_data_has_no_python_exception(raw):
    with pytest.raises(OperationFailure) as error:
        _parse_resume_token({"_data": raw})
    assert error.value.code == PARSE_ERROR
