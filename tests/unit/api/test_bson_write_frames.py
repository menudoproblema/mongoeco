"""Physical frame boundaries checked against the driver's independent builder."""

import io

from types import SimpleNamespace

import pytest

from bson import BSON, Binary
from bson.codec_options import CodecOptions as DriverCodecOptions
from bson.errors import InvalidDocument
from pymongo.errors import DocumentTooLarge
from pymongo.message import _batched_op_msg_impl

from mongoeco.api import bson_input
from mongoeco.api._async import _collection_bulk
from mongoeco.api.bson_input import iter_bson_write_frames
from mongoeco.types import CodecOptions


@pytest.mark.parametrize("maximum", [140, 180, 240])
@pytest.mark.parametrize("portable", [False, True])
def test_sequence_boundaries_match_the_sdk_message_builder(
    maximum, portable, monkeypatch
):
    command = {"insert": "rows", "ordered": True, "$db": "test"}
    docs = [{"_id": i, "v": "x" * 30} for i in range(7)]
    context = SimpleNamespace(
        max_bson_size=16 * 1024 * 1024,
        max_write_batch_size=100_000,
        max_message_size=maximum,
    )
    remaining = docs
    expected = []
    while remaining:
        buf = io.BytesIO(b"\0" * 16)
        buf.seek(16)
        sent, length = _batched_op_msg_impl(
            0,
            command,
            remaining,
            ack=True,
            opts=DriverCodecOptions(),
            ctx=context,
            buf=buf,
        )
        assert length <= maximum
        expected.append([item["_id"] for item in sent])
        remaining = remaining[len(sent) :]
    if portable:
        monkeypatch.setattr(bson_input, "BSON", None)
    actual = iter_bson_write_frames(
        command,
        docs,
        codec_options=CodecOptions(),
        sequence_name="documents",
        max_message_size=maximum,
    )
    assert [[item["_id"] for item in rows] for _, rows in actual] == expected


def test_invalid_lookahead_does_not_publish_an_unsent_frame():
    docs = [{"v": "x" * 30}, {"v": object()}]
    frames = iter_bson_write_frames(
        {},
        docs,
        codec_options=CodecOptions(),
        sequence_name="documents",
        max_message_size=120,
    )
    with pytest.raises(InvalidDocument):
        next(frames)


def test_bulk_session_budget_matches_a_bson_uuid_session_document():
    metadata = {"lsid": {"id": Binary(b"\0" * 16, 4)}}
    expected = len(BSON.encode(metadata)) - len(BSON.encode({}))
    assert expected == _collection_bulk._CLASSIC_BULK_SESSION_BYTES


@pytest.mark.parametrize("sequence", ["documents", "updates", "deletes"])
@pytest.mark.parametrize("portable", [False, True])
def test_oversized_operation_fails_before_any_frame_is_published(
    sequence, portable, monkeypatch
):
    if portable:
        monkeypatch.setattr(bson_input, "BSON", None)
    frames = iter_bson_write_frames(
        {},
        [{"v": "x" * 256}],
        codec_options=CodecOptions(),
        sequence_name=sequence,
        max_message_size=128,
    )
    with pytest.raises(DocumentTooLarge):
        next(frames)
