"""The shared decoder validates envelopes independently of their BSON contents."""

import asyncio
import struct

import pytest

from mongoeco.driver.security import TlsPolicy
from mongoeco.driver.transports import (
    StreamConnectionResource,
    WireProtocolCommandTransport,
)
from mongoeco.errors import ConnectionFailure
from mongoeco.wire._framing import (
    WireProtocolError,
    crc32c,
    op_msg_flags,
    valid_message_length,
)
from mongoeco.wire.protocol import decode_op_msg, parse_message_header

from tests.support_wire_envelopes import packet
from tests.unit.test_transports import _FakeReader, _FakeRegistry, _FakeWriter


def test_crc32c_standard_vector_and_empty_input():
    expected_crc = 0xE3069283
    assert crc32c(b"123456789") == expected_crc
    assert crc32c(b"") == 0


@pytest.mark.parametrize(
    ["length", "valid"],
    [(15, False), (16, True), (48_000_000, True), (48_000_001, False)],
)
def test_message_length_bounds(length, valid):
    assert valid_message_length(length) is valid


@pytest.mark.parametrize("flags", [0, 1 << 16, 1 << 17, 1 << 31])
def test_optional_bits_are_ignored_in_requests_and_responses(flags):
    raw = packet({"ok": 1}, 1, flags=flags)
    assert decode_op_msg(parse_message_header(raw[:16]), raw[16:]).body == {"ok": 1}


def test_checksum_covers_header_and_payload_in_both_directions():
    raw = packet({"ok": 1}, 1, flags=1, checksum=True)
    header = parse_message_header(raw[:16])
    assert decode_op_msg(header, raw[16:]).body == {"ok": 1}
    for damaged in (
        raw[:-1] + bytes([raw[-1] ^ 1]),
        raw[:4] + struct.pack("<i", 2) + raw[8:],
    ):
        with pytest.raises(WireProtocolError, match="checksum mismatch") as caught:
            decode_op_msg(parse_message_header(damaged[:16]), damaged[16:])
        assert caught.value.fatal


@pytest.mark.parametrize("payload", [b"", b"\x01\x00\x00\x00\x00"])
def test_truncated_flags_and_checksum_do_not_become_sections(payload):
    with pytest.raises(WireProtocolError):
        op_msg_flags(payload) if not payload else decode_op_msg(
            parse_message_header(struct.pack("<iiii", 16 + len(payload), 1, 0, 2013)),
            payload,
        )


def test_unknown_required_flag_is_fatal_before_bson():
    with pytest.raises(WireProtocolError) as caught:
        op_msg_flags(struct.pack("<I", 4))
    assert caught.value.fatal


@pytest.mark.parametrize("damaged", [False, True])
def test_driver_roundtrip_uses_shared_checksum_validation(damaged):
    async def exercise():
        registry = _FakeRegistry()
        transport = WireProtocolCommandTransport(
            registry,
            tls_policy=TlsPolicy(enabled=False, verify_certificates=True),
            connect_timeout_ms=250,
        )
        raw = packet({"ok": 1}, 99, flags=1, checksum=True)
        raw = raw[:8] + struct.pack("<i", 1) + raw[12:-4]
        raw += struct.pack("<I", crc32c(raw) ^ int(damaged))
        resource = StreamConnectionResource(
            reader=_FakeReader([raw[:16], raw[16:]]), writer=_FakeWriter()
        )
        if damaged:
            with pytest.raises(ConnectionFailure, match="checksum"):
                await transport._roundtrip(resource, {"ping": 1}, lease="lease")
            assert registry.discarded == ["lease"]
        else:
            assert await transport._roundtrip(resource, {"ping": 1}, lease="lease") == {
                "ok": 1
            }
            assert registry.discarded == []

    asyncio.run(exercise())
