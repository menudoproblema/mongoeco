"""Shared envelope validation for both ends of the wire transport."""

import struct


HEADER_LENGTH = 16
FLAGS_LENGTH = 4
MAX_MESSAGE_LENGTH = 48_000_000


class WireProtocolError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: int = 22,
        code_name: str | None = "InvalidBSON",
        fatal: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = None if fatal else code
        self.code_name = None if fatal else code_name
        self.fatal = fatal


def valid_message_length(length: int, maximum: int = MAX_MESSAGE_LENGTH) -> bool:
    return HEADER_LENGTH <= length <= maximum


def op_msg_flags(payload: bytes) -> int:
    if len(payload) < FLAGS_LENGTH:
        message = "OP_MSG payload is too short"
        raise WireProtocolError(message, fatal=True)
    flags = int(struct.unpack("<I", payload[:4])[0])
    if flags & 0xFFFF & ~3:
        message = f"unsupported OP_MSG flags: {flags}"
        raise WireProtocolError(message, fatal=True)
    return flags


def _table_entry(value: int) -> int:
    for _ in range(8):
        value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
    return value


_CRC32C_TABLE = tuple(_table_entry(value) for value in range(256))


def crc32c(data: bytes) -> int:
    value = 0xFFFFFFFF
    for byte in data:
        value = _CRC32C_TABLE[(value ^ byte) & 0xFF] ^ (value >> 8)
    return value ^ 0xFFFFFFFF
