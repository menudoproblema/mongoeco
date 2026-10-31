"""Decode transport BSON without erasing document order or undefined values."""

import struct

from typing import NoReturn

from mongoeco.types import UNDEFINED


_BSON_OBJECT = 3
_BSON_ARRAY = 4
_BSON_BINARY = 5
_BSON_UNDEFINED = 6
_BSON_CODE_WITH_SCOPE = 15


class _RawBsonDecoder:
    def __init__(self, raw):
        import bson  # noqa: PLC0415 - optional SDK, only at the transport boundary

        from bson.codec_options import DEFAULT_CODEC_OPTIONS  # noqa: PLC0415

        self.raw = raw if isinstance(raw, bytes) else bytes(raw)
        self.view = memoryview(self.raw)
        self.bson = bson
        self.options = DEFAULT_CODEC_OPTIONS

    def _fail(self, message) -> NoReturn:
        raise self.bson.errors.InvalidBSON(message) from None

    def _int32(self, position, limit):
        if position + 4 > limit:
            self._fail("truncated BSON length")
        return struct.unpack_from("<i", self.raw, position)[0]

    def _binary(self, position, limit):
        length = self._int32(position, limit)
        position += 4
        if length < 0 or position >= limit:
            self._fail("invalid binary length")
        subtype = self.raw[position]
        position += 1
        end = position + length
        if end > limit:
            self._fail("binary outside document boundaries")
        if subtype == 2:  # noqa: PLR2004 - deprecated BSON binary framing
            inner_length = self._int32(position, end)
            if inner_length != length - 4:
                self._fail("inconsistent old binary length")
            position += 4
        payload = self.raw[position:end]
        # The server accepts short UUID/MD5 binary fields in contexts where
        # expression constants are rejected later. SDK C decoding is stricter
        # than BSON framing here; preserve the value for the appropriate parser.
        value = payload if subtype == 0 else self.bson.Binary(payload, subtype)
        return value, end

    def _code_with_scope(self, position, limit):
        end = position + self._int32(position, limit)
        if end > limit or end < position + 14:
            self._fail("invalid code-with-scope length")
        string_start = position + 8
        length = self._int32(position + 4, end)
        scope_start = string_start + length
        if length < 1 or scope_start > end or self.raw[scope_start - 1] != 0:
            self._fail("invalid code string length")
        code = self.raw[string_start : scope_start - 1].decode("utf-8")
        scope, scope_end = self.document(scope_start, end)
        if scope_end != end:
            self._fail("scope outside code boundaries")
        return self.bson.Code(code, scope), end

    def document(self, start, limit, *, as_array=False):
        size = self._int32(start, limit)
        end = start + size
        if size < 5 or end > limit or self.raw[end - 1] != 0:  # noqa: PLR2004 - BSON document minimum
            self._fail("invalid BSON document length or terminator")
        position = start + 4
        document = [] if as_array else {}
        while position < end - 1:
            kind = self.raw[position]
            key_end = self.raw.find(b"\0", position + 1, end - 1)
            if key_end == -1:
                self._fail("unterminated BSON field name")
            key = self.raw[position + 1 : key_end].decode("utf-8")
            value_start = key_end + 1
            if kind in {_BSON_OBJECT, _BSON_ARRAY}:
                value, next_position = self.document(
                    value_start, end - 1, as_array=kind == _BSON_ARRAY
                )
            elif kind == _BSON_BINARY:
                value, next_position = self._binary(value_start, end - 1)
            elif kind == _BSON_UNDEFINED:
                value, next_position = UNDEFINED, value_start
            elif kind == _BSON_CODE_WITH_SCOPE:
                value, next_position = self._code_with_scope(value_start, end - 1)
            else:
                # Retain SDK primitive validation/decoding and keep its private
                # compatibility adapter confined to this existing owner.
                _, value, next_position = self.bson._element_to_dict(
                    self.raw, self.view, position, end - 1, self.options
                )
            if not value_start <= next_position <= end - 1:
                self._fail("BSON element outside document boundaries")
            if as_array:
                document.append(value)
            else:
                document[key] = value
            position = next_position
        return document, end


def decode_bson_document(raw: bytes) -> dict:
    decoder = _RawBsonDecoder(raw)
    try:
        document, end = decoder.document(0, len(raw))
        if end != len(raw):
            decoder._fail("trailing BSON data")
        return document
    except (UnicodeError, ValueError, IndexError, struct.error, RecursionError) as exc:
        decoder._fail(str(exc))
