"""Preserve deprecated BSON undefined when the optional driver decodes raw bytes."""

from mongoeco.types import UNDEFINED


_BSON_OBJECT = 3
_BSON_ARRAY = 4
_BSON_UNDEFINED = 6
_BSON_CODE_WITH_SCOPE = 15


def decode_bson_document(raw: bytes) -> dict:
    # PyMongo's public decoder intentionally maps both null and undefined to None.
    # Its element decoder supplies offsets for every supported BSON type. Keep this
    # optional SDK bridge local; never mutate PyMongo's global decoding tables.
    import bson  # noqa: PLC0415 - optional SDK, needed only for raw BSON

    from bson.codec_options import DEFAULT_CODEC_OPTIONS  # noqa: PLC0415 - optional SDK

    document = bson.BSON(raw).decode()
    if b"\x06" not in raw:
        return document
    view = memoryview(raw)
    position = 4
    while position < len(raw) - 1:
        kind = raw[position]
        key, _, end = bson._element_to_dict(
            raw, view, position, len(raw) - 1, DEFAULT_CODEC_OPTIONS
        )
        start = raw.index(0, position + 1) + 1
        if kind == _BSON_UNDEFINED:
            document[key] = UNDEFINED
        elif kind in {_BSON_OBJECT, _BSON_ARRAY}:
            child = decode_bson_document(raw[start:end])
            document[key] = list(child.values()) if kind == _BSON_ARRAY else child
        elif kind == _BSON_CODE_WITH_SCOPE:
            # code-with-scope: total length, code string length, string, scope.
            length = int.from_bytes(raw[start + 4 : start + 8], "little", signed=True)
            scope = decode_bson_document(raw[start + 8 + length : end])
            document[key] = bson.Code(str(document[key]), scope)
        position = end
    return document
