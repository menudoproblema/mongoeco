"""UUID conversion decisions do not depend on a decoder implementation."""

from uuid import UUID

import pytest

from bson.errors import InvalidBSON

from mongoeco.core.codec import DocumentCodec
from mongoeco.types import Binary, CodecOptions, UuidRepresentation


UUID_SIZE = 16


@pytest.mark.parametrize("subtype", [3, 4])
@pytest.mark.parametrize("length", [0, 2, 15, 16, 17])
@pytest.mark.parametrize("mode", list(UuidRepresentation))
def test_requested_uuid_conversion_enforces_the_payload_size(subtype, length, mode):
    payload = bytes(range(length))
    value = Binary(payload, subtype)
    converts = (subtype, mode) in {
        (4, UuidRepresentation.STANDARD),
        (3, UuidRepresentation.PYTHON_LEGACY),
        (3, UuidRepresentation.JAVA_LEGACY),
        (3, UuidRepresentation.CSHARP_LEGACY),
    }
    options = CodecOptions(uuid_representation=mode)
    if converts and length != UUID_SIZE:
        with pytest.raises(InvalidBSON):
            DocumentCodec._apply_uuid_codec_options(value, codec_options=options)
        return
    result = DocumentCodec._apply_uuid_codec_options(value, codec_options=options)
    if converts:
        expected = payload
        if mode is UuidRepresentation.JAVA_LEGACY:
            expected = payload[:8][::-1] + payload[8:][::-1]
        if mode is UuidRepresentation.CSHARP_LEGACY:
            assert result == UUID(bytes_le=payload)
        else:
            assert result == UUID(bytes=expected)
    else:
        assert bytes(result) == payload
        assert result.subtype == subtype
