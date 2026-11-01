"""Encode public client requests without changing the internal runtime codec."""

from collections.abc import Mapping
from datetime import datetime
from uuid import UUID

from mongoeco.core import codec
from mongoeco.core.bson_raw import decode_bson_document
from mongoeco.core.bson_scalars import INT64_MAX, INT64_MIN
from mongoeco.core.bson_size import bson_document_size
from mongoeco.core.bson_values import regex_parts
from mongoeco.core.codec import DocumentCodec
from mongoeco.types import (
    Binary,
    CodecOptions,
    DBRef,
    Decimal128,
    ObjectId,
    Regex,
    Timestamp,
    UuidRepresentation,
)


try:
    from bson import BSON
    from bson.binary import UuidRepresentation as DriverUuidRepresentation
    from bson.codec_options import CodecOptions as DriverCodecOptions
    from bson.errors import InvalidDocument, InvalidStringData
    from pymongo.errors import DocumentTooLarge
except ImportError:  # pragma: no cover - optional SDK profile
    BSON = None

    class InvalidDocument(Exception):  # noqa: N818 - BSON exception spelling
        """Portable counterpart of the driver's client BSON error."""

    class InvalidStringData(Exception):  # noqa: N818 - BSON exception spelling
        """Portable counterpart of invalid UTF-8 in a BSON cstring."""


if BSON is None:  # pragma: no cover - optional SDK profile

    class DocumentTooLarge(InvalidDocument):
        """Portable counterpart of an operation exceeding its message frame."""


def _invalid(value):
    message = f"cannot encode object: {value!r}, of type: {type(value)!r}"
    return InvalidDocument(message)


def _driver_value(value):  # noqa: PLR0911 - public BSON adaptation
    # Adapt only the library's public BSON types. Runtime numeric wrappers are
    # not public input, even though to_pymongo can materialize them as output.
    if isinstance(value, DocumentCodec._BSON_WRAPPER_TYPES):
        raise _invalid(value)
    if isinstance(value, Mapping):
        return {key: _driver_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_driver_value(item) for item in value]
    if codec.BsonCode is not None and isinstance(value, codec.BsonCode):
        return codec.BsonCode(
            str.__str__(value),
            None if value.scope is None else _driver_value(value.scope),
        )
    if isinstance(value, DBRef):
        return _driver_value(value.as_document())
    if codec.BsonDBRef is not None and isinstance(value, codec.BsonDBRef):
        return _driver_value(value.as_doc())
    if isinstance(value, Binary | Decimal128 | ObjectId | Regex | Timestamp):
        return DocumentCodec.to_pymongo(value)
    # In particular, do not coerce Decimal, bytearray, set or arbitrary objects.
    # The driver owns rejection and the precise exception class.
    return value


def _portable_value(value, options):  # noqa: PLR0911, PLR0912 - closed BSON input dispatch
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _invalid(key)
            if "\x00" in key:
                message = "BSON keys must not contain a NUL character"
                raise InvalidDocument(message)
            str.encode(key, "utf-8")
            result[key] = _portable_value(item, options)
        return result
    if isinstance(value, list | tuple):
        return [_portable_value(item, options) for item in value]
    if value is None or isinstance(value, bool | float | bytes):
        return value
    if isinstance(value, int):
        if not INT64_MIN <= value <= INT64_MAX:
            message = "MongoDB can only handle up to 8-byte ints"
            raise OverflowError(message)
        return value
    if isinstance(value, str):
        str.encode(value, "utf-8")
        return value
    if isinstance(value, UUID):
        mode = options.uuid_representation
        if mode is UuidRepresentation.UNSPECIFIED:
            message = (
                "cannot encode native uuid.UUID with UuidRepresentation.UNSPECIFIED"
            )
            raise ValueError(message)
        if mode is UuidRepresentation.STANDARD:
            return value
        payload = value.bytes
        if mode is UuidRepresentation.JAVA_LEGACY:
            payload = payload[:8][::-1] + payload[8:][::-1]
        elif mode is UuidRepresentation.CSHARP_LEGACY:
            payload = value.bytes_le
        return Binary(payload, 3)
    if isinstance(value, DBRef):
        document = _portable_value(value.as_document(), options)
        return DBRef(
            document["$ref"],
            document["$id"],
            document.get("$db"),
            {
                key: item
                for key, item in document.items()
                if key not in {"$ref", "$id", "$db"}
            },
        )
    regex = regex_parts(value)
    if regex is not None:
        pattern, flags = regex
        if isinstance(pattern, bytes):
            try:
                pattern = pattern.decode("utf-8")
            except UnicodeDecodeError as error:
                raise InvalidStringData(str(error)) from error
        str.encode(pattern, "utf-8")
        if "\x00" in pattern:
            message = "regex patterns must not contain a NUL character"
            raise InvalidDocument(message)
        return Regex(pattern, flags)
    if isinstance(value, datetime | ObjectId | Decimal128 | Timestamp):
        return value
    raise _invalid(value)


def _encode_bson_document(document, *, codec_options: CodecOptions):
    if BSON is None:
        value = _portable_value(document, codec_options)
        return value, None
    representations = {
        UuidRepresentation.UNSPECIFIED: DriverUuidRepresentation.UNSPECIFIED,
        UuidRepresentation.STANDARD: DriverUuidRepresentation.STANDARD,
        UuidRepresentation.PYTHON_LEGACY: DriverUuidRepresentation.PYTHON_LEGACY,
        UuidRepresentation.JAVA_LEGACY: DriverUuidRepresentation.JAVA_LEGACY,
        UuidRepresentation.CSHARP_LEGACY: DriverUuidRepresentation.CSHARP_LEGACY,
    }
    options = DriverCodecOptions(
        uuid_representation=representations[codec_options.uuid_representation]
    )
    raw = BSON.encode(_driver_value(document), codec_options=options)
    # SDK document decoding would canonicalize DBRef-shaped literals. The raw
    # reader retains field order and defers that interpretation to its owner.
    return decode_bson_document(raw), len(raw)


def encode_bson_request(document, *, codec_options: CodecOptions):
    """Admit and normalize one client request at its actual encoding boundary."""
    return _encode_bson_document(document, codec_options=codec_options)[0]


def iter_bson_write_frames(
    command, operations, *, codec_options, sequence_name, max_message_size
):
    """Encode a BSON sequence once, retaining one lookahead across frames.

    As in OP_MSG, encoding an invalid lookahead fails the still-unsent frame.
    Only a valid document proving byte overflow permits publishing its prefix.
    """
    header, header_size = _encode_bson_document(command, codec_options=codec_options)
    header_size = header_size or bson_document_size(header)
    # Message header, flags, section-0 tag, section-1 tag/size/cstring.
    overhead = 16 + 4 + 1 + 1 + 4 + len(sequence_name.encode("utf-8")) + 1
    size = overhead + header_size
    current = []
    for operation in operations:
        encoded, length = _encode_bson_document(operation, codec_options=codec_options)
        length = length or bson_document_size(encoded)
        if current and size + length > max_message_size:
            yield header, current
            header, header_size = _encode_bson_document(
                command, codec_options=codec_options
            )
            header_size = header_size or bson_document_size(header)
            current = []
            size = overhead + header_size
        if size + length > max_message_size:
            if sequence_name == "documents":
                message = (
                    f"BSON document too large ({length} bytes)"
                    " - the connected server supports"
                    f" BSON document sizes up to {16 * 1024 * 1024} bytes."
                )
            else:
                operation_name = "update" if sequence_name == "updates" else "delete"
                message = f"{operation_name} command document too large"
            raise DocumentTooLarge(message)
        current.append(encoded)
        size += length
    if current:
        yield header, current


_BSON_ARGUMENTS = frozenset(
    {
        "filter_spec",
        "projection",
        "pipeline",
        "update_spec",
        "replacement",
        "array_filters",
        "let",
        "comment",
        "collation",
        "max_time_ms",
        "skip",
        "limit",
        "batch_size",
        "key",
    }
)


def encode_operation_bson(options, *, codec_options):
    """Encode BSON arguments together; preserve non-BSON API/session arguments."""
    from mongoeco.api.argument_validation import (  # noqa: PLC0415
        normalize_sort_spec,
        validate_batch_size,
        validate_max_time_ms,
    )
    from mongoeco.api.operations import (  # noqa: PLC0415
        _normalize_limit,
        _normalize_skip,
    )

    for name, validator in (
        ("max_time_ms", validate_max_time_ms),
        ("batch_size", validate_batch_size),
        ("skip", _normalize_skip),
        ("limit", _normalize_limit),
    ):
        if options.get(name) is not None:
            validator(options[name])

    request = {
        name: value
        for name, value in options.items()
        if name in _BSON_ARGUMENTS and value is not None
    }
    for field in ("sort", "hint"):
        value = options.get(field)
        if value is not None:
            request[field] = (
                value
                if field == "hint" and isinstance(value, str)
                else dict(normalize_sort_spec(value))
            )
    encoded = encode_bson_request(request, codec_options=codec_options)
    for field in ("sort", "hint"):
        if isinstance(encoded.get(field), dict):
            encoded[field] = list(encoded[field].items())
    return {**options, **encoded}


def encode_write_operation_bson(options, *, codec_options, find_modify=False):
    """Admit one write frame; command headers precede batched q/u entries."""
    order = (
        (
            "filter_spec",
            "return_document",
            "let",
            "max_time_ms",
            "update_spec",
            "replacement",
            "comment",
            "projection",
            "sort",
            "upsert",
            "array_filters",
            "hint",
            "bypass_document_validation",
            "collation",
        )
        if find_modify
        else (
            "let",
            "comment",
            "bypass_document_validation",
            "filter_spec",
            "update_spec",
            "replacement",
            "upsert",
            "collation",
            "array_filters",
            "hint",
            "sort",
        )
    )
    document = {
        name: options[name]
        for name in order
        if name in options and options[name] is not None
    }
    encoded = encode_bson_request(document, codec_options=codec_options)
    for field in ("sort", "hint"):
        if isinstance(encoded.get(field), dict):
            encoded[field] = list(encoded[field].items())
    return {**options, **encoded}
