"""Build common result containers once without interleaving codec effects."""

from __future__ import annotations

import datetime
import uuid

from typing import Any

from mongoeco._types.concerns import CodecOptions
from mongoeco.core import codec
from mongoeco.core.bson_values import decoded_document_reference
from mongoeco.core.codec import DocumentCodec
from mongoeco.types import UndefinedType


try:
    from bson import BSON as _BSON
    from bson.binary import UuidRepresentation as _BsonUuidRepresentation
    from bson.errors import InvalidBSON as _InvalidBSON
    from bson.raw_bson import RawBSONDocument as _RawBSONDocument
    from bson.codec_options import (
        CodecOptions as _BsonCodecOptions,
        TypeDecoder as _TypeDecoder,
        TypeRegistry as _TypeRegistry,
    )
except ImportError:  # pragma: no cover - optional SDK
    _BSON = None
    _TypeDecoder = object

    class InvalidBSON(Exception):  # noqa: N818 - native BSON exception spelling
        """Portable family for a failure inside BSON response decoding."""

    _InvalidBSON = InvalidBSON


class _ResponseDecoder(_TypeDecoder):
    def __init__(self, value_type, transform):
        self._value_type = value_type
        self._transform = transform

    @property
    def bson_type(self):
        return self._value_type

    def transform_bson(self, value):
        return self._transform(value)


def cursor_response_document(batch, *, cursor_id, namespace, initial):
    """Retain native cursor field order, observable by client decoders."""
    return {
        "firstBatch" if initial else "nextBatch": batch,
        "id": (codec.BsonInt64Public or int)(cursor_id),
        "ns": namespace,
    }


def _encode_response_fallback(value):
    if isinstance(value, UndefinedType):
        return
    message = f"unsupported BSON response type: {type(value).__name__}"
    raise TypeError(message)


def _encode_response_document(document):
    encode_options = _BsonCodecOptions(
        uuid_representation=_BsonUuidRepresentation.STANDARD,
        type_registry=_TypeRegistry(fallback_encoder=_encode_response_fallback),
    )
    return _BSON.encode(
        DocumentCodec.to_pymongo(DocumentCodec.to_public(document)),
        codec_options=encode_options,
    )


def _decode_response_document(document, *, codec_options):
    """Apply public SDK decoding, or the supported portable codec policy."""
    if _BSON is None:
        # Root construction is outside BSON element decoding. Nested factory,
        # setter and decoder failures belong to that decoding boundary.
        decoded = codec_options.document_class()
        for key, value in document.items():
            public = DocumentCodec.to_pymongo(
                DocumentCodec.to_public(value), binary_subtype_zero_as_bytes=True
            )
            try:
                converted = (
                    public
                    if key == "id" and codec_options.type_registry
                    else DocumentCodec._apply_codec_options_recursive(
                        public, codec_options=codec_options
                    )
                )
            except Exception as error:
                raise _InvalidBSON(str(error)) from error
            decoded[key] = converted
        return decoded
    encoded = _encode_response_document(document)
    options = _BsonCodecOptions(
        document_class=codec_options.document_class,
        tz_aware=codec_options.tz_aware,
        tzinfo=codec_options.tzinfo,
        uuid_representation=getattr(
            _BsonUuidRepresentation, codec_options.uuid_representation.name
        ),
        type_registry=_TypeRegistry(
            [
                _ResponseDecoder(kind, transform)
                for kind, transform in codec_options.type_registry
            ]
        ),
    )
    return _BSON(encoded).decode(codec_options=options)


def materialize_command_response(document, *, codec_options):
    """Database.command decodes its whole reply without custom TypeDecoders."""
    options = CodecOptions(
        document_class=codec_options.document_class,
        tz_aware=codec_options.tz_aware,
        tzinfo=codec_options.tzinfo,
        uuid_representation=codec_options.uuid_representation,
    )
    return _decode_response_document(document, codec_options=options)


def _raw_response_value(value, *, codec_options):
    if not isinstance(value, dict):
        return value
    if _BSON is None:
        return materialize_public_value(value)
    options = _BsonCodecOptions(
        document_class=_RawBSONDocument,
        tz_aware=codec_options.tz_aware,
        tzinfo=codec_options.tzinfo,
        uuid_representation=getattr(
            _BsonUuidRepresentation, codec_options.uuid_representation.name
        ),
    )
    return _RawBSONDocument(_encode_response_document(value), codec_options=options)


def materialize_find_modify_response(document, *, codec_options):
    """Decode the selected value while retaining observable response effects."""
    if not codec_options.type_registry:
        return _decode_response_document(document, codec_options=codec_options)
    reply = codec_options.document_class()
    for key, value in document.items():
        if key == "value":
            # Selection decodes the entire reply at this point. Metadata hooks
            # run too, but only the selected value survives from this decode.
            reply[key] = _decode_response_document(
                document, codec_options=codec_options
            )[key]
        else:
            reply[key] = _raw_response_value(value, codec_options=codec_options)
    return reply


def materialize_cursor_response(batch, *, codec_options, namespace, cursor_id, initial):
    """Decode one completed standalone response, preserving factory effects.

    A registry selects the batch from a decoded cursor subdocument. Selection
    itself constructs command/cursor containers before decoding that subtree;
    id/ns callbacks run but their transformed values are discarded. Without a
    registry, factories see the whole command reply. Plain dict reads retain
    the fused materializer; raw never calls this function.
    """
    if codec_options.type_registry or codec_options.document_class is not dict:
        body = cursor_response_document(
            batch,
            cursor_id=cursor_id,
            namespace=namespace,
            initial=initial,
        )
        batch_key = "firstBatch" if initial else "nextBatch"
        if codec_options.type_registry:
            # These are selection containers, not recursively decoded BSON.
            # Constructor/setter failures here propagate without BSON wrapping.
            reply = codec_options.document_class()
            selected = codec_options.document_class()
            decoded = _decode_response_document(body, codec_options=codec_options)
            for key, value in body.items():
                selected[key] = decoded[key] if key == batch_key else value
            reply["cursor"] = selected
            reply["ok"] = 1.0
        else:
            reply = _decode_response_document(
                {"cursor": body, "ok": 1.0}, codec_options=codec_options
            )
        return reply["cursor"][batch_key]
    return [
        materialize_public_document(row, codec_options=codec_options) for row in batch
    ]


type _Container = dict[Any, Any] | list[Any]
type _Deferred = tuple[_Container, Any, Any]

_PLAIN_SCALARS = frozenset((str, int, float, bool, bytes, type(None)))
_CODEC_VALUES = tuple(
    kind
    for kind in (
        datetime.datetime,
        uuid.UUID,
        bytes,
        dict,
        list,
        tuple,
        codec.BsonCode,
        codec.BsonDBRef,
    )
    if kind is not None
)


def materialize_public_value(data: Any) -> Any:
    """Decode a BSON response value without document codec callbacks.

    Engine-owned wrappers must leave the response just like stored documents.
    Assigned insert IDs use DocumentCodec.to_pymongo's preserving default.
    """
    return _decode_dbrefs(
        DocumentCodec.to_pymongo(
            DocumentCodec.to_public(data), binary_subtype_zero_as_bytes=True
        )
    )


def _decode_dbrefs(value, *, root_document=False):
    """SDK response decoding converts nested DBRef shapes, never document roots."""
    if codec.BsonObjectId is None:
        return value
    if codec.BsonCode is not None and isinstance(value, codec.BsonCode):
        return codec.BsonCode(
            str(value),
            _decode_dbrefs(value.scope, root_document=True)
            if value.scope is not None
            else None,
        )
    if isinstance(value, codec.BsonDBRef):
        return _decode_dbrefs(value.as_doc())
    if isinstance(value, dict):
        items = ((key, _decode_dbrefs(item)) for key, item in value.items())
        document = (
            codec.BsonSON(items) if isinstance(value, codec.BsonSON) else dict(items)
        )
        return document if root_document else decoded_document_reference(document)
    if isinstance(value, list):
        return [_decode_dbrefs(item) for item in value]
    return value


def materialize_public_document(
    data: Any, *, codec_options: CodecOptions | None
) -> Any:
    """Materialize a decoded BSON read, then apply codec_options.

    Custom document factories/decoders retain the two-phase implementation.
    The common path constructs dict/list containers once; only values still
    needing codec conversion are revisited, after all BSON conversion succeeds.
    Tuple/subclass subtrees retain their existing conversion implementation.
    """
    if (
        codec_options is None
        or type(codec_options) is not CodecOptions
        or codec_options.document_class is not dict
        or codec_options.type_registry
        or codec.BsonObjectId is None
    ):
        return DocumentCodec.apply_codec_options(
            _decode_dbrefs(
                DocumentCodec.to_pymongo(
                    DocumentCodec.to_public(data), binary_subtype_zero_as_bytes=True
                ),
                root_document=True,
            ),
            codec_options=codec_options,
        )
    pending: list[_Deferred] = []
    root: list[Any] = [None]
    _convert_into(data, root, 0, pending, root_document=True)
    for container, key, converted in pending:
        container[key] = DocumentCodec.apply_codec_options(
            converted, codec_options=codec_options
        )
    return root[0]


def _convert_into(
    value: Any,
    parent: _Container,
    key: Any,
    pending: list[_Deferred],
    *,
    root_document=False,
) -> None:
    value_type = type(value)
    if value_type is dict:
        document: dict[Any, Any] = {}
        parent[key] = document
        pending_start = len(pending)
        for field, item in value.items():
            _convert_into(item, document, field, pending)
        if not root_document:
            converted = decoded_document_reference(document)
            parent[key] = converted
            if converted is not document:
                del pending[pending_start:]
                pending.append((parent, key, converted))
    elif value_type is list:
        array: list[Any] = [None] * len(value)
        parent[key] = array
        for index, item in enumerate(value):
            _convert_into(item, array, index, pending)
    elif value_type in _PLAIN_SCALARS:
        parent[key] = value
    else:
        converted = _decode_dbrefs(
            DocumentCodec.to_pymongo(
                DocumentCodec.to_public(value), binary_subtype_zero_as_bytes=True
            ),
            root_document=root_document,
        )
        parent[key] = converted
        if isinstance(converted, _CODEC_VALUES):
            pending.append((parent, key, converted))
