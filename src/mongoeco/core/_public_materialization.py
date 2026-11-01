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
    from bson.codec_options import (
        CodecOptions as _BsonCodecOptions,
        TypeDecoder as _TypeDecoder,
        TypeRegistry as _TypeRegistry,
    )
except ImportError:  # pragma: no cover - optional SDK
    _BSON = None
    _TypeDecoder = object


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
        "id": cursor_id,
        "ns": namespace,
    }


def _encode_response_fallback(value):
    if isinstance(value, UndefinedType):
        return
    message = f"unsupported BSON response type: {type(value).__name__}"
    raise TypeError(message)


def materialize_cursor_response(batch, *, codec_options, namespace, cursor_id, initial):
    """Decode a completed cursor response using the installed SDK's policy.

    Custom decoding selects the batch from a decoded cursor subdocument: this
    includes batch/row decoders and discarded id/ns decoder effects, in order.
    Plain reads keep the fused portable materializer; raw never calls this.
    Return the transformed batch without consuming it: query and command
    cursors own their distinct iteration and retrieved-count rules.
    """
    if _BSON is not None and (
        codec_options.type_registry
        or codec_options.document_class is not dict
    ):
        body = cursor_response_document(
            batch,
            cursor_id=codec.BsonInt64Public(cursor_id),
            namespace=namespace,
            initial=initial,
        )
        encode_options = _BsonCodecOptions(
            uuid_representation=_BsonUuidRepresentation.STANDARD,
            type_registry=_TypeRegistry(fallback_encoder=_encode_response_fallback),
        )
        encoded = _BSON.encode(
            DocumentCodec.to_pymongo(DocumentCodec.to_public(body)),
            codec_options=encode_options,
        )
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
        decoded = _BSON(encoded).decode(codec_options=options)
        return decoded["firstBatch" if initial else "nextBatch"]
    if codec_options.type_registry:
        body = cursor_response_document(
            DocumentCodec.to_pymongo(
                DocumentCodec.to_public(batch), binary_subtype_zero_as_bytes=True
            ),
            cursor_id=(codec.BsonInt64Public or codec.BsonInt64)(cursor_id),
            namespace=namespace,
            initial=initial,
        )
        decoded = DocumentCodec.apply_codec_options(body, codec_options=codec_options)
        return decoded["firstBatch" if initial else "nextBatch"]
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
