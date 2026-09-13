"""Build common result containers once without interleaving codec effects."""

from __future__ import annotations

import datetime
import uuid

from typing import Any

from mongoeco._types.concerns import CodecOptions
from mongoeco.core import codec
from mongoeco.core.codec import DocumentCodec


type _Container = dict[Any, Any] | list[Any]
type _Deferred = tuple[_Container, Any, Any]

_PLAIN_SCALARS = frozenset((str, int, float, bool, bytes, type(None)))
_CODEC_VALUES = (datetime.datetime, uuid.UUID, dict, list, tuple)


def materialize_public_document(
    data: Any, *, codec_options: CodecOptions | None
) -> Any:
    """Equivalent to to_pymongo followed by apply_codec_options.

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
            DocumentCodec.to_pymongo(data), codec_options=codec_options
        )
    pending: list[_Deferred] = []
    root: list[Any] = [None]
    _convert_into(data, root, 0, pending)
    for container, key, converted in pending:
        container[key] = DocumentCodec.apply_codec_options(
            converted, codec_options=codec_options
        )
    return root[0]


def _convert_into(
    value: Any, parent: _Container, key: Any, pending: list[_Deferred]
) -> None:
    value_type = type(value)
    if value_type is dict:
        document: dict[Any, Any] = {}
        parent[key] = document
        for field, item in value.items():
            _convert_into(item, document, field, pending)
    elif value_type is list:
        array: list[Any] = [None] * len(value)
        parent[key] = array
        for index, item in enumerate(value):
            _convert_into(item, array, index, pending)
    elif value_type in _PLAIN_SCALARS:
        parent[key] = value
    else:
        converted = DocumentCodec.to_pymongo(value)
        parent[key] = converted
        if isinstance(converted, _CODEC_VALUES):
            pending.append((parent, key, converted))
