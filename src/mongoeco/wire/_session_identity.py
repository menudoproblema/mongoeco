from __future__ import annotations

import uuid

from mongoeco.types import ObjectId


def wire_session_key(lsid: object) -> str:
    """Return the same stable key for equivalent wire session identifiers."""
    if isinstance(lsid, dict):
        return repr(tuple((key, _freeze(value)) for key, value in sorted(lsid.items())))
    return repr(_freeze(lsid))


def _freeze(value: object) -> object:
    if isinstance(value, dict):
        return tuple((key, _freeze(item)) for key, item in sorted(value.items()))
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, uuid.UUID):
        return ("uuid", str(value))
    if isinstance(value, (bytes, bytearray)):
        return ("bytes", bytes(value))
    if isinstance(value, ObjectId):
        return ("objectid", str(value))
    return value
