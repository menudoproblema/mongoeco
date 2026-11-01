"""Collection namespace client validation before prepared DDL execution."""

from mongoeco.api._command_bson import rename_collection_options
from mongoeco.api.admin_parsing import normalize_drop_collection_name
from mongoeco.api.bson_input import encode_bson_request
from mongoeco.errors import InvalidName
from mongoeco.session_guards import ensure_session_can_use_engine
from mongoeco.types import CodecOptions, UuidRepresentation


_NAMESPACE_OPTIONS = CodecOptions(uuid_representation=UuidRepresentation.UNSPECIFIED)


def prepare_drop(collection, *, session):
    ensure_session_can_use_engine(collection._engine, session)
    command = encode_bson_request(
        {"drop": collection.name},
        codec_options=_NAMESPACE_OPTIONS,
    )
    return normalize_drop_collection_name(
        command["drop"],
        dialect=collection._mongodb_dialect,
    )


def prepare_rename(collection, new_name, *, session, **kwargs):
    if not isinstance(new_name, str):
        message = "new_name must be a string"
        raise TypeError(message)
    if not new_name or ".." in new_name:
        message = "collection names cannot be empty"
        raise InvalidName(message)
    if new_name[0] == "." or new_name[-1] == ".":
        message = "collection names must not start or end with '.'"
        raise InvalidName(message)
    if "$" in new_name and not new_name.startswith("oplog.$main"):
        message = "collection names must not contain '$'"
        raise InvalidName(message)
    ensure_session_can_use_engine(collection._engine, session)
    # The driver constructs namespaces here: Code scope is intentionally absent.
    command = encode_bson_request(
        {
            "renameCollection": collection.full_name,
            "to": f"{collection._db_name}.{new_name}",
            **kwargs,
        },
        codec_options=_NAMESPACE_OPTIONS,
    )
    return rename_collection_options(command, "admin")
