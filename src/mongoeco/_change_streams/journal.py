from __future__ import annotations

import hashlib
import json
import os

from mongoeco.core.codec import DocumentCodec
from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventSnapshot


def snapshot_to_document(snapshot: ChangeEventSnapshot) -> dict[str, object]:
    document: dict[str, object] = {
        "token": snapshot.token,
        "operation_type": snapshot.operation_type,
        "db_name": snapshot.db_name,
        "coll_name": snapshot.coll_name,
        "document_key": snapshot.document_key,
    }
    if snapshot.full_document is not None:
        document["full_document"] = snapshot.full_document
    if snapshot.update_description is not None:
        document["update_description"] = snapshot.update_description
    if snapshot.post_image_available:
        document["post_image_available"] = True
    if snapshot.to_namespace is not None:
        document["to_namespace"] = snapshot.to_namespace
    encoded = DocumentCodec.encode(document)
    if not isinstance(encoded, dict):
        raise OperationFailure("change stream journal could not be encoded")
    return encoded


def snapshot_from_document(document: object) -> ChangeEventSnapshot:
    # Keep physical numeric widths for the cursor's normal materialization.
    document = DocumentCodec.decode(document, preserve_bson_wrappers=True)
    if not isinstance(document, dict):
        raise OperationFailure("change stream journal could not be loaded")
    token = document.get("token")
    operation_type = document.get("operation_type")
    db_name = document.get("db_name")
    coll_name = document.get("coll_name")
    document_key = document.get("document_key")
    update_description = document.get("update_description")
    post_image_available = document.get("post_image_available", False)
    to_namespace = document.get("to_namespace")
    if (
        not isinstance(token, int)
        or isinstance(token, bool)
        or token < 1
        or not isinstance(operation_type, str)
        or not isinstance(db_name, str)
        or not isinstance(coll_name, str)
        or not isinstance(document_key, dict)
        or not isinstance(post_image_available, bool)
        or (
            to_namespace is not None
            and (
                not isinstance(to_namespace, dict)
                or set(to_namespace) != {"db", "coll"}
                or not all(isinstance(value, str) for value in to_namespace.values())
            )
        )
        or (update_description is not None and not isinstance(update_description, dict))
    ):
        raise OperationFailure("change stream journal could not be loaded")
    full_document = document.get("full_document")
    if full_document is not None and not isinstance(full_document, dict):
        raise OperationFailure("change stream journal could not be loaded")
    return ChangeEventSnapshot(
        token=token,
        operation_type=operation_type,
        db_name=db_name,
        coll_name=coll_name,
        document_key=document_key,
        full_document=full_document,
        update_description=update_description,
        post_image_available=post_image_available,
        to_namespace=to_namespace,
    )


def journal_payload(
    *,
    base_offset: int,
    next_token: int,
    events: list[ChangeEventSnapshot],
) -> str:
    return json.dumps(
        {
            "version": 1,
            "base_offset": base_offset,
            "next_token": next_token,
            "events": [snapshot_to_document(event) for event in events],
        },
        separators=(",", ":"),
        ensure_ascii=True,
        sort_keys=False,
    )


def journal_event_entry(event: ChangeEventSnapshot) -> str:
    payload = json.dumps(
        snapshot_to_document(event),
        separators=(",", ":"),
        sort_keys=False,
    )
    return json.dumps(
        {
            # V1 checksums canonicalized keys recursively, losing BSON order.
            # V2 authenticates the actual ordered document; legacy logs remain
            # readable below without rewriting their checksums or tokens.
            "version": 2,
            "event": json.loads(payload),
            "checksum": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        },
        separators=(",", ":"),
        sort_keys=False,
    )


def snapshot_from_log_entry(document: object) -> ChangeEventSnapshot:
    if not isinstance(document, dict):
        raise OperationFailure("change stream journal could not be loaded")
    version = document.get("version")
    event = document.get("event")
    checksum = document.get("checksum")
    if (
        isinstance(version, bool)
        or version not in (1, 2)
        or not isinstance(checksum, str)
    ):
        raise OperationFailure("change stream journal could not be loaded")
    payload = json.dumps(event, separators=(",", ":"), sort_keys=version == 1)
    if hashlib.sha256(payload.encode("utf-8")).hexdigest() != checksum:
        raise OperationFailure("change stream journal could not be loaded")
    return snapshot_from_document(event)


def fsync_parent_directory(path: str) -> None:
    directory = os.path.dirname(path) or "."
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        return
    finally:
        os.close(descriptor)
