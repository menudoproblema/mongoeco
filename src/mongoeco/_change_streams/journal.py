from __future__ import annotations

import hashlib
import json
import os

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
    return document


def snapshot_from_document(document: object) -> ChangeEventSnapshot:
    if not isinstance(document, dict):
        raise OperationFailure("change stream journal could not be loaded")
    token = document.get("token")
    operation_type = document.get("operation_type")
    db_name = document.get("db_name")
    coll_name = document.get("coll_name")
    document_key = document.get("document_key")
    update_description = document.get("update_description")
    if (
        not isinstance(token, int)
        or isinstance(token, bool)
        or token < 1
        or not isinstance(operation_type, str)
        or not isinstance(db_name, str)
        or not isinstance(coll_name, str)
        or not isinstance(document_key, dict)
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
        sort_keys=True,
    )


def journal_event_entry(event: ChangeEventSnapshot) -> str:
    payload = json.dumps(
        snapshot_to_document(event),
        separators=(",", ":"),
        sort_keys=True,
    )
    return json.dumps(
        {
            "version": 1,
            "event": json.loads(payload),
            "checksum": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def snapshot_from_log_entry(document: object) -> ChangeEventSnapshot:
    if not isinstance(document, dict):
        raise OperationFailure("change stream journal could not be loaded")
    version = document.get("version")
    event = document.get("event")
    checksum = document.get("checksum")
    if version != 1 or not isinstance(checksum, str):
        raise OperationFailure("change stream journal could not be loaded")
    payload = json.dumps(event, separators=(",", ":"), sort_keys=True)
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
