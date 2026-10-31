"""Journal integrity covers ordered BSON; legacy checksums remain readable."""

import hashlib
import json

import pytest

from bson import BSON, Binary, Code, Int64

from mongoeco._change_streams.journal import (
    journal_event_entry,
    snapshot_from_log_entry,
)
from mongoeco.core.codec import DocumentCodec
from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventSnapshot


def _event():
    return ChangeEventSnapshot(
        token=1,
        operation_type="update",
        db_name="d",
        coll_name="c",
        document_key={"_id": {"z": Int64(1), "a": Binary(b"ab", 2)}},
        full_document={
            "_id": 1,
            "z": {"b": 1, "a": 2},
            "a": [Code("x", {"z": 1, "a": Int64(2)})],
        },
        update_description={
            "updatedFields": {
                "z": {"b": 1, "a": 2},
                "a": Code("x", {"z": 1, "a": Int64(2)}),
            },
            "removedFields": [],
            "truncatedArrays": [],
        },
    )


def test_new_journal_entry_retains_bson_order_and_authenticates_it():
    event = _event()
    encoded = json.loads(journal_event_entry(event))
    assert encoded["version"] == 2  # noqa: PLR2004 - order-sensitive format version
    loaded = snapshot_from_log_entry(encoded)
    for field in ("document_key", "full_document", "update_description"):
        assert BSON.encode(
            DocumentCodec.to_pymongo(getattr(loaded, field))
        ) == BSON.encode(getattr(event, field))
    fields = encoded["event"]["update_description"]["updatedFields"]
    encoded["event"]["update_description"]["updatedFields"] = dict(
        reversed(list(fields.items()))
    )
    with pytest.raises(OperationFailure, match="could not be loaded"):
        snapshot_from_log_entry(encoded)


def test_legacy_sorted_checksum_remains_readable_without_rewriting_token():
    # Independent V1 fixture, including its original canonical JSON checksum.
    legacy = {
        "token": 7,
        "operation_type": "update",
        "db_name": "d",
        "coll_name": "c",
        "document_key": {"_id": 1},
        "update_description": {
            "updatedFields": {"z": 2, "a": 1},
            "removedFields": [],
            "truncatedArrays": [],
        },
    }
    payload = json.dumps(legacy, sort_keys=True, separators=(",", ":"))
    loaded = snapshot_from_log_entry(
        {
            "version": 1,
            "event": json.loads(payload),
            "checksum": hashlib.sha256(payload.encode()).hexdigest(),
        }
    )
    assert loaded.token == legacy["token"]
    # Already-lost field order cannot be reconstructed from an old log.
    assert list(loaded.update_description["updatedFields"]) == ["a", "z"]


@pytest.mark.parametrize("version", [True, 0, 3, "2"])
def test_unknown_or_nonintegral_log_versions_are_rejected(version):
    encoded = json.loads(journal_event_entry(_event()))
    encoded["version"] = version
    with pytest.raises(OperationFailure, match="could not be loaded"):
        snapshot_from_log_entry(encoded)
