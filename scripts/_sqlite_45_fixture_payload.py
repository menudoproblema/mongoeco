from __future__ import annotations

import argparse
import asyncio
import sqlite3
import uuid

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import mongoeco

from mongoeco import ObjectId, SearchIndexDefinition
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.engines.sqlite import SQLiteEngine


FIXTURE_OPERATION_ID = "mongoeco-4.5.0-bridge-operation"
PENDING_COMMIT_SEQUENCE = 3
FIXTURE_UPDATED_AT_EPOCH = 1_786_795_200.0


def _prepare_output(path: Path) -> None:
    if mongoeco.__version__ != "4.5.0":
        message = (
            f"fixture generator requires mongoeco 4.5.0, found {mongoeco.__version__}"
        )
        raise RuntimeError(message)
    if path.exists():
        path.unlink()


async def _generate(path: Path) -> None:
    engine = SQLiteEngine(str(path))
    await engine.connect()
    try:
        engine.register_change_consumer(
            "bridge-durable",
            initial_checkpoint=0,
            durable=True,
        )
        base_context = replace(
            OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
                change_operation_type="insert",
            ),
            operation_id=FIXTURE_OPERATION_ID,
        )
        documents = (
            {
                "_id": ObjectId("64b000000000000000000001"),
                "kind": "note",
                "title": "Ada bridge",
                "created_at": datetime(
                    2026,
                    8,
                    15,
                    12,
                    34,
                    56,
                    789123,
                    tzinfo=UTC,
                ),
                "external_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                "nested": {
                    "items": [
                        {"code": "a", "value": 1},
                        {"code": "b", "value": 2},
                    ],
                },
            },
            {
                "_id": ObjectId("64b000000000000000000002"),
                "kind": "note",
                "title": "Grace bridge",
                "created_at": datetime(2026, 8, 15, 12, 35, tzinfo=UTC),
                "external_id": uuid.UUID("87654321-4321-8765-4321-876543218765"),
                "nested": {"items": []},
            },
            {
                "_id": ObjectId("64b000000000000000000003"),
                "kind": "archive",
                "title": "Pending bridge",
                "created_at": datetime(2026, 8, 15, 12, 36, tzinfo=UTC),
                "external_id": uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
                "nested": {"items": [[{"code": "deep"}]]},
            },
        )
        for event_index, document in enumerate(documents[:2]):
            outcome = await engine.insert_document(
                "bridge",
                "items",
                document,
                overwrite=False,
                operation_context=base_context.derive(
                    change_event_index=event_index,
                ),
            )
            if outcome.commit_sequence != event_index + 1:
                message = "unexpected 4.5 commit sequence while generating fixture"
                raise RuntimeError(message)
        delivered: list[int] = []
        engine.dispatch_committed_changes(
            "bridge-durable",
            lambda change: delivered.append(change.sequence),
        )
        if delivered != [1, 2]:
            message = "4.5 fixture checkpoint prefix was not delivered"
            raise RuntimeError(message)
        pending = await engine.insert_document(
            "bridge",
            "items",
            documents[2],
            overwrite=False,
            operation_context=base_context.derive(change_event_index=2),
        )
        if pending.commit_sequence != PENDING_COMMIT_SEQUENCE:
            message = "unexpected pending 4.5 commit sequence"
            raise RuntimeError(message)
        created_index = await engine.create_index(
            "bridge",
            "items",
            [("kind", 1)],
            name="kind_1",
        )
        if created_index != "kind_1":
            message = "4.5 scalar index fixture was not created"
            raise RuntimeError(message)
        created_search_index = await engine.create_search_index(
            "bridge",
            "items",
            SearchIndexDefinition(
                {
                    "mappings": {
                        "dynamic": False,
                        "fields": {
                            "title": {"type": "string"},
                            "kind": {"type": "token"},
                        },
                    },
                },
                name="by_text",
            ),
        )
        if created_search_index != "by_text":
            message = "4.5 Search index fixture was not created"
            raise RuntimeError(message)
    finally:
        await engine.disconnect()
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE change_outbox_consumers SET updated_at_epoch = ?",
            (FIXTURE_UPDATED_AT_EPOCH,),
        )
        connection.commit()
        connection.execute("VACUUM")
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    _prepare_output(output)
    asyncio.run(_generate(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
