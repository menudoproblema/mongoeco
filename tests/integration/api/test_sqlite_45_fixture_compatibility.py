from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
import unittest

from pathlib import Path

from mongoeco import AsyncMongoClient, ObjectId
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure


FIXTURE_ROOT = Path("tests/fixtures/sqlite")
FIXTURE = FIXTURE_ROOT / "mongoeco-4.5.0-bridge.sqlite"
METADATA = FIXTURE_ROOT / "mongoeco-4.5.0-bridge.json"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _copy_fixture(directory: str) -> Path:
    target = Path(directory) / FIXTURE.name
    shutil.copyfile(FIXTURE, target)
    return target


class SQLite45FixtureCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    def test_fixture_has_verifiable_provenance(self) -> None:
        metadata = json.loads(METADATA.read_text(encoding="utf-8"))
        self.assertEqual(metadata["schemaVersion"], "mongoeco-sqlite-fixture/v1")
        self.assertEqual(metadata["generator"]["version"], "4.5.0")
        self.assertEqual(
            metadata["generator"]["artifactSha256"],
            "f168ab9f4172abbf1a7e35f8996c3e01463a26557b213028c83ef64d102a2fd3",
        )
        self.assertEqual(
            metadata["generator"]["constraints"],
            "requirements/sqlite-45-fixture-constraints.txt",
        )
        self.assertEqual(
            metadata["generator"]["runtime"]["packages"]["mongoeco"],
            "4.5.0",
        )
        self.assertTrue(metadata["generator"]["runtime"]["sqlite"])
        self.assertEqual(metadata["fixtureSha256"], _sha256(FIXTURE))

    async def test_current_engine_reads_indexes_search_and_bson(self) -> None:
        original_hash = _sha256(FIXTURE)
        with tempfile.TemporaryDirectory() as directory:
            database = _copy_fixture(directory)
            async with AsyncMongoClient(SQLiteEngine(str(database))) as client:
                collection = client.bridge.items
                documents = await collection.find().sort("_id").to_list()
                self.assertEqual(
                    [document["_id"] for document in documents],
                    [
                        ObjectId("64b000000000000000000001"),
                        ObjectId("64b000000000000000000002"),
                        ObjectId("64b000000000000000000003"),
                    ],
                )
                self.assertEqual(documents[0]["created_at"].microsecond, 789000)
                self.assertEqual(
                    str(documents[0]["external_id"]),
                    "12345678-1234-5678-1234-567812345678",
                )
                self.assertEqual(
                    documents[0]["nested"]["items"][1],
                    {"code": "b", "value": 2},
                )
                indexes = await collection.list_indexes().to_list()
                self.assertGreaterEqual(
                    {index["name"] for index in indexes},
                    {"_id_", "kind_1"},
                )
                search_indexes = await collection.list_search_indexes().to_list()
                self.assertEqual(
                    [index["name"] for index in search_indexes],
                    ["by_text"],
                )
                hits = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "by_text",
                                "text": {"query": "bridge", "path": "title"},
                            },
                        },
                    ],
                ).to_list()
                self.assertEqual(
                    {hit["title"] for hit in hits},
                    {"Ada bridge", "Grace bridge", "Pending bridge"},
                )
        self.assertEqual(_sha256(FIXTURE), original_hash)

    async def test_current_engine_replays_only_pending_outbox_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = _copy_fixture(directory)
            engine = SQLiteEngine(str(database))
            await engine.connect()
            try:
                delivered: list[int] = []
                engine.dispatch_committed_changes(
                    "bridge-durable",
                    lambda change: delivered.append(change.sequence),
                )
                self.assertEqual(delivered, [3])
                engine.dispatch_committed_changes(
                    "bridge-durable",
                    lambda change: delivered.append(change.sequence),
                )
                self.assertEqual(delivered, [3])
            finally:
                await engine.disconnect()
            connection = sqlite3.connect(database)
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT checkpoint FROM change_outbox_consumers "
                        "WHERE consumer_id = ?",
                        ("bridge-durable",),
                    ).fetchone(),
                    (3,),
                )
            finally:
                connection.close()

    async def test_opening_fixture_is_idempotent_and_coordinates_connections(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = _copy_fixture(directory)
            first = SQLiteEngine(str(database))
            second = SQLiteEngine(str(database))
            await first.connect()
            await second.connect()
            await second.disconnect()
            await first.disconnect()
            third = SQLiteEngine(str(database))
            await third.connect()
            await third.disconnect()
            connection = sqlite3.connect(database)
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT version FROM mongoeco_schema_migrations "
                        "WHERE component = 'change_outbox'"
                    ).fetchone(),
                    (4,),
                )
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM documents").fetchone(),
                    (3,),
                )
            finally:
                connection.close()

    async def test_future_schema_rejection_does_not_mutate_fixture_copy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = _copy_fixture(directory)
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    "UPDATE mongoeco_schema_migrations SET version = 99 "
                    "WHERE component = 'change_outbox'"
                )
                connection.commit()
            finally:
                connection.close()
            before = _sha256(database)
            engine = SQLiteEngine(str(database))
            with self.assertRaisesRegex(OperationFailure, "newer than supported"):
                await engine.connect()
            self.assertIsNone(engine._pending_connection)
            self.assertIsNone(engine._connection)
            self.assertEqual(engine._connection_count, 0)
            self.assertEqual(_sha256(database), before)
