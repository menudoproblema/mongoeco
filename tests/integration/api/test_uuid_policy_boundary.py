"""UUID default rejection, inherited request options and physical persistence."""

import asyncio

from dataclasses import fields
from pathlib import Path
from uuid import UUID

import pytest

from bson import BSON, Binary
from bson.binary import UuidRepresentation as DriverRepresentation
from bson.codec_options import CodecOptions as DriverOptions
from pymongo import InsertOne

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import CodecOptions, UuidRepresentation


VALUE = UUID("00112233-4455-6677-8899-aabbccddeeff")
WRITERS = (
    UuidRepresentation.STANDARD,
    UuidRepresentation.PYTHON_LEGACY,
    UuidRepresentation.JAVA_LEGACY,
    UuidRepresentation.CSHARP_LEGACY,
)


def test_uuid_default_is_unspecified_in_constructor_and_dataclass():
    assert CodecOptions().uuid_representation is UuidRepresentation.UNSPECIFIED
    assert (
        next(
            field
            for field in fields(CodecOptions)
            if field.name == "uuid_representation"
        ).default
        is UuidRepresentation.UNSPECIFIED
    )


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize(
    "route", ["insert", "insert-many", "update", "pipeline", "find", "bulk", "command"]
)
def test_default_uuid_rejection_has_no_write_or_event_prefix(tmp_path, backend, route):
    path = str(tmp_path / "uuid.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        async with AsyncMongoClient(engine()) as client:
            collection = client.test.records
            assert (
                client.codec_options.uuid_representation
                is UuidRepresentation.UNSPECIFIED
            )
            assert (
                collection.database.codec_options.uuid_representation
                is UuidRepresentation.UNSPECIFIED
            )
            await collection.create_index("unique", unique=True)
            await collection.insert_one({"_id": 0, "unique": "seed"})
            stream = collection.watch(max_await_time_ms=1)
            try:

                async def invoke():  # noqa: PLR0911 - independent public routes
                    if route == "insert":
                        return await collection.insert_one({"_id": VALUE})
                    if route == "insert-many":
                        return await collection.insert_many(
                            [{"_id": 1, "unique": "free"}, {"_id": 2, "u": VALUE}]
                        )
                    if route == "update":
                        return await collection.update_one(
                            {"_id": 0}, {"$set": {"u": VALUE}}
                        )
                    if route == "pipeline":
                        return await collection.aggregate(
                            [{"$project": {"u": {"$literal": VALUE}}}]
                        ).to_list()
                    if route == "find":
                        cursor = collection.find({"u": VALUE})
                        # Construction is lazy; failure belongs to consumption.
                        return await cursor.to_list()
                    if route == "bulk":
                        return await collection.bulk_write(
                            [
                                InsertOne({"_id": 1, "unique": "free"}),
                                InsertOne({"_id": 2, "u": VALUE}),
                            ]
                        )
                    return await collection.database.command(
                        {
                            "insert": collection.name,
                            "documents": [{"_id": 1, "u": VALUE}],
                        }
                    )

                with pytest.raises(ValueError, match="UNSPECIFIED"):
                    await invoke()
                await collection.insert_one({"_id": 9, "unique": "free"})
                event = await stream.try_next()
                assert event["documentKey"] == {"_id": 9}
                assert await stream.try_next() is None
                assert await collection.find().sort("_id").to_list() == [
                    {"_id": 0, "unique": "seed"},
                    {"_id": 9, "unique": "free"},
                ]
            finally:
                await stream.close()
        if backend == "sqlite-file":
            async with AsyncMongoClient(engine()) as client:
                assert [
                    row["_id"] async for row in client.test.records.find().sort("_id")
                ] == [0, 9]
                assert sorted(
                    [
                        index["name"]
                        async for index in client.test.records.list_indexes()
                    ]
                ) == ["_id_", "unique_1"]

    asyncio.run(exercise())


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("mode", WRITERS)
def test_inherited_uuid_writer_and_independent_read_policies(tmp_path, backend, mode):
    path = str(tmp_path / "uuid.sqlite")
    implementation = MemoryEngine() if backend == "memory" else SQLiteEngine(path)
    expected_binary = Binary.from_uuid(
        VALUE, uuid_representation=getattr(DriverRepresentation, mode.name)
    )
    with MongoClient(
        implementation, codec_options=CodecOptions(uuid_representation=mode)
    ) as client:
        collection = client.test.records
        assert collection.codec_options.uuid_representation is mode
        collection.create_index("u", unique=True)
        assert collection.insert_one({"_id": VALUE, "u": VALUE}).inserted_id == VALUE
        for reader_mode in UuidRepresentation:
            selected = collection.with_options(
                codec_options=CodecOptions(uuid_representation=reader_mode)
            )
            row = selected.find_one({"_id": expected_binary})
            expected = BSON(BSON.encode({"v": expected_binary})).decode(
                codec_options=DriverOptions(
                    uuid_representation=getattr(DriverRepresentation, reader_mode.name)
                )
            )["v"]
            assert type(row["_id"]) is type(expected)
            assert row["_id"] == row["u"] == expected
        overridden = client.get_database(
            "test", codec_options=CodecOptions()
        ).get_collection("records")
        with pytest.raises(ValueError, match="UNSPECIFIED"):
            overridden.find_one({"_id": VALUE})
        assert overridden.find_one({"_id": expected_binary})["u"] == expected_binary
    if backend == "sqlite-file":
        with MongoClient(SQLiteEngine(path)) as client:
            row = client.test.records.find_one({"_id": expected_binary})
            assert row["_id"] == row["u"] == expected_binary


def test_prior_standard_default_sqlite_reopens_without_uuid_reencoding(tmp_path):
    fixture = (
        Path(__file__).resolve().parents[2]
        / "fixtures/sqlite_uuid_standard_4_11.sqlite"
    )
    path = tmp_path / "prior.sqlite"
    original = fixture.read_bytes()
    path.write_bytes(original)
    binary = Binary(VALUE.bytes, 4)
    with MongoClient(SQLiteEngine(path)) as client:
        collection = client.legacy.records
        row = collection.find_one({"_id": binary})
        assert row == {"_id": binary, "u": binary, "label": "old-default-standard"}
        with pytest.raises(ValueError, match="UNSPECIFIED"):
            collection.find_one({"_id": VALUE})
        selected = collection.with_options(
            codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        )
        assert selected.find_one({"_id": VALUE})["u"] == VALUE
        result = selected.update_one({"_id": VALUE}, {"$set": {"label": "updated"}})
        assert result.matched_count == result.modified_count == 1
        with pytest.raises(DuplicateKeyError):
            collection.insert_one({"_id": 1, "u": binary})
    with MongoClient(SQLiteEngine(path)) as client:
        assert client.legacy.records.find_one({"_id": binary}) == {
            "_id": binary,
            "u": binary,
            "label": "updated",
        }
    assert fixture.read_bytes() == original
