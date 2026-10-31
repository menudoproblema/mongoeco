"""Binary aliases cross real SQL, index, persisted-key and cache boundaries."""

from unittest.mock import patch
from uuid import UUID

import pytest

from bson import Binary as SDKBinary

from mongoeco import MongoClient
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.identity import (
    assert_document_matches_storage_key,
    materialize_replacement_document,
)
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import DuplicateKeyError, WriteError
from mongoeco.types import Binary


PAYLOAD = bytes(range(16))


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("field", ["v", "nested.v"])
@pytest.mark.parametrize("indexed", [False, True])
def test_binary_sort_fallback_precedes_skip_limit_and_tracks_changed_tags(
    dialect, field, indexed
):
    engine = SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        subject = client.binary_sort.records
        subject.insert_one({"_id": 1, "v": 1, "nested": {"v": 1}})
        if indexed:
            subject.create_index(field)
        # Prime the negative feature cache before changing to binary storage tags.
        assert [doc["_id"] for doc in subject.find({}).sort(field)] == [1]
        values = [
            SDKBinary(b"\xff", 0),
            SDKBinary(b"\0\0", 0),
            UUID(bytes=PAYLOAD),
            SDKBinary(PAYLOAD, 5),
        ]
        subject.update_one({"_id": 1}, {"$set": {field: values[0]}})
        for index, value in enumerate(values[1:], 2):
            subject.insert_one(
                {
                    "_id": index,
                    "v": value if field == "v" else 0,
                    "nested": {"v": value if field != "v" else 0},
                }
            )
        # BSON length wins before content; subtype 4 wins before subtype 5.
        for direction, expected in [(1, [1, 2, 3, 4]), (-1, [4, 3, 2, 1])]:
            assert [
                doc["_id"] for doc in subject.find({}).sort(field, direction)
            ] == expected
            page = subject.find({}).sort(field, direction).skip(1).limit(2)
            assert [doc["_id"] for doc in page] == expected[1:3]


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("index", ["none", "scalar", "multikey", "partial"])
@pytest.mark.parametrize("operator", ["$eq", "$in", "$ne", "$nin"])
def test_sqlite_binary_aliases_keep_candidates_with_and_without_translation(
    dialect, index, operator
):
    engine = SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.binary_paths.records
        collection.insert_many(
            [
                {"_id": i, "v": value, "n": [{"v": value}], "enabled": True}
                for i, value in enumerate(
                    (PAYLOAD, SDKBinary(PAYLOAD, 0), SDKBinary(PAYLOAD, 5)), 1
                )
            ]
        )
        path = "n.v" if index == "multikey" else "v"
        if index != "none":
            options = (
                {"partial_filter_expression": {"enabled": True}}
                if index == "partial"
                else {}
            )
            collection.create_index([(path, 1)], **options)
        for value, matching in ((PAYLOAD, [1, 2]), (SDKBinary(PAYLOAD, 5), [3])):
            operand = [value] if operator in {"$in", "$nin"} else value
            query = {"enabled": True, path: {operator: operand}}
            expected = (
                matching
                if operator in {"$eq", "$in"}
                else [i for i in (1, 2, 3) if i not in matching]
            )
            statements = []
            engine._connection.set_trace_callback(statements.append)
            with patch.object(
                QueryEngine, "match_plan", wraps=QueryEngine.match_plan
            ) as matcher:
                actual = sorted(doc["_id"] for doc in collection.find(query))
            engine._connection.set_trace_callback(None)
            assert actual == expected
            assert matcher.call_count > 0
            assert any(
                "SELECT" in sql and "FROM documents" in sql for sql in statements
            )
            with patch.object(
                engine,
                "_translate_query_plan_with_multikey",
                side_effect=NotImplementedError,
            ):
                assert sorted(doc["_id"] for doc in collection.find(query)) == expected


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("compound", [False, True])
def test_sqlite_reopen_preserves_binary_root_storage_key_and_index(
    tmp_path, dialect, compound
):
    path = str(tmp_path / "binary.sqlite")
    stored_id = {"k": SDKBinary(PAYLOAD, 0)} if compound else SDKBinary(PAYLOAD, 0)
    alias_id = {"k": PAYLOAD} if compound else PAYLOAD
    distinct_id = (
        {"k": SDKBinary(PAYLOAD, 128)} if compound else SDKBinary(PAYLOAD, 128)
    )
    engine = SQLiteEngine(path)
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.binary_paths.records
        collection.insert_one({"_id": stored_id, "v": 1})
        collection.create_index([("v", 1)])
        before = engine._connection.execute(
            "SELECT storage_key, document FROM documents"
        ).fetchone()
    engine = SQLiteEngine(path)
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.binary_paths.records
        assert collection.find_one({"_id": alias_id})["v"] == 1
        assert (
            collection.update_one({"_id": alias_id}, {"$set": {"v": 2}}).modified_count
            == 1
        )
        after = engine._connection.execute(
            "SELECT storage_key FROM documents"
        ).fetchone()[0]
        assert after == before[0]
        assert collection.count_documents({"v": 1}) == 0
        assert collection.count_documents({"v": 2}) == 1
        with pytest.raises(DuplicateKeyError):
            collection.insert_one({"_id": alias_id})
        collection.insert_one({"_id": distinct_id, "v": 3})
        assert collection.count_documents({}) == 2  # noqa: PLR2004 - two distinct BSON IDs


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("wrong", [b"other", Binary(PAYLOAD, 128), 1, 1.0])
def test_binary_alias_storage_guard_still_rejects_wrong_selected_key(factory, wrong):
    engine = factory()
    value = DocumentCodec.to_internal(SDKBinary(PAYLOAD, 0))
    with pytest.raises(WriteError) as error:
        assert_document_matches_storage_key(
            {"_id": value},
            engine._storage_key(wrong),
            storage_key_for_id=engine._storage_key,
        )
    assert error.value.code == 66  # noqa: PLR2004 - native immutable-ID error


def test_replacement_materialization_detaches_command_owned_root():
    replacement = DocumentCodec.to_internal({"_id": 1, "v": SDKBinary(PAYLOAD, 0)})
    materialized = materialize_replacement_document({"_id": 1}, replacement)
    materialized["v"] = b"changed"
    assert bytes(replacement["v"]) == PAYLOAD
    assert materialized is not replacement
