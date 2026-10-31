"""Mutation effects are atomic with state and survive retries and persistence."""

import pytest

from bson import BSON, Int64

from mongoeco import MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine, sqlite as sqlite_module
from mongoeco.engines.results import MutationOutcome
from mongoeco.types import UpdateResult


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("mode", ["classic", "pipeline", "replacement", "merge"])
def test_failed_change_recording_rolls_back_state_indexes_and_events(
    backend, dialect, mode, tmp_path, monkeypatch
):
    engine = (
        MemoryEngine()
        if backend == "memory"
        else SQLiteEngine(str(tmp_path / "effects.sqlite"))
    )
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.effects.records
        original = {"_id": 1, "v": 1, "pad": "x" * 1000}
        collection.insert_one(original)
        collection.create_index("v", unique=True)
        source = client.effects.source
        source.insert_one({"_id": 1, "v": 1.0})
        stream = collection.watch(max_await_time_ms=1)

        def mutate():
            if mode == "merge":
                return list(source.aggregate([{"$merge": {"into": collection.name}}]))
            if mode == "replacement":
                return collection.replace_one({}, {**original, "v": 1.0})
            update = {"$set": {"v": 1.0}}
            return collection.update_one({}, [update] if mode == "pipeline" else update)

        owner = engine if backend == "memory" else sqlite_module
        method = (
            "_record_committed_change"
            if backend == "memory"
            else "_sqlite_append_change"
        )
        record = getattr(owner, method)

        def fail_after_record(*args, **kwargs):
            record(*args, **kwargs)
            message = "injected mutation effect failure"
            raise RuntimeError(message)

        try:
            with monkeypatch.context() as patch:
                patch.setattr(owner, method, fail_after_record)
                with pytest.raises(RuntimeError, match="injected mutation effect"):
                    mutate()
            assert BSON.encode(
                collection.find_one({"v": 1}, hint="v_1")
            ) == BSON.encode(original)
            assert stream.try_next() is None
            mutate()
            collection.insert_one({"_id": "barrier", "v": 9})
            event = stream.try_next()
            assert event is not None
            assert event["documentKey"] == {"_id": 1}
            assert event["operationType"] == (
                "replace" if mode == "replacement" else "update"
            )
            if mode != "replacement":
                assert BSON.encode(event["updateDescription"]) == BSON.encode(
                    {
                        "updatedFields": {"v": 1.0},
                        "removedFields": [],
                        "truncatedArrays": [],
                    }
                )
            assert stream.try_next()["documentKey"] == {"_id": "barrier"}
            assert stream.try_next() is None
            assert BSON.encode(
                collection.find_one({"_id": 1}, hint="v_1")
            ) == BSON.encode({**original, "v": 1.0})
        finally:
            stream.close()


def test_mutation_outcome_owns_effect_payload_and_rejects_unapplied_effects():
    change = {"update_description": {"updatedFields": {"v": [1.0]}}}
    outcome = MutationOutcome(UpdateResult(1, 1), change=change)
    change["update_description"]["updatedFields"]["v"].append(2)
    assert outcome.change == {"update_description": {"updatedFields": {"v": [1.0]}}}
    with pytest.raises(ValueError, match="only an applied mutation"):
        MutationOutcome(UpdateResult(1, 0), change=change)


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("value", [1.0, Int64(1)])
def test_sqlite_reopen_preserves_update_contents_and_replays_once(
    tmp_path, dialect, compact, value
):
    path = str(tmp_path / "effects.sqlite")
    journal = str(tmp_path / "effects.json")
    journal_options = {"change_stream_journal_max_bytes": 1 if compact else None}
    with MongoClient(
        SQLiteEngine(path),
        mongodb_dialect=dialect,
        change_stream_journal_path=journal,
        **journal_options,
    ) as client:
        collection = client.persisted.records
        collection.insert_one({"_id": 1, "v": 1, "pad": "x" * 1000})
        stream = collection.watch(max_await_time_ms=1)
        try:
            collection.insert_one({"_id": "anchor"})
            anchor = stream.try_next()["_id"]
            collection.update_one({}, [{"$set": {"_id": value, "v": value}}])
            original = stream.try_next()
            assert original["operationType"] == "update"
            assert BSON.encode(original["documentKey"]) == BSON.encode({"_id": 1})
            assert BSON.encode(original["updateDescription"]) == BSON.encode(
                {
                    "updatedFields": {"_id": value, "v": value},
                    "removedFields": [],
                    "truncatedArrays": [],
                }
            )
        finally:
            stream.close()
    with MongoClient(
        SQLiteEngine(path),
        mongodb_dialect=dialect,
        change_stream_journal_path=journal,
        **journal_options,
    ) as client:
        collection = client.persisted.records
        stream = collection.watch(resume_after=anchor, max_await_time_ms=1)
        try:
            replayed = stream.try_next()
            assert BSON.encode(replayed) == BSON.encode(original)
            collection.insert_one({"_id": "barrier"})
            assert stream.try_next()["documentKey"] == {"_id": "barrier"}
            assert stream.try_next() is None
            assert BSON.encode(
                {"_id": collection.find_one({"v": 1})["_id"]}
            ) == BSON.encode({"_id": value})
        finally:
            stream.close()
