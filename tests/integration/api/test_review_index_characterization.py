"""Native simple-collation scenarios, with explicit legacy product boundaries."""

import asyncio

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, IndexModel, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.differential.review_improvement_cases import REVIEW_IMPROVEMENT_CASES
from tests.integration.api.test_review_improvement_parity import assert_native_outcome


FIXTURES = {
    dialect: loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / f"mongodb_review_improvements_{dialect.replace('.', '_')}.json"
        ).read_text()
    )["cases"]
    for dialect in ("7.0", "8.0", "9.0")
}
CASES = tuple(
    case
    for case in REVIEW_IMPROVEMENT_CASES
    if case.name
    in {
        "index_simple_recreate",
        "index_simple_explicit_then_implicit",
        "index_simple_batch",
    }
)


def index_subset(outcome):
    if not outcome["ok"]:
        return outcome
    # Native namespace existence and _id metadata are separate legacy boundaries.
    # Compare the advertised collation metadata contract. The legacy engine
    # also emits unique=False and omits native index version v=2.
    return {
        "ok": True,
        "result": sorted(
            [
                {key: document[key] for key in ("name", "key", "collation")}
                for document in outcome["result"]
                if document["name"] != "_id_"
            ],
            key=lambda document: document["name"],
        ),
    }


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_simple_index_creation_recreation_and_batch_match_native(
    backend, surface, case
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect="9.0") as client:
            actual = case.action(client.test.records)
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect="9.0") as client:
                collection = client.test.records
                definitions = []

                class CaptureIndexDefinitions:
                    def list_indexes(self):
                        return []

                    def create_index(self, keys, **options):
                        definitions.append((keys, options))

                    def create_indexes(self, models):
                        definitions.extend(
                            (
                                list(model.document["key"].items()),
                                {k: v for k, v in model.document.items() if k != "key"},
                            )
                            for model in models
                        )

                case.action(CaptureIndexDefinitions())
                before = {
                    "ok": True,
                    "result": await collection.list_indexes().to_list(),
                }
                calls = []
                try:
                    if case.name == "index_simple_batch":
                        calls.append(
                            {
                                "ok": True,
                                "result": await collection.create_indexes(
                                    [
                                        IndexModel(keys, **options)
                                        for keys, options in definitions
                                    ]
                                ),
                            }
                        )
                    else:
                        for keys, options in definitions:
                            calls.append(
                                {
                                    "ok": True,
                                    "result": await collection.create_index(
                                        keys, **options
                                    ),
                                }
                            )
                except OperationFailure as error:
                    message = "native scenario unexpectedly rejected"
                    raise AssertionError(message) from error
                return {
                    "before": before,
                    "calls": calls,
                    "after": {
                        "ok": True,
                        "result": await collection.list_indexes().to_list(),
                    },
                }

        actual = asyncio.run(exercise())
    expected = FIXTURES["9.0"][case.name]
    for actual_call, expected_call in zip(
        actual["calls"], expected["calls"], strict=True
    ):
        assert_native_outcome(actual_call, expected_call)
    for key in ("before", "after"):
        assert_native_outcome(index_subset(actual[key]), index_subset(expected[key]))


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_listed_non_id_metadata_roundtrips_without_rewriting_shared_storage(
    backend, tmp_path
):
    path = str(tmp_path / "indexes.db")
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine(path)
    with MongoClient(engine, mongodb_dialect="9.0") as client:
        records = client.test.records
        records.create_index("n", collation={"locale": "simple"})
        with client.start_session() as session:
            records.create_index("n", session=session)
            listed = next(
                index
                for index in records.list_indexes(session=session)
                if index["name"] == "n_1"
            )
            assert (
                records.create_index(
                    list(listed["key"].items()),
                    name=listed["name"],
                    collation=listed["collation"],
                    session=session,
                )
                == "n_1"
            )
        expected = FIXTURES["9.0"]["index_metadata_roundtrip"]
        assert index_subset(
            {"ok": True, "result": list(records.list_indexes())}
        ) == index_subset({"ok": True, "result": expected["after"]})
        raw = client._runner.run(engine.list_indexes("test", "records"))
        with MongoClient(engine, mongodb_dialect="7.0") as older:
            assert list(older.test.records.list_indexes()) == raw
            assert older._runner.run(engine.list_indexes("test", "records")) == raw
    if backend == "sqlite":
        with MongoClient(SQLiteEngine(path), mongodb_dialect="8.0") as reopened:
            assert list(reopened.test.records.list_indexes()) == raw


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_builtin_id_engine_contract_is_characterized_separately(dialect, backend):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        records = client.test.records
        listed = list(records.list_indexes())
        assert listed[0]["name"] == "_id_"
        assert listed[0]["unique"] is True
        # Native omits unique and lists nothing before namespace creation.
        native = FIXTURES[dialect]["index_id_unique_true_fresh"]
        assert native["before"] == {"ok": True, "result": []}
        invalid_index_specification_code = 197
        assert native["calls"][0]["code"] == invalid_index_specification_code
        assert records.create_index("_id", unique=True, name="_id_") == "_id_"
        if dialect == "9.0":
            assert (
                records.create_index(
                    "_id", unique=True, name="_id_", collation={"locale": "simple"}
                )
                == "_id_"
            )
