"""Public group witnesses replay native captures across APIs and transports."""

import asyncio
import hashlib
import inspect
import pickle

from copy import deepcopy
from decimal import ROUND_DOWN, ROUND_UP, FloatOperation, Inexact, localcontext
from pathlib import Path
from unittest.mock import patch

import pymongo
import pytest

from bson import BSON, Decimal128

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat import resolve_mongodb_dialect
from mongoeco.core.aggregation.expression_values import MISSING
from mongoeco.core.aggregation.preparation import prepare_pipeline
from mongoeco.core.aggregation.runtime_state import apply_pipeline_states
from mongoeco.core.aggregation.spill import (
    AggregationGroupSpool,
    AggregationSpillPolicy,
)
from mongoeco.core.aggregation.stages import apply_pipeline
from mongoeco.core.codec import DocumentCodec
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential._real_parity_base import _decode_capture, _normalize
from tests.differential.semantic_average_route_cases import GROUP, SEED as AVERAGE_SEED
from tests.differential.semantic_command_let_cases import VALID as COMMAND_LET_VALID
from tests.differential.semantic_corpora import SEMANTIC_CORPORA
from tests.differential.semantic_object_grammar_cases import numeric_group_identity
from tests.differential.semantic_sum_cases import STREAM_PIPELINE, STREAM_SEED
from tests.support_write_parity import BSONObservation


FIXTURES = Path(__file__).parents[2] / "fixtures"
DIALECTS = ("7.0", "8.0", "9.0")
CORPORA = {name: corpus.cases for name, corpus in SEMANTIC_CORPORA.items()}


def _capture(dialect, corpus="group_parity"):
    return _decode_capture(
        (FIXTURES / f"semantic_{corpus}_{dialect.replace('.', '_')}.json").read_text()
    )


class _AsyncCursorView:
    def __init__(self, cursor, loop):
        self.cursor = cursor
        self.loop = loop
        self._iterator_view = None

    def _invoke(self, method, *args, **kwargs):
        async def execute():
            result = getattr(self.cursor, method)(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result

        return asyncio.run_coroutine_threadsafe(execute(), self.loop).result(timeout=10)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            if self._iterator_view is None:
                self._iterator_view = _AsyncCursorView(
                    self._invoke("__aiter__"), self.loop
                )
            return self._iterator_view._invoke("__anext__")
        except StopAsyncIteration:
            raise StopIteration from None

    def sort(self, *args, **kwargs):
        self._invoke("sort", *args, **kwargs)
        return self

    def clone(self):
        return _AsyncCursorView(self._invoke("clone"), self.loop)

    def explain(self, *args, **kwargs):
        return self._invoke("explain", *args, **kwargs)

    def rewind(self):
        self._invoke("rewind")
        self._iterator_view = None
        return self

    def try_next(self):
        return self._invoke("try_next")

    def to_list(self, length=None):
        return self._invoke("to_list", length)

    @property
    def alive(self):
        return self.cursor.alive

    def close(self):
        return self._invoke("close")


class _AsyncChangeStreamView(_AsyncCursorView):
    """Bridge actual async stream protocols without inventing missing methods."""

    def __iter__(self):
        if self._invoke("__aiter__") is not self.cursor:
            message = "async change stream iteration must retain cursor identity"
            raise AssertionError(message)
        return self

    def next(self):
        try:
            return self._invoke("next")
        except StopAsyncIteration:
            raise StopIteration from None

    def __next__(self):
        try:
            return self._invoke("__anext__")
        except StopAsyncIteration:
            raise StopIteration from None

    def __enter__(self):
        if self._invoke("__aenter__") is not self.cursor:
            message = "async change stream context must retain cursor identity"
            raise AssertionError(message)
        return self

    def __exit__(self, *args):
        return self._invoke("__aexit__", *args)

    @property
    def resume_token(self):
        return self.cursor.resume_token


class _AsyncView:
    """Run the unchanged synchronous witness against an async collection."""

    def __init__(self, collection, loop, client=None):
        self.collection = collection
        self.loop = loop
        self._session_client = client

    def __getitem__(self, name):
        return _AsyncView(self.collection[name], self.loop, self._session_client)

    def create_collection(self, name, **kwargs):
        return _AsyncView(
            self._call("create_collection", name, **kwargs),
            self.loop,
            self._session_client,
        )

    def options(self, *args, **kwargs):
        return self._call("options", *args, **kwargs)

    def list_indexes(self, *args, **kwargs):
        return _AsyncCursorView(self._call("list_indexes", *args, **kwargs), self.loop)

    def list_collections(self, *args, **kwargs):
        return _AsyncCursorView(
            self._call("list_collections", *args, **kwargs), self.loop
        )

    def drop(self, *args, **kwargs):
        return self._call("drop", *args, **kwargs)

    def drop_collection(self, *args, **kwargs):
        return self._call("drop_collection", *args, **kwargs)

    def drop_database(self, *args, **kwargs):
        return self._call("drop_database", *args, **kwargs)

    def rename(self, *args, **kwargs):
        return self._call("rename", *args, **kwargs)

    def insert_many(self, *args, **kwargs):
        return self._call("insert_many", *args, **kwargs)

    def aggregate(self, pipeline, **kwargs):
        return _AsyncCursorView(self._call("aggregate", pipeline, **kwargs), self.loop)

    def aggregate_raw_batches(self, pipeline, **kwargs):
        return _AsyncCursorView(
            self._call("aggregate_raw_batches", pipeline, **kwargs), self.loop
        )

    def find_raw_batches(self, *args, **kwargs):
        return _AsyncCursorView(
            self._call("find_raw_batches", *args, **kwargs), self.loop
        )

    def with_options(self, **kwargs):
        return _AsyncView(
            self._call("with_options", **kwargs), self.loop, self._session_client
        )

    @property
    def codec_options(self):
        return self.collection.codec_options

    def _call(self, method, *args, **kwargs):
        async def execute():
            result = getattr(self.collection, method)(*args, **kwargs)
            return await result if inspect.isawaitable(result) else result

        return asyncio.run_coroutine_threadsafe(execute(), self.loop).result(timeout=10)

    @property
    def database(self):
        return _AsyncView(self.collection.database, self.loop, self._session_client)

    @property
    def full_name(self):
        return self.collection.full_name

    @property
    def client(self):
        return _AsyncView(self._session_client, self.loop, self._session_client)

    def start_session(self):
        return self._call("start_session")

    @property
    def name(self):
        return self.collection.name

    def delete_many(self, *args, **kwargs):
        return self._call("delete_many", *args, **kwargs)

    def delete_one(self, *args, **kwargs):
        return self._call("delete_one", *args, **kwargs)

    def create_search_index(self, *args, **kwargs):
        return self._call("create_search_index", *args, **kwargs)

    def create_index(self, *args, **kwargs):
        return self._call("create_index", *args, **kwargs)

    def drop_index(self, *args, **kwargs):
        return self._call("drop_index", *args, **kwargs)

    def drop_indexes(self, *args, **kwargs):
        return self._call("drop_indexes", *args, **kwargs)

    def index_information(self, *args, **kwargs):
        return self._call("index_information", *args, **kwargs)

    def create_indexes(self, *args, **kwargs):
        return self._call("create_indexes", *args, **kwargs)

    def insert_one(self, *args, **kwargs):
        return self._call("insert_one", *args, **kwargs)

    def find(self, *args, **kwargs):
        async def execute():
            return self.collection.find(*args, **kwargs)

        cursor = asyncio.run_coroutine_threadsafe(execute(), self.loop).result(
            timeout=10
        )
        return _AsyncCursorView(cursor, self.loop)

    def distinct(self, *args, **kwargs):
        return self._call("distinct", *args, **kwargs)

    def count_documents(self, *args, **kwargs):
        return self._call("count_documents", *args, **kwargs)

    def watch(self, *args, **kwargs):
        return _AsyncChangeStreamView(self._call("watch", *args, **kwargs), self.loop)

    def find_one(self, *args, **kwargs):
        return self._call("find_one", *args, **kwargs)

    def update_one(self, *args, **kwargs):
        return self._call("update_one", *args, **kwargs)

    def replace_one(self, *args, **kwargs):
        return self._call("replace_one", *args, **kwargs)

    def update_many(self, *args, **kwargs):
        return self._call("update_many", *args, **kwargs)

    def find_one_and_update(self, *args, **kwargs):
        return self._call("find_one_and_update", *args, **kwargs)

    def find_one_and_delete(self, *args, **kwargs):
        return self._call("find_one_and_delete", *args, **kwargs)

    def find_one_and_replace(self, *args, **kwargs):
        return self._call("find_one_and_replace", *args, **kwargs)

    def bulk_write(self, *args, **kwargs):
        return self._call("bulk_write", *args, **kwargs)

    def command(self, *args, **kwargs):
        return self._call("command", *args, **kwargs)


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize(
    ["corpus", "name", "action", "surface"],
    [
        (corpus, name, action, surface)
        for corpus, spec in SEMANTIC_CORPORA.items()
        for name, action in spec.cases
        for surface in spec.surfaces
    ],
)
def test_expression_matches_native_capture(  # noqa: PLR0913 - explicit pytest axes
    engine_factory, dialect, corpus, name, action, *, surface
):
    capture = _capture(dialect, corpus)
    seed = deepcopy(SEMANTIC_CORPORA[corpus].seed)
    expected = capture["cases"][name]
    if surface == "sync":
        with MongoClient(engine_factory(), mongodb_dialect=dialect) as client:
            collection = client.semantic_group.records
            collection.insert_many(seed)
            assert _normalize(action(collection)) == _normalize(expected)
        return

    async def async_action(client):
        collection = client.semantic_group.records
        await collection.insert_many(seed)
        result = await asyncio.to_thread(
            action, _AsyncView(collection, asyncio.get_running_loop(), client)
        )
        assert _normalize(result) == _normalize(expected)

    async def exercise():
        if surface == "async":
            async with AsyncMongoClient(
                engine_factory(), mongodb_dialect=dialect
            ) as client:
                await async_action(client)
        else:
            async with AsyncMongoEcoProxyServer(
                engine=engine_factory(), mongodb_dialect=dialect
            ) as proxy:
                uri = f"mongodb://{proxy.address.host}:{proxy.address.port}"
                if surface == "wire-sync":

                    def run():
                        with pymongo.MongoClient(
                            uri, serverSelectionTimeoutMS=3000
                        ) as client:
                            collection = client.semantic_group.records
                            collection.insert_many(seed)
                            assert _normalize(action(collection)) == _normalize(
                                expected
                            )

                    await asyncio.to_thread(run)
                else:
                    # Older SDKs can perform blocking socket reads in async bulk
                    # writes. Keep their loop separate from the server's loop.
                    async def run_client():
                        async with pymongo.AsyncMongoClient(
                            uri, serverSelectionTimeoutMS=3000
                        ) as client:
                            await async_action(client)

                    await asyncio.to_thread(asyncio.run, run_client())

    asyncio.run(exercise())


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_numeric_group_identity_encodes_one_partition_per_equal_key(
    dialect, engine_factory, surface
):
    expected = _capture(dialect, "object_grammar")["cases"][
        "semantic_object_numeric_group_identity"
    ]
    original = AggregationGroupSpool._open_temporary_partition
    with patch.object(
        AggregationGroupSpool,
        "_open_temporary_partition",
        autospec=True,
        side_effect=original,
    ) as opened:
        if surface == "sync":
            with MongoClient(
                engine_factory(aggregation_spill_threshold=1),
                mongodb_dialect=dialect,
            ) as client:
                actual = numeric_group_identity(
                    client.spill.records, allow_disk_use=True, batch_size=1
                )
        else:

            async def run():
                async with AsyncMongoClient(
                    engine_factory(aggregation_spill_threshold=1),
                    mongodb_dialect=dialect,
                ) as client:
                    return await asyncio.to_thread(
                        numeric_group_identity,
                        _AsyncView(client.spill.records, asyncio.get_running_loop()),
                        allow_disk_use=True,
                        batch_size=1,
                    )

            actual = asyncio.run(run())
        assert _normalize(actual) == _normalize(expected)
        assert opened.call_count > 0


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("corpus", list(CORPORA))
def test_capture_identifies_native_server_and_current_witnesses(dialect, corpus):
    capture = _capture(dialect, corpus)
    assert capture["source"] == "real-mongodb"
    assert capture["targetDialect"] == dialect
    assert capture["runtime"]["fcv"] == {"version": dialect}
    assert capture["runtime"]["build_info"]["version"].startswith(dialect + ".")
    cases = CORPORA[corpus]
    assert set(capture["cases"]) == {name for name, _ in cases}
    registration = SEMANTIC_CORPORA[corpus]
    assert capture["corpus"]["path"] == (f"tests/differential/{registration.module}.py")
    assert (
        capture["corpus"].get("observationEncoding", "bson")
        == registration.observation_encoding
    )
    if registration.requires_standalone:
        assert capture["corpus"]["topology"] == "standalone"
        hello = capture["runtime"]["hello"]
        assert hello["isWritablePrimary"]
        assert not hello.get("setName")
        assert hello.get("msg") != "isdbgrid"
    assert len(capture["case_manifests"]) == len(cases)
    for manifest in capture["case_manifests"]:
        # Canonical EJSON decodes a Python int64 as bson.Int64. Compare BSON
        # bytes here, and exercise the original public Python inputs above.
        assert BSON.encode({"seed": manifest["seed_documents"]}) == BSON.encode(
            {"seed": registration.seed}
        )
    path = Path(__file__).parents[3] / capture["corpus"]["path"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == capture["corpus"]["sha256"]


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_direct_cursor_clone_retains_program_against_fresh_native_request(
    dialect, engine_factory, surface
):
    # PyMongo 4.19 clone drops let. The direct API promises to preserve options;
    # its reference is an equivalent fresh native find, not that SDK omission.
    seed = SEMANTIC_CORPORA["command_let"].seed
    expected = _capture(dialect, "command_let")["cases"]["semantic_command_let_cursors"]

    def exercise(collection):
        for name in ("literal_string", "literal_operator", "literal_ejson"):
            kind = "string" if name == "literal_string" else "object"
            cursor = collection.find(
                {"$expr": {"$eq": [{"$type": "$$x"}, kind]}},
                let=deepcopy(COMMAND_LET_VALID[name]),
                sort=[("_id", 1)],
            )
            # Both an unconsumed and a consumed source can be cloned.
            before = cursor.clone()
            list(cursor)
            after = cursor.clone()
            for clone in (before, after):
                observed = BSONObservation(seed).documents(list(clone))
                assert _normalize(observed) == _normalize(expected[name]["fresh"])

    if surface == "sync":
        with MongoClient(engine_factory(), mongodb_dialect=dialect) as client:
            collection = client.semantic_group.records
            collection.insert_many(deepcopy(seed))
            exercise(collection)
        return

    async def run():
        async with AsyncMongoClient(
            engine_factory(), mongodb_dialect=dialect
        ) as client:
            collection = client.semantic_group.records
            await collection.insert_many(deepcopy(seed))
            await asyncio.to_thread(
                exercise, _AsyncView(collection, asyncio.get_running_loop())
            )

    asyncio.run(run())


def test_corpus_registration_has_unique_case_names_and_capture_selectors():
    names = [name for corpus in SEMANTIC_CORPORA.values() for name, _ in corpus.cases]
    assert len(names) == len(set(names))
    selectors = [corpus.case_set for corpus in SEMANTIC_CORPORA.values()]
    assert len(selectors) == len(set(selectors))
    assert all(corpus.cases for corpus in SEMANTIC_CORPORA.values())


@pytest.mark.parametrize("precision", [6, 28, 34])
@pytest.mark.parametrize("rounding", [ROUND_DOWN, ROUND_UP])
@pytest.mark.parametrize(
    "corpus",
    [
        "numeric",
        "decimal",
        "decimal_edges",
        "decimal_advanced",
        "decimal_advanced_edges",
        "decimal_controls",
        "sum",
        "sum_stream",
        "average_routes",
        "overflow",
        "overflow_writes",
        "overflow_numeric_controls",
    ],
)
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_numeric_rules_are_independent_of_decimal_context(
    precision, rounding, corpus, engine_factory
):
    registration = SEMANTIC_CORPORA[corpus]
    capture = _capture("8.0", corpus)
    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        context.traps[Inexact] = True
        context.traps[FloatOperation] = True
        before_flags = context.flags.copy()
        for name, action in registration.cases:
            with MongoClient(engine_factory(), mongodb_dialect="8.0") as client:
                collection = client.semantic_numeric.records
                collection.insert_many(registration.seed)
                assert _normalize(action(collection)) == _normalize(
                    capture["cases"][name]
                )
        # SignalDict equality compares internal invalid-operation subflags too;
        # its public bool mapping can be identical after ConversionSyntax.
        # Preserve and compare every observable flag, including inherited ones.
        assert dict(context.flags) == before_flags


def test_observer_distinguishes_boolean_and_numeric_literals():
    assert _normalize({"v": True}) != _normalize({"v": 1})
    assert _normalize({"v": 1}) != _normalize({"v": 1.0})
    assert _normalize({}) != _normalize({"v": None})


def test_observer_compares_nan_values_without_losing_bson_type():
    assert _normalize({"v": [float("nan")]}) == _normalize({"v": [float("nan")]})
    assert _normalize({"v": float("nan")}) != _normalize({"v": "NaN"})
    assert _normalize({"v": float("nan")}) != _normalize({"v": Decimal128("NaN")})
    assert _normalize({"v": float("nan")}) != _normalize({"v": None})


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_nonexpiring_deadline_preserves_group(engine_factory, dialect):
    capture = _capture(dialect)
    expected = capture["cases"]["semantic_fix_group_route_boolean"]["facet"]
    with MongoClient(engine_factory(), mongodb_dialect=dialect) as client:
        collection = client.semantic_group.records
        collection.insert_many([{"_id": 1}, {"_id": 2}])
        pipeline = [
            {"$facet": {"result": [{"$group": {"_id": None, "value": {"$sum": True}}}]}}
        ]
        result = list(collection.aggregate(pipeline, max_time_ms=10000))
        assert _normalize(result) == _normalize(expected)


def test_absence_keeps_identity_through_copy_and_spill_serialization():
    assert deepcopy({"value": MISSING})["value"] is MISSING
    payload = pickle.dumps({"value": MISSING})
    restored = pickle.loads(payload)  # noqa: S301 - trusted payload created in this test
    assert restored["value"] is MISSING


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("threshold", [None, 2])
@pytest.mark.parametrize("dialect", DIALECTS)
def test_numeric_group_state_survives_actual_partition_spill(
    engine_factory, threshold, dialect, monkeypatch
):
    opened = []
    original = AggregationSpillPolicy.open_group_spool

    def record_spool(self, *args, **kwargs):
        opened.append(self.threshold)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(AggregationSpillPolicy, "open_group_spool", record_spool)
    engine = engine_factory(aggregation_spill_threshold=threshold)
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.semantic_sum_spill.records
        collection.insert_many(STREAM_SEED)
        cursor = collection.aggregate(
            STREAM_PIPELINE, allow_disk_use=True, batch_size=2
        )
        actual = list(cursor)
    expected = _capture(dialect, "sum_stream")["cases"]["semantic_sum_stream_groups"]
    assert bool(opened) == (threshold is not None)
    assert _normalize(actual) == _normalize(expected)


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("view", ["slice", "list", "copy", "pickle", "relocate"])
def test_group_average_metadata_survives_physical_pipeline_views(dialect, view):
    resolved = resolve_mongodb_dialect(dialect)
    prepared = prepare_pipeline([{"$unwind": "$items"}, GROUP], dialect=resolved)
    tail = prepared[-1:]
    expected_key = "prefix_unwind"
    if view == "list":
        tail = list(tail)
    elif view == "copy":
        tail = deepcopy(tail)
    elif view == "pickle":
        tail = pickle.loads(pickle.dumps(list(tail)))  # noqa: S301 - trusted local payload
    elif view == "relocate":
        tail = prepare_pipeline(tail, dialect=resolved, path=("new-logical-pipeline",))
        expected_key = "prefix_root"
    actual = apply_pipeline(AVERAGE_SEED, tail, dialect=resolved)
    expected = _capture(dialect, "average_routes")["cases"]["semantic_average_routes"][
        expected_key
    ]
    assert _normalize(DocumentCodec.to_public(actual)) == _normalize(expected)


@pytest.mark.parametrize("dialect", DIALECTS)
def test_provenance_execution_preserves_native_group_average(dialect):
    resolved = resolve_mongodb_dialect(dialect)
    actual = apply_pipeline_states(AVERAGE_SEED, [GROUP], dialect=resolved)
    expected = _capture(dialect, "average_routes")["cases"]["semantic_average_routes"][
        "prefix_root"
    ]
    assert _normalize(
        DocumentCodec.to_public([state.document for state in actual])
    ) == _normalize(expected)
