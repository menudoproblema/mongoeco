import asyncio
import copy
import importlib.util
import json
import os
import unittest
import uuid

from typing import Any

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.cases import REAL_PARITY_CASES, RealParityCase
from tests.differential.semantic_corpora import SEMANTIC_CORPORA
from tests.support_bson_observation import bson_comparison_value


def _normalize(value: Any) -> Any:
    return bson_comparison_value(value)


def _decode_capture(text):
    from bson.json_util import loads  # noqa: PLC0415 - test-only BSON dependency

    document = loads(text)
    encoding = document.get("corpus", {}).get("observationEncoding", "bson")
    if encoding == "canonical-json":
        # Canonical write observations are JSON strings inside the BSON envelope.
        # This keeps ordinary counters distinct from literal EJSON type tags;
        # metadata/seed manifests still decode as native BSON.
        document["cases"] = {
            name: json.loads(observation)
            for name, observation in document["cases"].items()
        }
    elif encoding != "bson":
        message = f"unknown capture observation encoding: {encoding}"
        raise ValueError(message)
    return document


_SEMANTIC_CASE_CORPUS = {
    name: (corpus_name, corpus)
    for corpus_name, corpus in SEMANTIC_CORPORA.items()
    for name, _action in corpus.cases
}


def _assert_native_observation(actual, expected, *, corpus=None, name=None):
    """Compare supported behavior, retaining the explicit capped exclusion."""
    checker = unittest.TestCase()
    if corpus == "bson_catalog" and name == "semantic_catalog_create_options":
        actual, expected = copy.deepcopy(actual), copy.deepcopy(expected)
        for key in ("capped-size-string", "capped-max-zero"):
            observed, supported = actual.pop(key), expected.pop(key)
            checker.assertTrue(supported["ok"])
            checker.assertTrue(supported["aux_exists"])
            checker.assertEqual(
                observed,
                {
                    "ok": False,
                    "phase": "invoke",
                    "exception": "OperationFailure",
                    "code": 115,
                    "state": supported["state"],
                    "aux_exists": False,
                    "indexes": [],
                },
            )
    checker.assertEqual(_normalize(actual), _normalize(expected))


class MongoRealParityBase(unittest.TestCase):
    TARGET_VERSION: tuple[int, int] = (7, 0)
    _real_client = None
    _mongo_client_class = None
    _uri = os.getenv("MONGOECO_REAL_MONGODB_URI")

    @classmethod
    def setUpClass(cls) -> None:
        if not cls._uri:
            raise unittest.SkipTest("MONGOECO_REAL_MONGODB_URI is not configured")
        if importlib.util.find_spec("pymongo") is None:
            raise unittest.SkipTest(
                "pymongo is not installed; install mongoeco[mongodb-real]"
            )

        from pymongo import MongoClient as PyMongoClient

        cls._mongo_client_class = PyMongoClient
        try:
            cls._real_client = PyMongoClient(cls._uri, serverSelectionTimeoutMS=3000)
            cls._real_client.admin.command("ping")
            version_array = cls._real_client.server_info().get("versionArray", [])
        except Exception as exc:  # pragma: no cover - depends on external environment
            if cls._real_client is not None:
                cls._real_client.close()
                cls._real_client = None
            raise unittest.SkipTest(f"cannot connect to real MongoDB: {exc}") from exc

        expected_major, expected_minor = cls.TARGET_VERSION
        if not (
            len(version_array) >= 2
            and version_array[0] == expected_major
            and version_array[1] == expected_minor
        ):
            cls._real_client.close()
            cls._real_client = None
            raise unittest.SkipTest(
                f"requires MongoDB {expected_major}.{expected_minor}.x, got {version_array!r}"
            )

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._real_client is not None:
            cls._real_client.close()
            cls._real_client = None

    def _assert_matches_real_case(self, case: RealParityCase) -> None:
        for engine_name, engine_factory in (
            ("memory", MemoryEngine),
            ("sqlite", SQLiteEngine),
        ):
            with self.subTest(engine=engine_name, case=case.name):
                database_name = f"mongoeco_diff_{uuid.uuid4().hex}"
                collection_name = "cases"

                real_database = self._real_client[database_name]
                real_collection = real_database[collection_name]
                try:
                    for document in copy.deepcopy(case.seed_documents):
                        real_collection.insert_one(document)
                    real_result = case.action(real_collection)

                    eco_result = self._observe_eco_case(
                        case, engine_factory, database_name, collection_name
                    )
                    corpus_name, _corpus = _SEMANTIC_CASE_CORPUS.get(
                        case.name, (None, None)
                    )
                    _assert_native_observation(
                        eco_result, real_result, corpus=corpus_name, name=case.name
                    )
                finally:
                    self._real_client.drop_database(database_name)

    def _observe_eco_case(self, case, engine_factory, database_name, collection_name):
        _corpus_name, corpus = _SEMANTIC_CASE_CORPUS.get(case.name, (None, None))
        if corpus is not None and "sync" not in corpus.surfaces:
            if "wire-sync" not in corpus.surfaces:
                message = f"no supported synchronous parity surface for {case.name}"
                raise ValueError(message)

            async def observe_wire():
                async with AsyncMongoEcoProxyServer(
                    engine=engine_factory(), mongodb_dialect=self._target_dialect_key()
                ) as proxy:

                    def observe():
                        uri = f"mongodb://{proxy.address.host}:{proxy.address.port}"
                        with self._mongo_client_class(
                            uri, serverSelectionTimeoutMS=3000
                        ) as client:
                            collection = client[database_name][collection_name]
                            collection.insert_many(copy.deepcopy(case.seed_documents))
                            return case.action(collection)

                    return await asyncio.to_thread(observe)

            return asyncio.run(observe_wire())

        with MongoClient(
            engine_factory(), mongodb_dialect=self._target_dialect_key()
        ) as client:
            collection = client.get_database(database_name).get_collection(
                collection_name
            )
            for document in copy.deepcopy(case.seed_documents):
                collection.insert_one(document)
            return case.action(collection)

    def _target_dialect_key(self) -> str:
        major, minor = self.TARGET_VERSION
        return f"{major}.{minor}"


def _make_case_test(case: RealParityCase):
    def _test(self: MongoRealParityBase) -> None:
        if not case.supports(self.TARGET_VERSION):
            self.skipTest(
                f"case {case.name!r} requires at least MongoDB {case.min_version[0]}.{case.min_version[1]}"
            )
        self._assert_matches_real_case(case)

    _test.__name__ = f"test_{case.name}_matches_real_mongodb"
    return _test


for _case in REAL_PARITY_CASES:
    setattr(
        MongoRealParityBase,
        f"test_{_case.name}_matches_real_mongodb",
        _make_case_test(_case),
    )
