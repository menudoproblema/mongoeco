import copy
import importlib.util
import os
import unittest
import uuid
from collections.abc import Callable
from typing import Any

from mongoeco import MongoClient
from mongoeco.core.identity import canonical_document_id
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


def _normalize(value: Any) -> Any:
    if isinstance(value, tuple):
        return tuple(_normalize(item) for item in value)
    if isinstance(value, list):
        return tuple(_normalize(item) for item in value)
    return canonical_document_id(value)


class MongoDB7RealParityTests(unittest.TestCase):
    _real_client = None
    _mongo_client_class = None
    _uri = os.getenv("MONGOECO_REAL_MONGODB_URI")

    @classmethod
    def setUpClass(cls) -> None:
        if not cls._uri:
            raise unittest.SkipTest("MONGOECO_REAL_MONGODB_URI is not configured")
        if importlib.util.find_spec("pymongo") is None:
            raise unittest.SkipTest("pymongo is not installed; install mongoeco[mongodb7]")

        from pymongo import MongoClient as PyMongoClient

        cls._mongo_client_class = PyMongoClient
        try:
            cls._real_client = PyMongoClient(cls._uri, serverSelectionTimeoutMS=3000)
            cls._real_client.admin.command("ping")
            version_array = cls._real_client.server_info().get("versionArray", [])
        except Exception as exc:
            raise unittest.SkipTest(f"cannot connect to real MongoDB: {exc}") from exc

        if not (len(version_array) >= 2 and version_array[0] == 7 and version_array[1] == 0):
            raise unittest.SkipTest(f"requires MongoDB 7.0.x, got {version_array!r}")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls._real_client is not None:
            cls._real_client.close()

    def _assert_matches_real(
        self,
        seed_documents: list[dict[str, Any]],
        action: Callable[[Any], Any],
    ) -> None:
        for engine_name, engine_factory in (("memory", MemoryEngine), ("sqlite", SQLiteEngine)):
            with self.subTest(engine=engine_name):
                database_name = f"mongoeco_diff_{uuid.uuid4().hex}"
                collection_name = "cases"

                real_database = self._real_client[database_name]
                real_collection = real_database[collection_name]
                try:
                    for document in copy.deepcopy(seed_documents):
                        real_collection.insert_one(document)
                    real_result = action(real_collection)

                    with MongoClient(engine_factory()) as eco_client:
                        eco_collection = eco_client.get_database(database_name).get_collection(collection_name)
                        for document in copy.deepcopy(seed_documents):
                            eco_collection.insert_one(document)
                        eco_result = action(eco_collection)

                    self.assertEqual(_normalize(eco_result), _normalize(real_result))
                finally:
                    self._real_client.drop_database(database_name)

    def test_find_expr_compare_fields_matches_real_mongodb(self) -> None:
        self._assert_matches_real(
            [
                {"_id": "1", "spent": 12, "budget": 10},
                {"_id": "2", "spent": 8, "budget": 10},
            ],
            lambda collection: [
                document["_id"]
                for document in collection.find({"$expr": {"$gt": ["$spent", "$budget"]}}, sort=[("_id", 1)])
            ],
        )

    def test_find_subdocument_order_sensitive_equality_matches_real_mongodb(self) -> None:
        self._assert_matches_real(
            [
                {"_id": "ordered", "value": {"a": 1, "b": 2}},
                {"_id": "reordered", "value": {"b": 2, "a": 1}},
            ],
            lambda collection: [
                document["_id"]
                for document in collection.find({"value": {"a": 1, "b": 2}}, sort=[("_id", 1)])
            ],
        )

    def test_find_all_with_multiple_elem_match_matches_real_mongodb(self) -> None:
        self._assert_matches_real(
            [
                {
                    "_id": "1",
                    "items": [
                        {"kind": "a", "qty": 1},
                        {"kind": "b", "qty": 2},
                        {"kind": "b", "qty": 5},
                    ],
                },
                {"_id": "2", "items": [{"kind": "a", "qty": 1}, {"kind": "b", "qty": 2}]},
            ],
            lambda collection: [
                document["_id"]
                for document in collection.find(
                    {
                        "items": {
                            "$all": [
                                {"$elemMatch": {"kind": "a"}},
                                {"$elemMatch": {"kind": "b", "qty": {"$gte": 5}}},
                            ]
                        }
                    },
                    sort=[("_id", 1)],
                )
            ],
        )

    def test_update_add_to_set_document_order_sensitive_matches_real_mongodb(self) -> None:
        def action(collection: Any) -> tuple[int, int, dict[str, Any] | None]:
            result = collection.update_one(
                {"_id": "1"},
                {"$addToSet": {"items": {"qty": 1, "kind": "a"}}},
            )
            return (
                result.matched_count,
                result.modified_count,
                collection.find_one({"_id": "1"}),
            )

        self._assert_matches_real(
            [{"_id": "1", "items": [{"kind": "a", "qty": 1}]}],
            action,
        )

    def test_find_expr_truthiness_array_matches_real_mongodb(self) -> None:
        self._assert_matches_real(
            [
                {"_id": "array", "flag": []},
                {"_id": "zero", "flag": 0},
                {"_id": "false", "flag": False},
                {"_id": "string", "flag": ""},
            ],
            lambda collection: [
                document["_id"]
                for document in collection.find({"$expr": "$flag"}, sort=[("_id", 1)])
            ],
        )

    def test_aggregate_project_array_traversal_matches_real_mongodb(self) -> None:
        self._assert_matches_real(
            [
                {"_id": "1", "items": [{"kind": "a"}, {"kind": "b"}]},
                {"_id": "2", "items": [{"kind": "c"}]},
            ],
            lambda collection: list(
                collection.aggregate(
                    [
                        {"$project": {"_id": 1, "kinds": "$items.kind"}},
                        {"$sort": {"_id": 1}},
                    ]
                )
            ),
        )

    def test_aggregate_get_field_literal_name_matches_real_mongodb(self) -> None:
        self._assert_matches_real(
            [{"_id": "1", "a.b.c": 1, "$price": 2, "x..y": 3}],
            lambda collection: list(
                collection.aggregate(
                    [
                        {
                            "$project": {
                                "_id": 0,
                                "dotted": {"$getField": {"field": "a.b.c"}},
                                "dollar": {"$getField": {"field": {"$literal": "$price"}}},
                                "double_dot": {"$getField": {"field": "x..y"}},
                            }
                        }
                    ]
                )
            ),
        )
