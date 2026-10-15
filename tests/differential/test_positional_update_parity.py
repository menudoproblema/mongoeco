"""Compare positional-update codes and stored state against a live MongoDB."""

import os
import unittest
import uuid

from itertools import product

from pymongo import MongoClient as PyMongoClient

from mongoeco import MongoClient

from tests.support import ENGINE_FACTORIES
from tests.support_positional_updates import (
    POSITIONAL_UPDATE_CASES,
    observe_positional_update,
)


class PositionalUpdateRealParityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        uri = os.getenv("MONGOECO_REAL_MONGODB_URI")
        if not uri:
            message = "MONGOECO_REAL_MONGODB_URI is not configured"
            raise unittest.SkipTest(message)
        cls.real_client = PyMongoClient(uri, serverSelectionTimeoutMS=3000)
        cls.addClassCleanup(cls.real_client.close)
        cls.target = os.getenv("MONGOECO_REAL_MONGODB_TARGET", "8.0")
        version = cls.real_client.server_info()["versionArray"]
        if version[:2] != [int(part) for part in cls.target.split(".")]:
            message = f"positional parity requires MongoDB {cls.target}, got {version}"
            raise AssertionError(message)
        cls.database_name = f"mongoeco_positional_{uuid.uuid4().hex}"
        cls.addClassCleanup(cls.real_client.drop_database, cls.database_name)

    def test_live_error_codes_bulk_errors_and_documents_match(self):
        real = self.real_client[self.database_name]
        for (engine, factory), profile in product(
            ENGINE_FACTORIES.items(), ("4.9", "4.19")
        ):
            with MongoClient(
                factory(), mongodb_dialect=self.target, pymongo_profile=profile
            ) as client:
                for index, case in enumerate(POSITIONAL_UPDATE_CASES):
                    with self.subTest(
                        engine=engine, profile=profile, case=case["name"]
                    ):
                        collection_name = (
                            f"{engine}_{profile.replace('.', '_')}_{index}"
                        )
                        native = observe_positional_update(real[collection_name], case)
                        actual = observe_positional_update(
                            client[self.database_name][collection_name], case
                        )
                        self.assertEqual(native, case["expected"])
                        self.assertEqual(actual, native)
