import unittest

from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import InvalidOperation


class WatchHelperTests(unittest.TestCase):
    def test_async_watch_rejects_explicit_sessions_across_client_database_and_collection(self):
        client = AsyncMongoClient(MemoryEngine())
        session = object()

        with self.assertRaisesRegex(InvalidOperation, "watch does not support explicit sessions"):
            client.watch(session=session)

        with self.assertRaisesRegex(InvalidOperation, "watch does not support explicit sessions"):
            client.get_database("db").watch(session=session)

        with self.assertRaisesRegex(InvalidOperation, "watch does not support explicit sessions"):
            client.get_database("db").get_collection("coll").watch(session=session)
