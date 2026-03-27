import unittest

from mongoeco.errors import ExecutionTimeout
from mongoeco.wire.executor import WireCommandExecutor
from mongoeco.wire.proxy import AsyncMongoEcoProxyServer


class WireProxyUnitTests(unittest.TestCase):
    def test_error_document_includes_catalog_metadata_and_labels(self):
        exc = ExecutionTimeout("took too long")

        document = WireCommandExecutor.error_document(exc)

        self.assertEqual(document["ok"], 0.0)
        self.assertEqual(document["errmsg"], "took too long")
        self.assertEqual(document["code"], 50)
        self.assertEqual(document["codeName"], "MaxTimeMSExpired")
        self.assertNotIn("errorLabels", document)

    def test_get_more_with_unknown_cursor_returns_empty_batch(self):
        proxy = AsyncMongoEcoProxyServer()

        response = proxy._cursor_store.get_more({"getMore": 99, "collection": "events"}, db_name="alpha")

        self.assertEqual(
            response,
            {
                "cursor": {
                    "id": 0,
                    "ns": "alpha.events",
                    "nextBatch": [],
                },
                "ok": 1.0,
            },
        )

    def test_cursor_store_splits_first_batch_and_serves_get_more(self):
        proxy = AsyncMongoEcoProxyServer()
        result = proxy._cursor_store.materialize_command_result(
            {"find": "events", "batchSize": 2},
            {
                "cursor": {
                    "id": 0,
                    "ns": "alpha.events",
                    "firstBatch": [{"seq": 1}, {"seq": 2}, {"seq": 3}],
                },
                "ok": 1.0,
            },
        )

        cursor_id = result["cursor"]["id"]
        self.assertNotEqual(cursor_id, 0)
        self.assertEqual(result["cursor"]["firstBatch"], [{"seq": 1}, {"seq": 2}])

        next_page = proxy._cursor_store.get_more(
            {"getMore": cursor_id, "collection": "events", "batchSize": 2},
            db_name="alpha",
        )
        self.assertEqual(next_page["cursor"]["id"], 0)
        self.assertEqual(next_page["cursor"]["nextBatch"], [{"seq": 3}])

    def test_kill_cursors_reports_killed_and_unknown_ids(self):
        proxy = AsyncMongoEcoProxyServer()
        result = proxy._cursor_store.materialize_command_result(
            {"find": "events", "batchSize": 1},
            {
                "cursor": {
                    "id": 0,
                    "ns": "alpha.events",
                    "firstBatch": [{"seq": 1}, {"seq": 2}],
                },
                "ok": 1.0,
            },
        )
        cursor_id = result["cursor"]["id"]

        response = proxy._cursor_store.kill_cursors(
            {"killCursors": "events", "cursors": [cursor_id, 999]}
        )

        self.assertEqual(response["cursorsKilled"], [cursor_id])
        self.assertEqual(response["cursorsUnknown"], [999])
