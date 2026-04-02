import asyncio
from types import SimpleNamespace
import unittest

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.session import ClientSession


class CollectionRuntimeCoordinatorTests(unittest.TestCase):
    def test_record_operation_metadata_updates_session_and_engine(self):
        class EngineStub:
            def __init__(self):
                self.calls = []

            def _record_operation_metadata(self, session, **kwargs):
                self.calls.append((session, kwargs))

        engine = EngineStub()
        collection = AsyncCollection(engine, "db", "coll")
        session = ClientSession()

        collection._runtime.record_operation_metadata(
            operation="find",
            comment="trace",
            max_time_ms=5,
            hint=[("_id", 1)],
            session=session,
        )

        self.assertEqual(len(engine.calls), 1)
        self.assertEqual(engine.calls[0][0], session)
        self.assertEqual(
            engine.calls[0][1],
            {
                "operation": "find",
                "comment": "trace",
                "max_time_ms": 5,
                "hint": [("_id", 1)],
            },
        )
        self.assertIsNotNone(session.operation_time)
        self.assertIsNotNone(session.cluster_time)

    def test_select_first_document_uses_collection_build_cursor(self):
        class EngineStub:
            pass

        class CursorStub:
            async def first(self):
                return {"_id": "selected"}

        collection = AsyncCollection(EngineStub(), "db", "coll")
        calls = []

        def _build_cursor(operation, *, session=None):
            calls.append((operation.filter_spec, session))
            return CursorStub()

        collection._build_cursor = _build_cursor  # type: ignore[method-assign]

        result = asyncio.run(
            collection._runtime.select_first_document(
                {"name": "Ada"},
                comment="trace",
                session="session-token",
            )
        )

        self.assertEqual(result, {"_id": "selected"})
        self.assertEqual(calls, [({"name": "Ada"}, "session-token")])

    def test_publish_change_event_is_noop_without_hub_and_delegates_with_hub(self):
        class EngineStub:
            pass

        hub_calls = []
        hub = SimpleNamespace(publish=lambda **payload: hub_calls.append(payload))
        collection = AsyncCollection(EngineStub(), "db", "coll", change_hub=hub)

        collection._runtime.publish_change_event(
            operation_type="insert",
            document_key={"_id": "1"},
            full_document={"_id": "1", "name": "Ada"},
        )

        self.assertEqual(
            hub_calls,
            [
                {
                    "operation_type": "insert",
                    "db_name": "db",
                    "coll_name": "coll",
                    "document_key": {"_id": "1"},
                    "full_document": {"_id": "1", "name": "Ada"},
                    "update_description": None,
                }
            ],
        )

