import asyncio
from types import SimpleNamespace
import unittest

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api._async._collection_runtime import CollectionRuntimeCoordinator
from mongoeco.engines.results import DeleteOutcome, MutationOutcome
from mongoeco.session import ClientSession
from mongoeco.types import DeleteResult, UpdateResult


class CollectionRuntimeCoordinatorTests(unittest.TestCase):
    def test_document_by_id_crosses_adapter_with_operation_context(self):
        class EngineStub:
            async def get_document(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs
                return {'_id': args[2]}

        engine = EngineStub()
        collection = AsyncCollection(engine, 'db', 'coll')

        result = asyncio.run(collection._runtime.document_by_id('value'))

        self.assertEqual(result, {'_id': 'value'})
        self.assertEqual(engine.kwargs['context'], None)

    def test_profile_operation_tolerates_profiler_and_planner_failures(self):
        class EngineStub:
            def __init__(self):
                self.records = []

            def _profile_is_active(self, *_args, **_kwargs):
                raise RuntimeError('profile state failed')

            async def plan_find_execution(self, *_args, **_kwargs):
                raise RuntimeError('planner failed')

            def _record_profile_event(self, *args, **kwargs):
                self.records.append((args, kwargs))

        engine = EngineStub()
        collection = AsyncCollection(engine, 'db', 'coll')
        operation = collection.find({'kind': 'event'})._base_operation()

        asyncio.run(
            collection._runtime.profile_operation(
                op='query',
                command_factory=lambda: {'find': 'coll'},
                duration_ns=2_000,
                operation=operation,
            )
        )

        self.assertEqual(len(engine.records), 1)
        self.assertEqual(engine.records[0][1]['command'], {'find': 'coll'})
        self.assertEqual(engine.records[0][1]['execution_lineage'], ())

    def test_profile_operation_inactive_and_recorder_failures_are_nonfatal(self):
        class EngineStub:
            def _profile_is_active(self, *_args, **_kwargs):
                return False

            def _record_profile_event(self, *_args, **_kwargs):
                raise RuntimeError('recorder failed')

        collection = AsyncCollection(EngineStub(), 'db', 'coll')

        asyncio.run(
            collection._runtime.profile_operation(
                op='query',
                duration_ns=1,
                errmsg='failed',
            )
        )

    def test_profile_operation_skips_profile_namespace_and_missing_recorder(self):
        for name in ('system.profile', 'ordinary'):
            collection = AsyncCollection(object(), 'db', name)
            asyncio.run(
                collection._runtime.profile_operation(
                    op='query',
                    duration_ns=1,
                )
            )

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

        def _build_cursor(
            operation,
            *,
            session=None,
            apply_codec_options=True,
        ):
            calls.append(
                (operation.filter_spec, session, apply_codec_options)
            )
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
        self.assertEqual(
            calls,
            [({"name": "Ada"}, "session-token", False)],
        )

    def test_publish_change_event_is_noop_without_hub_and_delegates_with_hub(self):
        class EngineStub:
            pass

        AsyncCollection(EngineStub(), "db", "coll")._runtime.publish_change_event(
            operation_type="insert",
            document_key={"_id": "noop"},
        )

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

    def test_publish_change_event_returns_early_when_collection_has_no_change_hub(self):
        collection = SimpleNamespace(
            _change_hub=None,
            _db_name="db",
            _collection_name="coll",
        )

        CollectionRuntimeCoordinator(collection).publish_change_event(
            operation_type="update",
            document_key={"_id": "1"},
        )

    def test_change_events_queue_until_commit_and_discard_on_abort(self):
        class Hub:
            def __init__(self):
                self.events = []
                self.gaps = 0

            def should_publish_events(self):
                return True

            def publish(self, **payload):
                self.events.append(payload)

            def mark_gap(self):
                self.gaps += 1

        hub = Hub()
        collection = AsyncCollection(object(), 'db', 'coll', change_hub=hub)
        session = ClientSession()

        session.start_transaction()
        collection._runtime.publish_change_event(
            operation_type='insert',
            document_key={'_id': 1},
            full_document={'_id': 1},
            update_description={'updatedFields': {}},
            session=session,
        )
        self.assertEqual(hub.events, [])
        session.commit_transaction()
        self.assertEqual(len(hub.events), 1)

        session.start_transaction()
        collection._runtime.publish_change_event(
            operation_type='delete',
            document_key={'_id': 1},
            session=session,
        )
        session.abort_transaction()
        self.assertEqual(len(hub.events), 1)

    def test_change_event_gap_and_publish_failure_are_observable(self):
        failures = []
        hub = SimpleNamespace(
            should_publish_events=lambda: False,
            mark_gap=lambda: failures.append('gap'),
            publish=lambda **_payload: (_ for _ in ()).throw(
                RuntimeError('publish failed')
            ),
            mark_publish_failure=failures.append,
        )
        collection = AsyncCollection(object(), 'db', 'coll', change_hub=hub)

        collection._runtime.publish_change_event(
            operation_type='insert',
            document_key={'_id': 1},
        )
        collection._runtime._publish_change_payload(
            {'operation_type': 'insert'}
        )

        self.assertEqual(failures[0], 'gap')
        self.assertIsInstance(failures[1], RuntimeError)

    def test_change_event_helpers_cover_noop_and_captured_outcomes(self):
        events = []
        hub = SimpleNamespace(
            should_publish_events=lambda: True,
            publish=lambda **payload: events.append(payload),
            mark_gap=lambda: events.append('gap'),
        )
        collection = AsyncCollection(object(), 'db', 'coll', change_hub=hub)
        runtime = collection._runtime

        self.assertTrue(runtime.should_publish_change_events())
        runtime.mark_change_event_gap()
        runtime._publish_captured_update_event(
            MutationOutcome(result=UpdateResult(0, 0)),
            matched_operation_type='update',
            session=None,
        )
        runtime._publish_captured_update_event(
            MutationOutcome(
                result=UpdateResult(0, 0, upserted_id='new'),
                after_document={'_id': 'new'},
            ),
            matched_operation_type='update',
            session=None,
        )
        runtime._publish_captured_delete_event(
            DeleteOutcome(result=DeleteResult(0)),
            session=None,
        )
        runtime._publish_captured_delete_event(
            DeleteOutcome(
                result=DeleteResult(1),
                deleted_document={'_id': 'new'},
            ),
            session=None,
        )

        self.assertEqual(events[0], 'gap')
        self.assertEqual(events[1]['operation_type'], 'insert')
        self.assertEqual(events[2]['operation_type'], 'delete')
