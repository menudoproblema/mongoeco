import asyncio
import unittest
from unittest.mock import patch

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.cxp import MONGODB_CATALOG
from mongoeco.cxp.driver_telemetry import (
    _classify_command,
    _normalize_command_name,
    _resolve_namespace,
    _resolve_search_operator,
    _resolve_vector_similarity,
    _resolve_write_operation_name,
)
from mongoeco.cxp.telemetry import DriverTelemetryProjector, TelemetryBuffer
from mongoeco.driver.monitoring import (
    CommandFailedEvent,
    CommandStartedEvent,
    CommandSucceededEvent,
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    DriverMonitor,
    ServerSelectedEvent,
)


class DriverTelemetryProjectorTests(unittest.TestCase):
    def test_constructor_validates_buffer_provider_id_and_exposes_provider_id(self) -> None:
        buffer = TelemetryBuffer("other-provider")
        with self.assertRaisesRegex(ValueError, "provider_id does not match"):
            DriverTelemetryProjector(
                provider_id="mongoeco-driver",
                buffer=buffer,
            )

        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        self.assertEqual(projector.provider_id, "mongoeco-driver")

    def test_successful_read_command_projects_canonical_operation_telemetry(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")

        projector.handle_driver_event(
            CommandStartedEvent(
                database="demo",
                command_name="find",
                command={"find": "items", "filter": {"_id": 1}},
                server_address="db1:27017",
                connection_id="conn-1",
                attempt_number=1,
                read_only=True,
                request_id="req-read-1",
            )
        )
        projector.handle_driver_event(
            CommandSucceededEvent(
                database="demo",
                command_name="find",
                reply={"ok": 1.0, "cursor": {"id": 0}},
                server_address="db1:27017",
                connection_id="conn-1",
                attempt_number=1,
                duration_ms=12.5,
                request_id="req-read-1",
            )
        )

        snapshot = projector.snapshot()

        self.assertEqual(snapshot.provider_id, "mongoeco-driver")
        self.assertEqual(len(snapshot.spans), 1)
        self.assertEqual(len(snapshot.metrics), 1)
        self.assertEqual(len(snapshot.events), 1)
        self.assertEqual(snapshot.spans[0].trace_id, "req-read-1")
        self.assertEqual(snapshot.spans[0].name, "db.client.operation")
        self.assertEqual(snapshot.spans[0].attributes["db.operation.name"], "find")
        self.assertEqual(snapshot.spans[0].attributes["db.namespace.name"], "demo.items")
        self.assertEqual(
            snapshot.metrics[0].labels["db.operation.outcome"],
            "succeeded",
        )
        self.assertEqual(
            snapshot.events[0].payload["db.operation.outcome"],
            "succeeded",
        )
        self.assertTrue(
            MONGODB_CATALOG.is_telemetry_snapshot_compliant(snapshot, ("read",))
        )

    def test_failed_write_command_projects_canonical_operation_telemetry(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")

        projector.handle_driver_event(
            CommandStartedEvent(
                database="demo",
                command_name="update",
                command={
                    "update": "items",
                    "updates": [{"q": {"_id": 1}, "u": {"$set": {"name": "Ada"}}, "multi": False}],
                },
                server_address="db1:27017",
                connection_id="conn-2",
                attempt_number=1,
                read_only=False,
                request_id="req-write-1",
            )
        )
        projector.handle_driver_event(
            CommandFailedEvent(
                database="demo",
                command_name="update",
                failure="write failed",
                server_address="db1:27017",
                connection_id="conn-2",
                attempt_number=1,
                duration_ms=7.25,
                retryable=False,
                request_id="req-write-1",
            )
        )

        snapshot = projector.snapshot()

        self.assertEqual(snapshot.events[0].severity, "error")
        self.assertEqual(
            snapshot.spans[0].attributes["db.operation.name"],
            "update_one",
        )
        self.assertEqual(
            snapshot.events[0].payload["db.operation.outcome"],
            "failed",
        )
        self.assertTrue(
            MONGODB_CATALOG.is_telemetry_snapshot_compliant(snapshot, ("write",))
        )

    def test_aggregate_search_and_vector_commands_project_specialized_telemetry(self) -> None:
        cases = (
            (
                "aggregate",
                {"aggregate": "items", "pipeline": [{"$match": {"kind": "doc"}}, {"$limit": 5}]},
                "db.client.aggregate",
                "db.client.aggregate.duration",
                "db.client.aggregate.completed",
                ("aggregation",),
                {"db.pipeline.stage.count": 2},
            ),
            (
                "search",
                {
                    "aggregate": "items",
                    "pipeline": [{"$search": {"index": "search", "compound": {"must": []}}}],
                },
                "db.client.search",
                "db.client.search.duration",
                "db.client.search.completed",
                ("search",),
                {
                    "db.pipeline.stage.name": "$search",
                    "db.search.operator": "compound",
                },
            ),
            (
                "vector",
                {
                    "aggregate": "items",
                    "pipeline": [
                        {
                            "$vectorSearch": {
                                "index": "vec",
                                "path": "embedding",
                                "queryVector": [0.1, 0.2],
                                "similarity": "cosine",
                            }
                        }
                    ],
                },
                "db.client.vector_search",
                "db.client.vector_search.duration",
                "db.client.vector_search.completed",
                ("vector_search",),
                {
                    "db.pipeline.stage.name": "$vectorSearch",
                    "db.vector_search.similarity": "cosine",
                },
            ),
        )

        for label, command, span_name, metric_name, event_type, capabilities, required in cases:
            with self.subTest(label=label):
                projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
                projector.handle_driver_event(
                    CommandStartedEvent(
                        database="demo",
                        command_name="aggregate",
                        command=command,
                        server_address="db1:27017",
                        connection_id="conn-3",
                        attempt_number=1,
                        read_only=True,
                        request_id=f"req-{label}",
                    )
                )
                projector.handle_driver_event(
                    CommandSucceededEvent(
                        database="demo",
                        command_name="aggregate",
                        reply={"ok": 1.0, "cursor": {"id": 0}},
                        server_address="db1:27017",
                        connection_id="conn-3",
                        attempt_number=1,
                        duration_ms=9.0,
                        request_id=f"req-{label}",
                    )
                )

                snapshot = projector.snapshot()

                self.assertEqual(snapshot.spans[0].name, span_name)
                self.assertEqual(snapshot.metrics[0].name, metric_name)
                self.assertEqual(snapshot.events[0].event_type, event_type)
                for key, value in required.items():
                    self.assertEqual(snapshot.spans[0].attributes[key], value)
                    self.assertEqual(snapshot.events[0].payload[key], value)
                self.assertTrue(
                    MONGODB_CATALOG.is_telemetry_snapshot_compliant(
                        snapshot,
                        capabilities,
                    )
                )

    def test_connection_lifecycle_events_do_not_emit_canonical_telemetry(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        projector.handle_driver_event(
            ServerSelectedEvent(
                database="demo",
                command_name="find",
                server_address="db1:27017",
                attempt_number=1,
                read_only=True,
                request_id="req-conn-1",
            )
        )
        projector.handle_driver_event(
            ConnectionCheckedOutEvent(
                database="demo",
                command_name="find",
                server_address="db1:27017",
                connection_id="conn-4",
                attempt_number=1,
                request_id="req-conn-1",
            )
        )
        projector.handle_driver_event(
            ConnectionCheckedInEvent(
                database="demo",
                command_name="find",
                server_address="db1:27017",
                connection_id="conn-4",
                attempt_number=1,
                request_id="req-conn-1",
            )
        )

        snapshot = projector.snapshot()

        self.assertEqual(snapshot.events, ())
        self.assertEqual(snapshot.metrics, ())
        self.assertEqual(snapshot.spans, ())

    def test_missing_required_search_or_vector_fields_skip_canonical_projection(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        projector.handle_driver_event(
            CommandStartedEvent(
                database="demo",
                command_name="aggregate",
                command={"aggregate": "items", "pipeline": [{"$search": {"index": "search"}}]},
                server_address="db1:27017",
                connection_id="conn-5",
                attempt_number=1,
                read_only=True,
                request_id="req-search-missing",
            )
        )
        projector.handle_driver_event(
            CommandSucceededEvent(
                database="demo",
                command_name="aggregate",
                reply={"ok": 1.0},
                server_address="db1:27017",
                connection_id="conn-5",
                attempt_number=1,
                duration_ms=5.0,
                request_id="req-search-missing",
            )
        )

        self.assertEqual(projector.snapshot().spans, ())

        projector.handle_driver_event(
            CommandStartedEvent(
                database="demo",
                command_name="aggregate",
                command={
                    "aggregate": "items",
                    "pipeline": [{"$vectorSearch": {"index": "vec", "path": "embedding"}}],
                },
                server_address="db1:27017",
                connection_id="conn-6",
                attempt_number=1,
                read_only=True,
                request_id="req-vector-missing",
            )
        )
        projector.handle_driver_event(
            CommandSucceededEvent(
                database="demo",
                command_name="aggregate",
                reply={"ok": 1.0},
                server_address="db1:27017",
                connection_id="conn-6",
                attempt_number=1,
                duration_ms=5.0,
                request_id="req-vector-missing",
            )
        )

        self.assertEqual(projector.snapshot().spans, ())

    def test_attach_detach_and_clear_work_with_driver_monitors_and_clients(self) -> None:
        monitor = DriverMonitor()
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        projector.attach(monitor)
        projector.attach(monitor)
        monitor.emit(
            CommandStartedEvent(
                database="demo",
                command_name="find",
                command={"find": "items", "filter": {}},
                server_address="db1:27017",
                connection_id="conn-7",
                attempt_number=1,
                read_only=True,
                request_id="req-monitor-1",
            )
        )
        projector.clear()
        projector.detach(monitor)
        monitor.emit(
            CommandSucceededEvent(
                database="demo",
                command_name="find",
                reply={"ok": 1.0},
                server_address="db1:27017",
                connection_id="conn-7",
                attempt_number=1,
                duration_ms=1.0,
                request_id="req-monitor-1",
            )
        )
        self.assertEqual(projector.snapshot().spans, ())

        async def _exercise_async() -> None:
            client = AsyncMongoClient(uri="mongodb://db1:27017/")
            try:
                async_projector = DriverTelemetryProjector(provider_id="async-client")
                async_projector.attach(client.driver_monitor)
                result = await client.execute_driver_command(
                    "demo",
                    "insert",
                    {"insert": "items", "documents": [{"_id": 1, "name": "Ada"}]},
                    read_only=False,
                )
                self.assertTrue(result.outcome.ok)
                snapshot = async_projector.snapshot()
                self.assertEqual(snapshot.provider_id, "async-client")
                self.assertTrue(
                    MONGODB_CATALOG.is_telemetry_snapshot_compliant(snapshot, ("write",))
                )
            finally:
                await client.close()

        asyncio.run(_exercise_async())

        client = MongoClient(uri="mongodb://db1:27017/")
        try:
            sync_projector = DriverTelemetryProjector(provider_id="sync-client")
            sync_projector.attach(client.driver_monitor)
            result = client.execute_driver_command(
                "demo",
                "insert",
                {"insert": "items", "documents": [{"_id": 1, "name": "Ada"}]},
                read_only=False,
            )
            self.assertTrue(result.outcome.ok)
            snapshot = sync_projector.snapshot()
            self.assertEqual(snapshot.provider_id, "sync-client")
            self.assertTrue(
                MONGODB_CATALOG.is_telemetry_snapshot_compliant(snapshot, ("write",))
            )
        finally:
            client.close()

    def test_projector_ignores_started_and_finished_events_without_request_id(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        projector.handle_driver_event(
            CommandStartedEvent(
                database="demo",
                command_name="find",
                command={"find": "items", "filter": {}},
                server_address="db1:27017",
                connection_id="conn-8",
                attempt_number=1,
                read_only=True,
            )
        )
        projector.handle_driver_event(
            CommandSucceededEvent(
                database="demo",
                command_name="find",
                reply={"ok": 1.0},
                server_address="db1:27017",
                connection_id="conn-8",
                attempt_number=1,
                duration_ms=2.0,
            )
        )
        self.assertEqual(projector.snapshot().spans, ())

    def test_finished_event_without_pending_started_event_is_ignored(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        projector.handle_driver_event(
            CommandSucceededEvent(
                database="demo",
                command_name="find",
                reply={"ok": 1.0},
                server_address="db1:27017",
                connection_id="conn-9",
                attempt_number=1,
                duration_ms=2.0,
                request_id="missing-start",
            )
        )
        self.assertEqual(projector.snapshot().events, ())

    def test_finished_event_uses_non_decreasing_end_time(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        projector.handle_driver_event(
            CommandStartedEvent(
                database="demo",
                command_name="find",
                command={"find": "items", "filter": {}},
                server_address="db1:27017",
                connection_id="conn-10",
                attempt_number=1,
                read_only=True,
                request_id="req-time-1",
            )
        )
        pending = projector._pending["req-time-1"]
        with patch("mongoeco.cxp.driver_telemetry.time.time", return_value=pending.start_time - 1):
            projector.handle_driver_event(
                CommandSucceededEvent(
                    database="demo",
                    command_name="find",
                    reply={"ok": 1.0},
                    server_address="db1:27017",
                    connection_id="conn-10",
                    attempt_number=1,
                    duration_ms=2.0,
                    request_id="req-time-1",
                )
            )
        snapshot = projector.snapshot()
        self.assertEqual(snapshot.spans[0].start_time, snapshot.spans[0].end_time)

    def test_classification_helpers_cover_unknown_and_fallback_paths(self) -> None:
        self.assertEqual(_normalize_command_name("find-And"), "find_and")
        self.assertIsNone(_resolve_namespace("demo", "find", {"filter": {}}))
        self.assertIsNone(_resolve_search_operator("bad"))
        self.assertIsNone(_resolve_vector_similarity("bad"))
        self.assertEqual(
            _resolve_write_operation_name("insert", {"documents": "bad"}),
            "insert",
        )
        self.assertEqual(
            _resolve_write_operation_name("update", {"updates": [{}, {}]}),
            "update",
        )
        self.assertEqual(
            _resolve_write_operation_name("delete", {"deletes": [{}, {}]}),
            "delete",
        )
        self.assertEqual(
            _resolve_write_operation_name(
                "delete",
                {"deletes": [{"q": {"_id": 1}, "limit": 0}]},
            ),
            "delete_many",
        )
        self.assertEqual(
            _resolve_write_operation_name("bulkwrite", {}),
            "bulk_write",
        )
        self.assertEqual(
            _resolve_write_operation_name(
                "findandmodify",
                {"remove": True},
            ),
            "delete_one",
        )
        self.assertEqual(
            _resolve_write_operation_name(
                "findandmodify",
                {"update": {"name": "Ada"}},
            ),
            "replace_one",
        )
        self.assertEqual(
            _resolve_write_operation_name(
                "findandmodify",
                {"update": {"$set": {"name": "Ada"}}},
            ),
            "update_one",
        )
        self.assertEqual(
            _resolve_write_operation_name(
                "findandmodify",
                {},
            ),
            "findAndModify",
        )
        self.assertIsNone(_resolve_write_operation_name("unknown", {}))

    def test_classify_command_covers_unknown_and_aggregate_edge_paths(self) -> None:
        unknown = CommandStartedEvent(
            database="demo",
            command_name="ping",
            command={"ping": 1},
            server_address="db1:27017",
            connection_id="conn-11",
            attempt_number=1,
            read_only=True,
            request_id="req-unknown",
        )
        self.assertIsNone(_classify_command(unknown))

        missing_namespace = CommandStartedEvent(
            database="demo",
            command_name="find",
            command={"filter": {}},
            server_address="db1:27017",
            connection_id="conn-12",
            attempt_number=1,
            read_only=True,
            request_id="req-no-namespace",
        )
        self.assertIsNone(_classify_command(missing_namespace))

        aggregate_missing_namespace = CommandStartedEvent(
            database="demo",
            command_name="aggregate",
            command={"pipeline": []},
            server_address="db1:27017",
            connection_id="conn-13",
            attempt_number=1,
            read_only=True,
            request_id="req-agg-no-namespace",
        )
        self.assertIsNone(_classify_command(aggregate_missing_namespace))

        aggregate_missing_pipeline = CommandStartedEvent(
            database="demo",
            command_name="aggregate",
            command={"aggregate": "items"},
            server_address="db1:27017",
            connection_id="conn-14",
            attempt_number=1,
            read_only=True,
            request_id="req-agg-no-pipeline",
        )
        self.assertIsNone(_classify_command(aggregate_missing_pipeline))

    def test_detach_tolerates_unknown_monitor(self) -> None:
        projector = DriverTelemetryProjector(provider_id="mongoeco-driver")
        attached_monitor = DriverMonitor()
        projector.attach(attached_monitor)
        projector.detach(DriverMonitor())
        projector.detach(attached_monitor)
