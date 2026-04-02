import unittest

from mongoeco.types import ExecutionLineageStep
from mongoeco.engines.profiling import EngineProfiler


class EngineProfilerTests(unittest.TestCase):
    def test_set_level_supports_status_query_and_validates_input(self):
        profiler = EngineProfiler("memory")

        current = profiler.set_level("db", -1)
        enabled = profiler.set_level("db", 2, slow_ms=0)

        self.assertEqual(current.previous_level, 0)
        self.assertEqual(current.slow_ms, 100)
        self.assertEqual(current.current_level, 0)
        self.assertEqual(current.entry_count, 0)
        self.assertFalse(current.namespace_visible)
        self.assertEqual(current.tracked_databases, 0)
        self.assertEqual(current.visible_namespaces, 0)
        self.assertEqual(enabled.previous_level, 0)
        self.assertEqual(enabled.slow_ms, 0)
        self.assertEqual(enabled.current_level, 2)
        self.assertEqual(enabled.entry_count, 0)
        self.assertTrue(enabled.namespace_visible)
        self.assertEqual(enabled.tracked_databases, 1)
        self.assertEqual(enabled.visible_namespaces, 1)

        with self.assertRaises(ValueError):
            profiler.set_level("db", 3)
        with self.assertRaises(ValueError):
            profiler.set_level("db", 1, slow_ms=-1)

    def test_record_respects_profile_levels_thresholds_and_visibility(self):
        profiler = EngineProfiler("sqlite")

        profiler.record(
            "db",
            op="query",
            namespace="db.users",
            command={"find": "users"},
            duration_micros=100,
        )
        self.assertEqual(profiler.count_entries("db"), 0)
        self.assertFalse(profiler.namespace_visible("db"))

        profiler.set_level("db", 1, slow_ms=5)
        profiler.record(
            "db",
            op="query",
            namespace="db.users",
            command={"find": "users"},
            duration_micros=4_000,
        )
        self.assertEqual(profiler.count_entries("db"), 0)

        profiler.record(
            "db",
            op="query",
            namespace="db.users",
            command={"find": "users"},
            duration_micros=6_000,
        )
        self.assertEqual(profiler.count_entries("db"), 1)
        self.assertTrue(profiler.namespace_visible("db"))

    def test_record_keeps_command_copy_and_serializes_lineage_and_error(self):
        profiler = EngineProfiler("memory")
        profiler.set_level("db", 2, slow_ms=0)
        command = {"find": "users", "filter": {"name": "Ada"}}

        profiler.record(
            "db",
            op="query",
            namespace="db.users",
            command=command,
            duration_micros=2_500,
            execution_lineage=(
                ExecutionLineageStep(runtime="sql", phase="scan", detail="pushdown"),
                ExecutionLineageStep(runtime="python", phase="project", detail="fallback"),
            ),
            fallback_reason="unsupported projection",
            ok=0.0,
            errmsg="boom",
        )
        command["filter"]["name"] = "Grace"

        entry = profiler.get_entry("db", 1)

        assert entry is not None
        self.assertEqual(entry["command"]["filter"]["name"], "Ada")
        self.assertEqual(entry["engine"], "memory")
        self.assertEqual(entry["executionLineage"][0]["runtime"], "sql")
        self.assertEqual(entry["fallbackReason"], "unsupported projection")
        self.assertEqual(entry["errmsg"], "boom")
        self.assertEqual(entry["ok"], 0.0)
        self.assertEqual(entry["micros"], 2500)
        self.assertEqual(entry["millis"], 2.5)

    def test_clear_removes_entries_but_not_settings(self):
        profiler = EngineProfiler("memory")
        profiler.set_level("db", 2, slow_ms=0)
        profiler.record(
            "db",
            op="insert",
            namespace="db.users",
            command={"insert": "users"},
            duration_micros=100,
        )

        profiler.clear("db")

        self.assertEqual(profiler.count_entries("db"), 0)
        self.assertEqual(profiler.get_settings("db").level, 2)
        self.assertTrue(profiler.namespace_visible("db"))

    def test_status_snapshot_summarizes_tracked_databases_and_entries(self):
        profiler = EngineProfiler("memory")
        profiler.set_level("alpha", 2, slow_ms=0)
        profiler.record(
            "alpha",
            op="query",
            namespace="alpha.users",
            command={"find": "users"},
            duration_micros=100,
        )
        profiler.set_level("beta", 0, slow_ms=100)

        self.assertEqual(
            profiler.status_snapshot(),
            {
                "tracked_databases": 2,
                "visible_namespaces": 1,
                "entry_count": 1,
            },
        )
