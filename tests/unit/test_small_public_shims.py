import asyncio
import unittest
from types import SimpleNamespace

from mongoeco.api._async._materialized_cursor import AsyncMaterializedCursor
from mongoeco.api._sync._materialized_cursor import MaterializedCursor
from mongoeco.api._sync.database_commands import DatabaseCommandService
from mongoeco.wire import __getattr__ as wire_getattr


class SmallPublicShimTests(unittest.TestCase):
    def test_async_materialized_cursor_iterates_and_rewinds(self):
        async def _loader() -> list[int]:
            return [1, 2, 3]

        async def _run() -> None:
            cursor = AsyncMaterializedCursor(_loader)
            self.assertEqual([item async for item in cursor], [1, 2, 3])
            self.assertFalse(cursor.alive)
            cursor.rewind()
            self.assertEqual(await cursor.first(), 1)

        asyncio.run(_run())

    def test_sync_materialized_cursor_iter_and_cached_first(self):
        class _AsyncCursor:
            alive = True

            async def to_list(self) -> list[int]:
                return [1, 2]

            async def first(self) -> int | None:
                return 1

            def rewind(self):
                return self

            def close(self) -> None:
                self.alive = False

        class _Client:
            @staticmethod
            def _run(awaitable):
                return asyncio.run(awaitable)

        cursor = MaterializedCursor(_Client(), _AsyncCursor())
        self.assertEqual(list(cursor), [1, 2])
        self.assertEqual(cursor.first(), 1)
        cursor.close()
        self.assertFalse(cursor.alive)

    def test_sync_database_command_service_delegates_parse_and_execute(self):
        async def _execute_document(command, **kwargs):
            return {"ok": 1, "command": command, "kwargs": kwargs}

        async_commands = SimpleNamespace(
            parse_raw_command=lambda command, **kwargs: ("parsed", command, kwargs),
            execute_document=_execute_document,
        )
        async_database = lambda: SimpleNamespace(_admin=SimpleNamespace(_commands=async_commands))
        client = SimpleNamespace(_run=lambda awaitable: asyncio.run(awaitable))
        admin = SimpleNamespace(_client=client, _async_database=async_database)

        service = DatabaseCommandService(admin)
        self.assertEqual(
            service.parse_raw_command({"ping": 1}, codec="x"),
            ("parsed", {"ping": 1}, {"codec": "x"}),
        )
        self.assertEqual(
            service.command({"ping": 1}),
            {"ok": 1, "command": {"ping": 1}, "kwargs": {"session": None}},
        )

    def test_wire_module_getattr_exports_known_symbols_and_rejects_unknown(self):
        self.assertEqual(wire_getattr("AsyncMongoEcoProxyServer").__name__, "AsyncMongoEcoProxyServer")
        self.assertEqual(wire_getattr("WireAuthUser").__name__, "WireAuthUser")
        with self.assertRaises(AttributeError):
            wire_getattr("missing")
