import unittest
from types import SimpleNamespace

from mongoeco.errors import OperationFailure
from mongoeco.wire._executor_handlers import WireSpecialCommandHandlers
from mongoeco.wire._executor_passthrough import _materialize_passthrough_result, execute_passthrough_command
from mongoeco.wire._executor_validation import _make_collection_name_validator
from mongoeco.wire.auth import WireAuthUser, WireAuthenticationService
from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.connections import WireConnectionContext
from mongoeco.wire.cursors import WireCursorStore
from mongoeco.wire.requests import WireRequestContext
from mongoeco.wire.sessions import WireSessionStore
from mongoeco.wire.surface import WireSurface


class _FakeDatabase:
    def __init__(self, result):
        self._result = result

    async def command(self, *_args, **_kwargs):
        return self._result


class _FakeClient:
    mongodb_dialect = None

    def __init__(self, result):
        self._result = result

    def get_database(self, _name):
        return _FakeDatabase(self._result)


class WireExecutorHelperCoverageTests(unittest.IsolatedAsyncioTestCase):
    async def test_connection_status_passthrough_requires_document_response(self):
        context = WireRequestContext(
            db_name="admin",
            command_name="connectionStatus",
            command_document={"connectionStatus": 1},
            raw_body={"connectionStatus": 1, "$db": "admin"},
            capability=resolve_wire_command_capability("connectionStatus"),
            connection=WireConnectionContext(connection_id=1),
        )

        with self.assertRaisesRegex(OperationFailure, "document response"):
            await execute_passthrough_command(
                context,
                client=_FakeClient(["bad"]),
                cursor_store=WireCursorStore(),
                auth=SimpleNamespace(require_authenticated=lambda *_args, **_kwargs: None),
            )

        with self.assertRaisesRegex(OperationFailure, "document response"):
            _materialize_passthrough_result({"find": "events"}, ["bad"], cursor_store=WireCursorStore())

    async def test_special_command_handlers_dispatch_authenticate(self):
        auth = WireAuthenticationService((WireAuthUser("ada", "pencil"),))
        handlers = WireSpecialCommandHandlers(
            client=_FakeClient({"ok": 1.0}),
            cursor_store=WireCursorStore(),
            session_store=WireSessionStore(),
            surface=WireSurface(),
            auth=auth,
        )
        connection = WireConnectionContext(connection_id=2)
        context = WireRequestContext(
            db_name="admin",
            command_name="authenticate",
            command_document={
                "authenticate": 1,
                "mechanism": "SCRAM-SHA-256",
                "user": "ada",
                "pwd": "pencil",
                "db": "admin",
            },
            raw_body={
                "authenticate": 1,
                "mechanism": "SCRAM-SHA-256",
                "user": "ada",
                "pwd": "pencil",
                "db": "admin",
                "$db": "admin",
            },
            capability=resolve_wire_command_capability("authenticate"),
            connection=connection,
        )

        self.assertEqual(await handlers.dispatch(context), {"ok": 1.0})
        self.assertEqual(connection.authenticated_users[0]["user"], "ada")

    def test_collection_name_validator_requires_non_empty_names(self):
        validator = _make_collection_name_validator("count")

        validator({}, {"count": "events"})
        with self.assertRaisesRegex(OperationFailure, "non-empty collection name"):
            validator({}, {"count": ""})
