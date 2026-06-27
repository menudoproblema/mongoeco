from __future__ import annotations

import unittest
from types import SimpleNamespace

from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession


class ActiveOperationTrackingTests(unittest.TestCase):
    def test_track_active_operation_rejects_foreign_session_before_begin(self) -> None:
        class EngineStub:
            def __init__(self) -> None:
                self.begin_called = False

            def _ensure_session_can_use_engine(self, session: ClientSession | None) -> None:
                raise InvalidOperation("foreign session")

            def _begin_active_operation(self, **kwargs: object) -> SimpleNamespace:
                self.begin_called = True
                return SimpleNamespace(opid="op")

        engine = EngineStub()

        with self.assertRaisesRegex(InvalidOperation, "foreign session"):
            with track_active_operation(
                engine,
                command_name="find",
                operation_type="query",
                namespace="db.coll",
                session=ClientSession(),
            ):
                pass

        self.assertFalse(engine.begin_called)

    def test_track_active_operation_rejects_unbound_session_for_engine_key_fallback(self) -> None:
        class EngineStub:
            def __init__(self) -> None:
                self.begin_called = False

            def _engine_key(self) -> object:
                return object()

            def _begin_active_operation(self, **kwargs: object) -> SimpleNamespace:
                self.begin_called = True
                return SimpleNamespace(opid="op")

        engine = EngineStub()

        with self.assertRaisesRegex(InvalidOperation, "This session was not created"):
            with track_active_operation(
                engine,
                command_name="find",
                operation_type="query",
                namespace="db.coll",
                session=ClientSession(),
            ):
                pass

        self.assertFalse(engine.begin_called)

