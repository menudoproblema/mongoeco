from __future__ import annotations

import unittest

from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.session_guards import ensure_session_can_use_engine


class SessionGuardTests(unittest.TestCase):
    def test_none_session_is_accepted_without_touching_engine(self) -> None:
        class EngineStub:
            def _ensure_session_can_use_engine(self, session: ClientSession | None) -> None:
                raise AssertionError("guard should not be called")

        ensure_session_can_use_engine(EngineStub(), None)

    def test_closed_session_fails_before_engine_ownership_check(self) -> None:
        class EngineStub:
            def _ensure_session_can_use_engine(self, session: ClientSession | None) -> None:
                raise InvalidOperation("ownership check should not run")

        session = ClientSession()
        session.close()

        with self.assertRaisesRegex(InvalidOperation, "sesion ya esta cerrada"):
            ensure_session_can_use_engine(EngineStub(), session)

    def test_engine_guard_is_delegated_before_engine_key_fallback(self) -> None:
        class EngineStub:
            def __init__(self) -> None:
                self.guarded_session: ClientSession | None = None

            def _ensure_session_can_use_engine(self, session: ClientSession | None) -> None:
                self.guarded_session = session

            def _engine_key(self) -> str:
                raise AssertionError("fallback should not run")

        session = ClientSession()
        engine = EngineStub()

        ensure_session_can_use_engine(engine, session)

        self.assertIs(engine.guarded_session, session)

    def test_engine_key_fallback_rejects_unbound_session(self) -> None:
        class EngineStub:
            def _engine_key(self) -> str:
                return "engine:key"

        with self.assertRaisesRegex(InvalidOperation, "This session was not created"):
            ensure_session_can_use_engine(EngineStub(), ClientSession())

    def test_stub_without_guard_or_engine_key_is_accepted(self) -> None:
        ensure_session_can_use_engine(object(), ClientSession())

