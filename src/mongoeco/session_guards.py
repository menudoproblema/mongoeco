from __future__ import annotations

from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession


def ensure_session_can_use_engine(engine: object, session: ClientSession | None) -> None:
    if session is None:
        return
    ensure_active = getattr(session, "ensure_active", None)
    if callable(ensure_active):
        ensure_active()
    ensure_engine = getattr(engine, "_ensure_session_can_use_engine", None)
    if callable(ensure_engine):
        ensure_engine(session)
        return
    engine_key = getattr(engine, "_engine_key", None)
    get_engine_context = getattr(session, "get_engine_context", None)
    if callable(engine_key) and callable(get_engine_context):
        if get_engine_context(engine_key()) is None:
            raise InvalidOperation(
                f"This session was not created by this {type(engine).__name__}"
            )

