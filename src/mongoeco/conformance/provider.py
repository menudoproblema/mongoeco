from __future__ import annotations

import inspect
import uuid

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable


@dataclass(frozen=True, slots=True)
class EngineConformanceProvider:
    """Factory and lifecycle contract supplied by an engine package."""

    name: str
    factory: Callable[[], object]
    namespace_prefix: str = "mongoeco_conformance"
    cleanup: Callable[[object, str], Awaitable[None] | None] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            message = "provider name must be a non-empty string"
            raise ValueError(message)
        if not callable(self.factory):
            message = "provider factory must be callable"
            raise TypeError(message)
        if not isinstance(self.namespace_prefix, str) or not self.namespace_prefix:
            message = "provider namespace_prefix must be a non-empty string"
            raise ValueError(message)
        if self.cleanup is not None and not callable(self.cleanup):
            message = "provider cleanup must be callable or None"
            raise TypeError(message)

    def namespace(self, check_name: str) -> tuple[str, str]:
        suffix = uuid.uuid4().hex
        normalized = check_name.replace("-", "_")
        return (
            f"{self.namespace_prefix}_{normalized}_{suffix}",
            "items",
        )

    async def cleanup_namespace(self, engine: object, db_name: str) -> None:
        cleanup = self.cleanup
        if cleanup is not None:
            result = cleanup(engine, db_name)
            if inspect.isawaitable(result):
                await result
            return
        drop_database = getattr(engine, "drop_database", None)
        if callable(drop_database):
            result = drop_database(db_name)
            if inspect.isawaitable(result):
                await result

    @asynccontextmanager
    async def open_engine(self) -> AsyncIterator[object]:
        engine = self.factory()
        if inspect.isawaitable(engine):
            engine = await engine
        connect = getattr(engine, "connect", None)
        disconnect = getattr(engine, "disconnect", None)
        if callable(connect):
            connected = connect()
            if inspect.isawaitable(connected):
                await connected
        try:
            yield engine
        finally:
            if callable(disconnect):
                disconnected: Any = disconnect()
                if inspect.isawaitable(disconnected):
                    await disconnected
