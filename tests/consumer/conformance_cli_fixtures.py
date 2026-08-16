from __future__ import annotations

import asyncio

from tests.consumer.engine_canary import ExternalCanaryEngine


not_callable = 42


def raising_factory() -> object:
    message = "factory exploded"
    raise RuntimeError(message)


async def async_canary_factory() -> ExternalCanaryEngine:
    await asyncio.sleep(0)
    return ExternalCanaryEngine()


async def cleanup_failure(_engine: object, _db_name: str) -> None:
    message = "cleanup exploded"
    raise RuntimeError(message)


class SlowCanaryEngine(ExternalCanaryEngine):
    async def connect(self) -> None:
        await asyncio.sleep(1)
