from contextlib import asynccontextmanager

from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


ENGINE_FACTORIES = {
    "memory": MemoryEngine,
    "sqlite": SQLiteEngine,
}

ENGINE_SESSION_KEYS = {
    "memory": "memory",
    "sqlite": "sqlite",
}


@asynccontextmanager
async def open_engine(engine_name: str):
    engine = ENGINE_FACTORIES[engine_name]()
    await engine.connect()
    try:
        yield engine
    finally:
        await engine.disconnect()


@asynccontextmanager
async def open_client(engine_name: str):
    async with open_engine(engine_name) as engine:
        async with AsyncMongoClient(engine) as client:
            yield client
