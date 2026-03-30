import asyncio
from contextlib import asynccontextmanager
import queue
import threading

from mongoeco.api._async.client import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco import MongoClient


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
async def open_client(engine_name: str, **client_kwargs):
    async with open_engine(engine_name) as engine:
        async with AsyncMongoClient(engine, **client_kwargs) as client:
            yield client


@asynccontextmanager
async def open_sync_client(engine_name: str, **client_kwargs):
    requests: queue.Queue[
        tuple[tuple[str, ...], tuple[object, ...], dict[str, object], asyncio.Future] | None
    ] = queue.Queue()
    ready = threading.Event()
    shutdown_error: list[BaseException] = []

    def _worker() -> None:
        try:
            with MongoClient(ENGINE_FACTORIES[engine_name](), **client_kwargs) as client:
                ready.set()
                while True:
                    request = requests.get()
                    if request is None:
                        return
                    path, args, kwargs, future = request
                    if future.cancelled():
                        continue
                    try:
                        target = client
                        for segment in path:
                            target = getattr(target, segment)
                        result = target(*args, **kwargs)
                    except BaseException as exc:  # pragma: no cover - forward worker exceptions
                        future.get_loop().call_soon_threadsafe(future.set_exception, exc)
                    else:
                        future.get_loop().call_soon_threadsafe(future.set_result, result)
        except BaseException as exc:  # pragma: no cover - worker setup failure
            shutdown_error.append(exc)
            ready.set()

    class _AsyncSyncProxy:
        def __init__(self, path: tuple[str, ...] = ()) -> None:
            self._path = path

        def __getattr__(self, name: str):
            return _AsyncSyncProxy((*self._path, name))

        async def __call__(self, *args, **kwargs):
            loop = asyncio.get_running_loop()
            future: asyncio.Future = loop.create_future()
            requests.put((self._path, args, kwargs, future))
            return await future

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()
    await asyncio.to_thread(ready.wait)
    if shutdown_error:
        raise shutdown_error[0]
    try:
        yield _AsyncSyncProxy()
    finally:
        requests.put(None)
        await asyncio.to_thread(worker.join)
        if shutdown_error:
            raise shutdown_error[0]
