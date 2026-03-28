from typing import Any

__all__ = ["AsyncMongoEcoProxyServer"]


def __getattr__(name: str) -> Any:
    if name == "AsyncMongoEcoProxyServer":
        from mongoeco.wire.proxy import AsyncMongoEcoProxyServer

        return AsyncMongoEcoProxyServer
    raise AttributeError(name)
