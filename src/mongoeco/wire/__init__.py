from typing import Any

__all__ = ["AsyncMongoEcoProxyServer", "WireAuthUser"]


def __getattr__(name: str) -> Any:
    if name == "AsyncMongoEcoProxyServer":
        from mongoeco.wire.proxy import AsyncMongoEcoProxyServer

        return AsyncMongoEcoProxyServer
    if name == "WireAuthUser":
        from mongoeco.wire.auth import WireAuthUser

        return WireAuthUser
    raise AttributeError(name)
