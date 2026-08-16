from importlib import import_module
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from mongoeco.api._async.client import (
        AsyncDatabase as AsyncDatabase,
        AsyncMongoClient as AsyncMongoClient,
        NowFactory as NowFactory,
    )
    from mongoeco.api._async.collection import AsyncCollection as AsyncCollection
    from mongoeco.api._sync.client import (
        Database as Database,
        MongoClient as MongoClient,
    )
    from mongoeco.api._sync.collection import Collection as Collection
    from mongoeco.session import ClientSession as ClientSession

__all__ = [
    "AsyncMongoClient",
    "NowFactory",
    "AsyncDatabase",
    "AsyncCollection",
    "MongoClient",
    "Database",
    "Collection",
    "ClientSession",
]

_EXPORT_MODULES = {
    "AsyncMongoClient": "mongoeco.api._async.client",
    "NowFactory": "mongoeco.api._async.client",
    "AsyncDatabase": "mongoeco.api._async.client",
    "AsyncCollection": "mongoeco.api._async.collection",
    "MongoClient": "mongoeco.api._sync.client",
    "Database": "mongoeco.api._sync.client",
    "Collection": "mongoeco.api._sync.collection",
    "ClientSession": "mongoeco.session",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
