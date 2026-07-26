from importlib import import_module

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


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
