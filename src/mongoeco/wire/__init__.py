from importlib import import_module

__all__ = ["AsyncMongoEcoProxyServer", "WireAuthUser"]

_EXPORT_MODULES = {
    "AsyncMongoEcoProxyServer": "mongoeco.wire.proxy",
    "WireAuthUser": "mongoeco.wire.auth",
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
