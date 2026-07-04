from importlib import import_module

__all__ = ["AsyncStorageEngine", "MemoryEngine", "SQLiteEngine"]

_EXPORT_MODULES = {
    "AsyncStorageEngine": "mongoeco.engines.base",
    "MemoryEngine": "mongoeco.engines.memory",
    "SQLiteEngine": "mongoeco.engines.sqlite",
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
