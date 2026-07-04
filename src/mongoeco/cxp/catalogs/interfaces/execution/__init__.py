from importlib import import_module

_ENGINE_EXPORTS = (
    'EXECUTION_ENGINE_CATALOG',
    'EXECUTION_ENGINE_EXECUTION_STATUS',
    'EXECUTION_ENGINE_EXECUTION_STREAM',
    'EXECUTION_ENGINE_FAMILY_CATALOG',
    'EXECUTION_ENGINE_FAMILY_INTERFACE',
    'EXECUTION_ENGINE_INTERFACE',
    'EXECUTION_ENGINE_INPUT_VALIDATION',
    'EXECUTION_ENGINE_PLANNING',
    'EXECUTION_ENGINE_RUN',
)

_PLAN_RUN_EXPORTS = (
    'PLAN_RUN_EXECUTION_CATALOG',
    'PLAN_RUN_EXECUTION_EXECUTION_STATUS',
    'PLAN_RUN_EXECUTION_EXECUTION_STREAM',
    'PLAN_RUN_EXECUTION_INPUT_VALIDATION',
    'PLAN_RUN_EXECUTION_INTERFACE',
    'PLAN_RUN_EXECUTION_PLANNING',
    'PLAN_RUN_EXECUTION_RUN',
)

__all__ = (
    *_ENGINE_EXPORTS,
    *_PLAN_RUN_EXPORTS,
)

_EXPORT_MODULES = {
    **dict.fromkeys(_ENGINE_EXPORTS, 'mongoeco.cxp.catalogs.interfaces.execution.engine'),
    **dict.fromkeys(_PLAN_RUN_EXPORTS, 'mongoeco.cxp.catalogs.interfaces.execution.plan_run'),
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
