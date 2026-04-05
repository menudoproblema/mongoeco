from . import database as _database
from .execution import engine as _engine


def _reexport(module) -> tuple[str, ...]:
    names = tuple(getattr(module, '__all__', ()))
    globals().update({name: getattr(module, name) for name in names})
    return names


_DATABASE_EXPORTS = _reexport(_database)
_ENGINE_EXPORTS = _reexport(_engine)

__all__ = [*_DATABASE_EXPORTS, *_ENGINE_EXPORTS]
