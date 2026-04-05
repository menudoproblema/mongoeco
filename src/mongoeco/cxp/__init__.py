from . import capabilities as _capabilities
from .catalogs.interfaces import database as _database


def _reexport(module) -> tuple[str, ...]:
    names = tuple(getattr(module, '__all__', ()))
    globals().update({name: getattr(module, name) for name in names})
    return names


_CAPABILITY_EXPORTS = _reexport(_capabilities)
_DATABASE_EXPORTS = _reexport(_database)

__all__ = [*_CAPABILITY_EXPORTS, *_DATABASE_EXPORTS]
