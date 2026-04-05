from . import mongodb as _mongodb


def _reexport(module) -> tuple[str, ...]:
    names = tuple(getattr(module, '__all__', ()))
    globals().update({name: getattr(module, name) for name in names})
    return names


_MONGODB_EXPORTS = _reexport(_mongodb)

__all__ = [*_MONGODB_EXPORTS]
