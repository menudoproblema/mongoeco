from collections.abc import Callable


def finalize_best_effort(owner: object, cleanup: Callable[[], object]) -> bool:
    """Schedule abandoned-resource cleanup without executing user code from GC."""
    defer_cleanup = getattr(owner, "_defer_cleanup", None)
    if not callable(defer_cleanup):
        return False
    try:
        return bool(defer_cleanup(cleanup))
    except Exception:
        return False
