from collections.abc import Callable


def finalize_best_effort(owner: object, cleanup: Callable[[], object]) -> bool:
    """Release abandoned sync resources without blocking or raising from GC."""
    defer_cleanup = getattr(owner, "_defer_cleanup", None)
    if callable(defer_cleanup):
        try:
            if defer_cleanup(cleanup):
                return True
        except Exception:
            defer_cleanup = None
    try:
        cleanup()
    except Exception:
        return False
    return True
