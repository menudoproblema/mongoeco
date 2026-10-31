"""Deliver committed engine effects for collection and namespace operations."""

from contextlib import contextmanager


def prepare_change_delivery(adapter, hub, *, session=None):
    adapter.prepare_change_delivery(hub)
    if (
        hub is None
        or session is None
        or not session.in_transaction
        or adapter.capabilities.change_delivery
        not in {"commit-sequence", "transactional-outbox"}
    ):
        return
    session.register_transaction_hooks(
        f"change_outbox_dispatch:{id(adapter.engine)}:{id(hub)}",
        commit=lambda _session: dispatch_change_delivery(adapter, hub),
    )


def dispatch_change_delivery(adapter, hub, *, session=None):
    if session is not None and session.in_transaction:
        return
    try:
        adapter.dispatch_committed_changes(hub)
    except Exception as exc:
        if hub is not None:
            mark_failure = getattr(hub, "mark_publish_failure", None)
            if callable(mark_failure):
                mark_failure(exc)


@contextmanager
def change_delivery_scope(adapter, hub, *, session=None):
    """Dispatch even when a request fails after committing some of its effects."""
    prepare_change_delivery(adapter, hub, session=session)
    try:
        yield
    finally:
        dispatch_change_delivery(adapter, hub, session=session)
