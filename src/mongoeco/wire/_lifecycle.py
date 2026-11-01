from __future__ import annotations

import asyncio

from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Coroutine


async def finish_cleanup(cleanup: Coroutine[Any, Any, None]) -> None:
    """Finish owned cleanup even if its caller is cancelled repeatedly."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # The synchronous Memory runner can finish immediate coroutines inline.
        await cleanup
        return
    task = asyncio.create_task(cleanup)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError
