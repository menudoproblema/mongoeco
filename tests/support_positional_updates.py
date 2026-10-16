"""Historical observations projected from the complete native write observer."""

from typing import Any

from tests.differential.positional_cases import (
    POSITIONAL_UPDATE_CASES,  # noqa: F401 - legacy export
)
from tests.differential.write_cases import positional_write_case
from tests.support_write_parity import observe_async_write_case, observe_write_case


def _legacy_projection(observation, documents):
    error = observation.get("error", {})
    return {
        "status": observation["status"],
        "code": error.get("code"),
        "write_errors": error.get("write_errors", []),
        "documents": documents,
    }


def observe_positional_update(collection: Any, case: dict[str, Any]) -> dict[str, Any]:
    """Preserve the historical projection of the complete independent observer."""
    observation = observe_write_case(
        collection.database,
        collection.name,
        positional_write_case(case, "legacy"),
        existing=True,
    )[-1]
    return _legacy_projection(observation, list(collection.find({}).sort("_id", 1)))


async def observe_async_positional_update(
    collection: Any, case: dict[str, Any]
) -> dict[str, Any]:
    """Preserve that same historical projection through the asynchronous API."""
    observation = (
        await observe_async_write_case(
            collection.database,
            collection.name,
            positional_write_case(case, "legacy"),
            existing=True,
        )
    )[-1]
    return _legacy_projection(
        observation, await collection.find({}).sort("_id", 1).to_list()
    )
