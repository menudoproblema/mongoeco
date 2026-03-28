from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from mongoeco.driver.requests import PreparedRequestExecution
from mongoeco.errors import OperationFailure

if TYPE_CHECKING:
    from mongoeco.api._async.client import AsyncMongoClient


@dataclass(frozen=True, slots=True)
class CallbackCommandTransport:
    callback: Callable[[PreparedRequestExecution], Awaitable[dict[str, Any]]]

    async def send(self, execution: PreparedRequestExecution) -> dict[str, Any]:
        return await self.callback(execution)


class LocalCommandTransport:
    def __init__(self, client: "AsyncMongoClient"):
        self._client = client

    async def send(self, execution: PreparedRequestExecution) -> dict[str, Any]:
        request = execution.plan.request
        database = self._client.get_database(request.database)
        response = await database.command(
            request.payload,
            session=request.session,
        )
        if not isinstance(response, dict):
            raise OperationFailure("driver local transport expected a document response")
        return response
