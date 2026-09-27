from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mongoeco.errors import OperationFailure
from mongoeco.wire._session_identity import wire_session_key


if TYPE_CHECKING:
    from mongoeco.wire.connections import WireConnectionContext


def _principal_key(
    connection: WireConnectionContext | None,
) -> tuple[tuple[str, str], ...]:
    if connection is None:
        return ()
    identities: set[tuple[str, str]] = set()
    for item in connection.authenticated_users:
        user = item.get("user")
        database = item.get("db")
        if (
            not isinstance(user, str)
            or not user
            or not isinstance(database, str)
            or not database
        ):
            message = "wire connection user identity is incomplete"
            raise OperationFailure(message)
        identities.add((database, user))
    return tuple(sorted(identities))


@dataclass(slots=True)
class WireCursorState:
    namespace: str
    remaining_batch: list[object]
    session_key: str | None = None
    principal_key: tuple[tuple[str, str], ...] = ()


class WireCursorStore:
    def __init__(self) -> None:
        self._next_cursor_id = 1
        self._state: dict[int, WireCursorState] = {}

    def clear(self) -> None:
        self._state.clear()

    def materialize_command_result(
        self,
        command_document: dict[str, Any],
        result: dict[str, Any],
        *,
        lsid: object | None = None,
        connection: WireConnectionContext | None = None,
    ) -> dict[str, Any]:
        cursor = result.get("cursor")
        if not isinstance(cursor, dict):
            return result
        first_batch = cursor.get("firstBatch")
        if not isinstance(first_batch, list):
            return result
        batch_size = self._resolve_batch_size(command_document)
        if batch_size is None or batch_size <= 0 or len(first_batch) <= batch_size:
            cursor["id"] = 0
            return result
        namespace = cursor.get("ns")
        if not isinstance(namespace, str) or not namespace:
            message = "wire cursor namespace must be a non-empty string"
            raise OperationFailure(message)
        cursor_id = self._next_cursor_id
        self._next_cursor_id += 1
        self._state[cursor_id] = WireCursorState(
            namespace=namespace,
            remaining_batch=list(first_batch[batch_size:]),
            session_key=wire_session_key(lsid) if lsid is not None else None,
            principal_key=_principal_key(connection),
        )
        cursor["id"] = cursor_id
        cursor["firstBatch"] = list(first_batch[:batch_size])
        return result

    def get_more(
        self,
        command_document: dict[str, Any],
        *,
        db_name: str,
        lsid: object | None = None,
        connection: WireConnectionContext | None = None,
    ) -> dict[str, Any]:
        cursor_id = command_document.get("getMore")
        if not isinstance(cursor_id, int) or isinstance(cursor_id, bool):
            raise TypeError("getMore cursor id must be an integer")
        collection_name = command_document.get("collection", "")
        if not isinstance(collection_name, str):
            raise TypeError("collection must be a string")
        batch_size = command_document.get("batchSize")
        if batch_size is not None and (
            not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 0
        ):
            raise TypeError("batchSize must be a non-negative integer")
        state = self._state.get(cursor_id)
        if state is None:
            return {
                "cursor": {
                    "id": 0,
                    "ns": f"{db_name}.{collection_name}",
                    "nextBatch": [],
                },
                "ok": 1.0,
            }
        expected_namespace = f"{db_name}.{collection_name}"
        if state.namespace != expected_namespace:
            message = "getMore cursor namespace does not match the command"
            raise OperationFailure(message)
        request_session_key = wire_session_key(lsid) if lsid is not None else None
        if state.session_key != request_session_key:
            message = "getMore cursor session does not match the creating command"
            raise OperationFailure(message)
        if state.principal_key != _principal_key(connection):
            message = "getMore cursor user does not match the creating command"
            raise OperationFailure(message)
        effective_batch_size = len(state.remaining_batch) if not batch_size else batch_size
        next_batch = list(state.remaining_batch[:effective_batch_size])
        state.remaining_batch = state.remaining_batch[effective_batch_size:]
        next_cursor_id = cursor_id
        if not state.remaining_batch:
            self._state.pop(cursor_id, None)
            next_cursor_id = 0
        return {
            "cursor": {
                "id": next_cursor_id,
                "ns": state.namespace,
                "nextBatch": next_batch,
            },
            "ok": 1.0,
        }

    def kill_cursors(
        self,
        command_document: dict[str, Any],
        *,
        db_name: str,
        connection: WireConnectionContext | None = None,
    ) -> dict[str, Any]:
        collection_name = command_document.get("killCursors")
        if not isinstance(collection_name, str) or not collection_name:
            raise TypeError("killCursors must name a collection")
        cursors = command_document.get("cursors")
        if not isinstance(cursors, list):
            raise TypeError("cursors must be a list")
        killed: list[int] = []
        not_found: list[int] = []
        expected_namespace = f"{db_name}.{collection_name}"
        request_principal = _principal_key(connection)
        for cursor_id in cursors:
            if not isinstance(cursor_id, int) or isinstance(cursor_id, bool):
                raise TypeError("cursor ids must be integers")
            state = self._state.get(cursor_id)
            if (
                state is None
                or state.namespace != expected_namespace
                or state.principal_key != request_principal
            ):
                not_found.append(cursor_id)
            else:
                self._state.pop(cursor_id)
                killed.append(cursor_id)
        return {
            "cursorsKilled": killed,
            "cursorsUnknown": not_found,
            "cursorsAlive": [],
            "cursorsNotFound": not_found,
            "ok": 1.0,
        }

    @staticmethod
    def _resolve_batch_size(command_document: dict[str, Any]) -> int | None:
        batch_size = command_document.get("batchSize")
        if batch_size is None:
            cursor_spec = command_document.get("cursor")
            if isinstance(cursor_spec, dict):
                batch_size = cursor_spec.get("batchSize")
        if batch_size is None:
            return None
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 0:
            raise TypeError("batchSize must be a non-negative integer")
        return batch_size
