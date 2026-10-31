from __future__ import annotations

import asyncio

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from mongoeco.errors import OperationFailure
from mongoeco.wire._lifecycle import finish_cleanup
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
    source: Any = None
    lock: Any = field(default_factory=asyncio.Lock)


class WireCursorStore:
    def __init__(self) -> None:
        self._next_cursor_id = 1
        self._state: dict[int, WireCursorState] = {}

    def __len__(self) -> int:
        return len(self._state)

    def clear(self) -> None:
        if any(state.source is not None for state in self._state.values()):
            message = "live wire cursors require async cleanup"
            raise RuntimeError(message)
        self._state.clear()

    async def aclear(self):
        states = list(self._state.values())
        self._state.clear()
        await finish_cleanup(self._close_states(states))

    @staticmethod
    async def _close_states(states):
        errors = []
        for state in states:
            if state.source is not None:
                try:
                    async with state.lock:
                        await state.source.close()
                except Exception as error:
                    errors.append(error)
        if errors:
            message = "wire cursor cleanup failed"
            raise ExceptionGroup(message, errors)

    @staticmethod
    def _source_alive(source):
        return not (source._closed or getattr(source, "_exhausted", False))

    async def open_stream(
        self, source, namespace, *, command_document, lsid=None, connection=None
    ):
        try:
            session_key = wire_session_key(lsid) if lsid is not None else None
            principal = _principal_key(connection)
            maximum = self._resolve_batch_size(command_document)
            maximum = 101 if maximum is None else maximum
            batch = await self._read_source(source, maximum, initial=True)
            cursor_id = 0
            if self._source_alive(source):
                cursor_id = self._next_cursor_id
                self._next_cursor_id += 1
                self._state[cursor_id] = WireCursorState(
                    namespace=namespace,
                    remaining_batch=[],
                    source=source,
                    session_key=session_key,
                    principal_key=principal,
                )
            else:
                await finish_cleanup(source.close())
            return {
                "cursor": {"id": cursor_id, "ns": namespace, "firstBatch": batch},
                "ok": 1.0,
            }
        except BaseException as error:
            try:
                await finish_cleanup(source.close())
            except BaseException as cleanup_error:
                error.add_note(f"wire cursor cleanup failed: {cleanup_error}")
            raise

    @staticmethod
    async def _read_source(source, maximum, *, initial=False):
        read_response = getattr(source, "_read_response", None)
        if callable(read_response):
            return await read_response(maximum, initial=initial)
        return await source.to_list(length=maximum)

    async def get_more_async(
        self, command_document, *, db_name, lsid=None, connection=None
    ):
        cursor_id, batch_size, state = self._resolve_get_more(
            command_document, db_name=db_name, lsid=lsid, connection=connection
        )
        if state is None or state.source is None:
            return self.get_more(
                command_document, db_name=db_name, lsid=lsid, connection=connection
            )
        async with state.lock:
            if self._state.get(cursor_id) is not state:
                return {
                    "cursor": {"id": 0, "ns": state.namespace, "nextBatch": []},
                    "ok": 1.0,
                }
            try:
                batch = await self._read_source(state.source, batch_size or None)
                next_cursor_id = cursor_id
                if not self._source_alive(state.source):
                    self._state.pop(cursor_id, None)
                    next_cursor_id = 0
                    await finish_cleanup(state.source.close())
                return {
                    "cursor": {
                        "id": next_cursor_id,
                        "ns": state.namespace,
                        "nextBatch": batch,
                    },
                    "ok": 1.0,
                }
            except BaseException as error:
                self._state.pop(cursor_id, None)
                try:
                    await finish_cleanup(state.source.close())
                except BaseException as cleanup_error:
                    error.add_note(f"wire cursor cleanup failed: {cleanup_error}")
                raise

    async def kill_cursors_async(
        self, command_document, *, db_name, lsid=None, connection=None
    ):
        states = (
            {
                cursor_id: self._state[cursor_id]
                for cursor_id in command_document.get("cursors", ())
                if isinstance(cursor_id, int) and cursor_id in self._state
            }
            if isinstance(command_document.get("cursors"), list)
            else {}
        )
        result = self.kill_cursors(
            command_document, db_name=db_name, lsid=lsid, connection=connection
        )
        await finish_cleanup(
            self._close_states(
                [states[cursor_id] for cursor_id in result["cursorsKilled"]]
            )
        )
        return result

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

    def _resolve_get_more(
        self, command_document, *, db_name, lsid=None, connection=None
    ):
        cursor_id = command_document.get("getMore")
        if not isinstance(cursor_id, int) or isinstance(cursor_id, bool):
            message = "getMore cursor id must be an integer"
            raise TypeError(message)
        collection_name = command_document.get("collection", "")
        if not isinstance(collection_name, str):
            message = "collection must be a string"
            raise TypeError(message)
        batch_size = command_document.get("batchSize")
        if batch_size is not None and (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size < 0
        ):
            message = "batchSize must be a non-negative integer"
            raise TypeError(message)
        state = self._state.get(cursor_id)
        if state is None:
            return cursor_id, batch_size, state
        if state.principal_key != _principal_key(connection):
            message = "getMore cursor user does not match the creating command"
            raise OperationFailure(message, code=13)
        expected_namespace = f"{db_name}.{collection_name}"
        if state.namespace != expected_namespace:
            message = "getMore cursor namespace does not match the command"
            raise OperationFailure(message, code=13)
        request_session_key = wire_session_key(lsid) if lsid is not None else None
        if state.session_key != request_session_key:
            message = "getMore cursor session does not match the creating command"
            code = (
                50736
                if state.session_key is None
                else 50737
                if request_session_key is None
                else 50738
            )
            raise OperationFailure(message, code=code)
        return cursor_id, batch_size, state

    def get_more(
        self,
        command_document: dict[str, Any],
        *,
        db_name: str,
        lsid: object | None = None,
        connection: WireConnectionContext | None = None,
    ) -> dict[str, Any]:
        cursor_id, batch_size, state = self._resolve_get_more(
            command_document, db_name=db_name, lsid=lsid, connection=connection
        )
        if state is None:
            return {
                "cursor": {
                    "id": 0,
                    "ns": f"{db_name}.{command_document.get('collection', '')}",
                    "nextBatch": [],
                },
                "ok": 1.0,
            }
        effective_batch_size = (
            len(state.remaining_batch) if not batch_size else batch_size
        )
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
        lsid: object | None = None,
        connection: WireConnectionContext | None = None,
    ) -> dict[str, Any]:
        collection_name = command_document.get("killCursors")
        if not isinstance(collection_name, str) or not collection_name:
            raise TypeError("killCursors must name a collection")
        cursors = command_document.get("cursors")
        if not isinstance(cursors, list):
            raise TypeError("cursors must be a list")
        if any(
            not isinstance(cursor_id, int) or isinstance(cursor_id, bool)
            for cursor_id in cursors
        ):
            raise TypeError("cursor ids must be integers")
        killed: list[int] = []
        not_found: list[int] = []
        expected_namespace = f"{db_name}.{collection_name}"
        request_session_key = wire_session_key(lsid) if lsid is not None else None
        request_principal = _principal_key(connection)
        for cursor_id in cursors:
            state = self._state.get(cursor_id)
            if (
                state is None
                or state.namespace != expected_namespace
                or state.principal_key != request_principal
                or (
                    request_session_key is not None
                    and state.session_key != request_session_key
                )
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
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size < 0
        ):
            message = "batchSize must be a non-negative integer"
            raise TypeError(message)
        return batch_size
