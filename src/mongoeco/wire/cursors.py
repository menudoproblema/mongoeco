from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mongoeco.errors import OperationFailure


@dataclass(slots=True)
class WireCursorState:
    namespace: str
    remaining_batch: list[object]


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
        cursor_id = self._next_cursor_id
        self._next_cursor_id += 1
        self._state[cursor_id] = WireCursorState(
            namespace=str(cursor.get("ns", "")),
            remaining_batch=list(first_batch[batch_size:]),
        )
        cursor["id"] = cursor_id
        cursor["firstBatch"] = list(first_batch[:batch_size])
        return result

    def get_more(
        self,
        command_document: dict[str, Any],
        *,
        db_name: str,
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
                "ns": state.namespace or f"{db_name}.{collection_name}",
                "nextBatch": next_batch,
            },
            "ok": 1.0,
        }

    def kill_cursors(self, command_document: dict[str, Any]) -> dict[str, Any]:
        collection_name = command_document.get("killCursors")
        if not isinstance(collection_name, str) or not collection_name:
            raise TypeError("killCursors must name a collection")
        cursors = command_document.get("cursors")
        if not isinstance(cursors, list):
            raise TypeError("cursors must be a list")
        killed: list[int] = []
        not_found: list[int] = []
        for cursor_id in cursors:
            if not isinstance(cursor_id, int) or isinstance(cursor_id, bool):
                raise TypeError("cursor ids must be integers")
            if self._state.pop(cursor_id, None) is None:
                not_found.append(cursor_id)
            else:
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

