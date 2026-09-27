"""Command-level oracles for write options on API and wire surfaces."""

from __future__ import annotations

import asyncio

from collections.abc import Awaitable, Callable
from typing import Any
from unittest.mock import patch

import pytest

from mongoeco.api import AsyncMongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine, _sqlite_update_with_operation
from mongoeco.errors import BulkWriteError, ExecutionTimeout, OperationFailure
from mongoeco.wire import AsyncMongoEcoProxyServer


ENGINE_TYPES = [MemoryEngine, SQLiteEngine]
SURFACES = ["api", "wire"]
Command = Callable[[dict[str, object]], Awaitable[dict[str, Any]]]


async def with_command(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    exercise: Callable[[Command], Awaitable[None]],
) -> None:
    engine = engine_type()
    if surface == "api":
        async with AsyncMongoClient(engine) as client:
            await exercise(client.audit.command)
        return
    async with AsyncMongoEcoProxyServer(engine=engine) as proxy:
        connection = proxy._connections.create(("127.0.0.1", 27017))

        async def execute(document: dict[str, object]) -> dict[str, Any]:
            return await proxy._executor.execute_command(
                {**document, "$db": "audit"}, connection=connection
            )

        await exercise(execute)


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("command_name", ["insert", "update", "delete"])
@pytest.mark.parametrize("ordered", [True, False])
def test_ordered_write_batch_controls_later_success_after_first_error(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    command_name: str,
    *,
    ordered: bool,
) -> None:
    async def exercise(execute: Command) -> None:
        await execute(
            {"insert": "items", "documents": [{"_id": 1, "value": "initial"}]}
        )
        if command_name == "insert":
            request: dict[str, object] = {
                "insert": "items",
                "documents": [
                    {"_id": 1, "value": "duplicate"},
                    {"_id": 2, "value": "later"},
                ],
                "ordered": ordered,
            }
        elif command_name == "update":
            request = {
                "update": "items",
                "updates": [
                    {"q": {"_id": 1}, "u": {"$set": {"value": "bad"}}, "multi": "bad"},
                    {"q": {"_id": 1}, "u": {"$set": {"value": "later"}}},
                ],
                "ordered": ordered,
            }
        else:
            request = {
                "delete": "items",
                "deletes": [
                    {"q": {"_id": 1}, "limit": 2},
                    {"q": {"_id": 1}, "limit": 1},
                ],
                "ordered": ordered,
            }
        with pytest.raises(BulkWriteError) as failure:
            await execute(request)
        assert [row["index"] for row in failure.value.details["writeErrors"]] == [0]
        found = await execute({"find": "items"})
        documents = found["cursor"]["firstBatch"]
        if command_name == "insert":
            assert {row["_id"] for row in documents} == ({1} if ordered else {1, 2})
        elif command_name == "update":
            assert documents[0]["value"] == ("initial" if ordered else "later")
        else:
            assert len(documents) == (1 if ordered else 0)
        if not ordered:
            count_fields = {
                "insert": "nInserted",
                "update": "nMatched",
                "delete": "nRemoved",
            }
            count_field = count_fields[
                command_name
            ]
            assert failure.value.details[count_field] == 1

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_create_indexes_max_time_ms_bounds_index_batch(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    async def exercise(execute: Command) -> None:
        request = {
            "createIndexes": "items",
            "indexes": [{"key": {"kind": 1}, "name": "kind_idx"}],
        }

        def fail_on_deadline(deadline: float | None) -> None:
            if deadline is not None:
                message = "controlled index deadline"
                raise ExecutionTimeout(message)

        with patch(
            "mongoeco.api._async._collection_indexing.enforce_deadline",
            side_effect=fail_on_deadline,
        ), pytest.raises(ExecutionTimeout, match="controlled index deadline"):
            await execute({**request, "maxTimeMS": 17})
        indexes = await execute({"listIndexes": "items"})
        assert "kind_idx" not in {
            row["name"] for row in indexes["cursor"]["firstBatch"]
        }
        result = await execute(request)
        assert result["numIndexesAfter"] == result["numIndexesBefore"] + 1
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            await execute({**request, "maxTimeMS": "17"})

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("command_name", ["insert", "update", "findAndModify"])
def test_bypass_document_validation_changes_schema_enforcement(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    command_name: str,
) -> None:
    async def exercise(execute: Command) -> None:
        await execute(
            {
                "create": "items",
                "validator": {
                    "$jsonSchema": {
                        "required": ["name"],
                        "properties": {"name": {"bsonType": "string"}},
                    }
                },
            }
        )
        if command_name != "insert":
            await execute(
                {"insert": "items", "documents": [{"_id": 1, "name": "valid"}]}
            )
        if command_name == "insert":
            request: dict[str, object] = {
                "insert": "items",
                "documents": [{"_id": 2}],
            }
        elif command_name == "update":
            request = {
                "update": "items",
                "updates": [
                    {"q": {"_id": 1}, "u": {"$unset": {"name": ""}}}
                ],
            }
        else:
            request = {
                "findAndModify": "items",
                "query": {"_id": 1},
                "update": {"$unset": {"name": ""}},
                "new": True,
            }
        with pytest.raises((BulkWriteError, OperationFailure)):
            await execute(request)
        with pytest.raises((BulkWriteError, OperationFailure)):
            await execute({**request, "bypassDocumentValidation": False})
        accepted = await execute({**request, "bypassDocumentValidation": True})
        assert accepted["ok"] == 1.0
        target_id = 2 if command_name == "insert" else 1
        found = await execute({"find": "items", "filter": {"_id": target_id}})
        assert found["cursor"]["firstBatch"] == [{"_id": target_id}]
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            await execute({**request, "bypassDocumentValidation": 1})

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("command_name", ["update", "findAndModify"])
def test_array_filters_select_only_matching_nested_elements(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    command_name: str,
) -> None:
    async def exercise(execute: Command) -> None:
        original = {
            "_id": 1,
            "entries": [
                {"kind": "view", "marked": False},
                {"kind": "click", "marked": False},
            ],
        }
        await execute({"insert": "items", "documents": [original]})
        mutation = {"$set": {"entries.$[entry].marked": True}}
        if command_name == "update":
            request: dict[str, object] = {
                "update": "items",
                "updates": [
                    {
                        "q": {"_id": 1},
                        "u": mutation,
                        "arrayFilters": [{"entry.kind": "view"}],
                    }
                ],
            }
            invalid = {
                "update": "items",
                "updates": [
                    {"q": {"_id": 1}, "u": mutation, "arrayFilters": "invalid"}
                ],
            }
        else:
            request = {
                "findAndModify": "items",
                "query": {"_id": 1},
                "update": mutation,
                "arrayFilters": [{"entry.kind": "view"}],
                "new": True,
            }
            invalid = {**request, "arrayFilters": "invalid"}
        await execute(request)
        found = await execute({"find": "items", "filter": {"_id": 1}})
        assert found["cursor"]["firstBatch"] == [
            {
                "_id": 1,
                "entries": [
                    {"kind": "view", "marked": True},
                    {"kind": "click", "marked": False},
                ],
            }
        ]
        with pytest.raises((BulkWriteError, TypeError, ValueError, OperationFailure)):
            await execute(invalid)

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_find_and_modify_sort_selects_first_matching_document(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    async def exercise(execute: Command) -> None:
        await execute(
            {
                "insert": "items",
                "documents": [
                    {"_id": 1, "kind": "view", "rank": 2},
                    {"_id": 2, "kind": "view", "rank": 1},
                ],
            }
        )
        request = {
            "findAndModify": "items",
            "query": {"kind": "view"},
            "sort": {"rank": 1},
            "update": {"$set": {"selected": True}},
            "new": True,
        }
        result = await execute(request)
        selected_id = 2
        assert result["value"]["_id"] == selected_id
        found = await execute({"find": "items", "sort": {"_id": 1}})
        assert [row.get("selected") for row in found["cursor"]["firstBatch"]] == [
            None,
            True,
        ]
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            await execute({**request, "sort": "rank"})

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_find_and_modify_let_binds_expression_filter(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    async def exercise(execute: Command) -> None:
        await execute(
            {
                "insert": "items",
                "documents": [
                    {"_id": 1, "kind": "view"},
                    {"_id": 2, "kind": "click"},
                ],
            }
        )
        request = {
            "findAndModify": "items",
            "query": {"$expr": {"$eq": ["$kind", "$$target"]}},
            "update": {"$set": {"selected": True}},
            "let": {"target": "click"},
            "new": True,
        }
        result = await execute(request)
        selected_id = 2
        assert result["value"]["_id"] == selected_id
        found = await execute({"find": "items", "sort": {"_id": 1}})
        assert [row.get("selected") for row in found["cursor"]["firstBatch"]] == [
            None,
            True,
        ]
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            await execute({**request, "let": "click"})

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_find_and_modify_hint_applies_to_selection(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    async def exercise(execute: Command) -> None:
        await execute(
            {
                "insert": "items",
                "documents": [{"_id": 1, "kind": "view"}],
            }
        )
        await execute(
            {
                "createIndexes": "items",
                "indexes": [{"key": {"kind": 1}, "name": "kind_idx"}],
            }
        )
        request = {
            "findAndModify": "items",
            "query": {"kind": "view"},
            "update": {"$set": {"selected": True}},
            "hint": "kind_idx",
            "new": True,
        }
        result = await execute(request)
        assert result["value"]["selected"] is True
        with pytest.raises(OperationFailure):
            await execute({**request, "hint": "missing_idx"})
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            await execute({**request, "hint": 1})

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
def test_find_and_modify_max_time_ms_reaches_update_deadline(
    engine_type: type[MemoryEngine | SQLiteEngine], surface: str
) -> None:
    async def exercise(execute: Command) -> None:
        await execute(
            {"insert": "items", "documents": [{"_id": 1, "value": "before"}]}
        )
        request = {
            "findAndModify": "items",
            "query": {"_id": 1},
            "update": {"$set": {"value": "after"}},
            "new": True,
        }

        def fail_on_deadline(deadline: float | None) -> None:
            if deadline is not None:
                message = "controlled update deadline"
                raise ExecutionTimeout(message)

        engine_module = (
            "mongoeco.engines.memory"
            if engine_type is MemoryEngine
            else "mongoeco.engines.sqlite"
        )
        with patch(
            f"{engine_module}.enforce_deadline", side_effect=fail_on_deadline
        ), pytest.raises(ExecutionTimeout, match="controlled update deadline"):
            await execute({**request, "maxTimeMS": 17})
        unchanged = await execute({"find": "items", "filter": {"_id": 1}})
        assert unchanged["cursor"]["firstBatch"] == [
            {"_id": 1, "value": "before"}
        ]
        result = await execute(request)
        assert result["value"]["value"] == "after"
        with pytest.raises((TypeError, ValueError, OperationFailure)):
            await execute({**request, "maxTimeMS": "17"})

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
def test_find_and_modify_deadline_after_mutation_rolls_back(
    engine_type: type[MemoryEngine | SQLiteEngine],
) -> None:
    async def exercise() -> None:
        engine = engine_type()
        async with AsyncMongoClient(engine) as client:
            database = client.audit
            await database.command(
                {"insert": "items", "documents": [{"_id": 1, "value": "before"}]}
            )
            mutation_complete = False

            def fail_after_mutation(deadline: float | None) -> None:
                if deadline is not None and mutation_complete:
                    message = "controlled post-mutation deadline"
                    raise ExecutionTimeout(message)

            request = {
                "findAndModify": "items",
                "query": {"_id": 1},
                "update": {"$set": {"value": "after"}},
                "maxTimeMS": 17_000,
                "new": True,
            }
            if engine_type is MemoryEngine:
                original_set = engine._set_storage_document_locked

                def set_and_mark(*args: Any, **kwargs: Any) -> Any:
                    nonlocal mutation_complete
                    result = original_set(*args, **kwargs)
                    mutation_complete = True
                    return result

                with patch.object(
                    engine, "_set_storage_document_locked", side_effect=set_and_mark
                ), patch(
                    "mongoeco.engines.memory.enforce_deadline",
                    side_effect=fail_after_mutation,
                ), pytest.raises(
                    ExecutionTimeout, match="controlled post-mutation deadline"
                ):
                    await database.command(request)
            else:
                def update_and_mark(*args: Any, **kwargs: Any) -> Any:
                    nonlocal mutation_complete
                    result = _sqlite_update_with_operation(*args, **kwargs)
                    mutation_complete = True
                    return result

                with patch(
                    "mongoeco.engines.sqlite._sqlite_update_with_operation",
                    side_effect=update_and_mark,
                ), patch(
                    "mongoeco.engines.sqlite.enforce_deadline",
                    side_effect=fail_after_mutation,
                ), pytest.raises(
                    ExecutionTimeout, match="controlled post-mutation deadline"
                ):
                    await database.command(request)
            assert mutation_complete
            found = await database.command({"find": "items", "filter": {"_id": 1}})
            assert found["cursor"]["firstBatch"] == [
                {"_id": 1, "value": "before"}
            ]

    asyncio.run(exercise())


def test_sqlite_find_and_modify_deadline_includes_worker_queue_wait() -> None:
    async def exercise() -> None:
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine) as client:
            database = client.audit
            await database.command(
                {"insert": "items", "documents": [{"_id": 1, "value": "before"}]}
            )
            original_run_blocking = engine._run_blocking

            async def delayed_worker(*args: Any, **kwargs: Any) -> Any:
                await asyncio.sleep(0.1)
                return await original_run_blocking(*args, **kwargs)

            request = {
                "findAndModify": "items",
                "query": {"_id": 1},
                "update": {"$set": {"value": "after"}},
                "maxTimeMS": 20,
            }
            with patch.object(
                engine, "_run_blocking", side_effect=delayed_worker
            ), pytest.raises(ExecutionTimeout):
                await database.command(request)
            found = await database.command({"find": "items", "filter": {"_id": 1}})
            assert found["cursor"]["firstBatch"] == [
                {"_id": 1, "value": "before"}
            ]

    asyncio.run(exercise())


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("command_name", ["insert", "update", "delete"])
def test_ordered_write_batch_rejects_wrong_type_before_mutation(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    command_name: str,
) -> None:
    async def exercise(execute: Command) -> None:
        operations: dict[str, dict[str, object]] = {
            "insert": {"insert": "items", "documents": [{"_id": 1}]},
            "update": {
                "update": "items",
                "updates": [{"q": {}, "u": {"$set": {"value": 1}}}],
            },
            "delete": {"delete": "items", "deletes": [{"q": {}, "limit": 1}]},
        }
        with pytest.raises((TypeError, ValueError)):
            await execute({**operations[command_name], "ordered": "yes"})
        found = await execute({"find": "items"})
        assert found["cursor"]["firstBatch"] == []

    asyncio.run(with_command(engine_type, surface, exercise))


@pytest.mark.parametrize("engine_type", ENGINE_TYPES)
@pytest.mark.parametrize("surface", SURFACES)
@pytest.mark.parametrize("command_name", ["update", "delete"])
def test_command_let_controls_write_filter_and_spec_override(
    engine_type: type[MemoryEngine | SQLiteEngine],
    surface: str,
    command_name: str,
) -> None:
    async def exercise(execute: Command) -> None:
        await execute(
            {
                "insert": "items",
                "documents": [
                    {"_id": 1, "kind": "view"},
                    {"_id": 2, "kind": "click"},
                ],
            }
        )
        query = {"$expr": {"$eq": ["$kind", "$$target"]}}
        if command_name == "update":
            request: dict[str, object] = {
                "update": "items",
                "updates": [
                    {"q": query, "u": {"$set": {"selected": True}}, "multi": True}
                ],
                "let": {"target": "view"},
            }
        else:
            request = {
                "delete": "items",
                "deletes": [{"q": query, "limit": 0}],
                "let": {"target": "view"},
            }
        await execute(request)
        found = await execute({"find": "items", "sort": {"_id": 1}})
        documents = found["cursor"]["firstBatch"]
        if command_name == "update":
            assert [(row["_id"], row.get("selected")) for row in documents] == [
                (1, True),
                (2, None),
            ]
        else:
            assert [row["_id"] for row in documents] == [2]

        if command_name == "update":
            override: dict[str, object] = {
                "update": "items",
                "updates": [
                    {
                        "q": query,
                        "u": {"$set": {"selected": "override"}},
                        "let": {"target": "click"},
                    }
                ],
                "let": {"target": "view"},
            }
        else:
            override = {
                "delete": "items",
                "deletes": [
                    {"q": query, "limit": 1, "let": {"target": "click"}}
                ],
                "let": {"target": "view"},
            }
        await execute(override)
        after_override = await execute({"find": "items", "sort": {"_id": 1}})
        if command_name == "update":
            values = [
                row["selected"] for row in after_override["cursor"]["firstBatch"]
            ]
            assert values == [
                True,
                "override",
            ]
        else:
            assert after_override["cursor"]["firstBatch"] == []

        with pytest.raises((TypeError, ValueError)):
            await execute({**request, "let": "view"})
        malformed = dict(override)
        spec_name = "updates" if command_name == "update" else "deletes"
        malformed[spec_name] = [{**override[spec_name][0], "let": "click"}]
        with pytest.raises(BulkWriteError):
            await execute(malformed)

    asyncio.run(with_command(engine_type, surface, exercise))
