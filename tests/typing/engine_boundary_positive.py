"""The eight existing boundaries remain typed through their API consumer."""

from typing import assert_type

from mongoeco.api._async._collection_runtime import CollectionRuntimeCoordinator
from mongoeco.api.operations import UpdateOperation
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines import Document
from mongoeco.engines.adapter import EngineSpiAdapter
from mongoeco.engines.results import (
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.semantic_core import EngineFindSemantics
from mongoeco.engines.snapshots import ReadSnapshot


async def exercise(
    adapter: EngineSpiAdapter,
    runtime: CollectionRuntimeCoordinator,
    operation: UpdateOperation,
    semantics: EngineFindSemantics,
    context: OperationContext,
) -> None:
    assert_type(runtime._engine_spi, EngineSpiAdapter)
    updates: list[MutationOutcome] = []
    deletes: list[DeleteOutcome] = []
    inserts: list[InsertOutcome] = []
    merges: list[MergeOutcome] = []
    document: Document = {"_id": "b"}
    for boundary in (adapter, runtime._engine_spi):
        assert_type(
            await boundary.update_outcome(
                "db",
                "items",
                operation,
                upsert=False,
                upsert_seed=document,
                operation_context=context,
                selector_filter={"revision": 1},
                bypass_document_validation=True,
                replacement_document=document,
                on_commit=updates.append,
            ),
            MutationOutcome,
        )
        assert_type(
            await boundary.delete_outcome(
                "db",
                "items",
                operation,
                operation_context=context,
                selector_filter={},
                on_commit=deletes.append,
            ),
            DeleteOutcome,
        )
        assert_type(
            await boundary.insert_outcome(
                "db",
                "items",
                document,
                overwrite=False,
                operation_context=context,
                bypass_document_validation=False,
                on_commit=inserts.append,
            ),
            InsertOutcome,
        )
        assert_type(
            await boundary.insert_many_outcomes(
                "db",
                "items",
                (document,),
                operation_context=context,
                on_commit=inserts.append,
            ),
            tuple[InsertOutcome, ...],
        )
        assert_type(
            await boundary.merge_outcome(
                "db",
                "items",
                document,
                when_matched="merge",
                when_not_matched="insert",
                operation_context=context,
                on_commit=merges.append,
            ),
            MergeOutcome,
        )
        assert_type(
            boundary.open_read_snapshot(
                "db",
                "items",
                semantics,
                operation_context=context,
            ),
            ReadSnapshot,
        )
        assert_type(
            await boundary.get_document(
                "db",
                "items",
                "b",
                operation_context=context,
                projection={"_id": 1},
            ),
            Document | None,
        )
        assert_type(
            await boundary.count_documents(
                "db",
                "items",
                semantics,
                operation_context=context,
            ),
            int,
        )
