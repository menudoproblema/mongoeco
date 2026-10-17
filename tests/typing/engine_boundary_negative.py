# ruff: noqa: E501 - diagnostic markers must share the error line
# fmt: off
"""Wrong control arguments must fail directly and through the coordinator."""

from mongoeco.api._async._collection_runtime import CollectionRuntimeCoordinator
from mongoeco.api._async.cursor import AsyncCursor
from mongoeco.api.operations import UpdateOperation
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines.adapter import EngineSpiAdapter
from mongoeco.engines.semantic_core import EngineFindSemantics


def invalid_cursor(cursor: AsyncCursor) -> None:
    cursor._base_operation().missing_member()  # typing-error: attr-defined
    cursor._base_semantics().missing_member()  # typing-error: attr-defined
    cursor._open_read_source(skip="1", limit=1)  # typing-error: arg-type
    cursor._open_read_source(skip=0, limit="1")  # typing-error: arg-type
    cursor._open_read_source(skip=0)  # typing-error: call-arg
    cursor._open_read_source(skip=0, limit=1, other=True)  # typing-error: call-arg
    cursor._scan(limit="1")  # typing-error: arg-type


async def invalid(
    *,
    adapter: EngineSpiAdapter,
    runtime: CollectionRuntimeCoordinator,
    operation: UpdateOperation,
    semantics: EngineFindSemantics,
    context: OperationContext,
) -> None:
    for boundary in (adapter, runtime._engine_spi):
        await boundary.update_outcome("db", "c", semantics, operation_context=context)  # typing-error: arg-type
        await boundary.update_outcome("db", "c", operation)  # typing-error: call-arg
        await boundary.update_outcome("db", "c", operation, operation_context="invalid")  # typing-error: arg-type
        await boundary.update_outcome("db", "c", operation, operation_context=context, replacement_document=3)  # typing-error: arg-type
        await boundary.update_outcome("db", "c", operation, operation_context=context, upsert="yes")  # typing-error: arg-type
        await boundary.delete_outcome("db", "c", semantics, operation_context=context)  # typing-error: arg-type
        await boundary.delete_outcome("db", "c", operation, operation_context=context, selector_filter=3)  # typing-error: arg-type
        await boundary.insert_outcome("db", "c", {}, operation_context=context, overwrite="no")  # typing-error: arg-type
        await boundary.insert_outcome("db", "c", {}, operation_context=context, bypass_document_validation=1)  # typing-error: arg-type
        await boundary.insert_many_outcomes("db", "c", [3], operation_context=context)  # typing-error: list-item
        await boundary.merge_outcome("db", "c", {}, operation_context=context)  # typing-error: call-arg
        await boundary.merge_outcome("db", "c", {}, when_matched=1, when_not_matched="insert", operation_context=context)  # typing-error: arg-type
        boundary.open_read_snapshot("db", "c", operation, operation_context=context)  # typing-error: arg-type
        await boundary.get_document("db", "c", "b", operation_context=context, projection=42)  # typing-error: arg-type
        await boundary.count_documents("db", "c", operation, operation_context=context)  # typing-error: arg-type
        await boundary.count_documents("db", "c", semantics, operation_context=context, unknown=True)  # typing-error: call-arg
        await boundary.count_documents(42, "c", semantics, operation_context=context)  # typing-error: arg-type
        await boundary.plan_find_execution("db", "c", operation)  # typing-error: arg-type
