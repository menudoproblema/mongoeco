from __future__ import annotations

import asyncio

from copy import deepcopy
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from mongoeco.engines import (
    DeleteOutcome,
    DeleteResult,
    Document,
    EngineCapabilities,
    EngineFindSemantics,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
    OperationContext,
    UpdateOperation,
    UpdateResult,
)


class ExternalCanaryEngine:
    """Small independent SPI v2 consumer using only the curated public API."""

    capabilities = EngineCapabilities(
        batch_inserts=False,
        explicit_read_snapshots=False,
        injected_clock=False,
        change_delivery="none",
        search=None,
    )

    def __init__(self) -> None:
        self._databases: dict[str, dict[str, dict[object, Document]]] = {}
        self._mutation_lock = asyncio.Lock()

    def _collection(self, db_name: str, coll_name: str) -> dict[object, Document]:
        database = self._databases.setdefault(db_name, {})
        return database.setdefault(coll_name, {})

    @staticmethod
    def _matches(document: Document, filter_spec: object) -> bool:
        if not isinstance(filter_spec, dict):
            return False
        return all(document.get(key) == value for key, value in filter_spec.items())

    async def insert_document(  # noqa: PLR0913 - mirrors the public SPI
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        overwrite: bool = True,  # noqa: FBT001, FBT002 - public SPI signature
        *,
        operation_context: OperationContext,
        bypass_document_validation: bool = False,
    ) -> InsertOutcome:
        del operation_context, bypass_document_validation
        owned = deepcopy(document)
        identifier = owned.get("_id")
        collection = self._collection(db_name, coll_name)
        if not overwrite and identifier in collection:
            return InsertOutcome(applied=False)
        collection[identifier] = owned
        return InsertOutcome(applied=True, document=owned)

    async def get_document(
        self,
        db_name: str,
        coll_name: str,
        doc_id: object,
        *,
        projection: object | None = None,
        operation_context: OperationContext,
    ) -> Document | None:
        del projection, operation_context
        document = self._collection(db_name, coll_name).get(doc_id)
        return deepcopy(document)

    async def update_with_operation(  # noqa: PLR0913 - mirrors the public SPI
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        upsert: bool = False,  # noqa: FBT001, FBT002 - public SPI signature
        upsert_seed: Document | None = None,
        *,
        operation_context: OperationContext,
        selector_filter: object | None = None,
        bypass_document_validation: bool = False,
        replacement_document: Document | None = None,
    ) -> MutationOutcome:
        del operation_context, upsert, upsert_seed, bypass_document_validation
        effective_filter = selector_filter or operation.filter_spec
        async with self._mutation_lock:
            collection = self._collection(db_name, coll_name)
            for identifier, current in collection.items():
                if not self._matches(current, effective_filter):
                    continue
                before = deepcopy(current)
                after = (
                    deepcopy(replacement_document)
                    if replacement_document is not None
                    else self._apply_update(current, operation.update_spec)
                )
                after["_id"] = identifier
                collection[identifier] = after
                return MutationOutcome(
                    UpdateResult(1, int(after != before)),
                    before_document=before,
                    after_document=after,
                )
        return MutationOutcome(UpdateResult(0, 0))

    @staticmethod
    def _apply_update(document: Document, update_spec: object) -> Document:
        after = deepcopy(document)
        if not isinstance(update_spec, dict):
            return after
        set_values = update_spec.get("$set", {})
        if isinstance(set_values, dict):
            after.update(deepcopy(set_values))
        increments = update_spec.get("$inc", {})
        if isinstance(increments, dict):
            for key, value in increments.items():
                after[key] = after.get(key, 0) + value
        return after

    async def delete_with_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        *,
        operation_context: OperationContext,
        selector_filter: object | None = None,
    ) -> DeleteOutcome:
        del operation_context
        effective_filter = selector_filter or operation.filter_spec
        async with self._mutation_lock:
            collection = self._collection(db_name, coll_name)
            for identifier, current in tuple(collection.items()):
                if self._matches(current, effective_filter):
                    del collection[identifier]
                    return DeleteOutcome(DeleteResult(1), deepcopy(current))
        return DeleteOutcome(DeleteResult(0))

    async def merge_document(  # noqa: PLR0913 - mirrors the public SPI
        self,
        db_name: str,
        coll_name: str,
        document: Document,
        *,
        when_matched: str,
        when_not_matched: str,
        operation_context: OperationContext,
    ) -> MergeOutcome:
        del when_matched, when_not_matched, operation_context
        owned = deepcopy(document)
        identifier = owned.get("_id")
        collection = self._collection(db_name, coll_name)
        before = deepcopy(collection.get(identifier))
        collection[identifier] = owned
        if before is None:
            return MergeOutcome(
                matched=False,
                applied=True,
                operation_type="insert",
                after_document=owned,
            )
        return MergeOutcome(
            matched=True,
            applied=True,
            operation_type="replace",
            before_document=before,
            after_document=owned,
        )

    async def count_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        operation_context: OperationContext,
    ) -> int:
        del operation_context
        return sum(
            self._matches(document, semantics.filter_spec or {})
            for document in self._collection(db_name, coll_name).values()
        )

    def scan_find_semantics(
        self,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: object | None = None,
    ) -> AsyncIterator[Document]:
        del context
        documents = [
            deepcopy(document)
            for document in self._collection(db_name, coll_name).values()
            if self._matches(document, semantics.filter_spec or {})
        ]
        if semantics.sort:
            for key, direction in reversed(semantics.sort):
                documents.sort(
                    key=lambda document, field=key: document.get(field),
                    reverse=direction < 0,
                )
        start = semantics.skip
        stop = None if semantics.limit is None else start + semantics.limit

        async def scan() -> AsyncIterator[Document]:
            for document in documents[start:stop]:
                yield document

        return scan()

    async def drop_database(self, db_name: str) -> None:
        self._databases.pop(db_name, None)
