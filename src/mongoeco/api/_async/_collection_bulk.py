from __future__ import annotations

import asyncio
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING

from mongoeco.errors import BulkWriteError, OperationFailure, WriteError
from mongoeco.types import (
    BulkWriteErrorDetails,
    BulkWriteResult,
    DeleteMany,
    DeleteOne,
    Document,
    DocumentId,
    InsertOne,
    ObjectId,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
    UpsertedWriteEntry,
    WriteErrorEntry,
    WriteModel,
)

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.session import ClientSession


@dataclass(slots=True)
class PreparedBulkWriteRequest:
    index: int
    request: WriteModel
    insert_document: Document | None = None
    replacement_document: Document | None = None
    preparation_error: Exception | None = None


class BulkWritePreparationContext:
    def __init__(
        self,
        collection: AsyncCollection,
        requests: list[WriteModel],
    ) -> None:
        self._collection = collection
        self._requests = requests

    async def prepare(self) -> list[PreparedBulkWriteRequest]:
        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(None, partial(self._prepare_request, index, request))
            for index, request in enumerate(self._requests)
        ]
        return list(await asyncio.gather(*tasks))

    def _prepare_request(self, index: int, request: WriteModel) -> PreparedBulkWriteRequest:
        collection = self._collection
        try:
            collection._validate_bulk_write_request_against_profile(request)
            if isinstance(request, InsertOne):
                original = collection._require_document(request.document)
                if "_id" not in original:
                    original["_id"] = ObjectId()
                return PreparedBulkWriteRequest(
                    index=index,
                    request=request,
                    insert_document=deepcopy(original),
                )
            if isinstance(request, ReplaceOne):
                collection._normalize_filter(request.filter)
                collection._normalize_hint(request.hint)
                if request.sort is not None:
                    collection._normalize_sort(request.sort)
                if request.let is not None:
                    collection._normalize_let(request.let)
                return PreparedBulkWriteRequest(
                    index=index,
                    request=request,
                    replacement_document=deepcopy(collection._require_replacement(request.replacement)),
                )
            if isinstance(request, (UpdateOne, UpdateMany)):
                collection._normalize_filter(request.filter)
                collection._require_update(request.update)
                collection._normalize_hint(request.hint)
                if request.array_filters is not None:
                    collection._normalize_array_filters(request.array_filters)
                if request.let is not None:
                    collection._normalize_let(request.let)
                if isinstance(request, UpdateOne) and request.sort is not None:
                    collection._normalize_sort(request.sort)
                return PreparedBulkWriteRequest(index=index, request=request)
            if isinstance(request, (DeleteOne, DeleteMany)):
                collection._normalize_filter(request.filter)
                collection._normalize_hint(request.hint)
                if request.let is not None:
                    collection._normalize_let(request.let)
                return PreparedBulkWriteRequest(index=index, request=request)
            raise TypeError("bulk_write requests must be write model instances")
        except (TypeError, ValueError, OperationFailure, WriteError) as exc:
            return PreparedBulkWriteRequest(index=index, request=request, preparation_error=exc)


async def execute_bulk_write(
    collection: AsyncCollection,
    requests: Sequence[WriteModel],
    *,
    ordered: bool,
    bypass_document_validation: bool,
    comment: object | None,
    let: dict[str, object] | None,
    session: ClientSession | None,
) -> BulkWriteResult[DocumentId]:
    prepared_requests = await BulkWritePreparationContext(collection, list(requests)).prepare()

    inserted_count = 0
    matched_count = 0
    modified_count = 0
    deleted_count = 0
    upserted_ids: dict[int, DocumentId] = {}
    write_errors: list[WriteErrorEntry] = []

    for prepared in prepared_requests:
        index = prepared.index
        request = prepared.request
        if prepared.preparation_error is not None:
            exc = prepared.preparation_error
            if ordered:
                raise exc
            write_errors.append(
                WriteErrorEntry(
                    index=index,
                    code=getattr(exc, "code", None),
                    errmsg=str(exc),
                    operation=request.__class__.__name__,
                )
            )
            continue
        try:
            if isinstance(request, InsertOne):
                insert_kwargs = {"session": session}
                if bypass_document_validation:
                    insert_kwargs["bypass_document_validation"] = True
                await collection.insert_one(prepared.insert_document or request.document, **insert_kwargs)
                inserted_count += 1
            elif isinstance(request, UpdateOne):
                update_one_kwargs = {
                    "sort": request.sort,
                    "array_filters": request.array_filters,
                    "hint": request.hint,
                    "comment": request.comment if request.comment is not None else comment,
                    "let": request.let if request.let is not None else let,
                    "session": session,
                }
                if bypass_document_validation:
                    update_one_kwargs["bypass_document_validation"] = True
                result = await collection.update_one(
                    request.filter,
                    request.update,
                    request.upsert,
                    **update_one_kwargs,
                )
                matched_count += result.matched_count
                modified_count += result.modified_count
                if result.upserted_id is not None:
                    upserted_ids[index] = result.upserted_id
            elif isinstance(request, UpdateMany):
                update_many_kwargs = {
                    "array_filters": request.array_filters,
                    "hint": request.hint,
                    "comment": request.comment if request.comment is not None else comment,
                    "let": request.let if request.let is not None else let,
                    "session": session,
                }
                if bypass_document_validation:
                    update_many_kwargs["bypass_document_validation"] = True
                result = await collection.update_many(
                    request.filter,
                    request.update,
                    request.upsert,
                    **update_many_kwargs,
                )
                matched_count += result.matched_count
                modified_count += result.modified_count
                if result.upserted_id is not None:
                    upserted_ids[index] = result.upserted_id
            elif isinstance(request, ReplaceOne):
                replace_one_kwargs = {
                    "sort": request.sort,
                    "hint": request.hint,
                    "comment": request.comment if request.comment is not None else comment,
                    "let": request.let if request.let is not None else let,
                    "session": session,
                }
                if bypass_document_validation:
                    replace_one_kwargs["bypass_document_validation"] = True
                result = await collection.replace_one(
                    request.filter,
                    prepared.replacement_document or request.replacement,
                    request.upsert,
                    **replace_one_kwargs,
                )
                matched_count += result.matched_count
                modified_count += result.modified_count
                if result.upserted_id is not None:
                    upserted_ids[index] = result.upserted_id
            elif isinstance(request, DeleteOne):
                result = await collection.delete_one(
                    request.filter,
                    hint=request.hint,
                    comment=request.comment if request.comment is not None else comment,
                    let=request.let if request.let is not None else let,
                    session=session,
                )
                deleted_count += result.deleted_count
            elif isinstance(request, DeleteMany):
                result = await collection.delete_many(
                    request.filter,
                    hint=request.hint,
                    comment=request.comment if request.comment is not None else comment,
                    let=request.let if request.let is not None else let,
                    session=session,
                )
                deleted_count += result.deleted_count
        except (WriteError, OperationFailure, TypeError, ValueError) as exc:
            write_errors.append(
                WriteErrorEntry(
                    index=index,
                    code=getattr(exc, "code", None),
                    errmsg=str(exc),
                    operation=request.__class__.__name__,
                )
            )
            if ordered:
                break

    result = BulkWriteResult(
        inserted_count=inserted_count,
        matched_count=matched_count,
        modified_count=modified_count,
        deleted_count=deleted_count,
        upserted_count=len(upserted_ids),
        upserted_ids=upserted_ids,
    )
    if write_errors:
        raise BulkWriteError(
            "bulk write failed",
            details=BulkWriteErrorDetails(
                write_errors=write_errors,
                inserted_count=result.inserted_count,
                matched_count=result.matched_count,
                modified_count=result.modified_count,
                removed_count=result.deleted_count,
                upserted=[
                    UpsertedWriteEntry(index=op_index, document_id=upserted_id)
                    for op_index, upserted_id in upserted_ids.items()
                ],
            ).to_document(),
        )
    return result
