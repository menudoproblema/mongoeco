from __future__ import annotations

import asyncio

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from functools import partial
from typing import TYPE_CHECKING


try:
    from pymongo import (
        DeleteMany as PyMongoDeleteMany,
        DeleteOne as PyMongoDeleteOne,
        InsertOne as PyMongoInsertOne,
        ReplaceOne as PyMongoReplaceOne,
        UpdateMany as PyMongoUpdateMany,
        UpdateOne as PyMongoUpdateOne,
    )
except Exception:  # pragma: no cover - optional dependency
    PyMongoDeleteMany = None
    PyMongoDeleteOne = None
    PyMongoInsertOne = None
    PyMongoReplaceOne = None
    PyMongoUpdateMany = None
    PyMongoUpdateOne = None

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import normalize_collation
from mongoeco.core.identity import assert_valid_root_document_id
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


def _pymongo_sort_spec(value: object | None) -> object | None:
    if isinstance(value, Mapping):
        return list(value.items())
    return deepcopy(value)


def _pymongo_collation(value: object | None) -> object | None:
    if value is None:
        return None
    document = getattr(value, "document", value)
    return deepcopy(document)


def normalize_bulk_write_request(request: object) -> WriteModel:
    """Adapt one official PyMongo write model to the internal contract."""
    if isinstance(
        request,
        (InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany),
    ):
        return request
    if PyMongoInsertOne is not None and isinstance(request, PyMongoInsertOne):
        return InsertOne(document=request._doc)
    if PyMongoUpdateOne is not None and isinstance(request, PyMongoUpdateOne):
        return UpdateOne(
            filter=deepcopy(request._filter),
            update=deepcopy(request._doc),
            upsert=bool(request._upsert),
            sort=_pymongo_sort_spec(getattr(request, '_sort', None)),
            array_filters=deepcopy(request._array_filters),
            hint=deepcopy(request._hint),
            collation=_pymongo_collation(request._collation),
        )
    if PyMongoUpdateMany is not None and isinstance(
        request, PyMongoUpdateMany
    ):
        return UpdateMany(
            filter=deepcopy(request._filter),
            update=deepcopy(request._doc),
            upsert=bool(request._upsert),
            array_filters=deepcopy(request._array_filters),
            hint=deepcopy(request._hint),
            collation=_pymongo_collation(request._collation),
        )
    if PyMongoReplaceOne is not None and isinstance(
        request, PyMongoReplaceOne
    ):
        return ReplaceOne(
            filter=deepcopy(request._filter),
            replacement=deepcopy(request._doc),
            upsert=bool(request._upsert),
            sort=_pymongo_sort_spec(getattr(request, '_sort', None)),
            hint=deepcopy(request._hint),
            collation=_pymongo_collation(request._collation),
        )
    if PyMongoDeleteOne is not None and isinstance(request, PyMongoDeleteOne):
        return DeleteOne(
            filter=deepcopy(request._filter),
            hint=deepcopy(request._hint),
            collation=_pymongo_collation(request._collation),
        )
    if PyMongoDeleteMany is not None and isinstance(
        request, PyMongoDeleteMany
    ):
        return DeleteMany(
            filter=deepcopy(request._filter),
            hint=deepcopy(request._hint),
            collation=_pymongo_collation(request._collation),
        )
    raise TypeError('bulk_write requests must be write model instances')


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
                    original["_id"] = DocumentCodec.to_pymongo(ObjectId())
                assert_valid_root_document_id(original["_id"])
                return PreparedBulkWriteRequest(
                    index=index,
                    request=request,
                    insert_document=DocumentCodec.to_internal(
                        deepcopy(original)
                    ),
                )
            if isinstance(request, ReplaceOne):
                collection._normalize_filter(request.filter)
                normalize_collation(request.collation)
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
                normalize_collation(request.collation)
                collection._require_update(request.update)
                collection._normalize_hint(request.hint)
                if request.let is not None:
                    collection._normalize_let(request.let)
                if isinstance(request, UpdateOne) and request.sort is not None:
                    collection._normalize_sort(request.sort)
                return PreparedBulkWriteRequest(index=index, request=request)
            if isinstance(request, (DeleteOne, DeleteMany)):
                collection._normalize_filter(request.filter)
                normalize_collation(request.collation)
                collection._normalize_hint(request.hint)
                if request.let is not None:
                    collection._normalize_let(request.let)
                return PreparedBulkWriteRequest(index=index, request=request)
            raise TypeError("bulk_write requests must be write model instances")
        except (TypeError, ValueError, OperationFailure, WriteError) as exc:
            return PreparedBulkWriteRequest(index=index, request=request, preparation_error=exc)


_CLASSIC_BULK_BATCH_LIMIT = 100_000


def _bulk_family(request: WriteModel) -> str:
    if isinstance(request, InsertOne):
        return "insert"
    if isinstance(request, (UpdateOne, UpdateMany, ReplaceOne)):
        return "update"
    if isinstance(request, (DeleteOne, DeleteMany)):
        return "delete"
    raise TypeError("bulk_write requests must be write model instances")


def _classic_bulk_batches(
    prepared_requests: list[PreparedBulkWriteRequest],
    *,
    ordered: bool,
) -> list[list[PreparedBulkWriteRequest]]:
    """Forma lotes lógicos clásicos sin imponer límites de tamaño BSON.

    En el modo ordenado sólo se agrupan runs contiguos; el modo no ordenado
    puede agrupar familias separadas, igual que los comandos insert/update/delete
    tradicionales. Cada lote conserva los índices de los modelos para informar
    errores y upserts con el contrato de la API.
    """
    if ordered:
        batches: list[list[PreparedBulkWriteRequest]] = []
        current: list[PreparedBulkWriteRequest] = []
        current_family: str | None = None
        for prepared in prepared_requests:
            family = None if prepared.preparation_error is not None else _bulk_family(prepared.request)
            if (
                current
                and (
                    family != current_family
                    or len(current) >= _CLASSIC_BULK_BATCH_LIMIT
                )
            ):
                batches.append(current)
                current = []
            current.append(prepared)
            current_family = family
            if prepared.preparation_error is not None:
                batches.append(current)
                current = []
                current_family = None
        if current:
            batches.append(current)
        return batches

    grouped: dict[str, list[PreparedBulkWriteRequest]] = {
        "insert": [],
        "update": [],
        "delete": [],
    }
    invalid: list[PreparedBulkWriteRequest] = []
    for prepared in prepared_requests:
        if prepared.preparation_error is not None:
            invalid.append(prepared)
        else:
            grouped[_bulk_family(prepared.request)].append(prepared)
    batches = [[prepared] for prepared in invalid]
    for family in ("insert", "update", "delete"):
        requests = grouped[family]
        batches.extend(
            requests[index : index + _CLASSIC_BULK_BATCH_LIMIT]
            for index in range(0, len(requests), _CLASSIC_BULK_BATCH_LIMIT)
        )
    return batches


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

    stop = False
    for batch in _classic_bulk_batches(prepared_requests, ordered=ordered):
        if stop:
            break
        batch_context = None
        for prepared in batch:
            index = prepared.index
            request = prepared.request
            if prepared.preparation_error is not None:
                exc = prepared.preparation_error
                if ordered and not isinstance(exc, WriteError):
                    raise exc
                write_errors.append(
                    WriteErrorEntry(
                        index=index,
                        code=getattr(exc, "code", None),
                        errmsg=str(exc),
                        operation=request.__class__.__name__,
                    )
                )
                if ordered:
                    stop = True
                continue
            if batch_context is None:
                batch_context = collection._new_execution_context()
            variables = batch_context.with_bindings(
                request.let if getattr(request, "let", None) is not None else let or {}
            )
            try:
                if isinstance(request, InsertOne):
                    insert_kwargs = {"session": session}
                    if bypass_document_validation:
                        insert_kwargs["bypass_document_validation"] = True
                    await collection.insert_one(
                        prepared.insert_document or request.document,
                        _execution_context=batch_context,
                        **insert_kwargs,
                    )
                    inserted_count += 1
                elif isinstance(request, UpdateOne):
                    update_one_kwargs = {
                        "collation": request.collation,
                        "sort": request.sort,
                        "array_filters": request.array_filters,
                        "hint": request.hint,
                        "comment": request.comment if request.comment is not None else comment,
                        "let": variables,
                        "session": session,
                    }
                    if bypass_document_validation:
                        update_one_kwargs["bypass_document_validation"] = True
                    result = await collection.update_one(request.filter, request.update, request.upsert, **update_one_kwargs)
                    matched_count += result.matched_count
                    modified_count += result.modified_count
                    if result.upserted_id is not None:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, UpdateMany):
                    update_many_kwargs = {
                        "collation": request.collation,
                        "array_filters": request.array_filters,
                        "hint": request.hint,
                        "comment": request.comment if request.comment is not None else comment,
                        "let": variables,
                        "session": session,
                    }
                    if bypass_document_validation:
                        update_many_kwargs["bypass_document_validation"] = True
                    result = await collection.update_many(request.filter, request.update, request.upsert, **update_many_kwargs)
                    matched_count += result.matched_count
                    modified_count += result.modified_count
                    if result.upserted_id is not None:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, ReplaceOne):
                    replace_one_kwargs = {
                        "collation": request.collation,
                        "sort": request.sort,
                        "hint": request.hint,
                        "comment": request.comment if request.comment is not None else comment,
                        "let": variables,
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
                        collation=request.collation,
                        hint=request.hint,
                        comment=request.comment if request.comment is not None else comment,
                        let=variables,
                        session=session,
                    )
                    deleted_count += result.deleted_count
                elif isinstance(request, DeleteMany):
                    result = await collection.delete_many(
                        request.filter,
                        collation=request.collation,
                        hint=request.hint,
                        comment=request.comment if request.comment is not None else comment,
                        let=variables,
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
                    stop = True
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
        write_errors.sort(key=lambda entry: entry.index)
        raise BulkWriteError(
            "bulk write failed",
            details=BulkWriteErrorDetails(
                write_errors=write_errors,
                inserted_count=result.inserted_count,
                matched_count=result.matched_count,
                modified_count=result.modified_count,
                removed_count=result.deleted_count,
                upserted=[
                    UpsertedWriteEntry(
                        index=op_index,
                        document_id=DocumentCodec.to_pymongo(upserted_id),
                    )
                    for op_index, upserted_id in upserted_ids.items()
                ],
            ).to_document(),
        )
    return result
