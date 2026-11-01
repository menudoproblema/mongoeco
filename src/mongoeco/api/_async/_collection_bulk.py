from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

from mongoeco.api._async import _collection_modify
from mongoeco.api._async._write_command_validation import (
    validate_delete_command_shapes,
    validate_update_command_shapes,
)
from mongoeco.api._async._write_failures import record_write_failure
from mongoeco.api._command_bson import command_boolean
from mongoeco.api.admin_parsing import normalize_command_ordered
from mongoeco.api.argument_validation import replacement_document, update_document
from mongoeco.api.bson_input import iter_bson_write_frames


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

from mongoeco.core._public_materialization import materialize_public_value
from mongoeco.errors import (
    BulkWriteError,
    InvalidOperation,
    OperationFailure,
    WriteError,
)
from mongoeco.types import (
    BulkWriteErrorDetails,
    BulkWriteResult,
    DeleteMany,
    DeleteOne,
    Document,
    DocumentId,
    InsertOne,
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


_PYMONGO_FIELD_MISSING = object()


def _pymongo_model_field(
    request: object,
    field_name: str,
    *,
    default: object = _PYMONGO_FIELD_MISSING,
) -> object:
    try:
        return getattr(request, field_name)
    except AttributeError as exc:
        if default is not _PYMONGO_FIELD_MISSING:
            return default
        model_name = type(request).__name__
        message = (
            f"unsupported PyMongo {model_name} layout: missing {field_name}; "
            "MongoEco supports PyMongo >=4.9,<5"
        )
        raise TypeError(message) from exc


def _pymongo_sort_spec(value: object | None) -> object | None:
    if isinstance(value, Mapping):
        return list(value.items())
    return value


def _pymongo_collation(value: object | None) -> object | None:
    if value is None:
        return None
    return getattr(value, "document", value)


def normalize_bulk_write_request(request: object) -> WriteModel:
    """Adapt one official PyMongo write model to the internal contract."""
    if isinstance(
        request,
        (InsertOne, UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany),
    ):
        return request
    if PyMongoInsertOne is not None and isinstance(request, PyMongoInsertOne):
        return InsertOne(
            document=_pymongo_model_field(request, "_doc"),
        )
    if PyMongoUpdateOne is not None and isinstance(request, PyMongoUpdateOne):
        return UpdateOne(
            filter=_pymongo_model_field(request, "_filter"),
            update=_pymongo_model_field(request, "_doc"),
            upsert=bool(_pymongo_model_field(request, "_upsert")),
            sort=_pymongo_sort_spec(
                _pymongo_model_field(request, "_sort", default=None)
            ),
            array_filters=_pymongo_model_field(request, "_array_filters"),
            hint=_pymongo_model_field(request, "_hint"),
            collation=_pymongo_collation(_pymongo_model_field(request, "_collation")),
        )
    if PyMongoUpdateMany is not None and isinstance(request, PyMongoUpdateMany):
        return UpdateMany(
            filter=_pymongo_model_field(request, "_filter"),
            update=_pymongo_model_field(request, "_doc"),
            upsert=_pymongo_model_field(request, "_upsert"),
            array_filters=_pymongo_model_field(request, "_array_filters"),
            hint=_pymongo_model_field(request, "_hint"),
            collation=_pymongo_collation(_pymongo_model_field(request, "_collation")),
        )
    if PyMongoReplaceOne is not None and isinstance(request, PyMongoReplaceOne):
        return ReplaceOne(
            filter=_pymongo_model_field(request, "_filter"),
            replacement=_pymongo_model_field(request, "_doc"),
            upsert=_pymongo_model_field(request, "_upsert"),
            sort=_pymongo_sort_spec(
                _pymongo_model_field(request, "_sort", default=None)
            ),
            hint=_pymongo_model_field(request, "_hint"),
            collation=_pymongo_collation(_pymongo_model_field(request, "_collation")),
        )
    if PyMongoDeleteOne is not None and isinstance(request, PyMongoDeleteOne):
        return DeleteOne(
            filter=_pymongo_model_field(request, "_filter"),
            hint=_pymongo_model_field(request, "_hint"),
            collation=_pymongo_collation(_pymongo_model_field(request, "_collation")),
        )
    if PyMongoDeleteMany is not None and isinstance(request, PyMongoDeleteMany):
        return DeleteMany(
            filter=_pymongo_model_field(request, "_filter"),
            hint=_pymongo_model_field(request, "_hint"),
            collation=_pymongo_collation(_pymongo_model_field(request, "_collation")),
        )
    message = "bulk_write requests must be write model instances"
    raise TypeError(message)


@dataclass(slots=True)
class PreparedBulkWriteRequest:
    index: int
    request: WriteModel
    insert_document: Document | None = None
    replacement_document: Document | None = None
    preparation_error: Exception | None = None
    error_operation: Document | None = None


def _bulk_operation_document(request: WriteModel) -> Document:
    if isinstance(request, InsertOne):
        return request.document
    command = {"q": request.filter}
    if isinstance(request, (UpdateOne, UpdateMany, ReplaceOne)):
        command["u"] = (
            request.replacement if isinstance(request, ReplaceOne) else request.update
        )
        if not isinstance(request, ReplaceOne):
            command["multi"] = isinstance(request, UpdateMany)
        upsert = request.upsert
        if isinstance(request, UpdateOne):
            upsert = bool(upsert)
        if upsert is not None:
            command["upsert"] = upsert
    else:
        command["limit"] = 1 if isinstance(request, DeleteOne) else 0
    if request.collation is not None:
        command["collation"] = _pymongo_collation(request.collation)
    if (
        isinstance(request, (UpdateOne, UpdateMany))
        and request.array_filters is not None
    ):
        command["arrayFilters"] = request.array_filters
    if request.hint is not None:
        command["hint"] = (
            request.hint if isinstance(request.hint, str) else dict(request.hint)
        )
    if isinstance(request, (UpdateOne, ReplaceOne)) and request.sort is not None:
        command["sort"] = dict(request.sort)
    return command


class BulkWritePreparationContext:
    def __init__(
        self,
        collection: AsyncCollection,
        requests: list[WriteModel],
    ) -> None:
        self._collection = collection
        self._requests = requests

    async def prepare(self) -> list[PreparedBulkWriteRequest]:
        # Shape checks and ID assignment are ordered client effects. BSON and
        # server validation belong to the later physical family boundary.
        prepared = []
        for index, request in enumerate(self._requests):
            item = self._prepare_request(index, request)
            if item.preparation_error is not None:
                raise item.preparation_error
            prepared.append(item)
        if not prepared:
            message = "No operations to execute"
            raise InvalidOperation(message)
        return prepared

    def _prepare_request(
        self, index: int, request: WriteModel
    ) -> PreparedBulkWriteRequest:
        collection = self._collection
        try:
            request = normalize_bulk_write_request(request)
            collection._validate_bulk_write_request_against_profile(request)
            if isinstance(request, InsertOne):
                original = collection._prepare_insert_document(request.document)
                return PreparedBulkWriteRequest(
                    index=index, request=request, insert_document=original
                )
            if isinstance(request, ReplaceOne):
                replacement_document(request.replacement)
                return PreparedBulkWriteRequest(
                    index=index,
                    request=request,
                    replacement_document=request.replacement,
                )
            if isinstance(request, (UpdateOne, UpdateMany)):
                update_document(request.update)
                if request.array_filters is not None and not isinstance(
                    request.array_filters, list
                ):
                    message = "array_filters must be a list"
                    raise TypeError(message)
            elif not isinstance(request, (DeleteOne, DeleteMany)):
                message = "bulk_write requests must be write model instances"
                raise TypeError(message)
            return PreparedBulkWriteRequest(index=index, request=request)
        except AttributeError:
            # PyMongo catches AttributeError from _add_to_bulk, including an
            # integer first key's missing startswith, as an invalid model.
            message = "bulk_write contains an invalid request"
            return PreparedBulkWriteRequest(
                index=index, request=request, preparation_error=TypeError(message)
            )
        except (TypeError, ValueError) as error:
            return PreparedBulkWriteRequest(
                index=index, request=request, preparation_error=error
            )


def _encoded_bulk_batches(  # noqa: PLR0913 - one command header owns its frame sequence
    collection, requests, *, ordered, comment, let, bypass_document_validation
):
    for batch in _classic_bulk_batches(requests, ordered=ordered):
        family = _bulk_family(batch[0].request)
        command = {family: collection.name, "ordered": ordered}
        if comment:
            command["comment"] = comment
        if bypass_document_validation is not None:
            command["bypassDocumentValidation"] = bypass_document_validation
        if let is not None and family != "insert":
            command["let"] = let
        command["$db"] = collection.database.name

        def operations(batch=batch):
            for item in batch:
                request = item.request
                operation = _bulk_operation_document(request)
                if not isinstance(request, InsertOne):
                    if request.comment is not None:
                        operation = {**operation, "comment": request.comment}
                    if request.let is not None:
                        operation = {**operation, "let": request.let}
                yield operation

        sequence = {"insert": "documents", "update": "updates", "delete": "deletes"}[
            family
        ]
        offset = 0
        for header, specs in iter_bson_write_frames(
            command,
            operations(),
            codec_options=collection._codec_options,
            sequence_name=sequence,
            max_message_size=_CLASSIC_BULK_MESSAGE_LIMIT - _CLASSIC_BULK_SESSION_BYTES,
        ):
            frame = batch[offset : offset + len(specs)]
            offset += len(specs)
            yield frame, {**header, "operations": specs}


def _admit_bulk_batch(collection, batch, encoded):
    """Validate one completely encoded server frame before any of its effects."""
    family = _bulk_family(batch[0].request)
    frame_ordered = normalize_command_ordered(encoded.get("ordered"))
    bypass = command_boolean(
        encoded.get("bypassDocumentValidation"),
        "bypassDocumentValidation",
        numeric=True,
    )
    specs = encoded["operations"]
    command_specs = (
        [
            {k: v for k, v in spec.items() if k not in {"comment", "let"}}
            for spec in specs
        ]
        if family != "insert"
        else specs
    )
    if family == "update":
        validate_update_command_shapes(command_specs, collection._mongodb_dialect.key)
    elif family == "delete":
        validate_delete_command_shapes(command_specs)
    for item, spec in zip(batch, specs, strict=True):
        request = item.request
        item.error_operation = deepcopy(_bulk_operation_document(request))
        if isinstance(request, InsertOne):
            item.insert_document = spec
            continue
        fields = {
            "filter": spec["q"],
            "collation": spec.get("collation"),
            "hint": list(spec["hint"].items())
            if isinstance(spec.get("hint"), dict)
            else spec.get("hint"),
            "comment": spec.get("comment"),
            "let": spec.get("let"),
        }
        if isinstance(request, (UpdateOne, UpdateMany, ReplaceOne)):
            fields["upsert"] = spec.get("upsert", False)
            if isinstance(request, ReplaceOne):
                fields["replacement"] = spec["u"]
                item.replacement_document = spec["u"]
            else:
                fields.update(update=spec["u"], array_filters=spec.get("arrayFilters"))
            if isinstance(request, (UpdateOne, ReplaceOne)):
                fields["sort"] = (
                    list(spec["sort"].items())
                    if isinstance(spec.get("sort"), dict)
                    else spec.get("sort")
                )
        item.request = replace(request, **fields)
    return encoded.get("comment"), encoded.get("let"), bypass, frame_ordered


_CLASSIC_BULK_BATCH_LIMIT = 100_000
_CLASSIC_BULK_MESSAGE_LIMIT = 48_000_000
# A UUID lsid subdocument in every native command: implicit or explicit session.
_CLASSIC_BULK_SESSION_BYTES = 36


def _bulk_family(request: WriteModel) -> str:
    if isinstance(request, InsertOne):
        return "insert"
    if isinstance(request, (UpdateOne, UpdateMany, ReplaceOne)):
        return "update"
    if isinstance(request, (DeleteOne, DeleteMany)):
        return "delete"
    message = "bulk_write requests must be write model instances"
    raise TypeError(message)


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
            family = _bulk_family(prepared.request)
            if current and (
                family != current_family or len(current) >= _CLASSIC_BULK_BATCH_LIMIT
            ):
                batches.append(current)
                current = []
            current.append(prepared)
            current_family = family
        if current:
            batches.append(current)
        return batches

    grouped: dict[str, list[PreparedBulkWriteRequest]] = {
        "insert": [],
        "update": [],
        "delete": [],
    }
    for prepared in prepared_requests:
        grouped[_bulk_family(prepared.request)].append(prepared)
    batches = []
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
    prepared_requests = await BulkWritePreparationContext(
        collection, list(requests)
    ).prepare()
    collection._ensure_session_active(session)
    normalized_bulk_let = let

    inserted_count = 0
    matched_count = 0
    modified_count = 0
    deleted_count = 0
    upserted_ids: dict[int, DocumentId] = {}
    write_errors: list[WriteErrorEntry] = []

    stop = False
    for batch, encoded in _encoded_bulk_batches(
        collection,
        prepared_requests,
        ordered=ordered,
        comment=comment,
        let=let,
        bypass_document_validation=bypass_document_validation,
    ):
        if stop:
            break
        batch_comment, normalized_bulk_let, batch_bypass, frame_ordered = (
            _admit_bulk_batch(collection, batch, encoded)
        )
        batch_context = None
        command_failure = None
        for prepared in batch:
            index = prepared.index
            request = prepared.request
            if batch_context is None:
                batch_context = collection._new_execution_context()
            try:
                variables = (
                    batch_context
                    if isinstance(request, InsertOne)
                    else batch_context.with_command_let(
                        request.let
                        if getattr(request, "let", None) is not None
                        else normalized_bulk_let,
                        dialect=collection._mongodb_dialect,
                        write=True,
                    )
                )
                if isinstance(request, InsertOne):
                    insert_kwargs = {"session": session}
                    if batch_bypass:
                        insert_kwargs["bypass_document_validation"] = True
                    await collection._insert_one(
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
                        "comment": request.comment
                        if request.comment is not None
                        else batch_comment,
                        "let": variables,
                        "session": session,
                    }
                    if batch_bypass:
                        update_one_kwargs["bypass_document_validation"] = True
                    result = await _collection_modify.execute_server_update(
                        collection,
                        request.filter,
                        request.update,
                        upsert=request.upsert,
                        **update_one_kwargs,
                    )
                    matched_count += 0 if result.did_upsert else result.matched_count
                    modified_count += result.modified_count
                    if result.did_upsert:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, UpdateMany):
                    update_many_kwargs = {
                        "collation": request.collation,
                        "array_filters": request.array_filters,
                        "hint": request.hint,
                        "comment": request.comment
                        if request.comment is not None
                        else batch_comment,
                        "let": variables,
                        "session": session,
                    }
                    if batch_bypass:
                        update_many_kwargs["bypass_document_validation"] = True
                    result = await _collection_modify.execute_server_update(
                        collection,
                        request.filter,
                        request.update,
                        multi=True,
                        upsert=request.upsert,
                        **update_many_kwargs,
                    )
                    matched_count += 0 if result.did_upsert else result.matched_count
                    modified_count += result.modified_count
                    if result.did_upsert:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, ReplaceOne):
                    replace_one_kwargs = {
                        "collation": request.collation,
                        "sort": request.sort,
                        "hint": request.hint,
                        "comment": request.comment
                        if request.comment is not None
                        else batch_comment,
                        "let": variables,
                        "session": session,
                    }
                    if batch_bypass:
                        replace_one_kwargs["bypass_document_validation"] = True
                    result = await _collection_modify.execute_server_update(
                        collection,
                        request.filter,
                        prepared.replacement_document or request.replacement,
                        replacement=True,
                        upsert=request.upsert,
                        **replace_one_kwargs,
                    )
                    matched_count += 0 if result.did_upsert else result.matched_count
                    modified_count += result.modified_count
                    if result.did_upsert:
                        upserted_ids[index] = result.upserted_id
                elif isinstance(request, DeleteOne):
                    result = await _collection_modify.execute_server_delete(
                        collection,
                        request.filter,
                        collation=request.collation,
                        hint=request.hint,
                        comment=request.comment
                        if request.comment is not None
                        else batch_comment,
                        let=variables,
                        session=session,
                    )
                    deleted_count += result.deleted_count
                elif isinstance(request, DeleteMany):
                    result = await _collection_modify.execute_server_delete(
                        collection,
                        request.filter,
                        multi=True,
                        collation=request.collation,
                        hint=request.hint,
                        comment=request.comment
                        if request.comment is not None
                        else batch_comment,
                        let=variables,
                        session=session,
                    )
                    deleted_count += result.deleted_count
            except (WriteError, OperationFailure, TypeError, ValueError) as exc:
                command_failure = record_write_failure(
                    write_errors,
                    exc,
                    index=index,
                    operation=prepared.error_operation,
                    first_failure=command_failure,
                )
                if frame_ordered:
                    # Driver grouping and the server's null default are distinct:
                    # ordered=None groups unordered, but each command is ordered.
                    stop = bool(ordered)
                    break

        if command_failure is not None:
            raise command_failure
        if stop:
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
                        document_id=materialize_public_value(upserted_id),
                    )
                    for op_index, upserted_id in upserted_ids.items()
                ],
            ).to_document(),
        )
    return result
