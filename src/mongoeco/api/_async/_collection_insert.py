"""Public insert batches retain per-document errors above the atomic batch SPI."""

from __future__ import annotations

import time

from copy import deepcopy
from typing import TYPE_CHECKING

from mongoeco.api._async._active_operations import track_active_operation
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.identity import (
    assert_valid_root_document_id,
    is_valid_root_document_id,
)
from mongoeco.core.operation_context import ChangePublicationPolicy
from mongoeco.errors import (
    BulkWriteError,
    DocumentValidationFailure,
    DuplicateKeyError,
    WriteError,
)
from mongoeco.types import (
    BulkWriteErrorDetails,
    InsertManyResult,
    ObjectId,
    WriteErrorEntry,
)


if TYPE_CHECKING:
    from collections.abc import Iterable

    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.session import ClientSession
    from mongoeco.types import Document, DocumentId


async def execute_insert_many(  # noqa: PLR0913, PLR0915 - one public batch owns preparation, errors and publication
    collection: AsyncCollection,
    documents: Iterable[Document],
    *,
    ordered: bool,
    bypass_document_validation: bool,
    session: ClientSession | None,
    comment: object | None,
) -> InsertManyResult[DocumentId]:
    collection._ensure_session_active(session)
    comment = deepcopy(comment)
    if not isinstance(ordered, bool):
        message = "ordered must be a bool"
        raise TypeError(message)
    normalized = []
    public_documents = []
    for original in collection._require_documents(documents):
        if "_id" not in original:
            original["_id"] = DocumentCodec.to_pymongo(ObjectId())
        public_documents.append(deepcopy(original))
        normalized.append(DocumentCodec.to_internal(deepcopy(original)))
    context = collection._new_operation_context(
        session=session,
        change_operation_type="insert",
        publication=(
            ChangePublicationPolicy.EMIT
            if collection._should_publish_change_events(session=session)
            else ChangePublicationPolicy.RECORD_GAP
        ),
    )
    errors = []
    inserted = []

    def failed(index, error):
        errors.append(
            WriteErrorEntry(
                index=index,
                code=error.code,
                errmsg=str(error),
                operation=public_documents[index],
            )
        )

    pending = [(normalized, 0)]

    async def insert_batch(batch, offset):  # noqa: PLR0911, PLR0912 - batch and single writes share ordered outcomes
        for position, document in enumerate(batch):
            if is_valid_root_document_id(document["_id"]):
                continue
            if position:
                # Execute the valid prefix before accounting for this server
                # error. Do not require an engine to return partial ID failures.
                pending.append((batch[position:], offset + position))
                pending.append((batch[:position], offset))
                return
            try:
                assert_valid_root_document_id(document["_id"])
            except WriteError as error:
                failed(offset, error)
            if not ordered and batch[1:]:
                pending.append((batch[1:], offset + 1))
            return
        try:
            if not collection._runtime._engine_spi.capabilities.batch_inserts:
                # The adapter's single-write fallback can raise after a committed
                # prefix. Consume each outcome here so errors retain that prefix.
                raise NotImplementedError
            outcomes = await collection._runtime.engine_insert_documents(
                batch,
                session=session,
                bypass_document_validation=bypass_document_validation,
                operation_context=context.derive(change_event_index=offset),
                on_commit=lambda outcome: collection._publish_insert_outcome(
                    outcome, session=session
                ),
            )
        except DuplicateKeyError as error:
            # A singleton secondary-index collision raises; larger batches return
            # the committed prefix followed by a failed outcome.
            if len(batch) != 1:
                raise
            failed(offset, error)
            return
        except DocumentValidationFailure as error:
            # The batch SPI validates before publishing anything. Divide only a
            # rejected batch, retaining block preparation for the successful path.
            # Each sub-batch still validates under the engine's write authority.
            if len(batch) == 1:
                failed(offset, error)
                return
            middle = len(batch) // 2
            pending.append((batch[middle:], offset + middle))
            pending.append((batch[:middle], offset))
            return
        except NotImplementedError:
            # Engines without optional batch insertion keep their single-write
            # path, publication context and per-document validation.
            for index, document in enumerate(batch, offset):
                try:
                    outcome = await collection._runtime.engine_insert_document(
                        document,
                        overwrite=False,
                        session=session,
                        bypass_document_validation=bypass_document_validation,
                        operation_context=context.derive(change_event_index=index),
                        on_commit=lambda value: collection._publish_insert_outcome(
                            value, session=session
                        ),
                    )
                except WriteError as error:
                    failed(index, error)
                else:
                    if outcome:
                        inserted.append(document["_id"])
                        collection._notify_instrumented_insert_outcome(
                            outcome, session=session
                        )
                    else:
                        failed(index, DuplicateKeyError("Duplicate key"))
                if ordered and errors:
                    break
            return
        if len(outcomes) > len(batch) or (
            len(outcomes) != len(batch) and (not outcomes or outcomes[-1])
        ):
            message = (
                "bulk insert engine returned a result count "
                "different from the number of documents"
            )
            raise RuntimeError(message)
        for position, outcome in enumerate(outcomes):
            if not outcome:
                failed(offset + position, DuplicateKeyError("Duplicate key"))
                if not ordered and batch[position + 1 :]:
                    pending.append((batch[position + 1 :], offset + position + 1))
                return
            inserted.append(batch[position]["_id"])
            collection._notify_instrumented_insert_outcome(outcome, session=session)

    started = time.perf_counter_ns()

    def command():
        document = {
            "insert": collection._collection_name,
            "documents": deepcopy(normalized),
            "ordered": ordered,
            "bypassDocumentValidation": bypass_document_validation,
        }
        if comment is not None:
            document["comment"] = deepcopy(comment)
        return document

    try:
        with track_active_operation(
            collection._engine,
            command_name="insert",
            operation_type="write",
            namespace=f"{collection._db_name}.{collection._collection_name}",
            session=session,
            comment=comment,
            metadata={"kind": "insert_many", "documents": len(normalized)},
            killable=False,
        ):
            while pending and (not ordered or not errors):
                batch, offset = pending.pop()
                await insert_batch(batch, offset)
        collection._record_operation_metadata(
            operation="insert_many", comment=comment, session=session
        )
        if errors:
            message = "batch insert failed"
            raise BulkWriteError(
                message,
                details=BulkWriteErrorDetails(
                    write_errors=errors,
                    inserted_count=len(inserted),
                ).to_document(),
            )
    except Exception as error:
        await collection._profile_operation(
            op="insert",
            command_factory=command,
            duration_ns=time.perf_counter_ns() - started,
            errmsg=str(error),
        )
        raise
    await collection._profile_operation(
        op="insert",
        command_factory=command,
        duration_ns=time.perf_counter_ns() - started,
    )
    return InsertManyResult(
        inserted_ids=[DocumentCodec.to_pymongo(value) for value in inserted]
    )
