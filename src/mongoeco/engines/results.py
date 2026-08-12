from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:
    from mongoeco.types import (
        BulkWriteResult,
        DeleteResult,
        Document,
        DocumentId,
        UpdateResult,
    )


@dataclass(frozen=True, slots=True)
class MutationOutcome:
    result: UpdateResult[DocumentId]
    before_document: Document | None = None
    after_document: Document | None = None
    commit_sequence: int | None = None

    @property
    def matched_count(self) -> int:
        return self.result.matched_count

    @property
    def modified_count(self) -> int:
        return self.result.modified_count

    @property
    def upserted_id(self) -> DocumentId | None:
        return self.result.upserted_id


@dataclass(frozen=True, slots=True)
class DeleteOutcome:
    result: DeleteResult
    deleted_document: Document | None = None
    commit_sequence: int | None = None

    @property
    def deleted_count(self) -> int:
        return self.result.deleted_count


@dataclass(frozen=True, slots=True)
class InsertOutcome:
    applied: bool
    document: Document | None = None
    commit_sequence: int | None = None

    def __bool__(self) -> bool:
        return self.applied


@dataclass(frozen=True, slots=True)
class BulkOutcome:
    result: BulkWriteResult[DocumentId]
    mutations: tuple[InsertOutcome | MutationOutcome | DeleteOutcome, ...] = ()


@dataclass(frozen=True, slots=True)
class FindAndModifyOutcome:
    captured: MutationOutcome
    value: Document | None


@dataclass(frozen=True, slots=True)
class MergeOutcome:
    matched: bool
    applied: bool
    operation_type: Literal['insert', 'replace', 'update'] | None = None
    before_document: Document | None = None
    after_document: Document | None = None
    commit_sequence: int | None = None


@dataclass(frozen=True, slots=True)
class CommittedChange:
    sequence: int
    payload: Document | None

    @property
    def is_gap(self) -> bool:
        return self.payload is None


# Compatibility aliases for the provisional outcome names introduced in 4.2.
EngineUpdateResult = MutationOutcome
EngineDeleteResult = DeleteOutcome
MergeDocumentResult = MergeOutcome
