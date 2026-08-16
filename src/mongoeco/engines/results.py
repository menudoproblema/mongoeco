from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from mongoeco.types import BulkWriteResult


if TYPE_CHECKING:
    from mongoeco.types import (
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

    def __post_init__(self) -> None:
        matched = self.result.matched_count
        modified = self.result.modified_count
        if matched not in {0, 1} or modified not in {0, 1} or modified > matched:
            message = "mutation counts are inconsistent"
            raise ValueError(message)
        upserted = self.result.upserted_id is not None
        if upserted and matched != 0:
            message = "an upsert cannot also match an existing document"
            raise ValueError(message)
        if (
            matched == 0
            and not upserted
            and (self.before_document is not None or self.after_document is not None)
        ):
            message = "a non-matching mutation cannot expose images"
            raise ValueError(message)
        if upserted and (
            self.before_document is not None or self.after_document is None
        ):
            message = "an upsert must expose only its after image"
            raise ValueError(message)
        _validate_commit_sequence(self.commit_sequence)
        if self.commit_sequence is not None and modified == 0 and not upserted:
            message = "an unapplied mutation cannot have a commit sequence"
            raise ValueError(message)
        object.__setattr__(self, "before_document", deepcopy(self.before_document))
        object.__setattr__(self, "after_document", deepcopy(self.after_document))

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

    def __post_init__(self) -> None:
        deleted = self.result.deleted_count
        if deleted not in {0, 1}:
            message = "a single delete outcome must delete zero or one document"
            raise ValueError(message)
        if deleted == 0 and self.deleted_document is not None:
            message = "an unapplied delete cannot expose an image"
            raise ValueError(message)
        _validate_commit_sequence(self.commit_sequence)
        if deleted == 0 and self.commit_sequence is not None:
            message = "an unapplied delete cannot have a commit sequence"
            raise ValueError(message)
        object.__setattr__(self, "deleted_document", deepcopy(self.deleted_document))

    @property
    def deleted_count(self) -> int:
        return self.result.deleted_count


@dataclass(frozen=True, slots=True)
class InsertOutcome:
    applied: bool
    document: Document | None = None
    commit_sequence: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.applied, bool):
            message = "applied must be a bool"
            raise TypeError(message)
        if not self.applied and self.document is not None:
            message = "an unapplied insert cannot expose a document"
            raise ValueError(message)
        _validate_commit_sequence(self.commit_sequence)
        if not self.applied and self.commit_sequence is not None:
            message = "an unapplied insert cannot have a commit sequence"
            raise ValueError(message)
        object.__setattr__(self, "document", deepcopy(self.document))

    def __bool__(self) -> bool:
        return self.applied


@dataclass(frozen=True, slots=True)
class BulkOutcome:
    result: BulkWriteResult[DocumentId]
    mutations: tuple[InsertOutcome | MutationOutcome | DeleteOutcome, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.result, BulkWriteResult):
            message = "result must be a BulkWriteResult"
            raise TypeError(message)
        if not isinstance(self.mutations, tuple) or not all(
            isinstance(item, (InsertOutcome, MutationOutcome, DeleteOutcome))
            for item in self.mutations
        ):
            message = "mutations must be a tuple of write outcomes"
            raise TypeError(message)


@dataclass(frozen=True, slots=True)
class FindAndModifyOutcome:
    captured: MutationOutcome
    value: Document | None

    def __post_init__(self) -> None:
        if not isinstance(self.captured, MutationOutcome):
            message = "captured must be a MutationOutcome"
            raise TypeError(message)
        object.__setattr__(self, "value", deepcopy(self.value))


@dataclass(frozen=True, slots=True)
class MergeOutcome:
    matched: bool
    applied: bool
    operation_type: Literal["insert", "replace", "update"] | None = None
    before_document: Document | None = None
    after_document: Document | None = None
    commit_sequence: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.matched, bool) or not isinstance(
            self.applied,
            bool,
        ):
            message = "matched and applied must be bools"
            raise TypeError(message)
        if self.applied and (
            self.operation_type is None or self.after_document is None
        ):
            message = "an applied merge requires an operation and after image"
            raise ValueError(message)
        if not self.matched and self.before_document is not None:
            message = "an unmatched merge cannot expose a before image"
            raise ValueError(message)
        if self.applied and self.matched and self.operation_type == "insert":
            message = "a matched merge cannot apply an insert"
            raise ValueError(message)
        if self.applied and not self.matched and self.operation_type != "insert":
            message = "an unmatched merge can only apply an insert"
            raise ValueError(message)
        if self.applied and self.matched and self.before_document is None:
            message = "an applied matched merge requires a before image"
            raise ValueError(message)
        if (
            not self.applied
            and not self.matched
            and (self.operation_type is not None or self.after_document is not None)
        ):
            message = "an unapplied unmatched merge cannot expose an effect"
            raise ValueError(message)
        _validate_commit_sequence(self.commit_sequence)
        if not self.applied and self.commit_sequence is not None:
            message = "an unapplied merge cannot have a commit sequence"
            raise ValueError(message)
        object.__setattr__(self, "before_document", deepcopy(self.before_document))
        object.__setattr__(self, "after_document", deepcopy(self.after_document))


@dataclass(frozen=True, slots=True)
class CommittedChange:
    sequence: int
    payload: Document | None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.sequence, int)
            or isinstance(self.sequence, bool)
            or self.sequence < 1
        ):
            message = "committed change sequence must be positive"
            raise ValueError(message)
        object.__setattr__(self, "payload", deepcopy(self.payload))

    @property
    def is_gap(self) -> bool:
        return self.payload is None

    def for_delivery(self) -> CommittedChange:
        """Return an isolated callback-owned view of this committed record."""
        return CommittedChange(self.sequence, self.payload)


def _validate_commit_sequence(sequence: int | None) -> None:
    if sequence is None:
        return
    if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
        message = "commit_sequence must be a positive integer"
        raise ValueError(message)


# Compatibility aliases for the provisional outcome names introduced in 4.2.
EngineUpdateResult = MutationOutcome
EngineDeleteResult = DeleteOutcome
MergeDocumentResult = MergeOutcome
