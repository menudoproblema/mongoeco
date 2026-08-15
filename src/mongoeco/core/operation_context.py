from __future__ import annotations

import uuid

from copy import deepcopy
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Literal

from mongoeco.compat import MongoDialect
from mongoeco.core.collation import CollationSpec, normalize_collation
from mongoeco.core.expression_context import (
    ExpressionExecutionContext,
    ensure_expression_context,
)
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession
from mongoeco.types import CodecOptions, normalize_codec_options


if TYPE_CHECKING:
    from collections.abc import Mapping


type ChangeOperationType = Literal['insert', 'update', 'replace', 'delete']
_UNSET = object()


class ChangePublicationPolicy(StrEnum):
    DISABLED = 'disabled'
    RECORD_GAP = 'record-gap'
    EMIT = 'emit'


@dataclass(frozen=True, slots=True)
class OperationContext:
    """Immutable values captured once at the public operation boundary."""

    dialect: MongoDialect
    expressions: ExpressionExecutionContext
    codec_options: CodecOptions
    session: ClientSession | None = None
    collation: CollationSpec | None = None
    publication: ChangePublicationPolicy = ChangePublicationPolicy.DISABLED
    change_operation_type: ChangeOperationType | None = None
    operation_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    change_event_index: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.dialect, MongoDialect):
            message = 'dialect must be a MongoDialect'
            raise TypeError(message)
        if not isinstance(self.expressions, ExpressionExecutionContext):
            message = 'expressions must be an ExpressionExecutionContext'
            raise TypeError(message)
        if not isinstance(self.codec_options, CodecOptions):
            message = 'codec_options must be CodecOptions'
            raise TypeError(message)
        if self.collation is not None and not isinstance(
            self.collation,
            CollationSpec,
        ):
            message = 'collation must be a CollationSpec'
            raise TypeError(message)
        if self.session is not None and not isinstance(
            self.session,
            ClientSession,
        ):
            message = 'session must be a ClientSession'
            raise TypeError(message)
        if not isinstance(self.operation_id, str) or not self.operation_id:
            message = 'operation_id must be a non-empty string'
            raise ValueError(message)
        if not isinstance(self.publication, ChangePublicationPolicy):
            message = 'publication must be a ChangePublicationPolicy'
            raise TypeError(message)
        if self.change_operation_type not in {
            None,
            'insert',
            'update',
            'replace',
            'delete',
        }:
            message = 'change_operation_type is not supported'
            raise ValueError(message)
        if (
            not isinstance(self.change_event_index, int)
            or isinstance(self.change_event_index, bool)
            or self.change_event_index < 0
        ):
            message = 'change_event_index must be a non-negative integer'
            raise ValueError(message)

    @classmethod
    def create(  # noqa: PLR0913 - canonical public-operation boundary
        cls,
        *,
        dialect: MongoDialect,
        codec_options: CodecOptions | None = None,
        session: ClientSession | None = None,
        collation: object | None = None,
        bindings: Mapping[str, object] | None = None,
        expressions: ExpressionExecutionContext | None = None,
        publication: ChangePublicationPolicy = (
            ChangePublicationPolicy.DISABLED
        ),
        change_operation_type: ChangeOperationType | None = None,
        change_event_index: int = 0,
    ) -> OperationContext:
        if expressions is not None and bindings is not None:
            message = 'expressions and bindings are mutually exclusive'
            raise ValueError(message)
        execution_context = (
            expressions
            if expressions is not None
            else ensure_expression_context(
                None if bindings is None else deepcopy(dict(bindings)),
            )
        )
        normalized_collation = normalize_collation(collation)
        return cls(
            dialect=dialect,
            expressions=execution_context,
            codec_options=normalize_codec_options(codec_options),
            session=session,
            collation=(
                None
                if normalized_collation is None
                else deepcopy(normalized_collation)
            ),
            publication=publication,
            change_operation_type=change_operation_type,
            change_event_index=change_event_index,
        )

    def derive(
        self,
        *,
        bindings: Mapping[str, object] | None = None,
        collation: object = _UNSET,
        publication: ChangePublicationPolicy | None = None,
        change_operation_type: ChangeOperationType | object | None = _UNSET,
        change_event_index: int | None = None,
    ) -> OperationContext:
        next_collation = self.collation
        if collation is not _UNSET:
            normalized = normalize_collation(collation)
            next_collation = (
                None if normalized is None else deepcopy(normalized)
            )
        return replace(
            self,
            expressions=self.expressions.with_bindings(
                None if bindings is None else deepcopy(dict(bindings)),
            ),
            collation=next_collation,
            publication=(
                self.publication if publication is None else publication
            ),
            change_operation_type=(
                self.change_operation_type
                if change_operation_type is _UNSET
                else change_operation_type
            ),
            change_event_index=(
                self.change_event_index
                if change_event_index is None
                else change_event_index
            ),
        )

    def for_unpublishable_change(self) -> OperationContext:
        """Preserve sequence continuity when no valid event can be emitted."""
        if self.publication is not ChangePublicationPolicy.EMIT:
            return self
        return self.derive(publication=ChangePublicationPolicy.RECORD_GAP)


def resolve_operation_session(
    context: OperationContext | None,
    requested_session: ClientSession | None,
) -> ClientSession | None:
    """Resolve the sole session authority for a bound operation."""
    if context is None:
        return requested_session
    if (
        requested_session is not None
        and requested_session is not context.session
    ):
        message = 'requested session diverges from the bound OperationContext'
        raise InvalidOperation(message)
    return context.session
