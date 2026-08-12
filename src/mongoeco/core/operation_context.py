from __future__ import annotations

import uuid

from copy import deepcopy
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Literal

from mongoeco.core.collation import CollationSpec, normalize_collation
from mongoeco.core.expression_context import (
    ExpressionExecutionContext,
    ensure_expression_context,
)
from mongoeco.types import CodecOptions, normalize_codec_options


if TYPE_CHECKING:
    from collections.abc import Mapping

    from mongoeco.compat import MongoDialect
    from mongoeco.session import ClientSession


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

    def __post_init__(self) -> None:
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
        )

    def derive(
        self,
        *,
        bindings: Mapping[str, object] | None = None,
        collation: object = _UNSET,
        publication: ChangePublicationPolicy | None = None,
        change_operation_type: ChangeOperationType | None | object = _UNSET,
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
        )
