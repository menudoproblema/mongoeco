from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any

from mongoeco.core.bson_scalars import normalize_utc_bson_datetime, utc_bson_now


_CURRENT_EXECUTION_NOW: ContextVar[datetime | None] = ContextVar(
    'mongoeco_execution_now', default=None
)


def current_execution_now() -> datetime | None:
    return _CURRENT_EXECUTION_NOW.get()


def set_execution_now(now: datetime) -> None:
    _CURRENT_EXECUTION_NOW.set(now)


@dataclass(frozen=True, slots=True)
class ExpressionExecutionContext(Mapping[str, Any]):
    """Bindings inmutables compartidos por una ejecución de expresiones."""

    bindings: Mapping[str, Any] = field(default_factory=dict)
    now: datetime = field(default_factory=utc_bson_now)

    def __post_init__(self) -> None:
        immutable_bindings = MappingProxyType(dict(self.bindings))
        object.__setattr__(self, "bindings", immutable_bindings)

    def __getitem__(self, key: str) -> Any:
        if key == "NOW":
            return self.now
        return self.bindings[key]

    def __iter__(self) -> Iterator[str]:
        yield from self.bindings
        if "NOW" not in self.bindings:
            yield "NOW"

    def __len__(self) -> int:
        return len(self.bindings) + (0 if "NOW" in self.bindings else 1)

    def with_bindings(
        self,
        bindings: Mapping[str, Any] | None,
    ) -> ExpressionExecutionContext:
        if not bindings:
            return self
        inherited = (
            bindings.bindings
            if isinstance(bindings, ExpressionExecutionContext)
            else bindings
        )
        combined_bindings = {**self.bindings, **inherited}
        return ExpressionExecutionContext(combined_bindings, now=self.now)


def ensure_expression_context(
    variables: Mapping[str, Any] | None,
    *,
    now: datetime | None = None,
) -> ExpressionExecutionContext:
    if isinstance(variables, ExpressionExecutionContext):
        return variables
    if variables is None:
        return (
            ExpressionExecutionContext(now=now)
            if now is not None
            else ExpressionExecutionContext()
        )
    bindings = dict(variables)
    inherited_now = bindings.pop("NOW", None)
    if inherited_now is not None:
        return ExpressionExecutionContext(
            bindings, now=normalize_utc_bson_datetime(inherited_now)
        )
    return (
        ExpressionExecutionContext(bindings, now=now)
        if now is not None
        else ExpressionExecutionContext(bindings)
    )
