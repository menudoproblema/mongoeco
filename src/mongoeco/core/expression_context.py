from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from types import MappingProxyType
from typing import Any

from mongoeco.core.bson_scalars import normalize_utc_bson_datetime, utc_bson_now
from mongoeco.core.codec import DocumentCodec


_CURRENT_EXECUTION_NOW: ContextVar[datetime | None] = ContextVar(
    'mongoeco_execution_now', default=None
)


class _FrozenDocument(dict[str, Any]):
    def _immutable(self, *_args: object, **_kwargs: object) -> None:
        message = 'expression bindings are immutable'
        raise TypeError(message)

    __setitem__ = _immutable
    __delitem__ = _immutable
    __ior__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable

    def __deepcopy__(self, _memo: dict[int, object]) -> _FrozenDocument:
        return self


class _FrozenList(list[Any]):
    def _immutable(self, *_args: object, **_kwargs: object) -> None:
        message = 'expression bindings are immutable'
        raise TypeError(message)

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable

    def __deepcopy__(self, _memo: dict[int, object]) -> _FrozenList:
        return self


def _freeze_bson(value: Any) -> Any:
    if isinstance(value, dict):
        frozen = _FrozenDocument()
        for key, item in value.items():
            dict.__setitem__(frozen, key, _freeze_bson(item))
        return frozen
    if isinstance(value, list):
        frozen_list = _FrozenList()
        for item in value:
            list.append(frozen_list, _freeze_bson(item))
        return frozen_list
    return value


def current_execution_now() -> datetime | None:
    return _CURRENT_EXECUTION_NOW.get()


@contextmanager
def execution_now_scope(now: datetime):
    token = _CURRENT_EXECUTION_NOW.set(now)
    try:
        yield
    finally:
        _CURRENT_EXECUTION_NOW.reset(token)


@dataclass(frozen=True, slots=True)
class ExpressionExecutionContext(Mapping[str, Any]):
    """Bindings inmutables compartidos por una ejecución de expresiones."""

    bindings: Mapping[str, Any] = field(default_factory=dict)
    now: datetime = field(default_factory=utc_bson_now)

    def __post_init__(self) -> None:
        source = self.bindings
        normalized = _freeze_bson(
            source
            if DocumentCodec.is_internal(source)
            else DocumentCodec.to_internal(dict(source)),
        )
        immutable_bindings = MappingProxyType(normalized)
        object.__setattr__(self, "bindings", immutable_bindings)
        object.__setattr__(self, 'now', normalize_utc_bson_datetime(self.now))

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
        normalized_inherited = (
            inherited
            if DocumentCodec.is_internal(inherited)
            else DocumentCodec.to_internal(dict(inherited))
        )
        combined_bindings = DocumentCodec.mark_internal(
            {**self.bindings, **normalized_inherited},
        )
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
