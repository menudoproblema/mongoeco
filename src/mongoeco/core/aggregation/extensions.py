from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock
from typing import TYPE_CHECKING, Any

from mongoeco.compat import MongoDialect
from mongoeco.types import Document

if TYPE_CHECKING:
    from mongoeco.core.aggregation.runtime import AggregationStageContext


@dataclass(frozen=True, slots=True)
class AggregationExpressionExtensionContext:
    evaluate_expression: Callable[[Document, object, dict[str, Any] | None], Any]
    evaluate_expression_with_missing: Callable[[Document, object, dict[str, Any] | None], Any]
    require_expression_args: Callable[[str, object, int, int | None], list[object]]
    missing_sentinel: object


type AggregationExpressionExtensionHandler = Callable[
    [Document, object, dict[str, Any], MongoDialect, AggregationExpressionExtensionContext],
    Any,
]

type AggregationStageExtensionHandler = Callable[
    [list[Document], object, "AggregationStageContext"],
    list[Document],
]


_expression_lock = RLock()
_stage_lock = RLock()
_expression_handlers: dict[str, AggregationExpressionExtensionHandler] = {}
_stage_handlers: dict[str, AggregationStageExtensionHandler] = {}


def _require_operator_name(name: str) -> str:
    if not isinstance(name, str) or not name.startswith("$") or len(name) == 1:
        raise ValueError("aggregation extension names must be non-empty operators starting with '$'")
    return name


def register_aggregation_expression_operator(
    name: str,
    handler: AggregationExpressionExtensionHandler,
) -> None:
    operator = _require_operator_name(name)
    if not callable(handler):
        raise TypeError("handler must be callable")
    with _expression_lock:
        _expression_handlers[operator] = handler


def unregister_aggregation_expression_operator(name: str) -> None:
    operator = _require_operator_name(name)
    with _expression_lock:
        _expression_handlers.pop(operator, None)


def get_registered_aggregation_expression_operator(
    name: str,
) -> AggregationExpressionExtensionHandler | None:
    with _expression_lock:
        return _expression_handlers.get(name)


def registered_aggregation_expression_operator(
    name: str,
) -> Callable[[AggregationExpressionExtensionHandler], AggregationExpressionExtensionHandler]:
    def _decorator(
        handler: AggregationExpressionExtensionHandler,
    ) -> AggregationExpressionExtensionHandler:
        register_aggregation_expression_operator(name, handler)
        return handler

    return _decorator


def register_aggregation_stage(
    name: str,
    handler: AggregationStageExtensionHandler,
) -> None:
    operator = _require_operator_name(name)
    if not callable(handler):
        raise TypeError("handler must be callable")
    with _stage_lock:
        _stage_handlers[operator] = handler


def unregister_aggregation_stage(name: str) -> None:
    operator = _require_operator_name(name)
    with _stage_lock:
        _stage_handlers.pop(operator, None)


def get_registered_aggregation_stage(
    name: str,
) -> AggregationStageExtensionHandler | None:
    with _stage_lock:
        return _stage_handlers.get(name)


def registered_aggregation_stage(
    name: str,
) -> Callable[[AggregationStageExtensionHandler], AggregationStageExtensionHandler]:
    def _decorator(
        handler: AggregationStageExtensionHandler,
    ) -> AggregationStageExtensionHandler:
        register_aggregation_stage(name, handler)
        return handler

    return _decorator
