"""Mongoeco-owned telemetry primitives migrated from CXP 4.3.0 (MIT)."""

from __future__ import annotations

import time

from collections import deque
from collections.abc import Mapping  # noqa: TC003 - msgspec resolves field types
from threading import RLock
from typing import Any, Literal
from uuid import uuid4

import msgspec


type TelemetrySeverity = Literal["debug", "info", "warning", "error", "critical"]
type TelemetryItemKind = Literal["event", "metric", "span"]
type TelemetryOverflowPolicy = Literal["raise", "drop_newest", "drop_oldest"]
type ComponentStatus = Literal[
    "healthy",
    "degraded",
    "unhealthy",
    "starting",
    "stopping",
]


class TelemetryEvent(msgspec.Struct, frozen=True):
    """Un hecho puntual ocurrido en el componente."""

    event_type: str
    severity: TelemetrySeverity = "info"
    payload: Mapping[str, Any] = msgspec.field(default_factory=dict)
    timestamp: float = msgspec.field(default_factory=time.time)
    trace_id: str | None = None


class TelemetryMetric(msgspec.Struct, frozen=True):
    """Una medida cuantitativa agregada."""

    name: str
    value: float | int
    unit: str | None = None
    labels: Mapping[str, str] = msgspec.field(default_factory=dict)
    timestamp: float = msgspec.field(default_factory=time.time)


class TelemetrySpan(msgspec.Struct, frozen=True):
    """Una unidad temporal de trabajo ya cerrada."""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    start_time: float
    end_time: float
    attributes: Mapping[str, Any] = msgspec.field(default_factory=dict)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time


class TelemetryBufferOverflow(ValueError):  # noqa: N818 - public name
    """Raised when `TelemetryBuffer` exceeds capacity under `raise` policy."""


class TelemetrySnapshot(msgspec.Struct, frozen=True):
    """Un paquete de telemetría agregado para transporte eficiente."""

    provider_id: str
    status: ComponentStatus = "healthy"
    events: tuple[TelemetryEvent, ...] = ()
    metrics: tuple[TelemetryMetric, ...] = ()
    spans: tuple[TelemetrySpan, ...] = ()
    is_heartbeat: bool = False
    dropped_items: int = 0

    @classmethod
    def heartbeat(
        cls, provider_id: str, status: ComponentStatus = "healthy"
    ) -> TelemetrySnapshot:
        """Crea un snapshot de tipo heartbeat de forma rápida."""
        return cls(provider_id=provider_id, status=status, is_heartbeat=True)


class TelemetryContext:
    """Helper para propagar contexto técnico y operacional en telemetría."""

    def __init__(
        self,
        trace_id: str | None = None,
        *,
        request_id: str | None = None,
        session_id: str | None = None,
        operation_id: str | None = None,
        parent_operation_id: str | None = None,
    ) -> None:
        self.trace_id = trace_id
        self.request_id = request_id
        self.session_id = session_id
        self.operation_id = operation_id
        self.parent_operation_id = parent_operation_id
        self._generated_trace_id: str | None = None

    def _context_fields(self) -> dict[str, str]:
        fields: dict[str, str] = {}
        if self.request_id is not None:
            fields["cxp.request.id"] = self.request_id
        if self.session_id is not None:
            fields["cxp.session.id"] = self.session_id
        if self.operation_id is not None:
            fields["cxp.operation.id"] = self.operation_id
        if self.parent_operation_id is not None:
            fields["cxp.parent.operation.id"] = self.parent_operation_id
        return fields

    def _merge_mapping(
        self,
        values: Mapping[str, Any] | None,
    ) -> Mapping[str, Any]:
        merged: dict[str, Any] = self._context_fields()
        if values is not None:
            merged.update(values)
        return merged

    def _merge_labels(
        self,
        labels: Mapping[str, str] | None,
    ) -> Mapping[str, str]:
        merged = self._context_fields()
        if labels is not None:
            merged.update(labels)
        return merged

    def _effective_trace_id(self) -> str:
        if self.trace_id is not None:
            return self.trace_id

        if self._generated_trace_id is None:
            self._generated_trace_id = uuid4().hex

        return self._generated_trace_id

    def create_event(
        self,
        event_type: str,
        severity: TelemetrySeverity = "info",
        payload: Mapping[str, Any] | None = None,
    ) -> TelemetryEvent:
        """Crea un evento inyectando el trace_id del contexto."""
        return TelemetryEvent(
            event_type=event_type,
            severity=severity,
            payload=self._merge_mapping(payload),
            trace_id=self._effective_trace_id(),
        )

    def create_metric(
        self,
        name: str,
        value: float | int,  # noqa: PYI041 - preserve accepted values
        unit: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> TelemetryMetric:
        """Crea una métrica inyectando los IDs contextuales del contexto."""
        return TelemetryMetric(
            name=name,
            value=value,
            unit=unit,
            labels=self._merge_labels(labels),
        )

    def create_span(  # noqa: PLR0913 - preserve telemetry call contract
        self,
        name: str,
        *,
        start_time: float,
        end_time: float,
        parent_span_id: str | None = None,
        attributes: Mapping[str, Any] | None = None,
        span_id: str | None = None,
    ) -> TelemetrySpan:
        """Crea un span ya finalizado usando el trace_id del contexto."""
        return TelemetrySpan(
            trace_id=self._effective_trace_id(),
            span_id=span_id or uuid4().hex,
            parent_span_id=parent_span_id,
            name=name,
            start_time=start_time,
            end_time=end_time,
            attributes=self._merge_mapping(attributes),
        )


class TelemetryBuffer:
    """Buffer mutable para recolectar telemetría antes de un flush."""

    def __init__(
        self,
        provider_id: str,
        *,
        max_items: int | None = None,
        overflow_policy: TelemetryOverflowPolicy = "raise",
    ) -> None:
        if max_items is not None and max_items <= 0:
            msg = "TelemetryBuffer max_items must be greater than zero"
            raise ValueError(msg)

        self.provider_id = provider_id
        self.max_items = max_items
        self.overflow_policy = overflow_policy
        self._events: deque[TelemetryEvent] = deque()
        self._metrics: deque[TelemetryMetric] = deque()
        self._spans: deque[TelemetrySpan] = deque()
        self._items_order: deque[TelemetryItemKind] = deque()
        self._dropped_items = 0
        self._lock = RLock()

    def _item_count(self) -> int:
        return len(self._events) + len(self._metrics) + len(self._spans)

    @property
    def dropped_items(self) -> int:
        with self._lock:
            return self._dropped_items

    def _ensure_capacity(self, additional_items: int = 1) -> bool:
        if self.max_items is None:
            return True

        if self._item_count() + additional_items <= self.max_items:
            return True

        if self.overflow_policy == "raise":
            msg = f"TelemetryBuffer capacity exceeded: max_items={self.max_items}"
            raise TelemetryBufferOverflow(msg)

        self._dropped_items += additional_items
        if self.overflow_policy == "drop_newest":
            return False

        for _ in range(additional_items):
            self._drop_oldest_item()
        return True

    def _drop_oldest_item(self) -> None:
        if not self._items_order:
            return

        item_kind = self._items_order.popleft()
        if item_kind == "event":
            self._events.popleft()
        elif item_kind == "metric":
            self._metrics.popleft()
        else:
            self._spans.popleft()

    def _record_item(
        self,
        item_kind: TelemetryItemKind,
        item: TelemetryEvent | TelemetryMetric | TelemetrySpan,
    ) -> None:
        if not self._ensure_capacity():
            return

        if item_kind == "event":
            if not isinstance(item, TelemetryEvent):
                message = "Expected a telemetry event"
                raise TypeError(message)
            self._events.append(item)
        elif item_kind == "metric":
            if not isinstance(item, TelemetryMetric):
                message = "Expected a telemetry metric"
                raise TypeError(message)
            self._metrics.append(item)
        else:
            if not isinstance(item, TelemetrySpan):
                message = "Expected a telemetry span"
                raise TypeError(message)
            self._spans.append(item)
        self._items_order.append(item_kind)

    def record_event(self, event: TelemetryEvent) -> None:
        """Añade un evento al buffer."""
        with self._lock:
            self._record_item("event", event)

    def record_metric(
        self,
        name: str,
        value: float | int,  # noqa: PYI041 - preserve accepted values
        unit: str | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> None:
        """Crea y añade una métrica al buffer."""
        with self._lock:
            self._record_item(
                "metric",
                TelemetryMetric(
                    name=name,
                    value=value,
                    unit=unit,
                    labels=labels or {},
                ),
            )

    def record_span(self, span: TelemetrySpan) -> None:
        """Añade un span cerrado al buffer."""
        with self._lock:
            self._record_item("span", span)

    def flush(
        self,
        status: ComponentStatus = "healthy",
        is_heartbeat: bool = False,  # noqa: FBT001, FBT002 - preserve positional API
    ) -> TelemetrySnapshot:
        """Genera un snapshot con los datos acumulados y vacía el buffer."""
        with self._lock:
            snapshot = TelemetrySnapshot(
                provider_id=self.provider_id,
                status=status,
                events=tuple(self._events),
                metrics=tuple(self._metrics),
                spans=tuple(self._spans),
                is_heartbeat=is_heartbeat,
                dropped_items=self._dropped_items,
            )
            self._events.clear()
            self._metrics.clear()
            self._spans.clear()
            self._items_order.clear()
            self._dropped_items = 0
            return snapshot
