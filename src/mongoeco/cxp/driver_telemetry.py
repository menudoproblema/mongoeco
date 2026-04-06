from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from cxp.telemetry import (
    TelemetryBuffer,
    TelemetryContext,
    TelemetryOverflowPolicy,
    TelemetrySnapshot,
)
from mongoeco.driver.monitoring import (
    CommandFailedEvent,
    CommandStartedEvent,
    CommandSucceededEvent,
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    DriverEvent,
    DriverEventListener,
    DriverMonitor,
    ServerSelectedEvent,
)

_READ_COMMANDS = {
    "find": "find",
    "findone": "find_one",
    "find_one": "find_one",
    "count": "count_documents",
    "countdocuments": "count_documents",
    "count_documents": "count_documents",
    "estimateddocumentcount": "estimated_document_count",
    "estimated_document_count": "estimated_document_count",
    "distinct": "distinct",
}
_SEARCH_STAGE_NAME = "$search"
_VECTOR_SEARCH_STAGE_NAME = "$vectorSearch"
_SEARCH_NON_OPERATOR_KEYS = {"index"}


@dataclass(slots=True)
class _PendingTelemetry:
    trace_id: str
    span_name: str
    metric_name: str
    event_type: str
    start_time: float
    span_attributes: dict[str, Any]
    metric_labels: dict[str, str]
    event_payload: dict[str, Any]


class DriverTelemetryProjector:
    """Project driver monitor events into canonical CXP MongoDB telemetry."""

    def __init__(
        self,
        *,
        provider_id: str,
        buffer: TelemetryBuffer | None = None,
        max_items: int | None = None,
        overflow_policy: TelemetryOverflowPolicy = "raise",
    ) -> None:
        if buffer is not None and buffer.provider_id != provider_id:
            msg = "telemetry buffer provider_id does not match projector provider_id"
            raise ValueError(msg)
        self._provider_id = provider_id
        self._buffer = (
            TelemetryBuffer(
                provider_id,
                max_items=max_items,
                overflow_policy=overflow_policy,
            )
            if buffer is None
            else buffer
        )
        self._pending: dict[str, _PendingTelemetry] = {}
        self._attached: list[tuple[DriverMonitor, DriverEventListener]] = []

    @property
    def provider_id(self) -> str:
        return self._provider_id

    def attach(self, monitor: DriverMonitor) -> None:
        for existing_monitor, _listener in self._attached:
            if existing_monitor is monitor:
                return
        listener = self.handle_driver_event
        monitor.add_listener(listener)
        self._attached.append((monitor, listener))

    def detach(self, monitor: DriverMonitor) -> None:
        for index, (existing_monitor, listener) in enumerate(tuple(self._attached)):
            if existing_monitor is not monitor:
                continue
            monitor.remove_listener(listener)
            del self._attached[index]
            return

    def clear(self) -> None:
        self._pending.clear()
        self._buffer.flush()

    def snapshot(self) -> TelemetrySnapshot:
        return self._buffer.flush()

    def handle_driver_event(self, event: DriverEvent) -> None:
        if isinstance(event, CommandStartedEvent):
            self._handle_command_started(event)
            return
        if isinstance(event, CommandSucceededEvent):
            self._handle_command_finished(event, outcome="succeeded")
            return
        if isinstance(event, CommandFailedEvent):
            self._handle_command_finished(event, outcome="failed")
            return
        if isinstance(
            event,
            (ServerSelectedEvent, ConnectionCheckedOutEvent, ConnectionCheckedInEvent),
        ):
            return

    def _handle_command_started(self, event: CommandStartedEvent) -> None:
        if event.request_id is None:
            return
        pending = _build_pending_telemetry(event)
        if pending is None:
            return
        self._pending[event.request_id] = pending

    def _handle_command_finished(
        self,
        event: CommandSucceededEvent | CommandFailedEvent,
        *,
        outcome: str,
    ) -> None:
        if event.request_id is None:
            return
        pending = self._pending.pop(event.request_id, None)
        if pending is None:
            return
        context = TelemetryContext(trace_id=pending.trace_id)
        end_time = time.time()
        if end_time < pending.start_time:
            end_time = pending.start_time
        self._buffer.record_span(
            context.create_span(
                pending.span_name,
                start_time=pending.start_time,
                end_time=end_time,
                attributes=pending.span_attributes,
            )
        )
        metric_labels = {
            **pending.metric_labels,
            "db.operation.outcome": outcome,
        }
        duration_seconds = max(event.duration_ms, 0.0) / 1000
        self._buffer.record_metric(
            pending.metric_name,
            duration_seconds,
            unit="s",
            labels=metric_labels,
        )
        severity = "error" if outcome == "failed" else "info"
        self._buffer.record_event(
            context.create_event(
                pending.event_type,
                severity=severity,
                payload={
                    **pending.event_payload,
                    "db.operation.outcome": outcome,
                },
            )
        )


def _build_pending_telemetry(event: CommandStartedEvent) -> _PendingTelemetry | None:
    classification = _classify_command(event)
    if classification is None:
        return None
    return _PendingTelemetry(
        trace_id=event.request_id or "",
        span_name=classification["span_name"],
        metric_name=classification["metric_name"],
        event_type=classification["event_type"],
        start_time=time.time(),
        span_attributes=classification["span_attributes"],
        metric_labels=classification["metric_labels"],
        event_payload=classification["event_payload"],
    )


def _classify_command(event: CommandStartedEvent) -> dict[str, Any] | None:
    normalized_command_name = _normalize_command_name(event.command_name)
    if normalized_command_name == "aggregate":
        return _classify_aggregate_command(event)

    operation_name = _READ_COMMANDS.get(normalized_command_name)
    if operation_name is None:
        operation_name = _resolve_write_operation_name(
            normalized_command_name,
            event.command,
        )
    if operation_name is None:
        return None

    namespace = _resolve_namespace(event.database, event.command_name, event.command)
    if namespace is None:
        return None

    return _build_classification(
        span_name="db.client.operation",
        metric_name="db.client.operation.duration",
        event_type="db.client.operation.completed",
        operation_name=operation_name,
        namespace=namespace,
    )


def _classify_aggregate_command(
    event: CommandStartedEvent,
) -> dict[str, Any] | None:
    namespace = _resolve_namespace(event.database, event.command_name, event.command)
    if namespace is None:
        return None
    pipeline = event.command.get("pipeline")
    if not isinstance(pipeline, list):
        return None
    if pipeline and isinstance(pipeline[0], dict):
        first_stage = pipeline[0]
        if _SEARCH_STAGE_NAME in first_stage:
            operator = _resolve_search_operator(first_stage[_SEARCH_STAGE_NAME])
            if operator is None:
                return None
            return _build_classification(
                span_name="db.client.search",
                metric_name="db.client.search.duration",
                event_type="db.client.search.completed",
                operation_name="aggregate",
                namespace=namespace,
                stage_name=_SEARCH_STAGE_NAME,
                search_operator=operator,
            )
        if _VECTOR_SEARCH_STAGE_NAME in first_stage:
            similarity = _resolve_vector_similarity(first_stage[_VECTOR_SEARCH_STAGE_NAME])
            if similarity is None:
                return None
            return _build_classification(
                span_name="db.client.vector_search",
                metric_name="db.client.vector_search.duration",
                event_type="db.client.vector_search.completed",
                operation_name="aggregate",
                namespace=namespace,
                stage_name=_VECTOR_SEARCH_STAGE_NAME,
                vector_similarity=similarity,
            )
    return _build_classification(
        span_name="db.client.aggregate",
        metric_name="db.client.aggregate.duration",
        event_type="db.client.aggregate.completed",
        operation_name="aggregate",
        namespace=namespace,
        stage_count=len(pipeline),
    )


def _build_classification(
    *,
    span_name: str,
    metric_name: str,
    event_type: str,
    operation_name: str,
    namespace: str,
    stage_count: int | None = None,
    stage_name: str | None = None,
    search_operator: str | None = None,
    vector_similarity: str | None = None,
) -> dict[str, Any]:
    span_attributes: dict[str, Any] = {
        "db.system.name": "mongodb",
        "db.operation.name": operation_name,
        "db.namespace.name": namespace,
    }
    metric_labels: dict[str, str] = {
        "db.system.name": "mongodb",
        "db.operation.name": operation_name,
    }
    event_payload: dict[str, Any] = {
        "db.system.name": "mongodb",
        "db.operation.name": operation_name,
        "db.namespace.name": namespace,
    }
    if stage_count is not None:
        span_attributes["db.pipeline.stage.count"] = stage_count
        event_payload["db.pipeline.stage.count"] = stage_count
    if stage_name is not None:
        span_attributes["db.pipeline.stage.name"] = stage_name
        metric_labels["db.pipeline.stage.name"] = stage_name
        event_payload["db.pipeline.stage.name"] = stage_name
    if search_operator is not None:
        span_attributes["db.search.operator"] = search_operator
        event_payload["db.search.operator"] = search_operator
    if vector_similarity is not None:
        span_attributes["db.vector_search.similarity"] = vector_similarity
        metric_labels["db.vector_search.similarity"] = vector_similarity
        event_payload["db.vector_search.similarity"] = vector_similarity
    return {
        "span_name": span_name,
        "metric_name": metric_name,
        "event_type": event_type,
        "span_attributes": span_attributes,
        "metric_labels": metric_labels,
        "event_payload": event_payload,
    }


def _normalize_command_name(command_name: str) -> str:
    return command_name.replace("-", "_").casefold()


def _resolve_namespace(
    database: str,
    command_name: str,
    payload: dict[str, Any],
) -> str | None:
    collection_name = _resolve_collection_name(command_name, payload)
    if collection_name is None:
        return None
    return f"{database}.{collection_name}"


def _resolve_collection_name(
    command_name: str,
    payload: dict[str, Any],
) -> str | None:
    candidate_keys = (
        command_name,
        command_name.casefold(),
        "find",
        "count",
        "distinct",
        "aggregate",
        "insert",
        "update",
        "delete",
        "findAndModify",
        "findandmodify",
    )
    for key in candidate_keys:
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _resolve_write_operation_name(
    normalized_command_name: str,
    payload: dict[str, Any],
) -> str | None:
    if normalized_command_name == "insert":
        documents = payload.get("documents")
        if isinstance(documents, list):
            return "insert_one" if len(documents) == 1 else "insert_many"
        return "insert"
    if normalized_command_name == "update":
        updates = payload.get("updates")
        if (
            isinstance(updates, list)
            and len(updates) == 1
            and isinstance(updates[0], dict)
        ):
            return "update_many" if bool(updates[0].get("multi")) else "update_one"
        return "update"
    if normalized_command_name == "delete":
        deletes = payload.get("deletes")
        if (
            isinstance(deletes, list)
            and len(deletes) == 1
            and isinstance(deletes[0], dict)
        ):
            return "delete_one" if deletes[0].get("limit") == 1 else "delete_many"
        return "delete"
    if normalized_command_name in {"bulkwrite", "bulk_write"}:
        return "bulk_write"
    if normalized_command_name in {"findandmodify", "find_and_modify"}:
        if payload.get("remove") is True:
            return "delete_one"
        update_spec = payload.get("update")
        if isinstance(update_spec, dict):
            has_operator_keys = any(
                isinstance(key, str) and key.startswith("$")
                for key in update_spec
            )
            return "update_one" if has_operator_keys else "replace_one"
        return "findAndModify"
    return None


def _resolve_search_operator(search_spec: Any) -> str | None:
    if not isinstance(search_spec, dict):
        return None
    for key in search_spec:
        if key not in _SEARCH_NON_OPERATOR_KEYS:
            return str(key)
    return None


def _resolve_vector_similarity(vector_spec: Any) -> str | None:
    if not isinstance(vector_spec, dict):
        return None
    similarity = vector_spec.get("similarity")
    return similarity if isinstance(similarity, str) and similarity else None
