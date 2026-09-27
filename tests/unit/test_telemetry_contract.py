"""Owner oracles for telemetry context and bounded transport buffering."""

import pytest

from mongoeco.telemetry_contract import (
    TelemetryBuffer,
    TelemetryBufferOverflow,
    TelemetryContext,
    TelemetryEvent,
    TelemetrySnapshot,
    TelemetrySpan,
)


EXPECTED_DURATION = 2.25
EXPECTED_COUNT = 3


def _span() -> TelemetrySpan:
    return TelemetrySpan(
        trace_id="trace",
        span_id="span",
        parent_span_id=None,
        name="work",
        start_time=1.25,
        end_time=3.5,
    )


def test_context_propagates_ids_and_keeps_one_trace_across_signals() -> None:
    context = TelemetryContext(
        request_id="request",
        session_id="session",
        operation_id="operation",
        parent_operation_id="parent",
    )
    event = context.create_event("started", payload={"custom": True})
    metric = context.create_metric("duration", 2, "s", labels={"phase": "read"})
    span = context.create_span(
        "read", start_time=1.25, end_time=3.5, attributes={"custom": True}
    )

    expected_ids = {
        "cxp.request.id": "request",
        "cxp.session.id": "session",
        "cxp.operation.id": "operation",
        "cxp.parent.operation.id": "parent",
    }
    assert event.trace_id == span.trace_id
    assert event.trace_id
    assert event.payload == {**expected_ids, "custom": True}
    assert metric.labels == {**expected_ids, "phase": "read"}
    assert span.attributes == {**expected_ids, "custom": True}
    assert span.duration == EXPECTED_DURATION

    explicit = TelemetryContext(trace_id="fixed")
    assert explicit.create_event("done").trace_id == "fixed"
    assert explicit.create_metric("count", 1).labels == {}
    assert explicit.create_span("done", start_time=0, end_time=1).trace_id == "fixed"


def test_buffer_flush_preserves_each_kind_and_resets_state() -> None:
    buffer = TelemetryBuffer("provider")
    event = TelemetryEvent("started")
    span = _span()
    buffer.record_event(event)
    buffer.record_metric("count", EXPECTED_COUNT, labels={"phase": "read"})
    buffer.record_span(span)

    snapshot = buffer.flush(status="degraded", is_heartbeat=True)
    assert snapshot.provider_id == "provider"
    assert snapshot.status == "degraded"
    assert snapshot.is_heartbeat
    assert snapshot.events == (event,)
    assert snapshot.metrics[0].name == "count"
    assert snapshot.metrics[0].value == EXPECTED_COUNT
    assert snapshot.metrics[0].labels == {"phase": "read"}
    assert snapshot.spans == (span,)
    assert snapshot.dropped_items == 0
    assert buffer.flush().events == ()
    assert buffer.flush().metrics == ()
    assert buffer.flush().spans == ()
    assert TelemetrySnapshot.heartbeat("provider").is_heartbeat


def test_buffer_raise_and_drop_newest_preserve_the_existing_signal() -> None:
    event = TelemetryEvent("first")
    raising = TelemetryBuffer("provider", max_items=1)
    raising.record_event(event)
    with pytest.raises(TelemetryBufferOverflow, match="max_items=1"):
        raising.record_metric("later", 2)
    assert raising.flush().events == (event,)

    dropping = TelemetryBuffer(
        "provider", max_items=1, overflow_policy="drop_newest"
    )
    dropping.record_event(event)
    dropping.record_span(_span())
    assert dropping.dropped_items == 1
    snapshot = dropping.flush()
    assert snapshot.events == (event,)
    assert snapshot.spans == ()
    assert snapshot.dropped_items == 1
    assert dropping.dropped_items == 0


def test_drop_oldest_evicts_across_signal_kinds_without_reordering() -> None:
    event = TelemetryEvent("event")
    span = _span()
    cases = (
        ("event", "metric", "span"),
        ("metric", "span", "event"),
        ("span", "event", "metric"),
    )
    for kinds in cases:
        buffer = TelemetryBuffer(
            "provider", max_items=2, overflow_policy="drop_oldest"
        )
        for kind in kinds:
            if kind == "event":
                buffer.record_event(event)
            elif kind == "metric":
                buffer.record_metric("metric", 1)
            else:
                buffer.record_span(span)
        snapshot = buffer.flush()
        assert snapshot.dropped_items == 1
        assert bool(snapshot.events) == ("event" in kinds[1:])
        assert bool(snapshot.metrics) == ("metric" in kinds[1:])
        assert bool(snapshot.spans) == ("span" in kinds[1:])


def test_buffer_rejects_invalid_limits_policies_and_signal_types() -> None:
    with pytest.raises(ValueError, match="max_items"):
        TelemetryBuffer("provider", max_items=0)
    with pytest.raises(ValueError, match="overflow policy"):
        TelemetryBuffer("provider", overflow_policy="unbounded")  # type: ignore[arg-type]

    buffer = TelemetryBuffer("provider")
    with pytest.raises(TypeError, match="telemetry event"):
        buffer.record_event(_span())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="telemetry span"):
        buffer.record_span(TelemetryEvent("event"))  # type: ignore[arg-type]
    assert buffer.flush().events == ()
    assert buffer.flush().spans == ()
