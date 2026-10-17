"""Metadata capture remains bounded, detached and safe across server/test threads."""

import threading

import pytest

from mongoeco.wire._diagnostics import WireDiagnostics, classify_result
from mongoeco.wire.proxy import AsyncMongoEcoProxyServer


@pytest.mark.parametrize("capacity", [-1, 10_001, True, None, "128", 1.5])
def test_invalid_capacity_is_rejected_before_client_creation(capacity, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid diagnostic capacity must not construct a client")

    monkeypatch.setattr("mongoeco.wire.proxy.AsyncMongoClient", forbidden)
    with pytest.raises(ValueError, match="diagnostic_capacity"):
        AsyncMongoEcoProxyServer(diagnostic_capacity=capacity)


def test_disabled_capture_retains_nothing():
    capture = WireDiagnostics(0)
    capture.record("ignored", value="not retained")
    capture.resources(connections=4)
    assert capture.snapshot() == {
        "enabled": False,
        "capacity": 0,
        "dropped_events": 0,
        "events": [],
        "resources": {},
    }


def test_ring_counts_evictions_and_returns_detached_snapshots():
    capacity = 2
    capture = WireDiagnostics(capacity)
    for index in range(4):
        capture.record("request", request_id=index)
    capture.resources(connections=1)
    snapshot = capture.snapshot()
    assert snapshot["dropped_events"] == capacity
    assert [e["request_id"] for e in snapshot["events"]] == [2, 3]
    snapshot["events"][0]["request_id"] = "changed"
    snapshot["resources"]["connections"] = 100
    assert capture.snapshot()["events"][0]["request_id"] == capacity
    assert capture.snapshot()["resources"] == {"connections": 1}


def test_concurrent_snapshots_are_consistent():
    capacity = 16
    count = 100
    capture = WireDiagnostics(capacity)

    def produce():
        for index in range(count):
            capture.record("request", request_id=index)
            snapshot = capture.snapshot()
            assert len(snapshot["events"]) <= capacity

    threads = [threading.Thread(target=produce) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert capture.snapshot()["dropped_events"] == count * len(threads) - capacity


def test_first_error_survives_tail_eviction_and_is_detached():
    capacity = 2
    error_code = 50
    capture = WireDiagnostics(capacity)
    capture.record("request_completed", outcome="error", code=error_code)
    for index in range(10):
        capture.record("request_completed", outcome="completed", request_id=index)
    snapshot = capture.snapshot()
    assert snapshot["first_error"]["code"] == error_code
    assert len(snapshot["events"]) == capacity
    snapshot["first_error"]["code"] = 99
    assert capture.snapshot()["first_error"]["code"] == error_code


@pytest.mark.parametrize(
    ["result", "outcome", "code"],
    [
        ({"ok": 1}, "completed", None),
        ({"ok": 0}, "error", None),
        ({"ok": 0, "code": 50}, "error", 50),
        ({"ok": 1, "writeErrors": [{"code": 11000}]}, "error", 11000),
        ({"ok": 1, "writeConcernError": {"code": 64}}, "error", 64),
        ({"ok": 1, "writeConcernError": {}}, "error", None),
        ({"ok": 1, "writeErrors": [None]}, "error", None),
        ({"ok": 0, "code": "private"}, "error", None),
        ({"ok": 1, "writeErrors": [], "writeConcernError": {"code": 64}}, "error", 64),
    ],
)
def test_outcome_classification_covers_error_shapes(result, outcome, code):
    assert classify_result(result) == (outcome, code)
