"""Deterministic checkpoint interleavings and local retained-history boundaries."""

import asyncio
import threading

from unittest.mock import patch

import pytest

from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
    _parse_resume_token,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import encode_change_stream_token


def _publish(hub, kind="insert"):
    hub.publish(
        operation_type=kind,
        db_name="db",
        coll_name="c",
        document_key={"_id": hub.state.next_token},
    )


def _cursor(hub, **kwargs):
    return AsyncChangeStreamCursor(
        hub,
        scope=ChangeStreamScope(db_name="db", coll_name="c"),
        max_await_time_ms=1,
        **kwargs,
    )


def test_open_registers_watcher_and_captures_offset_checkpoint_under_one_guard():
    hub = ChangeStreamHub()
    started, published = threading.Event(), threading.Event()
    register = hub.register_watcher

    def write():
        started.set()
        _publish(hub)
        published.set()

    worker = threading.Thread(target=write)

    def registering():
        worker.start()
        assert started.wait(1)
        assert not published.wait(0.02)
        register()

    with patch.object(hub, "register_watcher", side_effect=registering):
        stream = _cursor(hub)
    try:
        worker.join(timeout=1)
        assert not worker.is_alive()
        assert _parse_resume_token(stream.resume_token) == 0
        assert asyncio.run(stream.try_next())["documentKey"] == {"_id": 1}
    finally:
        stream.close()
        hub.close()


def test_empty_read_does_not_checkpoint_over_a_concurrently_appended_event():
    hub = ChangeStreamHub()
    stream = _cursor(hub)
    before = stream.resume_token

    async def timeout_then_write(offset, **_kwargs):
        _publish(hub)
        return offset, None

    try:
        with patch.object(hub, "wait_for_event_async", side_effect=timeout_then_write):
            assert asyncio.run(stream.try_next()) is None
        assert stream.resume_token == before
        assert asyncio.run(stream.try_next())["documentKey"] == {"_id": 1}
    finally:
        stream.close()
        hub.close()


def test_transform_error_does_not_advance_the_public_checkpoint():
    hub = ChangeStreamHub()
    stream = _cursor(hub, pipeline=[{"$project": {"_id": 0}}])
    before = stream.resume_token
    _publish(hub)
    try:
        with pytest.raises(OperationFailure) as error:
            asyncio.run(stream.try_next())
        assert error.value.code == 280  # noqa: PLR2004 - native token preservation code
        assert stream.resume_token == before
        assert not stream.alive
        assert hub.watcher_count == 0
    finally:
        stream.close()
        hub.close()


@pytest.mark.parametrize("empty", [False, True])
def test_live_reads_never_cross_an_omitted_event(empty):
    hub = ChangeStreamHub()
    stream = _cursor(hub)
    if not empty:
        _publish(hub)
    hub.mark_gap()
    if not empty:
        _publish(hub)
        assert asyncio.run(stream.try_next())["documentKey"] == {"_id": 1}
    before = stream.resume_token
    try:
        with pytest.raises(OperationFailure, match="omitted event"):
            asyncio.run(stream.try_next())
        assert stream.resume_token == before
        assert not stream.alive
        assert hub.watcher_count == 0
    finally:
        stream.close()
        hub.close()


def test_new_checkpoint_after_a_gap_resumes_without_crossing_the_gap():
    hub = ChangeStreamHub()
    hub.mark_gap()
    initial = _cursor(hub)
    token = initial.resume_token
    initial.close()
    resumed = _cursor(hub, resume_after=token)
    try:
        _publish(hub)
        assert asyncio.run(resumed.try_next())["documentKey"] == {"_id": 2}
    finally:
        resumed.close()
        hub.close()


def test_checkpoint_at_retained_boundary_is_valid_but_lost_event_token_is_not():
    hub = ChangeStreamHub(max_retained_events=1)
    stream = _cursor(hub)
    _publish(hub)
    event_token = asyncio.run(stream.try_next())["_id"]
    assert asyncio.run(stream.try_next()) is None
    checkpoint = stream.resume_token
    stream.close()
    _publish(hub)
    try:
        with pytest.raises(OperationFailure, match="retained"):
            _cursor(hub, resume_after=event_token)
        resumed = _cursor(hub, resume_after=checkpoint)
        try:
            assert asyncio.run(resumed.try_next())["documentKey"] == {"_id": 2}
        finally:
            resumed.close()
        _publish(hub)
        with pytest.raises(OperationFailure, match="retained"):
            _cursor(hub, resume_after=checkpoint)
    finally:
        hub.close()


def test_journal_reopen_recovers_gaps_from_existing_coordinates(tmp_path):
    path = str(tmp_path / "journal.json")
    hub = ChangeStreamHub(journal_path=path)
    _publish(hub)
    hub.mark_gap()
    _publish(hub)
    hub.compact_journal()
    hub.close()
    restored = ChangeStreamHub(journal_path=path)
    try:
        with pytest.raises(OperationFailure, match="resume token"):
            _cursor(restored, resume_after={"_data": encode_change_stream_token(1)})
        stream = _cursor(
            restored,
            resume_after={"_data": encode_change_stream_token(2, invalidate=False)},
        )
        try:
            assert asyncio.run(stream.try_next())["documentKey"] == {"_id": 3}
        finally:
            stream.close()
    finally:
        restored.close()


def test_invalidate_token_and_high_water_checkpoint_are_unambiguous():
    hub = ChangeStreamHub()
    stream = _cursor(hub)
    _publish(hub, "invalidate")
    token = asyncio.run(stream.try_next())["_id"]
    legacy = {"_data": encode_change_stream_token(1)}
    try:
        for supplied in (token, legacy):
            with pytest.raises(OperationFailure) as error:
                _cursor(hub, resume_after=supplied)
            assert error.value.code == 260  # noqa: PLR2004 - native invalidate resume code
        marker = _cursor(hub)
        checkpoint = marker.resume_token
        marker.close()
        resumed = _cursor(hub, resume_after=checkpoint)
        started = _cursor(hub, start_after=token)
        try:
            _publish(hub)
            assert asyncio.run(resumed.try_next())["documentKey"] == {"_id": 2}
            assert asyncio.run(started.try_next())["documentKey"] == {"_id": 2}
        finally:
            resumed.close()
            started.close()
    finally:
        stream.close()
        hub.close()


def test_future_start_time_does_not_deliver_earlier_events():
    hub = ChangeStreamHub()
    stream = _cursor(hub, start_at_operation_time=3)
    try:
        assert stream.resume_token is None
        for _ in range(2):
            _publish(hub)
            assert asyncio.run(stream.try_next()) is None
        _publish(hub)
        assert asyncio.run(stream.try_next())["documentKey"] == {"_id": 3}
    finally:
        stream.close()
        hub.close()


def test_gap_search_matches_integer_interval_intersection():
    hub = ChangeStreamHub()
    hub._gaps = [(2, 3), (5, 5), (9, 12)]
    try:
        for position in range(15):
            for end in range(position, 17):
                omitted = any(
                    start <= end and stop > position for start, stop in hub._gaps
                )
                if omitted:
                    with pytest.raises(OperationFailure):
                        hub.validate_cursor_advance(position, end)
                else:
                    hub.validate_cursor_advance(position, end)
    finally:
        hub.close()


def test_expired_gap_metadata_is_bounded_by_retained_history():
    hub = ChangeStreamHub(max_retained_events=2)
    try:
        for _ in range(50):
            _publish(hub)
            hub.mark_gap()
            assert len(hub._gaps) <= 3  # noqa: PLR2004 - two retained events plus tail gap
    finally:
        hub.close()


@pytest.mark.parametrize(
    "scope", [ChangeStreamScope(), ChangeStreamScope(db_name="db")]
)
def test_other_scope_invalidation_advances_a_checkpoint_without_invalidating_it(scope):
    hub = ChangeStreamHub()
    stream = AsyncChangeStreamCursor(hub, scope=scope, max_await_time_ms=1)
    _publish(hub, "invalidate")
    try:
        assert asyncio.run(stream.try_next()) is None
        assert stream.alive
        token = stream.resume_token
        resumed = AsyncChangeStreamCursor(
            hub,
            scope=scope,
            resume_after=token,
            max_await_time_ms=1,
        )
        try:
            _publish(hub)
            assert asyncio.run(resumed.try_next())["documentKey"] == {"_id": 2}
        finally:
            resumed.close()
    finally:
        stream.close()
        hub.close()


def test_unavailable_future_token_has_a_typed_local_diagnostic_and_no_watcher():
    hub = ChangeStreamHub()
    try:
        with pytest.raises(OperationFailure) as error:
            _cursor(hub, resume_after={"_data": encode_change_stream_token(1)})
        assert error.value.code == 286  # noqa: PLR2004 - local history unavailable identity
        assert error.value.details["codeName"] == "ChangeStreamHistoryLost"
        assert hub.watcher_count == 0
    finally:
        hub.close()


@pytest.mark.parametrize("omission", ["gap", "align", "commit"])
def test_checkpoint_without_a_following_event_survives_journal_reopen(
    tmp_path, omission
):
    path = str(tmp_path / "journal.json")
    hub = ChangeStreamHub(journal_path=path)
    if omission == "gap":
        hub.mark_gap()
    elif omission == "align":
        hub.align_commit_sequence(2)
    else:
        hub.publish_committed(1, None)
    stream = _cursor(hub)
    checkpoint = stream.resume_token
    stream.close()
    hub.close()
    restored = ChangeStreamHub(journal_path=path)
    try:
        assert restored.state.next_token == 2  # noqa: PLR2004 - one omitted sequence
        resumed = _cursor(restored, resume_after=checkpoint)
        try:
            _publish(restored)
            assert asyncio.run(resumed.try_next())["documentKey"] == {"_id": 2}
        finally:
            resumed.close()
    finally:
        restored.close()
