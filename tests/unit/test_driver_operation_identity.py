"""One execution identity survives attempts without escaping into metric labels."""

import asyncio

from dataclasses import replace

import pytest

from mongoeco.driver import (
    CommandFailedEvent,
    CommandStartedEvent,
    CommandSucceededEvent,
    DriverRuntime,
    ServerSelectionFailedEvent,
    execute_request_pipeline,
)
from mongoeco.driver.telemetry_projector import DriverTelemetryProjector
from mongoeco.errors import ConnectionFailure
from mongoeco.types import ReadConcern, ReadPreference, WriteConcern


def runtime_for(uri="mongodb://localhost/?retryReads=true"):
    return DriverRuntime(
        uri=uri,
        write_concern=WriteConcern(),
        read_concern=ReadConcern(),
        read_preference=ReadPreference(),
    )


def test_retries_and_concurrent_reuse_have_distinct_logical_identities():
    async def exercise():
        runtime = runtime_for()
        plan = runtime.plan_command_request(
            "test", "find", {"find": "records"}, read_only=True
        )
        seen = {}

        class Transport:
            async def send(self, execution):
                attempts = seen.setdefault(execution.operation_id, [])
                attempts.append(execution)
                await asyncio.sleep(0)
                if len(attempts) == 1:
                    message = "retry"
                    raise ConnectionFailure(message)
                return {"ok": 1}

        transport = Transport()
        results = await asyncio.gather(
            runtime.execute_request(plan, transport),
            runtime.execute_request(plan, transport),
        )
        results.append(await runtime.execute_request(plan, transport))
        assert all(result.outcome.ok for result in results)
        logical_calls = len(results)
        assert len(seen) == logical_calls
        assert all(
            [item.attempt_number for item in attempts] == [1, 2]
            for attempts in seen.values()
        )
        assert (
            len({item.request_id for attempts in seen.values() for item in attempts})
            == logical_calls * 2
        )
        for event in runtime.monitor.history:
            assert event.operation_id in seen
            if event.request_id is not None:
                assert any(
                    item.request_id == event.request_id
                    for item in seen[event.operation_id]
                )
        assert all(
            snapshot.checked_out == 0 for snapshot in runtime.connection_snapshots
        )
        await runtime.clear_connections_async()

    asyncio.run(exercise())


def test_final_failure_and_selection_failure_remain_correlated():
    async def exercise():
        runtime = runtime_for("mongodb://localhost/?retryReads=false")
        plan = runtime.plan_command_request(
            "test", "find", {"find": "records"}, read_only=True
        )

        class Transport:
            async def send(self, execution):
                message = "final failure"
                raise ConnectionFailure(message)

        result = await runtime.execute_request(plan, Transport())
        assert not result.outcome.ok
        failed = [
            event
            for event in runtime.monitor.history
            if isinstance(event, CommandFailedEvent)
        ]
        assert len(failed) == 1
        assert failed[0].operation_id
        runtime.monitor.clear_history()
        empty = replace(plan, candidate_servers=(), dynamic_candidates=False)
        result = await runtime.execute_request(empty, Transport())
        assert not result.outcome.ok
        assert len(runtime.monitor.history) == 1
        event = runtime.monitor.history[0]
        assert isinstance(event, ServerSelectionFailedEvent)
        assert event.request_id is None
        assert event.operation_id
        assert event.operation_id != failed[0].operation_id
        await runtime.clear_connections_async()

    asyncio.run(exercise())


def test_cancellation_discards_lease_and_finishes_telemetry_once():
    async def exercise():
        runtime = runtime_for()
        projector = DriverTelemetryProjector(provider_id="mongoeco")
        projector.attach(runtime.monitor)
        started = asyncio.Event()
        plan = runtime.plan_command_request(
            "test", "find", {"find": "records"}, read_only=True
        )

        class Transport:
            async def send(self, execution):
                started.set()
                await asyncio.Future()

        task = asyncio.create_task(runtime.execute_request(plan, Transport()))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        commands = [
            event
            for event in runtime.monitor.history
            if isinstance(
                event, (CommandStartedEvent, CommandFailedEvent, CommandSucceededEvent)
            )
        ]
        assert [type(event) for event in commands] == [
            CommandStartedEvent,
            CommandFailedEvent,
        ]
        assert commands[0].operation_id == commands[1].operation_id
        assert commands[0].request_id == commands[1].request_id
        assert all(
            snapshot.total_size == 0 for snapshot in runtime.connection_snapshots
        )
        snapshot = projector.snapshot()
        assert len(snapshot.spans) == len(snapshot.events) == len(snapshot.metrics) == 1
        assert snapshot.spans[0].trace_id == commands[0].request_id
        assert (
            snapshot.spans[0].attributes["db.operation.id"] == commands[0].operation_id
        )
        assert "db.operation.id" not in snapshot.metrics[0].labels
        assert projector.snapshot().spans == ()
        await runtime.clear_connections_async()

    asyncio.run(exercise())


def test_explicit_attempt_identity_and_cancelled_pool_waiter_keep_pool_usable():
    async def exercise():
        runtime = runtime_for("mongodb://localhost/?maxPoolSize=1")
        plan = runtime.plan_command_request(
            "test", "find", {"find": "records"}, read_only=True
        )
        held = await runtime.prepare_request_execution_attempt(
            plan, attempt_number=1, operation_id="caller-owned-operation"
        )
        assert held.operation_id == "caller-owned-operation"
        history = tuple(runtime.monitor.history)
        waiting = asyncio.Event()

        async def prepare_waiter():
            waiting.set()
            return await runtime.prepare_request_execution(plan)

        task = asyncio.create_task(prepare_waiter())
        await waiting.wait()
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # No connection was acquired and no command was started by the waiter.
        assert tuple(runtime.monitor.history) == history
        await runtime.complete_request_execution(held)
        next_execution = await asyncio.wait_for(
            runtime.prepare_request_execution(plan), timeout=1
        )
        assert next_execution.operation_id != held.operation_id
        assert next_execution.request_id != held.request_id
        await runtime.complete_request_execution(next_execution)
        assert all(
            snapshot.checked_out == 0 for snapshot in runtime.connection_snapshots
        )
        await runtime.clear_connections_async()

    asyncio.run(exercise())


@pytest.mark.parametrize("opaque_signature", [False, True])
def test_public_pipeline_keeps_legacy_prepare_callback_and_owned_lease(
    opaque_signature,
):
    async def exercise():
        runtime = runtime_for()
        plan = runtime.plan_command_request(
            "test", "find", {"find": "records"}, read_only=True
        )
        prepared = []
        completed = []
        discarded = []

        async def legacy_prepare(current_plan, *, attempt_number):
            execution = await runtime.prepare_request_execution_attempt(
                current_plan, attempt_number=attempt_number
            )
            prepared.append(execution)
            return execution

        class OpaqueLegacyPrepare:
            @property
            def __signature__(self):
                message = "signature unavailable"
                raise ValueError(message)

            async def __call__(self, current_plan, *, attempt_number):
                return await legacy_prepare(current_plan, attempt_number=attempt_number)

        async def complete(execution):
            completed.append(execution)
            await runtime.complete_request_execution(execution)

        async def discard(execution):
            discarded.append(execution)
            await runtime.discard_request_execution(execution)

        seen = []

        class Transport:
            async def send(self, execution):
                seen.append(execution)
                if len(seen) == 1:
                    message = "retry once"
                    raise ConnectionFailure(message)
                return {"ok": 1}

        result = await execute_request_pipeline(
            plan=plan,
            prepare_execution=OpaqueLegacyPrepare()
            if opaque_signature
            else legacy_prepare,
            complete_execution=complete,
            discard_execution=discard,
            transport=Transport(),
            monitor=runtime.monitor,
            operation_id="legacy-owned-operation",
        )
        assert result.outcome.ok
        expected_attempts = 2
        assert len(prepared) == len(seen) == expected_attempts
        assert completed == [seen[-1]]
        assert discarded == [seen[0]]
        assert {e.operation_id for e in seen} == {"legacy-owned-operation"}
        assert len({e.request_id for e in seen}) == expected_attempts
        for original, execution in zip(prepared, seen, strict=True):
            assert execution.connection is original.connection
            assert execution.plan is original.plan
            assert execution.request_id == original.request_id
        commands = [
            e
            for e in runtime.monitor.history
            if isinstance(
                e, (CommandStartedEvent, CommandFailedEvent, CommandSucceededEvent)
            )
        ]
        assert len(commands) == expected_attempts * 2
        assert {e.operation_id for e in commands} == {"legacy-owned-operation"}
        assert all(
            snapshot.checked_out == 0 for snapshot in runtime.connection_snapshots
        )
        await runtime.clear_connections_async()

    asyncio.run(exercise())


def test_prepare_type_error_is_not_retried_as_a_signature_fallback():
    async def exercise():
        runtime = runtime_for()
        plan = runtime.plan_command_request(
            "test", "find", {"find": "records"}, read_only=True
        )
        calls = []

        async def prepare(current_plan, *, attempt_number, operation_id):
            calls.append((current_plan, attempt_number, operation_id))
            message = "callback implementation failed"
            raise TypeError(message)

        with pytest.raises(TypeError, match="callback implementation failed"):
            await execute_request_pipeline(
                plan=plan,
                prepare_execution=prepare,
                complete_execution=runtime.complete_request_execution,
                transport=None,
                operation_id="one-call",
            )
        assert calls == [(plan, 1, "one-call")]
        assert all(
            snapshot.checked_out == 0 for snapshot in runtime.connection_snapshots
        )
        await runtime.clear_connections_async()

    asyncio.run(exercise())
