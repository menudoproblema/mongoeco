"""Identity and an optional selector constrain the same atomic mutation."""

import asyncio

import pytest

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.conformance import (
    ConformanceStatus,
    EngineConformanceProvider,
    run_engine_conformance,
)
from mongoeco.conformance.runner import (
    _check_atomic_delete,
    _check_atomic_mutation,
    _check_identity_selector,
)
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.engines.results import DeleteOutcome, MutationOutcome
from mongoeco.types import DeleteResult, UpdateResult

from tests.consumer.engine_canary import ExternalCanaryEngine


def bound_operation(identifier, update=None):
    context = OperationContext.create(dialect=MONGODB_DIALECT_70)
    operation = compile_update_operation(
        {"_id": identifier}, update_spec=update, dialect=context.dialect
    ).bind(context)
    return operation, context


@pytest.mark.parametrize(
    "engine_type", [ExternalCanaryEngine, MemoryEngine, SQLiteEngine]
)
@pytest.mark.parametrize("action", ["update", "delete"])
@pytest.mark.parametrize("selector", [None, {}, {"eligible": 1}])
@pytest.mark.parametrize("eligible", [0, 1])
def test_identity_and_selector_are_both_required(
    engine_type, action, selector, eligible
):
    async def run():
        provider = EngineConformanceProvider("selection", engine_type)
        async with provider.open_engine() as engine:
            a = {"_id": "a", "eligible": 1}
            b = {"_id": "b", "eligible": eligible}
            operation, context = bound_operation("b", {"$set": {"eligible": 2}})
            for document in (a, b):
                await engine.insert_document(
                    "selection", "items", document, operation_context=context
                )
            method = getattr(engine, f"{action}_with_operation")
            outcome = await method(
                "selection",
                "items",
                operation,
                selector_filter=selector,
                operation_context=context,
            )
            matches = selector is None or not selector or eligible == 1
            assert (
                await engine.get_document(
                    "selection", "items", "a", operation_context=context
                )
                == a
            )
            stored = await engine.get_document(
                "selection", "items", "b", operation_context=context
            )
            if action == "update":
                assert outcome.matched_count == int(matches)
                assert outcome.modified_count == int(matches)
                assert not outcome.result.did_upsert
                assert outcome.before_document == (b if matches else None)
                expected = {"_id": "b", "eligible": 2} if matches else b
                assert outcome.after_document == (expected if matches else None)
                assert stored == expected
            else:
                assert outcome.deleted_count == int(matches)
                assert outcome.deleted_document == (b if matches else None)
                assert stored == (None if matches else b)

    asyncio.run(run())


@pytest.mark.parametrize("action", ["update", "delete"])
@pytest.mark.parametrize("fault", ["identity", "selector"])
def test_conformance_rejects_either_ignored_condition(action, fault):
    class BrokenSelection(ExternalCanaryEngine):
        async def update_with_operation(self, db, coll, operation, **kwargs):
            if action == "update":
                operation, kwargs = break_selection(operation, kwargs)
            return await super().update_with_operation(db, coll, operation, **kwargs)

        async def delete_with_operation(self, db, coll, operation, **kwargs):
            if action == "delete":
                operation, kwargs = break_selection(operation, kwargs)
            return await super().delete_with_operation(db, coll, operation, **kwargs)

    def break_selection(operation, kwargs):
        if kwargs.get("selector_filter") is not None:
            if fault == "selector":
                kwargs["selector_filter"] = None
            else:
                operation = compile_update_operation(
                    {},
                    update_spec=operation.update_spec,
                    dialect=operation.context.dialect,
                ).bind(operation.context)
        return operation, kwargs

    report = asyncio.run(
        run_engine_conformance(
            EngineConformanceProvider("broken-selection", BrokenSelection)
        )
    )
    check = next(check for check in report.checks if check.name == "compare-and-set")
    assert check.status is ConformanceStatus.FAILED
    assert "selection" in check.detail or "compare-and-set" in check.detail


@pytest.mark.parametrize("action", ["update", "delete"])
@pytest.mark.parametrize("fault", ["outcome", "repeat", "state"])
def test_selection_check_rejects_misreported_effects(action, fault):
    class MisreportedSelection(ExternalCanaryEngine):
        async def update_with_operation(self, db, coll, operation, **kwargs):
            outcome = await super().update_with_operation(db, coll, operation, **kwargs)
            return self.distort(db, coll, outcome) if action == "update" else outcome

        async def delete_with_operation(self, db, coll, operation, **kwargs):
            outcome = await super().delete_with_operation(db, coll, operation, **kwargs)
            return self.distort(db, coll, outcome) if action == "delete" else outcome

        def distort(self, db, coll, outcome):
            if fault == "outcome":
                return object()
            if fault == "state":
                self._collection(db, coll)["a"]["eligible"] = 9
            empty = (
                outcome.matched_count == 0
                if action == "update"
                else outcome.deleted_count == 0
            )
            if fault == "repeat" and empty:
                # The conditional target was already changed/deleted. A bogus
                # second outcome must be rejected even if final state is intact.
                if action == "update":
                    return MutationOutcome(
                        UpdateResult(1, 0),
                        before_document={"_id": "b"},
                        after_document={"_id": "b"},
                    )
                return DeleteOutcome(DeleteResult(1), {"_id": "b"})
            return outcome

    with pytest.raises(AssertionError, match="selection"):
        asyncio.run(_check_identity_selector(MisreportedSelection(), "db", "items"))


@pytest.mark.parametrize("fault", ["type", "winner", "state"])
def test_delete_competition_checks_images_counters_and_state(fault):
    class MisreportedDelete(ExternalCanaryEngine):
        async def delete_with_operation(self, db, coll, operation, **kwargs):
            outcome = await super().delete_with_operation(db, coll, operation, **kwargs)
            if fault == "type":
                return object()
            if fault == "winner":
                return DeleteOutcome(DeleteResult(0))
            self._collection(db, coll)["delete-decoy"]["eligible"] = 9
            return outcome

    with pytest.raises(AssertionError, match="delete selection"):
        asyncio.run(_check_atomic_delete(MisreportedDelete(), "db", "items"))


@pytest.mark.parametrize("fault", ["images", "other_identity"])
def test_update_competition_checks_images_and_other_identity(fault):
    class MisreportedUpdate(ExternalCanaryEngine):
        async def update_with_operation(self, db, coll, operation, **kwargs):
            outcome = await super().update_with_operation(db, coll, operation, **kwargs)
            if fault == "other_identity":
                self._collection(db, coll)["cas-decoy"]["fence"] = 9
            if fault == "images" and outcome.matched_count:
                return MutationOutcome(
                    UpdateResult(1, 0),
                    before_document=outcome.before_document,
                    after_document=outcome.after_document,
                )
            return outcome

    with pytest.raises(AssertionError, match="selection"):
        asyncio.run(_check_atomic_mutation(MisreportedUpdate(), "db", "items"))
