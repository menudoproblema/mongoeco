import unittest

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines.capabilities import (
    resolve_engine_capabilities,
    validate_engine_contract,
)
from mongoeco.engines.results import (
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy
from tests.support import ENGINE_FACTORIES, open_engine


def _context() -> OperationContext:
    return OperationContext.create(dialect=MONGODB_DIALECT_70)


class StorageEngineV2ContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_capabilities_and_required_methods_are_consistent(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    capabilities = resolve_engine_capabilities(engine)

                    assert capabilities.spi_version == 2
                    validate_engine_contract(engine, capabilities)

    async def test_insert_and_batch_insert_return_stable_outcomes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    single = await engine.insert_document(
                        'db',
                        'items',
                        {'_id': 'single'},
                        overwrite=False,
                        operation_context=_context(),
                    )
                    batch = await engine.insert_documents(
                        'db',
                        'items',
                        [{'_id': 'first'}, {'_id': 'second'}],
                        operation_context=_context(),
                    )

                    assert isinstance(single, InsertOutcome)
                    assert single.applied
                    assert all(isinstance(item, InsertOutcome) for item in batch)
                    assert [item.applied for item in batch] == [True, True]

    async def test_update_delete_and_merge_return_stable_outcomes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    await engine.insert_document(
                        'db',
                        'items',
                        {'_id': 'value', 'revision': 1},
                        overwrite=False,
                        operation_context=_context(),
                    )
                    update_context = _context()
                    operation = compile_update_operation(
                        {'_id': 'value'},
                        update_spec={'$set': {'revision': 2}},
                        dialect=MONGODB_DIALECT_70,
                    ).with_overrides(
                        context=update_context,
                        let=update_context.expressions,
                    )
                    updated = await engine.update_with_operation(
                        'db',
                        'items',
                        operation,
                        operation_context=update_context,
                    )
                    merged = await engine.merge_document(
                        'db',
                        'items',
                        {'_id': 'merged', 'revision': 1},
                        when_matched='replace',
                        when_not_matched='insert',
                        operation_context=_context(),
                    )
                    delete_context = _context()
                    delete_operation = compile_update_operation(
                        {'_id': 'value'},
                        dialect=MONGODB_DIALECT_70,
                    ).with_overrides(
                        context=delete_context,
                        let=delete_context.expressions,
                    )
                    deleted = await engine.delete_with_operation(
                        'db',
                        'items',
                        delete_operation,
                        operation_context=delete_context,
                    )

                    assert isinstance(updated, MutationOutcome)
                    assert updated.after_document['revision'] == 2
                    assert isinstance(merged, MergeOutcome)
                    assert merged.applied
                    assert isinstance(deleted, DeleteOutcome)
                    assert deleted.deleted_document['_id'] == 'value'

    async def test_read_snapshot_is_owned_and_stable(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_engine(engine_name) as engine:
                    context = _context()
                    await engine.insert_document(
                        'db',
                        'items',
                        {'_id': 'snapshot'},
                        overwrite=False,
                        operation_context=context,
                    )
                    snapshot = engine.open_read_snapshot(
                        'db',
                        'items',
                        compile_find_semantics({}),
                        operation_context=context,
                    )

                    assert isinstance(snapshot, ReadSnapshot)
                    assert snapshot.metadata.policy is SnapshotPolicy.STABLE
                    assert [document async for document in snapshot] == [
                        {'_id': 'snapshot'}
                    ]
                    assert snapshot.closed


if __name__ == '__main__':
    unittest.main()
