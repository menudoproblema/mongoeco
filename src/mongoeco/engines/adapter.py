from __future__ import annotations

import threading
import warnings
import weakref

from pathlib import Path
from typing import TYPE_CHECKING, Any

from mongoeco.engines.capabilities import (
    resolve_engine_capabilities,
    validate_engine_contract,
)
from mongoeco.engines.results import (
    CommittedChange,
    DeleteOutcome,
    InsertOutcome,
    MergeOutcome,
    MutationOutcome,
)
from mongoeco.engines.snapshots import ReadSnapshot, SnapshotPolicy


if TYPE_CHECKING:
    from collections.abc import Callable


_SPI_V2 = 2
_DOCUMENT_ARGUMENT_INDEX = 2
_LEGACY_WARNING_LOCK = threading.Lock()
_WARNED_LEGACY_ENGINE_TYPES: weakref.WeakSet[type[object]] = weakref.WeakSet()


def _require_callable(
    owner: object,
    name: str,
    *,
    message: str,
) -> Callable[..., Any]:
    method = getattr(owner, name, None)
    if not callable(method):
        raise TypeError(message)
    return method


class EngineSpiAdapter:
    """Canonical mutation boundary consumed by the API layer."""

    def __init__(self, engine: object) -> None:
        self.engine = engine
        self.capabilities = resolve_engine_capabilities(engine)
        validate_engine_contract(engine, self.capabilities)

    def prepare_change_delivery(self, sink: object | None) -> None:
        if self.capabilities.change_delivery not in {
            'commit-sequence',
            'transactional-outbox',
        } or sink is None:
            return
        register = _require_callable(
            self.engine,
            'register_change_consumer',
            message=(
                'transactional-outbox engine must implement '
                'register_change_consumer'
            ),
        )
        journal_path = getattr(sink, 'journal_path', None)
        initial_checkpoint = None
        if journal_path is not None:
            state = getattr(sink, 'state', None)
            next_token = getattr(state, 'next_token', None)
            if isinstance(next_token, int):
                initial_checkpoint = next_token - 1
        checkpoint = register(
            self._change_consumer_id(sink),
            initial_checkpoint=initial_checkpoint,
            durable=journal_path is not None,
        )
        align = getattr(sink, 'align_commit_sequence', None)
        if callable(align):
            align(int(checkpoint) + 1)

    def dispatch_committed_changes(self, sink: object | None) -> None:
        if self.capabilities.change_delivery not in {
            'commit-sequence',
            'transactional-outbox',
        } or sink is None:
            return
        dispatch = _require_callable(
            self.engine,
            'dispatch_committed_changes',
            message=(
                'transactional-outbox engine must implement '
                'dispatch_committed_changes'
            ),
        )
        dispatch(
            self._change_consumer_id(sink),
            lambda change: self._deliver_committed_change(sink, change),
        )

    def unregister_change_delivery(self, sink: object | None) -> None:
        if not self.capabilities.monotonic_commit_sequence or sink is None:
            return
        unregister = _require_callable(
            self.engine,
            'unregister_change_consumer',
            message=(
                'sequenced change-delivery engine must implement '
                'unregister_change_consumer'
            ),
        )
        unregister(self._change_consumer_id(sink))

    @staticmethod
    def _change_consumer_id(sink: object) -> str:
        journal_path = getattr(sink, 'journal_path', None)
        if isinstance(journal_path, str):
            canonical_path = Path(journal_path).expanduser().resolve()
            return f'journal-change-hub:{canonical_path}'
        return f'local-change-hub:{id(sink)}'

    @staticmethod
    def _deliver_committed_change(
        sink: object,
        change: CommittedChange,
    ) -> None:
        state = getattr(sink, 'state', None)
        next_token = getattr(state, 'next_token', None)
        if isinstance(next_token, int) and change.sequence < next_token:
            return
        align = getattr(sink, 'align_commit_sequence', None)
        if callable(align):
            align(change.sequence)
        if change.is_gap:
            mark_gap = _require_callable(
                sink,
                'mark_gap',
                message='change sink must implement mark_gap',
            )
            mark_gap()
            return
        publish = _require_callable(
            sink,
            'publish',
            message='change sink must implement publish',
        )
        payload = change.payload
        if payload is None:
            message = 'committed change event must contain a payload'
            raise RuntimeError(message)
        publish(**payload)

    async def update_outcome(
        self,
        *args: object,
        on_commit: Callable[[MutationOutcome], None] | None = None,
        **kwargs: object,
    ) -> MutationOutcome:
        method = _require_callable(
            self.engine,
            'update_with_operation',
            message='engine must implement update_with_operation',
        )
        call_kwargs = dict(kwargs)
        if self.capabilities.spi_version == 1:
            operation_context = call_kwargs.pop('operation_context', None)
            if operation_context is not None:
                call_kwargs['context'] = operation_context.session
            call_kwargs['capture_documents'] = True
        elif call_kwargs.get('operation_context') is not None:
            call_kwargs.pop('context', None)
            call_kwargs.pop('dialect', None)
        if self._uses_commit_callback and on_commit is not None:
            call_kwargs['on_commit'] = on_commit
        result = await method(*args, **call_kwargs)
        outcome = self._require_mutation_outcome(result)
        if self._publishes_after_return and on_commit is not None:
            on_commit(outcome)
        return outcome

    async def delete_outcome(
        self,
        *args: object,
        on_commit: Callable[[DeleteOutcome], None] | None = None,
        **kwargs: object,
    ) -> DeleteOutcome:
        method = _require_callable(
            self.engine,
            'delete_with_operation',
            message='engine must implement delete_with_operation',
        )
        call_kwargs = dict(kwargs)
        if self.capabilities.spi_version == 1:
            operation_context = call_kwargs.pop('operation_context', None)
            if operation_context is not None:
                call_kwargs['context'] = operation_context.session
            call_kwargs['capture_document'] = True
        elif call_kwargs.get('operation_context') is not None:
            call_kwargs.pop('context', None)
            call_kwargs.pop('dialect', None)
        if self._uses_commit_callback and on_commit is not None:
            call_kwargs['on_commit'] = on_commit
        result = await method(*args, **call_kwargs)
        outcome = self._require_delete_outcome(result)
        if self._publishes_after_return and on_commit is not None:
            on_commit(outcome)
        return outcome

    async def insert_outcome(
        self,
        *args: object,
        on_commit: Callable[[InsertOutcome], None] | None = None,
        **kwargs: object,
    ) -> InsertOutcome:
        if self.capabilities.spi_version >= _SPI_V2:
            method = _require_callable(
                self.engine,
                'insert_document',
                message='SPI v2 engine must implement insert_document',
            )
            call_kwargs = dict(kwargs)
            if self._uses_commit_callback and on_commit is not None:
                call_kwargs['on_commit'] = on_commit
            outcome = self._require_insert_outcome(
                await method(*args, **call_kwargs),
            )
        else:
            method = _require_callable(
                self.engine,
                'put_document',
                message='legacy engine must implement put_document',
            )
            document = (
                args[_DOCUMENT_ARGUMENT_INDEX]
                if len(args) > _DOCUMENT_ARGUMENT_INDEX
                else None
            )
            legacy_callback = None
            if on_commit is not None:

                def legacy_callback(committed: object) -> None:
                    on_commit(
                        InsertOutcome(applied=True, document=committed),
                    )
            call_kwargs = dict(kwargs)
            operation_context = call_kwargs.pop('operation_context', None)
            if operation_context is not None:
                call_kwargs['context'] = operation_context.session
            if self._uses_commit_callback and legacy_callback is not None:
                call_kwargs['on_commit'] = legacy_callback
            applied = bool(await method(*args, **call_kwargs))
            outcome = InsertOutcome(
                applied=applied,
                document=document if applied else None,
            )
        if self._publishes_after_return and on_commit is not None and outcome:
            on_commit(outcome)
        return outcome

    async def insert_many_outcomes(
        self,
        *args: object,
        on_commit: Callable[[InsertOutcome], None] | None = None,
        **kwargs: object,
    ) -> tuple[InsertOutcome, ...]:
        if self.capabilities.spi_version >= _SPI_V2:
            method = _require_callable(
                self.engine,
                'insert_documents',
                message='SPI v2 engine must implement insert_documents',
            )
            call_kwargs = dict(kwargs)
            if self._uses_commit_callback and on_commit is not None:
                call_kwargs['on_commit'] = on_commit
            results = tuple(await method(*args, **call_kwargs))
            outcomes = tuple(
                self._require_insert_outcome(item) for item in results
            )
        else:
            method = getattr(self.engine, 'put_documents_bulk', None)
            if not callable(method):
                raise NotImplementedError
            documents = (
                args[_DOCUMENT_ARGUMENT_INDEX]
                if len(args) > _DOCUMENT_ARGUMENT_INDEX
                else ()
            )
            legacy_callback = None
            if on_commit is not None:

                def legacy_callback(committed: object) -> None:
                    on_commit(
                        InsertOutcome(applied=True, document=committed),
                    )
            call_kwargs = dict(kwargs)
            operation_context = call_kwargs.pop('operation_context', None)
            if operation_context is not None:
                call_kwargs['context'] = operation_context.session
            if self._uses_commit_callback and legacy_callback is not None:
                call_kwargs['on_commit'] = legacy_callback
            applied_results = tuple(await method(*args, **call_kwargs))
            outcomes = tuple(
                InsertOutcome(
                    applied=bool(applied),
                    document=document if applied else None,
                )
                for document, applied in zip(
                    documents,
                    applied_results,
                    strict=False,
                )
            )
        if self._publishes_after_return and on_commit is not None:
            for outcome in outcomes:
                if outcome:
                    on_commit(outcome)
        return outcomes

    async def merge_outcome(
        self,
        *args: object,
        on_commit: Callable[[MergeOutcome], None] | None = None,
        **kwargs: object,
    ) -> MergeOutcome:
        method = _require_callable(
            self.engine,
            'merge_document',
            message='engine must implement merge_document',
        )
        call_kwargs = dict(kwargs)
        if self.capabilities.spi_version == 1:
            operation_context = call_kwargs.pop('operation_context', None)
            if operation_context is not None:
                call_kwargs['context'] = operation_context.session
        elif call_kwargs.get('operation_context') is not None:
            call_kwargs.pop('context', None)
        if self._uses_commit_callback and on_commit is not None:
            call_kwargs['on_commit'] = on_commit
        outcome = await method(*args, **call_kwargs)
        if not isinstance(outcome, MergeOutcome):
            message = 'engine did not return MergeOutcome'
            raise TypeError(message)
        if self._publishes_after_return and on_commit is not None:
            on_commit(outcome)
        return outcome

    def open_read_snapshot(
        self,
        *args: object,
        operation_context=None,
        **kwargs: object,
    ) -> ReadSnapshot:
        if self.capabilities.spi_version >= _SPI_V2:
            method = _require_callable(
                self.engine,
                'open_read_snapshot',
                message='SPI v2 engine must implement open_read_snapshot',
            )
            snapshot = method(
                *args,
                operation_context=operation_context,
                **kwargs,
            )
            if not isinstance(snapshot, ReadSnapshot):
                message = 'SPI v2 engine did not return ReadSnapshot'
                raise TypeError(message)
            return snapshot
        method = _require_callable(
            self.engine,
            'scan_find_semantics',
            message='legacy engine must implement scan_find_semantics',
        )
        source = method(
            *args,
            context=(
                None
                if operation_context is None
                else operation_context.session
            ),
            **kwargs,
        )
        return ReadSnapshot(
            source,
            policy=SnapshotPolicy.STABLE,
            operation_id=(
                None
                if operation_context is None
                else operation_context.operation_id
            ),
        )

    async def get_document(
        self,
        *args: object,
        operation_context=None,
        **kwargs: object,
    ):
        method = _require_callable(
            self.engine,
            'get_document',
            message='engine must implement get_document',
        )
        if self.capabilities.spi_version >= _SPI_V2:
            return await method(
                *args,
                operation_context=operation_context,
                **kwargs,
            )
        return await method(
            *args,
            context=(
                None
                if operation_context is None
                else operation_context.session
            ),
            **kwargs,
        )

    async def count_documents(
        self,
        *args: object,
        operation_context=None,
        **kwargs: object,
    ) -> int:
        method = _require_callable(
            self.engine,
            'count_find_semantics',
            message='engine must implement count_find_semantics',
        )
        if self.capabilities.spi_version >= _SPI_V2:
            return int(
                await method(
                    *args,
                    operation_context=operation_context,
                    **kwargs,
                ),
            )
        return int(
            await method(
                *args,
                context=(
                    None
                    if operation_context is None
                    else operation_context.session
                ),
                **kwargs,
            ),
        )

    @property
    def _uses_commit_callback(self) -> bool:
        return self.capabilities.change_delivery == 'legacy-callback'

    @property
    def _publishes_after_return(self) -> bool:
        return self.capabilities.change_delivery == 'none'

    def _require_mutation_outcome(self, result: Any) -> MutationOutcome:
        if isinstance(result, MutationOutcome):
            return result
        if self.capabilities.spi_version == 1:
            return MutationOutcome(result=result)
        message = 'SPI v2 engine did not return MutationOutcome'
        raise TypeError(message)

    def _require_delete_outcome(self, result: Any) -> DeleteOutcome:
        if isinstance(result, DeleteOutcome):
            return result
        if self.capabilities.spi_version == 1:
            return DeleteOutcome(result=result)
        message = 'SPI v2 engine did not return DeleteOutcome'
        raise TypeError(message)

    @staticmethod
    def _require_insert_outcome(result: Any) -> InsertOutcome:
        if isinstance(result, InsertOutcome):
            return result
        message = 'SPI v2 engine did not return InsertOutcome'
        raise TypeError(message)


class LegacyEngineAdapter(EngineSpiAdapter):
    """Explicit compatibility marker for engines implementing SPI v1."""


def adapt_engine(engine: object) -> EngineSpiAdapter:
    capabilities = resolve_engine_capabilities(engine)
    if capabilities.spi_version == 1:
        engine_type = type(engine)
        with _LEGACY_WARNING_LOCK:
            should_warn = engine_type not in _WARNED_LEGACY_ENGINE_TYPES
            if should_warn:
                _WARNED_LEGACY_ENGINE_TYPES.add(engine_type)
        if should_warn:
            warnings.warn(
                (
                    f'{engine_type.__name__} implements deprecated MongoEco '
                    'engine SPI v1; migrate to EngineCapabilities and SPI v2 '
                    'before MongoEco 5.0.0'
                ),
                DeprecationWarning,
                stacklevel=2,
            )
    adapter_type = (
        LegacyEngineAdapter
        if capabilities.spi_version == 1
        else EngineSpiAdapter
    )
    return adapter_type(engine)
