from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import inspect
import time
import uuid

from mongoeco.errors import InvalidOperation
from mongoeco.types import (
    ReadConcern,
    ReadPreference,
    TransactionOptions,
    WriteConcern,
    normalize_read_concern,
    normalize_read_preference,
    normalize_transaction_options,
    normalize_write_concern,
)


type SessionCallback = Callable[["ClientSession"], None]
_TRANSIENT_TRANSACTION_ERROR = "TransientTransactionError"
_UNKNOWN_TRANSACTION_COMMIT_RESULT = "UnknownTransactionCommitResult"
_TRANSACTION_RETRY_ATTEMPTS = 3


@dataclass
class _TransactionHooks:
    start: SessionCallback | None = None
    commit: SessionCallback | None = None
    abort: SessionCallback | None = None


@dataclass
class TransactionState:
    active: bool = False
    number: int = 0
    options: TransactionOptions | None = None


@dataclass
class EngineTransactionContext:
    engine_key: str
    connected: bool | None = None
    supports_transactions: bool | None = None
    transaction_active: bool | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    @classmethod
    def from_document(
        cls,
        engine_key: str,
        state: object,
    ) -> "EngineTransactionContext":
        if isinstance(state, EngineTransactionContext):
            return state
        if not isinstance(state, dict):
            return cls(engine_key=engine_key)
        return cls(
            engine_key=engine_key,
            connected=(
                bool(state["connected"])
                if "connected" in state
                else None
            ),
            supports_transactions=(
                bool(state["supports_transactions"])
                if "supports_transactions" in state
                else None
            ),
            transaction_active=(
                bool(state["transaction_active"])
                if "transaction_active" in state
                else None
            ),
            metadata={
                key: value
                for key, value in state.items()
                if key not in {"connected", "supports_transactions", "transaction_active"}
            },
        )

    def update(self, **updates: object) -> None:
        for key, value in updates.items():
            if key == "connected":
                self.connected = bool(value)
            elif key == "supports_transactions":
                self.supports_transactions = bool(value)
            elif key == "transaction_active":
                self.transaction_active = bool(value)
            else:
                self.metadata[key] = value

    def to_document(self) -> dict[str, object]:
        document = dict(self.metadata)
        if self.connected is not None:
            document["connected"] = self.connected
        if self.supports_transactions is not None:
            document["supports_transactions"] = self.supports_transactions
        if self.transaction_active is not None:
            document["transaction_active"] = self.transaction_active
        return document


@dataclass
class SessionState:
    active: bool = True
    transaction: TransactionState = field(default_factory=TransactionState)
    engine_contexts: dict[str, EngineTransactionContext] = field(default_factory=dict)


@dataclass
class ClientSession:
    """Contexto de sesion compartido entre cliente y engine."""

    session_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    default_transaction_options: TransactionOptions = field(default_factory=TransactionOptions)
    causal_consistency: bool = True
    cluster_time: int | None = None
    operation_time: int | None = None
    _state: SessionState = field(default_factory=SessionState, repr=False)
    _transaction_hooks: dict[str, _TransactionHooks] = field(default_factory=dict, repr=False)

    def ensure_active(self) -> None:
        if not self.active:
            raise InvalidOperation("La sesion ya esta cerrada")

    @property
    def active(self) -> bool:
        return self._state.active

    @property
    def has_ended(self) -> bool:
        return not self.active

    @property
    def in_transaction(self) -> bool:
        return self._state.transaction.active

    @property
    def transaction_active(self) -> bool:
        return self._state.transaction.active

    @property
    def transaction_number(self) -> int:
        return self._state.transaction.number

    @property
    def transaction_options(self) -> TransactionOptions | None:
        return self._state.transaction.options

    @property
    def engine_state(self) -> dict[str, dict[str, object]]:
        return {
            engine_key: context.to_document()
            for engine_key, context in self._state.engine_contexts.items()
        }

    def bind_engine_state(self, engine_key: str, state: object) -> None:
        self.ensure_active()
        self._state.engine_contexts[engine_key] = EngineTransactionContext.from_document(
            engine_key,
            state,
        )

    def bind_engine_context(
        self,
        context: EngineTransactionContext,
    ) -> None:
        self.ensure_active()
        self._state.engine_contexts[context.engine_key] = context

    def get_engine_state(self, engine_key: str) -> object | None:
        self.ensure_active()
        context = self._state.engine_contexts.get(engine_key)
        return None if context is None else context.to_document()

    def get_engine_context(
        self,
        engine_key: str,
    ) -> EngineTransactionContext | None:
        self.ensure_active()
        return self._state.engine_contexts.get(engine_key)

    def update_engine_state(self, engine_key: str, **updates: object) -> None:
        self.ensure_active()
        context = self._state.engine_contexts.get(engine_key)
        if context is not None:
            context.update(**updates)

    def register_transaction_hooks(
        self,
        engine_key: str,
        *,
        start: SessionCallback | None = None,
        commit: SessionCallback | None = None,
        abort: SessionCallback | None = None,
    ) -> None:
        self.ensure_active()
        self._transaction_hooks[engine_key] = _TransactionHooks(
            start=start,
            commit=commit,
            abort=abort,
        )

    def advance_cluster_time(self, value: int | None) -> None:
        self.ensure_active()
        if value is None:
            return
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise TypeError("cluster_time must be a non-negative int or None")
        if self.cluster_time is None or value > self.cluster_time:
            self.cluster_time = value

    def advance_operation_time(self, value: int | None) -> None:
        self.ensure_active()
        if value is None:
            return
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise TypeError("operation_time must be a non-negative int or None")
        if self.operation_time is None or value > self.operation_time:
            self.operation_time = value

    def observe_operation(self, value: int | None = None) -> int:
        self.ensure_active()
        observed = int(time.time_ns() // 1_000_000) if value is None else value
        self.advance_operation_time(observed)
        self.advance_cluster_time(observed)
        return observed

    @staticmethod
    def _error_labels(exc: Exception) -> tuple[str, ...]:
        labels = getattr(exc, "error_labels", ())
        if isinstance(labels, tuple):
            return labels
        if isinstance(labels, list):
            return tuple(label for label in labels if isinstance(label, str))
        return ()

    @classmethod
    def _should_retry_transaction_phase(cls, phase: str, exc: Exception) -> bool:
        labels = cls._error_labels(exc)
        if phase == "commit":
            return (
                _TRANSIENT_TRANSACTION_ERROR in labels
                or _UNKNOWN_TRANSACTION_COMMIT_RESULT in labels
            )
        if phase == "abort":
            return _TRANSIENT_TRANSACTION_ERROR in labels
        return False

    def _run_transaction_hooks(self, phase: str) -> None:
        for hooks in self._transaction_hooks.values():
            callback = getattr(hooks, phase)
            if callback is not None:
                callback(self)

    def _run_transaction_phase(self, phase: str) -> None:
        attempts = 0
        while True:
            attempts += 1
            try:
                self._run_transaction_hooks(phase)
                return
            except Exception as exc:
                if attempts >= _TRANSACTION_RETRY_ATTEMPTS or not self._should_retry_transaction_phase(phase, exc):
                    raise

    def start_transaction(
        self,
        *,
        read_concern: ReadConcern | None = None,
        write_concern: WriteConcern | None = None,
        read_preference: ReadPreference | None = None,
        max_commit_time_ms: int | None = None,
    ) -> None:
        self.ensure_active()
        if self.transaction_active:
            raise InvalidOperation("Ya hay una transaccion activa en esta sesion")
        resolved_read_concern = (
            self.default_transaction_options.read_concern
            if read_concern is None
            else normalize_read_concern(read_concern)
        )
        resolved_write_concern = (
            self.default_transaction_options.write_concern
            if write_concern is None
            else normalize_write_concern(write_concern)
        )
        resolved_read_preference = (
            self.default_transaction_options.read_preference
            if read_preference is None
            else normalize_read_preference(read_preference)
        )
        resolved_max_commit_time_ms = (
            self.default_transaction_options.max_commit_time_ms
            if max_commit_time_ms is None
            else max_commit_time_ms
        )
        self._state.transaction.number += 1
        self._state.transaction.active = True
        self._state.transaction.options = TransactionOptions(
            read_concern=resolved_read_concern,
            write_concern=resolved_write_concern,
            read_preference=resolved_read_preference,
            max_commit_time_ms=resolved_max_commit_time_ms,
        )
        if (
            resolved_write_concern.w is not None
            and isinstance(resolved_write_concern.w, int)
            and resolved_write_concern.w == 0
        ):
            self._state.transaction.active = False
            self._state.transaction.number -= 1
            self._state.transaction.options = None
            raise InvalidOperation("transactions do not support unacknowledged write concern w=0")
        try:
            self._run_transaction_hooks("start")
        except Exception:
            self._state.transaction.active = False
            self._state.transaction.number -= 1
            self._state.transaction.options = None
            raise

    def commit_transaction(self) -> None:
        self.ensure_active()
        if not self.transaction_active:
            raise InvalidOperation("No hay una transaccion activa en esta sesion")
        self._run_transaction_phase("commit")
        self._state.transaction.active = False
        self._state.transaction.options = None

    def abort_transaction(self) -> None:
        self.ensure_active()
        if not self.transaction_active:
            raise InvalidOperation("No hay una transaccion activa en esta sesion")
        self._run_transaction_phase("abort")
        self._state.transaction.active = False
        self._state.transaction.options = None

    def end_transaction(self) -> None:
        self.commit_transaction()

    def with_transaction(
        self,
        callback: Callable[["ClientSession"], object],
        *args,
        read_concern: ReadConcern | None = None,
        write_concern: WriteConcern | None = None,
        read_preference: ReadPreference | None = None,
        max_commit_time_ms: int | None = None,
        **kwargs,
    ) -> object:
        self.ensure_active()
        self.start_transaction(
            read_concern=read_concern,
            write_concern=write_concern,
            read_preference=read_preference,
            max_commit_time_ms=max_commit_time_ms,
        )
        try:
            result = callback(self, *args, **kwargs)
        except Exception:
            self.abort_transaction()
            raise
        if inspect.isawaitable(result):
            return self._wrap_async_transaction(result)
        if not self.in_transaction:
            return result
        self.commit_transaction()
        return result

    async def _wrap_async_transaction(self, awaitable: Awaitable[object]) -> object:
        try:
            result = await awaitable
        except Exception:
            self.abort_transaction()
            raise
        if not self.in_transaction:
            return result
        self.commit_transaction()
        return result

    def close(self) -> None:
        if not self.active:
            return
        abort_error: Exception | None = None
        if self.transaction_active:
            try:
                self._run_transaction_hooks("abort")
            except Exception as exc:
                abort_error = exc
            finally:
                self._state.transaction.active = False
                self._state.transaction.options = None
        self._transaction_hooks.clear()
        self._state.engine_contexts.clear()
        self._state.active = False
        if abort_error is not None:
            raise abort_error

    def __enter__(self) -> "ClientSession":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    async def __aenter__(self) -> "ClientSession":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False
