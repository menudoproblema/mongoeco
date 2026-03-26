from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import inspect
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


@dataclass
class _TransactionHooks:
    start: SessionCallback | None = None
    commit: SessionCallback | None = None
    abort: SessionCallback | None = None


@dataclass
class ClientSession:
    """Contexto de sesion compartido entre cliente y engine."""

    session_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    active: bool = True
    transaction_active: bool = False
    transaction_number: int = 0
    default_transaction_options: TransactionOptions = field(default_factory=TransactionOptions)
    engine_state: dict[str, object] = field(default_factory=dict)
    _current_transaction_options: TransactionOptions | None = field(default=None, repr=False)
    _transaction_hooks: dict[str, _TransactionHooks] = field(default_factory=dict, repr=False)

    def ensure_active(self) -> None:
        if not self.active:
            raise InvalidOperation("La sesion ya esta cerrada")

    @property
    def has_ended(self) -> bool:
        return not self.active

    @property
    def in_transaction(self) -> bool:
        return self.transaction_active

    @property
    def transaction_options(self) -> TransactionOptions | None:
        return self._current_transaction_options

    def bind_engine_state(self, engine_key: str, state: object) -> None:
        self.ensure_active()
        self.engine_state[engine_key] = state

    def get_engine_state(self, engine_key: str) -> object | None:
        self.ensure_active()
        return self.engine_state.get(engine_key)

    def update_engine_state(self, engine_key: str, **updates: object) -> None:
        self.ensure_active()
        state = self.engine_state.get(engine_key)
        if isinstance(state, dict):
            state.update(updates)

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

    def _run_transaction_hooks(self, phase: str) -> None:
        for hooks in self._transaction_hooks.values():
            callback = getattr(hooks, phase)
            if callback is not None:
                callback(self)

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
        self.transaction_number += 1
        self.transaction_active = True
        self._current_transaction_options = TransactionOptions(
            read_concern=resolved_read_concern,
            write_concern=resolved_write_concern,
            read_preference=resolved_read_preference,
            max_commit_time_ms=resolved_max_commit_time_ms,
        )
        try:
            self._run_transaction_hooks("start")
        except Exception:
            self.transaction_active = False
            self.transaction_number -= 1
            self._current_transaction_options = None
            raise

    def commit_transaction(self) -> None:
        self.ensure_active()
        if not self.transaction_active:
            raise InvalidOperation("No hay una transaccion activa en esta sesion")
        try:
            self._run_transaction_hooks("commit")
        finally:
            self.transaction_active = False
            self._current_transaction_options = None

    def abort_transaction(self) -> None:
        self.ensure_active()
        if not self.transaction_active:
            raise InvalidOperation("No hay una transaccion activa en esta sesion")
        try:
            self._run_transaction_hooks("abort")
        finally:
            self.transaction_active = False
            self._current_transaction_options = None

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
                self.transaction_active = False
                self._current_transaction_options = None
        self._transaction_hooks.clear()
        self.engine_state.clear()
        self.active = False
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
