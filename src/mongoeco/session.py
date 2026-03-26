from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
import inspect
import uuid

from mongoeco.errors import InvalidOperation


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
    engine_state: dict[str, object] = field(default_factory=dict)
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

    def bind_engine_state(self, engine_key: str, state: object) -> None:
        self.ensure_active()
        self.engine_state[engine_key] = state

    def get_engine_state(self, engine_key: str) -> object | None:
        self.ensure_active()
        return self.engine_state.get(engine_key)

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

    def start_transaction(self) -> None:
        self.ensure_active()
        if self.transaction_active:
            raise InvalidOperation("Ya hay una transaccion activa en esta sesion")
        self.transaction_number += 1
        self.transaction_active = True
        try:
            self._run_transaction_hooks("start")
        except Exception:
            self.transaction_active = False
            raise

    def commit_transaction(self) -> None:
        self.ensure_active()
        if not self.transaction_active:
            raise InvalidOperation("No hay una transaccion activa en esta sesion")
        self._run_transaction_hooks("commit")
        self.transaction_active = False

    def abort_transaction(self) -> None:
        self.ensure_active()
        if not self.transaction_active:
            raise InvalidOperation("No hay una transaccion activa en esta sesion")
        self._run_transaction_hooks("abort")
        self.transaction_active = False

    def end_transaction(self) -> None:
        self.commit_transaction()

    def with_transaction(self, callback: Callable[["ClientSession"], object], *args, **kwargs) -> object:
        self.ensure_active()
        self.start_transaction()
        try:
            result = callback(self, *args, **kwargs)
        except Exception:
            self.abort_transaction()
            raise
        if inspect.isawaitable(result):
            return self._wrap_async_transaction(result)
        self.commit_transaction()
        return result

    async def _wrap_async_transaction(self, awaitable: Awaitable[object]) -> object:
        try:
            result = await awaitable
        except Exception:
            self.abort_transaction()
            raise
        self.commit_transaction()
        return result

    def close(self) -> None:
        if not self.active:
            return
        if self.transaction_active:
            try:
                self._run_transaction_hooks("abort")
            finally:
                self.transaction_active = False
        self._transaction_hooks.clear()
        self.engine_state.clear()
        self.active = False

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
