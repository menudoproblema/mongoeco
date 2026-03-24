from dataclasses import dataclass, field
import uuid

from mongoeco.errors import InvalidOperation


@dataclass
class ClientSession:
    """Contexto minimo de operacion para reservar la API de sesiones."""

    session_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    active: bool = True
    transaction_active: bool = False
    engine_state: dict[str, object] = field(default_factory=dict)

    def ensure_active(self) -> None:
        if not self.active:
            raise InvalidOperation("La sesion ya esta cerrada")

    def bind_engine_state(self, engine_key: str, state: object) -> None:
        self.ensure_active()
        self.engine_state[engine_key] = state

    def get_engine_state(self, engine_key: str) -> object | None:
        self.ensure_active()
        return self.engine_state.get(engine_key)

    def start_transaction(self) -> None:
        self.ensure_active()
        self.transaction_active = True

    def end_transaction(self) -> None:
        self.transaction_active = False

    def close(self) -> None:
        self.end_transaction()
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
