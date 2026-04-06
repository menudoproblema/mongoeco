from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from mongoeco.driver.connections import ConnectionLease
from mongoeco.driver.policies import ConcernPolicy, RetryPolicy, SelectionPolicy, TimeoutPolicy
from mongoeco.driver.security import AuthPolicy, TlsPolicy
from mongoeco.driver.topology import ServerDescription, TopologyDescription
from mongoeco.session import ClientSession


@dataclass(frozen=True, slots=True)
class CommandRequest:
    database: str
    command_name: str
    payload: dict[str, Any]
    session: ClientSession | None = None
    read_only: bool = False

    @property
    def session_id(self) -> str | None:
        return None if self.session is None else self.session.session_id


@dataclass(frozen=True, slots=True)
class RequestExecutionPlan:
    request: CommandRequest
    topology: TopologyDescription
    timeout_policy: TimeoutPolicy
    retry_policy: RetryPolicy
    selection_policy: SelectionPolicy
    concern_policy: ConcernPolicy
    auth_policy: AuthPolicy
    tls_policy: TlsPolicy
    dynamic_candidates: bool = False
    candidate_servers: tuple[ServerDescription, ...] = field(default_factory=tuple)

    @property
    def primary_server(self) -> ServerDescription | None:
        return self.candidate_servers[0] if self.candidate_servers else None


@dataclass(frozen=True, slots=True)
class RequestOutcome:
    server_address: str | None
    ok: bool
    response: dict[str, Any] | None = None
    error: str | None = None
    retryable: bool = False


@dataclass(frozen=True, slots=True)
class PreparedRequestExecution:
    plan: RequestExecutionPlan
    selected_server: ServerDescription
    connection: ConnectionLease
    attempt_number: int = 1
    request_id: str = field(default_factory=lambda: uuid4().hex)
